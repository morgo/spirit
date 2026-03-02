package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func init() {
	Register(&RedundantIndexLinter{})
}

// RedundantIndexLinter checks for redundant indexes in tables.
// An index is considered redundant if:
// 1. Its columns are a prefix of the PRIMARY KEY columns
// 2. Another index's column list starts with this index's columns (prefix match)
// 3. Another index has exactly the same columns in the same order (duplicate)
// 4. The index ends with PRIMARY KEY columns (suffix redundancy)
//
// In InnoDB, secondary indexes automatically include PRIMARY KEY columns as a suffix,
// making certain index patterns redundant and wasteful.
type RedundantIndexLinter struct{}

func (l *RedundantIndexLinter) Name() string {
	return "redundant_indexes"
}

func (l *RedundantIndexLinter) Description() string {
	return "Detects redundant indexes including duplicates, prefix matches, and unnecessary PRIMARY KEY suffixes"
}

func (l *RedundantIndexLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation

	// Check both existing tables and CREATE TABLE statements in changes
	for table := range CreateTableStatements(existingTables, changes) {
		violations = append(violations, l.checkTableIndexes(table)...)
	}

	// Check ALTER TABLE statements that add indexes
	violations = append(violations, l.checkAlterTableStatements(existingTables, changes)...)

	return violations
}

func (l *RedundantIndexLinter) String() string {
	return Stringer(l)
}

// checkTableIndexes checks a single table for redundant indexes
func (l *RedundantIndexLinter) checkTableIndexes(table *statement.CreateTable) []Violation {
	var violations []Violation
	indexes := table.GetIndexes()
	primaryKey := indexes.ByName("PRIMARY")

	// Track which indexes we've already reported as redundant
	// to avoid duplicate violations
	reportedRedundant := make(map[string]bool)

	for _, index := range indexes {
		if reportedRedundant[index.Name] {
			continue
		}

		// Check 1: Redundant PK suffix (can coexist with other issues)
		// This checks if the index explicitly includes PK columns at the end
		if primaryKey != nil {
			if hasRedundant, colCount := hasRedundantPKSuffix(index, *primaryKey); hasRedundant {
				violations = append(violations, createPKSuffixViolation(
					table.GetTableName(),
					index,
					*primaryKey,
					colCount,
				))
				// Continue checking - index might also be fully redundant
			}
		}

		// Check 2: Redundant to another index
		for _, otherIndex := range indexes {
			if index.Name == otherIndex.Name {
				continue
			}

			if isRedundantToIndex(index, otherIndex) {
				isDuplicate := len(index.Columns) == len(otherIndex.Columns)
				violations = append(violations, createRedundancyViolation(
					table.GetTableName(),
					index,
					otherIndex,
					isDuplicate,
					otherIndex.Type == "PRIMARY KEY",
				))
				reportedRedundant[index.Name] = true
				break
			}
		}
	}

	return violations
}

// isRedundantToIndex checks if indexA is redundant to indexB.
// Returns true if indexA is a prefix of indexB OR if they're duplicates.
func isRedundantToIndex(indexA statement.Index, indexB statement.Index) bool {
	// Type compatibility check
	if !canMakeRedundant(indexB, indexA) {
		return false
	}

	// indexA cannot be redundant if it has MORE columns than indexB
	if len(indexA.Columns) > len(indexB.Columns) {
		return false
	}

	// Check if all columns of indexA match the prefix of indexB in exact order
	for i := range indexA.Columns {
		if indexA.Columns[i] != indexB.Columns[i] {
			return false
		}
	}

	// At this point, indexA is either:
	// - A prefix of indexB (len(indexA) < len(indexB)), OR
	// - A duplicate of indexB (len(indexA) == len(indexB))
	// Both cases are redundant!
	return true
}

// canMakeRedundant checks if the covering index can make the redundant index redundant
// based on their types. Different index types serve different purposes.
func canMakeRedundant(covering statement.Index, redundant statement.Index) bool {
	// FULLTEXT and SPATIAL indexes serve special purposes
	// Regular indexes cannot make them redundant, and they can't make others redundant
	if redundant.Type == "FULLTEXT" || redundant.Type == "SPATIAL" ||
		covering.Type == "FULLTEXT" || covering.Type == "SPATIAL" {
		return false
	}

	// PRIMARY KEY can make most indexes redundant (it acts like a UNIQUE index)
	if covering.Type == "PRIMARY KEY" {
		return true
	}

	// UNIQUE indexes provide additional constraints beyond lookup
	// A non-unique index cannot make a UNIQUE index redundant
	if redundant.Type == "UNIQUE" && covering.Type != "UNIQUE" {
		return false
	}

	return true
}

// hasRedundantPKSuffix checks if an index has a redundant PRIMARY KEY suffix.
// Returns true and the count of redundant columns if the index ends with PK columns
// (or a prefix of PK columns).
//
// In InnoDB, secondary indexes automatically include PK columns, so explicitly
// adding them at the end is redundant.
func hasRedundantPKSuffix(index statement.Index, primaryKey statement.Index) (bool, int) {
	if len(primaryKey.Columns) == 0 {
		return false, 0
	}

	// Index must be longer than PK to have a suffix
	if len(index.Columns) <= len(primaryKey.Columns) {
		return false, 0
	}

	pkLen := len(primaryKey.Columns)
	indexLen := len(index.Columns)

	// Try to match the full PK at the end
	fullMatch := true
	for i := range pkLen {
		if index.Columns[indexLen-pkLen+i] != primaryKey.Columns[i] {
			fullMatch = false
			break
		}
	}

	if fullMatch {
		return true, pkLen // Full PK suffix is redundant
	}

	// Check if index ends with a PREFIX of PK
	// e.g., PK (a, b, c) and INDEX (x, a, b) - the (a, b) suffix is redundant
	for prefixLen := pkLen - 1; prefixLen > 0; prefixLen-- {
		match := true
		for i := range prefixLen {
			if index.Columns[indexLen-prefixLen+i] != primaryKey.Columns[i] {
				match = false
				break
			}
		}
		if match {
			return true, prefixLen // PK prefix suffix is redundant
		}
	}

	return false, 0
}

// createRedundancyViolation creates a violation for prefix or duplicate redundancy
func createRedundancyViolation(tableName string, redundantIndex statement.Index, coveringIndex statement.Index, isDuplicate bool, redundantToPK bool) Violation {
	var message, suggestion string

	if isDuplicate {
		if redundantToPK {
			message = fmt.Sprintf(
				"Index '%s' on columns (%s) is a duplicate of the PRIMARY KEY",
				redundantIndex.Name,
				strings.Join(redundantIndex.Columns, ", "),
			)
			suggestion = fmt.Sprintf(
				"Drop index '%s' as it duplicates the PRIMARY KEY. "+
					"The PRIMARY KEY already provides all the functionality of this index.",
				redundantIndex.Name,
			)
		} else {
			message = fmt.Sprintf(
				"Index '%s' on columns (%s) is a duplicate of index '%s'",
				redundantIndex.Name,
				strings.Join(redundantIndex.Columns, ", "),
				coveringIndex.Name,
			)
			suggestion = fmt.Sprintf(
				"Drop index '%s' as it is an exact duplicate of '%s'. "+
					"Duplicate indexes waste space and slow down writes with no benefit.",
				redundantIndex.Name,
				coveringIndex.Name,
			)
		}
	} else {
		// Prefix redundancy
		if redundantToPK {
			message = fmt.Sprintf(
				"Index '%s' on columns (%s) is redundant - covered by PRIMARY KEY on columns (%s)",
				redundantIndex.Name,
				strings.Join(redundantIndex.Columns, ", "),
				strings.Join(coveringIndex.Columns, ", "),
			)
			suggestion = fmt.Sprintf(
				"Consider dropping index '%s' as it is fully covered by the PRIMARY KEY. "+
					"In InnoDB, the PRIMARY KEY is the clustered index and provides prefix lookup capability.",
				redundantIndex.Name,
			)
		} else {
			message = fmt.Sprintf(
				"Index '%s' on columns (%s) is redundant - covered by index '%s' on columns (%s)",
				redundantIndex.Name,
				strings.Join(redundantIndex.Columns, ", "),
				coveringIndex.Name,
				strings.Join(coveringIndex.Columns, ", "),
			)
			suggestion = fmt.Sprintf(
				"Consider dropping index '%s' as it is fully covered by '%s'. "+
					"The longer index can satisfy all queries that would use the shorter index.",
				redundantIndex.Name,
				coveringIndex.Name,
			)
		}
	}

	return Violation{
		Linter:     &RedundantIndexLinter{},
		Severity:   SeverityWarning,
		Message:    message,
		Location:   &Location{Table: tableName, Index: &redundantIndex.Name},
		Suggestion: &suggestion,
		Context: map[string]any{
			"redundant_index": redundantIndex.Name,
			"covering_index":  coveringIndex.Name,
			"is_duplicate":    isDuplicate,
			"redundant_to_pk": redundantToPK,
		},
	}
}

// createPKSuffixViolation creates a violation for redundant PRIMARY KEY suffix
func createPKSuffixViolation(tableName string, index statement.Index, primaryKey statement.Index, redundantColCount int) Violation {
	redundantSuffix := index.Columns[len(index.Columns)-redundantColCount:]
	usefulPrefix := index.Columns[:len(index.Columns)-redundantColCount]
	pkCols := primaryKey.Columns[:redundantColCount]

	var message, suggestion string

	if redundantColCount == len(primaryKey.Columns) {
		// Full PK is redundant
		message = fmt.Sprintf(
			"Index '%s' on columns (%s) has redundant PRIMARY KEY suffix (%s). "+
				"InnoDB automatically appends PK columns to secondary indexes.",
			index.Name,
			strings.Join(index.Columns, ", "),
			strings.Join(redundantSuffix, ", "),
		)
		suggestion = fmt.Sprintf(
			"Redefine index '%s' as INDEX (%s). "+
				"InnoDB will automatically append the PK columns (%s) internally, "+
				"so explicitly including them wastes space and provides no benefit.",
			index.Name,
			strings.Join(usefulPrefix, ", "),
			strings.Join(pkCols, ", "),
		)
	} else {
		// Prefix of PK is redundant
		message = fmt.Sprintf(
			"Index '%s' on columns (%s) has redundant PRIMARY KEY prefix suffix (%s). "+
				"InnoDB automatically appends full PK columns (%s) to secondary indexes.",
			index.Name,
			strings.Join(index.Columns, ", "),
			strings.Join(redundantSuffix, ", "),
			strings.Join(primaryKey.Columns, ", "),
		)
		suggestion = fmt.Sprintf(
			"Redefine index '%s' as INDEX (%s). "+
				"InnoDB will automatically append the full PK (%s) internally.",
			index.Name,
			strings.Join(usefulPrefix, ", "),
			strings.Join(primaryKey.Columns, ", "),
		)
	}

	return Violation{
		Linter:     &RedundantIndexLinter{},
		Severity:   SeverityWarning,
		Message:    message,
		Location:   &Location{Table: tableName, Index: &index.Name},
		Suggestion: &suggestion,
		Context: map[string]any{
			"index_name":          index.Name,
			"full_columns":        index.Columns,
			"useful_columns":      usefulPrefix,
			"redundant_suffix":    redundantSuffix,
			"primary_key_columns": primaryKey.Columns,
			"redundant_col_count": redundantColCount,
		},
	}
}

// checkAlterTableStatements checks ALTER TABLE statements that add indexes
// for potential redundancy with existing indexes or other indexes being added.
func (l *RedundantIndexLinter) checkAlterTableStatements(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation

	// Build a map of existing tables for quick lookup
	existingTableMap := make(map[string]*statement.CreateTable)
	for _, table := range existingTables {
		existingTableMap[table.GetTableName()] = table
	}

	// Process each ALTER TABLE statement
	for _, change := range changes {
		alterStmt, ok := change.AsAlterTable()
		if !ok {
			continue
		}

		tableName := change.Table

		// Get the existing table definition (if it exists)
		existingTable := existingTableMap[tableName]
		if existingTable == nil {
			// Table doesn't exist yet - might be created in this batch of changes
			// For now, we'll skip checking ALTER on non-existent tables
			continue
		}

		// Extract indexes being added in this ALTER TABLE
		newIndexes := l.extractIndexesFromAlter(alterStmt)
		if len(newIndexes) == 0 {
			continue
		}

		// Get existing indexes from the table
		existingIndexes := existingTable.GetIndexes()
		primaryKey := existingIndexes.ByName("PRIMARY")

		// Check each new index for redundancy
		for i, newIndex := range newIndexes {
			// Check against existing indexes
			if v := l.checkIndexRedundancy(tableName, newIndex, existingIndexes, primaryKey); v != nil {
				violations = append(violations, *v)
			}

			// Check against other new indexes being added in the same ALTER
			// (only check against indexes that come before this one to avoid duplicates)
			for j := range i {
				otherNewIndex := newIndexes[j]

				// Check if newIndex is redundant to otherNewIndex
				if isRedundantToIndex(newIndex, otherNewIndex) {
					isDuplicate := len(newIndex.Columns) == len(otherNewIndex.Columns)
					violations = append(violations, createRedundancyViolation(
						tableName,
						newIndex,
						otherNewIndex,
						isDuplicate,
						false, // not redundant to PK
					))
					break // Only report first redundancy
				}
			}
		}
	}

	return violations
}

// checkIndexRedundancy checks if a single index is redundant to any index in the provided list.
// Returns a violation if redundancy is found, nil otherwise.
func (l *RedundantIndexLinter) checkIndexRedundancy(tableName string, index statement.Index, existingIndexes statement.Indexes, primaryKey *statement.Index) *Violation {
	// Check for redundant PK suffix first
	if primaryKey != nil {
		if hasRedundant, colCount := hasRedundantPKSuffix(index, *primaryKey); hasRedundant {
			v := createPKSuffixViolation(tableName, index, *primaryKey, colCount)
			return &v
		}
	}

	// Check if redundant to any existing index
	for _, existingIndex := range existingIndexes {
		if isRedundantToIndex(index, existingIndex) {
			isDuplicate := len(index.Columns) == len(existingIndex.Columns)
			v := createRedundancyViolation(
				tableName,
				index,
				existingIndex,
				isDuplicate,
				existingIndex.Type == "PRIMARY KEY",
			)
			return &v
		}
	}

	return nil
}

// extractIndexesFromAlter extracts index definitions from an ALTER TABLE statement
func (l *RedundantIndexLinter) extractIndexesFromAlter(alterStmt *ast.AlterTableStmt) []statement.Index {
	var indexes []statement.Index

	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint != nil {
			index := l.constraintToIndex(spec.Constraint)
			if index != nil {
				indexes = append(indexes, *index)
			}
		}
	}

	return indexes
}

// constraintToIndex converts an ast.Constraint to a statement.Index
func (l *RedundantIndexLinter) constraintToIndex(constraint *ast.Constraint) *statement.Index {
	// Extract column names from the constraint
	var columns []string
	for _, key := range constraint.Keys {
		if key.Column != nil {
			columns = append(columns, key.Column.Name.O)
		}
	}

	if len(columns) == 0 {
		return nil
	}

	// Determine the index type
	var indexType string
	switch constraint.Tp { //nolint:exhaustive
	case ast.ConstraintPrimaryKey:
		indexType = "PRIMARY KEY"
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		indexType = "UNIQUE"
	case ast.ConstraintKey, ast.ConstraintIndex:
		indexType = "INDEX"
	case ast.ConstraintFulltext:
		indexType = "FULLTEXT"
	default:
		// Not an index constraint (e.g., foreign key, check)
		return nil
	}

	return &statement.Index{
		Name:    constraint.Name,
		Type:    indexType,
		Columns: columns,
		Raw:     constraint,
	}
}
