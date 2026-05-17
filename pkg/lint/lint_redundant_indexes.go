package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&RedundantIndexLinter{})
}

// RedundantIndexLinter checks for redundant indexes in tables.
// An index is considered redundant if:
//  1. Its columns are a prefix of the PRIMARY KEY columns
//  2. Another index's column list starts with this index's columns (prefix match)
//  3. Another index has exactly the same columns in the same order (duplicate)
//  4. The index ends with PRIMARY KEY columns (suffix redundancy) — InnoDB
//     auto-appends PK columns to secondary indexes, so spelling them out is
//     wasteful.
//  5. The index starts with the full PRIMARY KEY (prefix redundancy) — point
//     and equality lookups by PK are already served by the clustered index,
//     so the leading PK columns add no capability that a non-PK-leading
//     variant of the index wouldn't already provide.
//
// The linter evaluates the *post-state* of the schema (existing tables with
// CREATE TABLE / ALTER TABLE changes applied) so warnings fire when a
// redundant index is being introduced, and stay silent when it is being
// removed — matching the post-state convention established for other linters
// in #840.
type RedundantIndexLinter struct{}

func (l *RedundantIndexLinter) Name() string {
	return "redundant_indexes"
}

func (l *RedundantIndexLinter) Description() string {
	return "Detects redundant indexes including duplicates, prefix matches, and unnecessary PRIMARY KEY suffixes or prefixes"
}

func (l *RedundantIndexLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation
	for _, table := range PostState(existingTables, changes) {
		violations = append(violations, l.checkTableIndexes(table)...)
	}
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

		// PRIMARY KEY itself never participates in suffix/prefix-of-PK checks
		// against itself, and the checks below are only meaningful for
		// secondary indexes.
		if primaryKey != nil && index.Type != "PRIMARY KEY" {
			// Check: Redundant PK suffix
			// The index explicitly includes PK columns at its end. InnoDB
			// already appends PK columns to secondary indexes, so spelling
			// them out wastes space.
			if hasRedundant, colCount := hasRedundantPKSuffix(index, *primaryKey); hasRedundant {
				violations = append(violations, createPKSuffixViolation(
					table.GetTableName(),
					index,
					*primaryKey,
					colCount,
				))
				// Continue checking - index might also be fully redundant
			}

			// Check: Redundant PK prefix
			// The index leads with the full PK. Point and equality lookups by
			// PK are already served by the clustered index, and a non-PK-leading
			// variant of this index would auto-append PK anyway.
			if hasRedundant, colCount := hasRedundantPKPrefix(index, *primaryKey); hasRedundant {
				violations = append(violations, createPKPrefixViolation(
					table.GetTableName(),
					index,
					*primaryKey,
					colCount,
				))
			}
		}

		// Check: Redundant to another index
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
// Returns true if indexA is a prefix of indexB OR if they're duplicates,
// AND any constraint indexA enforces is also enforced by indexB.
func isRedundantToIndex(indexA statement.Index, indexB statement.Index) bool {
	// FULLTEXT and SPATIAL indexes serve specialized purposes and are not
	// interchangeable with B-tree indexes (or each other).
	if indexA.Type == "FULLTEXT" || indexA.Type == "SPATIAL" ||
		indexB.Type == "FULLTEXT" || indexB.Type == "SPATIAL" {
		return false
	}

	// PRIMARY KEY is a table-level constraint (clustered key, uniqueness,
	// implicit NOT NULL) that cannot be dropped in favor of another index —
	// not even a column-for-column duplicate. Reporting it as redundant
	// would suggest an unsafe rewrite, so never flag it.
	if indexA.Type == "PRIMARY KEY" {
		return false
	}

	// indexA cannot be redundant if it has MORE columns than indexB
	if len(indexA.Columns) > len(indexB.Columns) {
		return false
	}

	// A UNIQUE index enforces uniqueness over its exact column set. A wider
	// covering index (UNIQUE or PK on more columns) enforces a *different*
	// uniqueness scope and so does not subsume the constraint. UNIQUE is
	// therefore only redundant when an exact-column-set duplicate also
	// enforces uniqueness over those columns.
	if indexA.Type == "UNIQUE" {
		if len(indexA.Columns) != len(indexB.Columns) {
			return false
		}
		if indexB.Type != "UNIQUE" && indexB.Type != "PRIMARY KEY" {
			return false
		}
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

// hasRedundantPKPrefix checks if a secondary index leads with the full
// PRIMARY KEY. Returns true and the count of leading PK columns if so.
//
// Rationale: InnoDB's PRIMARY KEY is the clustered index, so point and
// equality lookups by PK are best served by PK directly. A secondary index
// of the form INDEX (pk, x, y) covers no lookup that either PK alone or
// INDEX (x, y) (with PK auto-appended) cannot already serve. The leading
// PK columns are therefore redundant.
//
// Unlike the suffix check, this only matches the *full* PK at the start —
// a partial PK prefix at the start is a defensible covering index pattern.
// The index must have additional columns beyond the PK (an index that *is*
// the PK is caught by the duplicate / prefix-of-PK rules instead).
func hasRedundantPKPrefix(index statement.Index, primaryKey statement.Index) (bool, int) {
	pkLen := len(primaryKey.Columns)
	if pkLen == 0 {
		return false, 0
	}
	if len(index.Columns) <= pkLen {
		return false, 0
	}
	for i := range pkLen {
		if index.Columns[i] != primaryKey.Columns[i] {
			return false, 0
		}
	}
	return true, pkLen
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

// createPKPrefixViolation creates a violation for a secondary index that
// leads with the full PRIMARY KEY.
func createPKPrefixViolation(tableName string, index statement.Index, primaryKey statement.Index, redundantColCount int) Violation {
	redundantPrefix := index.Columns[:redundantColCount]
	usefulSuffix := index.Columns[redundantColCount:]

	message := fmt.Sprintf(
		"Index '%s' on columns (%s) leads with PRIMARY KEY columns (%s). "+
			"Point and equality lookups by PRIMARY KEY are served by the clustered index directly, "+
			"and InnoDB appends PK columns to secondary indexes — the leading PK columns add no capability.",
		index.Name,
		strings.Join(index.Columns, ", "),
		strings.Join(redundantPrefix, ", "),
	)
	suggestion := fmt.Sprintf(
		"Redefine index '%s' as INDEX (%s) — InnoDB will append PRIMARY KEY (%s) internally — "+
			"or drop it entirely if the PRIMARY KEY alone covers the queries that use it.",
		index.Name,
		strings.Join(usefulSuffix, ", "),
		strings.Join(primaryKey.Columns, ", "),
	)

	return Violation{
		Linter:     &RedundantIndexLinter{},
		Severity:   SeverityWarning,
		Message:    message,
		Location:   &Location{Table: tableName, Index: &index.Name},
		Suggestion: &suggestion,
		Context: map[string]any{
			"index_name":          index.Name,
			"full_columns":        index.Columns,
			"redundant_prefix":    redundantPrefix,
			"useful_columns":      usefulSuffix,
			"primary_key_columns": primaryKey.Columns,
			"redundant_col_count": redundantColCount,
		},
	}
}
