package statement

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/pingcap/tidb/pkg/parser"
)

// Diff compares this CreateTable (source) with another CreateTable (target)
// and returns a list of ALTER TABLE statements needed to transform source into target.
// Returns nil if the tables are identical.
func (ct *CreateTable) Diff(target *CreateTable) ([]*AbstractStatement, error) {
	if ct.TableName != target.TableName {
		return nil, fmt.Errorf("cannot diff tables with different names: %s vs %s", ct.TableName, target.TableName)
	}

	var alterClauses []string

	// 1. Diff columns (DROP, ADD, MODIFY)
	columnClauses := ct.diffColumns(target)
	alterClauses = append(alterClauses, columnClauses...)

	// 2. Diff indexes (DROP, ADD)
	indexClauses := ct.diffIndexes(target)
	alterClauses = append(alterClauses, indexClauses...)

	// 3. Diff constraints (DROP, ADD)
	constraintClauses := ct.diffConstraints(target)
	alterClauses = append(alterClauses, constraintClauses...)

	// 4. Diff table options
	tableOptionClauses := ct.diffTableOptions(target)
	alterClauses = append(alterClauses, tableOptionClauses...)

	// If no changes, return nil
	if len(alterClauses) == 0 {
		return nil, nil
	}

	// Build the ALTER TABLE statement
	alterStmt := fmt.Sprintf("ALTER TABLE `%s` %s", ct.TableName, strings.Join(alterClauses, ", "))

	// Parse the ALTER statement to create an AbstractStatement
	p := parser.New()
	stmtNodes, _, err := p.Parse(alterStmt, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated ALTER statement: %w", err)
	}

	if len(stmtNodes) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(stmtNodes))
	}

	return []*AbstractStatement{
		{
			Table:     ct.TableName,
			Alter:     strings.Join(alterClauses, ", "),
			Statement: alterStmt,
			StmtNode:  &stmtNodes[0],
		},
	}, nil
}

// diffColumns compares columns and returns ALTER clauses for differences
func (ct *CreateTable) diffColumns(target *CreateTable) []string {
	var clauses []string

	// Build maps for easier lookup
	sourceColumns := make(map[string]*Column)
	for i := range ct.Columns {
		sourceColumns[ct.Columns[i].Name] = &ct.Columns[i]
	}

	targetColumns := make(map[string]*Column)
	for i := range target.Columns {
		targetColumns[target.Columns[i].Name] = &target.Columns[i]
	}

	// Check for PRIMARY KEY changes (inline column-level PRIMARY KEY)
	// We need to handle these before column modifications
	var pkDropped bool
	var pkAdded bool
	var pkColumn string

	for _, sourceCol := range ct.Columns {
		targetCol, exists := targetColumns[sourceCol.Name]
		if exists && sourceCol.PrimaryKey && !targetCol.PrimaryKey {
			// PRIMARY KEY was removed from this column
			pkDropped = true
			pkColumn = sourceCol.Name
			break
		}
	}

	for _, targetCol := range target.Columns {
		sourceCol, exists := sourceColumns[targetCol.Name]
		if exists && !sourceCol.PrimaryKey && targetCol.PrimaryKey {
			// PRIMARY KEY was added to this column
			pkAdded = true
			pkColumn = targetCol.Name
			break
		}
	}

	// Handle PRIMARY KEY drops first
	if pkDropped {
		clauses = append(clauses, "DROP PRIMARY KEY")
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string
	for _, sourceCol := range ct.Columns {
		if _, exists := targetColumns[sourceCol.Name]; !exists {
			dropClauses = append(dropClauses, fmt.Sprintf("DROP COLUMN `%s`", sourceCol.Name))
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Collect MODIFY and ADD operations in target order (for positioning)
	// We need to preserve target order for AFTER clauses to work correctly

	// First pass: identify which columns need explicit positioning
	// A column needs explicit positioning if:
	// 1. It's a new column (ADD)
	// 2. Its position changed relative to existing columns (not just due to ADD/DROP)
	// 3. The previous column was explicitly repositioned (cascading effect)
	needsExplicitPosition := make(map[string]bool)
	var prevColumn string
	for _, targetCol := range target.Columns {
		_, existsInSource := sourceColumns[targetCol.Name]

		if !existsInSource {
			// New column always needs explicit positioning
			needsExplicitPosition[targetCol.Name] = true
		} else {
			// Check if position changed
			sourcePrevCol := getPreviousColumn(ct.Columns, targetCol.Name)

			// Check if this is an implicit or explicit position change
			_, prevColExistedInSource := sourceColumns[prevColumn]
			_, sourcePrevColStillExists := targetColumns[sourcePrevCol]

			implicitChange := false
			if prevColumn == "" && sourcePrevCol == "" {
				// Both first, no real change
				implicitChange = true
			} else if prevColumn != "" && !prevColExistedInSource {
				// Previous column is new, position change is implicit
				implicitChange = true
			} else if sourcePrevCol != "" && !sourcePrevColStillExists {
				// Previous column was dropped, position change is implicit
				implicitChange = true
			} else if prevColumn == sourcePrevCol {
				// Same previous column, check if we need cascading
				// Cascading only happens if the previous column moved to FIRST position
				// (because that changes the absolute position of all following columns)
				if prevColumn != "" && needsExplicitPosition[prevColumn] && prevColExistedInSource {
					// Check if the previous column moved to FIRST
					sourcePrevOfPrev := getPreviousColumn(ct.Columns, prevColumn)
					if sourcePrevOfPrev != "" {
						// Previous column was NOT first in source, but is now first in target
						// So this column needs explicit positioning
						needsExplicitPosition[targetCol.Name] = true
					}
				}
				implicitChange = true
			} else {
				// Explicit reorder - previous column changed and both exist
				implicitChange = false
			}

			if !implicitChange {
				needsExplicitPosition[targetCol.Name] = true
			}
		}

		prevColumn = targetCol.Name
	}

	// Second pass: generate the ALTER clauses
	prevColumn = ""
	for _, targetCol := range target.Columns {
		sourceCol, existsInSource := sourceColumns[targetCol.Name]

		if !existsInSource {
			// ADD new column
			clause := fmt.Sprintf("ADD COLUMN %s", formatColumnDefinition(&targetCol))
			// Add positioning
			if prevColumn == "" {
				clause += " FIRST"
			} else {
				clause += fmt.Sprintf(" AFTER `%s`", prevColumn)
			}
			clauses = append(clauses, clause)
		} else {
			// Skip modification if only PRIMARY KEY changed (already handled above)
			// When PRIMARY KEY is dropped, nullability change is implicit, so skip it
			pkOnlyChange := sourceCol.PrimaryKey != targetCol.PrimaryKey &&
				columnsEqualIgnorePK(sourceCol, &targetCol)

			// Also check if only nullability changed due to PK drop
			// (PK columns are NOT NULL, dropping PK makes them NULL)
			pkDroppedNullabilityChange := pkDropped &&
				sourceCol.Name == pkColumn &&
				sourceCol.PrimaryKey && !targetCol.PrimaryKey &&
				sourceCol.Nullable == false && targetCol.Nullable == true

			// MODIFY existing column if:
			// 1. Column definition changed (and not just PK-related changes)
			// 2. Column needs explicit positioning
			needsModify := (!ct.columnsEqualWithContext(sourceCol, &targetCol, target) && !pkOnlyChange && !pkDroppedNullabilityChange) ||
				needsExplicitPosition[targetCol.Name]

			if needsModify {
				clause := fmt.Sprintf("MODIFY COLUMN %s", formatColumnDefinition(&targetCol))
				if needsExplicitPosition[targetCol.Name] {
					if prevColumn == "" {
						clause += " FIRST"
					} else {
						clause += fmt.Sprintf(" AFTER `%s`", prevColumn)
					}
				}
				clauses = append(clauses, clause)
			}
		}

		prevColumn = targetCol.Name
	}

	// Handle PRIMARY KEY adds last
	if pkAdded {
		clauses = append(clauses, fmt.Sprintf("ADD PRIMARY KEY (`%s`)", pkColumn))
	}

	return clauses
}

// diffIndexes compares indexes and returns ALTER clauses for differences
func (ct *CreateTable) diffIndexes(target *CreateTable) []string {
	var clauses []string

	// Build maps for easier lookup
	sourceIndexes := make(map[string]*Index)
	for i := range ct.Indexes {
		sourceIndexes[ct.Indexes[i].Name] = &ct.Indexes[i]
	}

	targetIndexes := make(map[string]*Index)
	for i := range target.Indexes {
		targetIndexes[target.Indexes[i].Name] = &target.Indexes[i]
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string
	for i := range ct.Indexes {
		sourceIdx := &ct.Indexes[i]
		targetIdx, existsInTarget := targetIndexes[sourceIdx.Name]

		if !existsInTarget {
			// Index removed completely
			if sourceIdx.Type == "PRIMARY KEY" {
				dropClauses = append(dropClauses, "DROP PRIMARY KEY")
			} else {
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX `%s`", sourceIdx.Name))
			}
		} else if !indexesEqual(sourceIdx, targetIdx) && !indexesEqualIgnoreVisibility(sourceIdx, targetIdx) {
			// Index exists but changed (and not just visibility) - need to drop and re-add
			if sourceIdx.Type == "PRIMARY KEY" {
				dropClauses = append(dropClauses, "DROP PRIMARY KEY")
			} else {
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX `%s`", sourceIdx.Name))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetIdx := range target.Indexes {
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if !existsInSource {
			// New index - add it
			addClauses = append(addClauses, formatAddIndex(&targetIdx))
		} else if !indexesEqual(sourceIdx, &targetIdx) {
			// Index exists but changed - check if only visibility changed
			if indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
				// Only visibility changed - skip for now, handle in ALTER INDEX section
				continue
			}
			// Other changes - need to drop and re-add (drop already handled above)
			addClauses = append(addClauses, formatAddIndex(&targetIdx))
		}
	}
	slices.Sort(addClauses)
	clauses = append(clauses, addClauses...)

	// Collect ALTER INDEX operations for visibility changes (must come after DROP/ADD)
	var alterClauses []string
	for _, targetIdx := range target.Indexes {
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if existsInSource && !indexesEqual(sourceIdx, &targetIdx) && indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
			// Only visibility changed
			targetVisible := targetIdx.Invisible == nil || !*targetIdx.Invisible
			if targetVisible {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX `%s` VISIBLE", targetIdx.Name))
			} else {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX `%s` INVISIBLE", targetIdx.Name))
			}
		}
	}
	slices.Sort(alterClauses)
	clauses = append(clauses, alterClauses...)

	return clauses
}

// diffConstraints compares constraints and returns ALTER clauses for differences
func (ct *CreateTable) diffConstraints(target *CreateTable) []string {
	var clauses []string

	// Build maps for easier lookup
	sourceConstraints := make(map[string]*Constraint)
	for i := range ct.Constraints {
		sourceConstraints[ct.Constraints[i].Name] = &ct.Constraints[i]
	}

	targetConstraints := make(map[string]*Constraint)
	for i := range target.Constraints {
		targetConstraints[target.Constraints[i].Name] = &target.Constraints[i]
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string
	for i := range ct.Constraints {
		sourceConstr := &ct.Constraints[i]
		targetConstr, exists := targetConstraints[sourceConstr.Name]

		// Drop if constraint doesn't exist in target OR if it changed
		if !exists || !constraintsEqual(sourceConstr, targetConstr) {
			switch sourceConstr.Type {
			case "FOREIGN KEY":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP FOREIGN KEY `%s`", sourceConstr.Name))
			case "CHECK":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP CHECK `%s`", sourceConstr.Name))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetConstr := range target.Constraints {
		sourceConstr, existsInSource := sourceConstraints[targetConstr.Name]

		if !existsInSource || !constraintsEqual(sourceConstr, &targetConstr) {
			addClauses = append(addClauses, formatAddConstraint(&targetConstr))
		}
	}
	slices.Sort(addClauses)
	clauses = append(clauses, addClauses...)

	return clauses
}

// diffTableOptions compares table options and returns ALTER clauses for differences
func (ct *CreateTable) diffTableOptions(target *CreateTable) []string {
	var clauses []string

	// Compare ENGINE
	if !stringPtrEqual(ct.TableOptions.getEngine(), target.TableOptions.getEngine()) {
		if engine := target.TableOptions.getEngine(); engine != nil {
			clauses = append(clauses, fmt.Sprintf("ENGINE=%s", *engine))
		}
	}

	// Compare CHARSET
	if !stringPtrEqual(ct.TableOptions.getCharset(), target.TableOptions.getCharset()) {
		if charset := target.TableOptions.getCharset(); charset != nil {
			clauses = append(clauses, fmt.Sprintf("DEFAULT CHARSET=%s", *charset))
		}
	}

	// Compare COLLATION
	if !stringPtrEqual(ct.TableOptions.getCollation(), target.TableOptions.getCollation()) {
		if collation := target.TableOptions.getCollation(); collation != nil {
			clauses = append(clauses, fmt.Sprintf("COLLATE=%s", *collation))
		}
	}

	// Compare COMMENT
	if !stringPtrEqual(ct.TableOptions.getComment(), target.TableOptions.getComment()) {
		if comment := target.TableOptions.getComment(); comment != nil {
			clauses = append(clauses, fmt.Sprintf("COMMENT='%s'", sqlescape.EscapeString(*comment)))
		}
	}

	// Compare ROW_FORMAT
	if !stringPtrEqual(ct.TableOptions.getRowFormat(), target.TableOptions.getRowFormat()) {
		if rowFormat := target.TableOptions.getRowFormat(); rowFormat != nil {
			clauses = append(clauses, fmt.Sprintf("ROW_FORMAT=%s", *rowFormat))
		}
	}

	// Note: AUTO_INCREMENT is typically ignored in diffs (like vitess schemadiff)

	return clauses
}

// Helper methods for TableOptions to handle nil safely
func (to *TableOptions) getEngine() *string {
	if to == nil {
		return nil
	}
	return to.Engine
}

func (to *TableOptions) getCharset() *string {
	if to == nil {
		return nil
	}
	return to.Charset
}

func (to *TableOptions) getCollation() *string {
	if to == nil {
		return nil
	}
	return to.Collation
}

func (to *TableOptions) getComment() *string {
	if to == nil {
		return nil
	}
	return to.Comment
}

func (to *TableOptions) getRowFormat() *string {
	if to == nil {
		return nil
	}
	return to.RowFormat
}

// columnsEqual checks if two columns are equal
func columnsEqual(a, b *Column) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !intPtrEqual(a.Length, b.Length) {
		return false
	}
	if !intPtrEqual(a.Precision, b.Precision) {
		return false
	}
	if !intPtrEqual(a.Scale, b.Scale) {
		return false
	}
	if !boolPtrEqual(a.Unsigned, b.Unsigned) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if !stringPtrEqual(a.Default, b.Default) {
		return false
	}
	if a.AutoInc != b.AutoInc {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	if a.Unique != b.Unique {
		return false
	}
	if !stringPtrEqual(a.Comment, b.Comment) {
		return false
	}
	if !stringPtrEqual(a.Charset, b.Charset) {
		return false
	}
	if !stringPtrEqual(a.Collation, b.Collation) {
		return false
	}
	if !slices.Equal(a.EnumValues, b.EnumValues) {
		return false
	}
	if !slices.Equal(a.SetValues, b.SetValues) {
		return false
	}
	return true
}

// columnsEqualWithContext checks if two columns are equal, considering table context for charset/collation
// If a column's charset is nil, it inherits from the table. If it's explicitly set to the same as the table,
// it's considered equal to nil (no explicit charset needed).
func (ct *CreateTable) columnsEqualWithContext(a, b *Column, target *CreateTable) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !intPtrEqual(a.Length, b.Length) {
		return false
	}
	if !intPtrEqual(a.Precision, b.Precision) {
		return false
	}
	if !intPtrEqual(a.Scale, b.Scale) {
		return false
	}
	if !boolPtrEqual(a.Unsigned, b.Unsigned) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if !stringPtrEqual(a.Default, b.Default) {
		return false
	}
	if a.AutoInc != b.AutoInc {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	if a.Unique != b.Unique {
		return false
	}
	if !stringPtrEqual(a.Comment, b.Comment) {
		return false
	}

	// Charset comparison with table context
	// If column charset is nil, it inherits from table
	// If column charset equals table charset, it's redundant (same as nil)
	sourceCharset := a.Charset
	targetCharset := b.Charset

	// Normalize: if charset equals table charset, treat as nil
	if sourceCharset != nil && ct.TableOptions.Charset != nil && *sourceCharset == *ct.TableOptions.Charset {
		sourceCharset = nil
	}
	if targetCharset != nil && target.TableOptions.Charset != nil && *targetCharset == *target.TableOptions.Charset {
		targetCharset = nil
	}

	if !stringPtrEqual(sourceCharset, targetCharset) {
		return false
	}

	// Collation comparison with table context
	sourceCollation := a.Collation
	targetCollation := b.Collation

	// Normalize: if collation equals table collation, treat as nil
	if sourceCollation != nil && ct.TableOptions.Collation != nil && *sourceCollation == *ct.TableOptions.Collation {
		sourceCollation = nil
	}
	if targetCollation != nil && target.TableOptions.Collation != nil && *targetCollation == *target.TableOptions.Collation {
		targetCollation = nil
	}

	if !stringPtrEqual(sourceCollation, targetCollation) {
		return false
	}

	if !slices.Equal(a.EnumValues, b.EnumValues) {
		return false
	}
	if !slices.Equal(a.SetValues, b.SetValues) {
		return false
	}
	return true
}

// columnsEqualIgnorePK checks if two columns are equal, ignoring the PrimaryKey attribute
func columnsEqualIgnorePK(a, b *Column) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !intPtrEqual(a.Length, b.Length) {
		return false
	}
	if !intPtrEqual(a.Precision, b.Precision) {
		return false
	}
	if !intPtrEqual(a.Scale, b.Scale) {
		return false
	}
	if !boolPtrEqual(a.Unsigned, b.Unsigned) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if !stringPtrEqual(a.Default, b.Default) {
		return false
	}
	if a.AutoInc != b.AutoInc {
		return false
	}
	// Skip PrimaryKey comparison
	if a.Unique != b.Unique {
		return false
	}
	if !stringPtrEqual(a.Comment, b.Comment) {
		return false
	}
	if !stringPtrEqual(a.Charset, b.Charset) {
		return false
	}
	if !stringPtrEqual(a.Collation, b.Collation) {
		return false
	}
	if !slices.Equal(a.EnumValues, b.EnumValues) {
		return false
	}
	if !slices.Equal(a.SetValues, b.SetValues) {
		return false
	}
	return true
}

// indexesEqual checks if two indexes are equal
func indexesEqual(a, b *Index) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}
	if !boolPtrEqual(a.Invisible, b.Invisible) {
		return false
	}
	if !stringPtrEqual(a.Using, b.Using) {
		return false
	}
	if !stringPtrEqual(a.Comment, b.Comment) {
		return false
	}
	return true
}

// indexesEqualIgnoreVisibility checks if two indexes are equal, ignoring the Invisible attribute
func indexesEqualIgnoreVisibility(a, b *Index) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}
	// Skip Invisible comparison
	if !stringPtrEqual(a.Using, b.Using) {
		return false
	}
	if !stringPtrEqual(a.Comment, b.Comment) {
		return false
	}
	return true
}

// constraintsEqual checks if two constraints are equal
func constraintsEqual(a, b *Constraint) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}
	if !stringPtrEqual(a.Expression, b.Expression) {
		return false
	}
	// Compare foreign key references
	if (a.References == nil) != (b.References == nil) {
		return false
	}
	if a.References != nil {
		if a.References.Table != b.References.Table {
			return false
		}
		if !slices.Equal(a.References.Columns, b.References.Columns) {
			return false
		}
	}
	return true
}

// formatColumnDefinition formats a column definition for ALTER TABLE
func formatColumnDefinition(col *Column) string {
	var parts []string

	// Column name and type
	typeDef := col.Type

	// Handle ENUM and SET types first (they have their own format)
	if col.Type == "enum" && len(col.EnumValues) > 0 {
		var values []string
		for _, v := range col.EnumValues {
			values = append(values, fmt.Sprintf("'%s'", sqlescape.EscapeString(v)))
		}
		typeDef = fmt.Sprintf("enum(%s)", strings.Join(values, ","))
	} else if col.Type == "set" && len(col.SetValues) > 0 {
		var values []string
		for _, v := range col.SetValues {
			values = append(values, fmt.Sprintf("'%s'", sqlescape.EscapeString(v)))
		}
		typeDef = fmt.Sprintf("set(%s)", strings.Join(values, ","))
	} else if col.Precision != nil && col.Scale != nil {
		// DECIMAL(precision, scale) - check Precision/Scale BEFORE Length!
		// DECIMAL columns have both Length and Precision/Scale set, but we want Precision/Scale
		typeDef = fmt.Sprintf("%s(%d,%d)", col.Type, *col.Precision, *col.Scale)
	} else if col.Precision != nil {
		// DECIMAL(precision) or other numeric types with precision only
		typeDef = fmt.Sprintf("%s(%d)", col.Type, *col.Precision)
	} else if col.Length != nil {
		// VARCHAR, CHAR, etc.
		typeDef = fmt.Sprintf("%s(%d)", col.Type, *col.Length)
	}

	if col.Unsigned != nil && *col.Unsigned {
		typeDef += " unsigned"
	}

	parts = append(parts, fmt.Sprintf("`%s` %s", col.Name, typeDef))

	// Charset and collation (skip for binary types and JSON)
	isBinaryType := col.Type == "varbinary" || col.Type == "binary" ||
		col.Type == "tinyblob" || col.Type == "blob" ||
		col.Type == "mediumblob" || col.Type == "longblob"
	isJSONType := col.Type == "json"

	if !isBinaryType && !isJSONType {
		if col.Charset != nil {
			parts = append(parts, fmt.Sprintf("CHARACTER SET %s", *col.Charset))
		}
		if col.Collation != nil {
			parts = append(parts, fmt.Sprintf("COLLATE %s", *col.Collation))
		}
	}

	// Nullable
	if !col.Nullable {
		parts = append(parts, "NOT NULL")
	} else {
		parts = append(parts, "NULL")
	}

	// Default value
	if col.Default != nil {
		// Check if it's a function/expression (no quotes) or a literal value (needs quotes)
		defaultVal := *col.Default
		if needsQuotes(defaultVal) {
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", sqlescape.EscapeString(defaultVal)))
		} else {
			parts = append(parts, fmt.Sprintf("DEFAULT %s", defaultVal))
		}
	}

	// Auto increment
	if col.AutoInc {
		parts = append(parts, "AUTO_INCREMENT")
	}

	// Comment
	if col.Comment != nil {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", sqlescape.EscapeString(*col.Comment)))
	}

	return strings.Join(parts, " ")
}

// formatAddIndex formats an ADD INDEX clause
func formatAddIndex(idx *Index) string {
	var parts []string

	// Index type
	switch idx.Type {
	case "PRIMARY KEY":
		parts = append(parts, "ADD PRIMARY KEY")
	case "UNIQUE":
		parts = append(parts, fmt.Sprintf("ADD UNIQUE INDEX `%s`", idx.Name))
	case "FULLTEXT":
		parts = append(parts, fmt.Sprintf("ADD FULLTEXT INDEX `%s`", idx.Name))
	case "SPATIAL":
		parts = append(parts, fmt.Sprintf("ADD SPATIAL INDEX `%s`", idx.Name))
	default:
		parts = append(parts, fmt.Sprintf("ADD INDEX `%s`", idx.Name))
	}

	// Columns
	var columns []string
	for _, col := range idx.Columns {
		columns = append(columns, fmt.Sprintf("`%s`", col))
	}
	parts = append(parts, fmt.Sprintf("(%s)", strings.Join(columns, ", ")))

	// USING clause
	if idx.Using != nil {
		parts = append(parts, fmt.Sprintf("USING %s", *idx.Using))
	}

	// Comment
	if idx.Comment != nil {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", sqlescape.EscapeString(*idx.Comment)))
	}

	// Visibility
	if idx.Invisible != nil && *idx.Invisible {
		parts = append(parts, "INVISIBLE")
	}

	return strings.Join(parts, " ")
}

// formatAddConstraint formats an ADD CONSTRAINT clause
func formatAddConstraint(constr *Constraint) string {
	var parts []string

	switch constr.Type {
	case "CHECK":
		if constr.Name != "" {
			parts = append(parts, fmt.Sprintf("ADD CONSTRAINT `%s` CHECK (%s)", constr.Name, *constr.Expression))
		} else {
			parts = append(parts, fmt.Sprintf("ADD CHECK (%s)", *constr.Expression))
		}
	case "FOREIGN KEY":
		var columns []string
		for _, col := range constr.Columns {
			columns = append(columns, fmt.Sprintf("`%s`", col))
		}
		var refColumns []string
		for _, col := range constr.References.Columns {
			refColumns = append(refColumns, fmt.Sprintf("`%s`", col))
		}
		if constr.Name != "" {
			parts = append(parts, fmt.Sprintf("ADD CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s)",
				constr.Name,
				strings.Join(columns, ", "),
				constr.References.Table,
				strings.Join(refColumns, ", ")))
		} else {
			parts = append(parts, fmt.Sprintf("ADD FOREIGN KEY (%s) REFERENCES `%s` (%s)",
				strings.Join(columns, ", "),
				constr.References.Table,
				strings.Join(refColumns, ", ")))
		}
	}

	return strings.Join(parts, " ")
}

// Helper functions for comparison
func stringPtrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func intPtrEqual(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// getPreviousColumn returns the name of the column before the given column name
func getPreviousColumn(columns []Column, name string) string {
	for i, col := range columns {
		if col.Name == name {
			if i == 0 {
				return ""
			}
			return columns[i-1].Name
		}
	}
	return ""
}

// needsQuotes determines if a default value needs to be quoted
func needsQuotes(value string) bool {
	// Common SQL functions/expressions that don't need quotes
	upper := strings.ToUpper(value)
	if upper == "NULL" ||
		upper == "CURRENT_TIMESTAMP" ||
		upper == "NOW()" ||
		strings.HasPrefix(upper, "CURRENT_TIMESTAMP(") {
		return false
	}

	// If it looks like a number, don't quote it
	if _, err := fmt.Sscanf(value, "%f", new(float64)); err == nil {
		return true // Actually, we should quote strings that look like numbers if they came as strings
	}

	// Default to quoting
	return true
}
