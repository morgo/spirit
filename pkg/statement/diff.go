package statement

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/pingcap/tidb/pkg/parser"
)

// DiffOptions controls the behavior of the Diff operation.
type DiffOptions struct {
	// IgnoreAutoIncrement skips diffing the AUTO_INCREMENT table option
	// (the table-level next-value counter, e.g. `AUTO_INCREMENT=100`).
	// Default: true (via NewDiffOptions).
	IgnoreAutoIncrement bool

	// IgnoreColumnAutoIncrement skips diffing the column-level AUTO_INCREMENT
	// attribute (whether a column carries the AUTO_INCREMENT flag). This is
	// distinct from IgnoreAutoIncrement, which only covers the table-option
	// counter. Default: false (via NewDiffOptions) — for general schema diffing
	// a column gaining or losing AUTO_INCREMENT is a real change. It is enabled
	// by consumers like the move-tables target-state check, where an unsharded
	// source legitimately differs from a sharded target that drops
	// AUTO_INCREMENT in favor of a Vitess sequence: the difference does not
	// affect copy correctness and must not block the move.
	IgnoreColumnAutoIncrement bool

	// IgnoreEngine skips diffing the ENGINE table option.
	// Default: true (via NewDiffOptions).
	IgnoreEngine bool

	// IgnoreCharsetCollation skips diffing CHARSET and COLLATION table options.
	// Default: false (via NewDiffOptions).
	IgnoreCharsetCollation bool

	// IgnorePartitioning skips diffing partition options entirely.
	// Default: false (via NewDiffOptions).
	IgnorePartitioning bool

	// IgnoreRowFormat skips diffing the ROW_FORMAT table option.
	// Default: true (via NewDiffOptions).
	// ROW_FORMAT=DYNAMIC is the InnoDB default in MySQL 8.0+, so differences
	// between an unspecified ROW_FORMAT and an explicit DYNAMIC are cosmetic.
	IgnoreRowFormat bool
}

// NewDiffOptions returns DiffOptions with sensible defaults.
// By default, AUTO_INCREMENT, ENGINE, and ROW_FORMAT differences are ignored.
func NewDiffOptions() *DiffOptions {
	return &DiffOptions{
		IgnoreAutoIncrement:       true,
		IgnoreColumnAutoIncrement: false,
		IgnoreEngine:              true,
		IgnoreCharsetCollation:    false,
		IgnorePartitioning:        false,
		IgnoreRowFormat:           true,
	}
}

// Diff compares this CreateTable (source) with another CreateTable (target)
// and returns ALTER TABLE statements needed to transform source into target.
// Most changes produce a single statement, but some (e.g. changing partition type)
// require multiple sequential statements.
// Returns nil if the tables are identical.
// If opts is nil, NewDiffOptions() defaults are used.
func (ct *CreateTable) Diff(target *CreateTable, opts *DiffOptions) ([]*AbstractStatement, error) {
	if opts == nil {
		opts = NewDiffOptions()
	}
	if ct.TableName != target.TableName {
		return nil, fmt.Errorf("cannot diff tables with different names: %s vs %s", ct.TableName, target.TableName)
	}

	var alterClauses []string

	// 1. Diff columns (DROP, ADD, MODIFY)
	columnClauses := ct.diffColumns(target, opts)
	alterClauses = append(alterClauses, columnClauses...)

	// 2. Diff indexes (DROP, ADD). Option-only index changes (same column
	// list, different WITH PARSER / KEY_BLOCK_SIZE / etc.) are returned as
	// separate statements because MySQL no-ops a combined DROP+ADD of the same
	// index in a single ALTER.
	indexClauses, separateIndexStatements := ct.diffIndexes(target)
	alterClauses = append(alterClauses, indexClauses...)

	// 3. Diff constraints (DROP, ADD)
	constraintClauses := ct.diffConstraints(target)
	alterClauses = append(alterClauses, constraintClauses...)

	// 4. Diff table options
	tableOptionClauses := ct.diffTableOptions(target, opts)
	alterClauses = append(alterClauses, tableOptionClauses...)

	// 5. Diff partition options — may produce additional statements
	var additionalStatements [][]string
	if !opts.IgnorePartitioning {
		partitionClauses, extraStatements := ct.diffPartitionOptions(target)
		alterClauses = append(alterClauses, partitionClauses...)
		additionalStatements = extraStatements
	}

	// Option-only index changes run as their own ALTER statements, after the
	// primary ALTER so they observe any column changes the re-add depends on.
	additionalStatements = append(additionalStatements, separateIndexStatements...)

	// Build the result
	var results []*AbstractStatement

	// Primary statement (columns, indexes, constraints, table options, and simple partition changes)
	if len(alterClauses) > 0 {
		stmt, err := ct.buildAlterStatement(alterClauses)
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	// Additional statements (e.g. second ALTER for partition type changes)
	for _, clauses := range additionalStatements {
		stmt, err := ct.buildAlterStatement(clauses)
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	if len(results) == 0 {
		return nil, nil
	}

	return results, nil
}

// buildAlterStatement constructs and parses an ALTER TABLE statement from clauses.
func (ct *CreateTable) buildAlterStatement(clauses []string) (*AbstractStatement, error) {
	alter := strings.Join(clauses, ", ")
	alterStmt := fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(ct.TableName), alter)

	p := parser.New()
	stmtNodes, _, err := p.Parse(alterStmt, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated ALTER statement: %w (SQL: %s)", err, alterStmt)
	}

	if len(stmtNodes) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(stmtNodes))
	}

	return &AbstractStatement{
		Table:     ct.TableName,
		Alter:     alter,
		Statement: alterStmt,
		StmtNode:  &stmtNodes[0],
	}, nil
}

// diffColumns compares columns and returns ALTER clauses for differences
func (ct *CreateTable) diffColumns(target *CreateTable, opts *DiffOptions) []string {
	var clauses []string

	// Build maps for easier lookup. Keys are lowercased so identifier
	// matching is case-insensitive — MySQL treats column names that
	// differ only in case as the same column.
	sourceColumns := make(map[string]*Column)
	for i := range ct.Columns {
		sourceColumns[strings.ToLower(ct.Columns[i].Name)] = &ct.Columns[i]
	}

	targetColumns := make(map[string]*Column)
	for i := range target.Columns {
		targetColumns[strings.ToLower(target.Columns[i].Name)] = &target.Columns[i]
	}

	// Handle PRIMARY KEY changes (inline column-level PRIMARY KEY)
	pkDropClause, pkAddClause, pkDropped, pkColumn := ct.diffPrimaryKey(target, sourceColumns, targetColumns)
	if pkDropClause != "" {
		clauses = append(clauses, pkDropClause)
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string
	for _, sourceCol := range ct.Columns {
		if _, exists := targetColumns[strings.ToLower(sourceCol.Name)]; !exists {
			dropClauses = append(dropClauses, fmt.Sprintf("DROP COLUMN %s", sqlescape.EscapeIdentifier(sourceCol.Name)))
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Determine which columns need explicit positioning
	// A column needs explicit positioning if:
	// 1. It's a new column (ADD) - always needs position
	// 2. Its previous column changed (explicit reorder)
	needsExplicitPosition := ct.calculateColumnPositioning(target, sourceColumns, targetColumns)

	// Generate the ALTER clauses in target order
	var prevColumn string
	for i, targetCol := range target.Columns {
		sourceCol, existsInSource := sourceColumns[strings.ToLower(targetCol.Name)]

		if !existsInSource {
			// ADD new column
			clause := fmt.Sprintf("ADD COLUMN %s", formatColumnDefinition(&targetCol))
			// Add positioning - only if not at the end
			isLastColumn := i == len(target.Columns)-1
			if prevColumn == "" {
				clause += " FIRST"
			} else if !isLastColumn {
				clause += fmt.Sprintf(" AFTER %s", sqlescape.EscapeIdentifier(prevColumn))
			}
			// If it's the last column, omit AFTER clause (implicit)
			clauses = append(clauses, clause)
		} else {
			// Skip modification if only PRIMARY KEY changed (already handled in diffIndexes)
			// When PRIMARY KEY is dropped, nullability change is implicit, so skip it
			pkOnlyChange := sourceCol.PrimaryKey != targetCol.PrimaryKey &&
				columnsEqualIgnorePK(sourceCol, &targetCol, opts)

			// Also check if only nullability changed due to PK drop
			// (PK columns are NOT NULL, dropping PK makes them NULL)
			pkDroppedNullabilityChange := pkDropped &&
				strings.EqualFold(sourceCol.Name, pkColumn) &&
				sourceCol.PrimaryKey && !targetCol.PrimaryKey &&
				!sourceCol.Nullable && targetCol.Nullable

			// MODIFY existing column if:
			// 1. Column definition changed (and not just PK-related changes)
			// 2. Column needs explicit positioning
			// needsExplicitPosition is keyed by lowercased column name, so
			// look up with the same normalization to avoid missing a
			// position-only change when the target's spelling is in mixed
			// or upper case.
			needsModify := (!ct.columnsEqualWithContext(sourceCol, &targetCol, target, opts) && !pkOnlyChange && !pkDroppedNullabilityChange) ||
				needsExplicitPosition[strings.ToLower(targetCol.Name)]

			if needsModify {
				clause := fmt.Sprintf("MODIFY COLUMN %s", formatColumnDefinition(&targetCol))
				if needsExplicitPosition[strings.ToLower(targetCol.Name)] {
					if prevColumn == "" {
						clause += " FIRST"
					} else {
						clause += fmt.Sprintf(" AFTER %s", sqlescape.EscapeIdentifier(prevColumn))
					}
				}
				clauses = append(clauses, clause)
			}
		}

		prevColumn = targetCol.Name
	}

	// Handle PRIMARY KEY adds last
	if pkAddClause != "" {
		clauses = append(clauses, pkAddClause)
	}

	return clauses
}

// calculateColumnPositioning determines which columns need explicit positioning (FIRST/AFTER).
// Returns a map of column names (lowercased) that need explicit positioning.
// Map keys are lowercased to match the source/target column maps built by
// the caller, since column identifiers in MySQL are case-insensitive.
func (ct *CreateTable) calculateColumnPositioning(target *CreateTable, sourceColumns, targetColumns map[string]*Column) map[string]bool {
	needsExplicitPosition := make(map[string]bool)

	var prevColumn string
	for _, targetCol := range target.Columns {
		_, existsInSource := sourceColumns[strings.ToLower(targetCol.Name)]

		if !existsInSource {
			// New columns always need explicit positioning
			needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
		} else {
			// Existing column - check if its position changed
			sourcePrevCol := getPreviousColumn(ct.Columns, targetCol.Name)

			// Check if this is an implicit or explicit position change
			_, prevColExistedInSource := sourceColumns[strings.ToLower(prevColumn)]
			_, sourcePrevColStillExists := targetColumns[strings.ToLower(sourcePrevCol)]

			implicitChange := false
			switch {
			case prevColumn == "" && sourcePrevCol == "":
				// Both first, no real change
				implicitChange = true
			case prevColumn != "" && !prevColExistedInSource:
				// Previous column is new, position change is implicit
				implicitChange = true
			case sourcePrevCol != "" && !sourcePrevColStillExists:
				// Previous column was dropped, position change is implicit
				implicitChange = true
			case strings.EqualFold(prevColumn, sourcePrevCol):
				// Same previous column, check if we need cascading
				// Cascading happens if the previous column was repositioned
				if prevColumn != "" && needsExplicitPosition[strings.ToLower(prevColumn)] && prevColExistedInSource {
					// Previous column was repositioned, so this column needs repositioning too
					needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
				}
				implicitChange = true
			default:
				// Explicit reorder - previous column changed and both exist
				implicitChange = false
			}

			if !implicitChange {
				needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
			}
		}

		prevColumn = targetCol.Name
	}

	return needsExplicitPosition
}

// diffPrimaryKey handles PRIMARY KEY changes between source and target tables.
// Returns: dropClause, addClause, pkDropped (bool), pkColumn (string)
func (ct *CreateTable) diffPrimaryKey(target *CreateTable, sourceColumns, targetColumns map[string]*Column) (string, string, bool, string) {
	var dropClause, addClause string
	var pkDropped bool
	var pkColumn string

	// Check if there's a table-level PK in source or target
	sourcePKIndex := ct.getPrimaryKeyIndex()
	targetPKIndex := target.getPrimaryKeyIndex()

	// Get the effective PK columns for both source and target.
	// PK can be defined as:
	// 1. Inline PK: column definition includes PRIMARY KEY (column.PrimaryKey = true)
	// 2. Table-level PK: separate PRIMARY KEY (col1, col2, ...) clause (in Indexes)
	sourcePKColumns := ct.getEffectivePrimaryKeyColumns()
	targetPKColumns := target.getEffectivePrimaryKeyColumns()

	// If the PK columns are the same (regardless of inline vs table-level representation),
	// no change is needed. MySQL normalizes inline PK to table-level PK in SHOW CREATE TABLE,
	// so we need to compare semantically rather than syntactically. Column
	// identifiers are case-insensitive in MySQL, so PK columns that differ
	// only in case (e.g. `PRIMARY KEY (id)` vs `PRIMARY KEY (ID)`) are the
	// same key.
	if slices.EqualFunc(sourcePKColumns, targetPKColumns, strings.EqualFold) {
		return "", "", false, ""
	}

	// Check for inline PK removal (column-level PRIMARY KEY)
	for _, sourceCol := range ct.Columns {
		targetCol, exists := targetColumns[strings.ToLower(sourceCol.Name)]
		if exists && sourceCol.PrimaryKey && !targetCol.PrimaryKey {
			// PRIMARY KEY was removed from this column (inline PK)
			// But only drop it if there's no table-level PK in target
			// (if target has table-level PK, diffIndexes will handle the transition)
			if targetPKIndex == nil {
				pkDropped = true
				pkColumn = sourceCol.Name
				dropClause = "DROP PRIMARY KEY"
				break
			}
		}
	}

	// Check for inline PK addition (column-level PRIMARY KEY)
	for _, targetCol := range target.Columns {
		sourceCol, exists := sourceColumns[strings.ToLower(targetCol.Name)]
		if exists && !sourceCol.PrimaryKey && targetCol.PrimaryKey {
			// PRIMARY KEY was added to this column (inline PK)
			// Add it if:
			// 1. There's no table-level PK in source (inline PK -> inline PK transition), OR
			// 2. Source has table-level PK and target has inline PK (table-level -> inline transition)
			if sourcePKIndex == nil || (sourcePKIndex != nil && targetPKIndex == nil) {
				pkColumn = targetCol.Name
				addClause = fmt.Sprintf("ADD PRIMARY KEY (%s)", sqlescape.EscapeIdentifier(pkColumn))
				break
			}
		}
	}

	// Special case: Table-level PK -> inline PK transition
	// We need to drop the table-level PK before adding the inline PK
	if sourcePKIndex != nil && targetPKIndex == nil && addClause != "" {
		dropClause = "DROP PRIMARY KEY"
	}

	return dropClause, addClause, pkDropped, pkColumn
}

// getEffectivePrimaryKeyColumns returns the column names that make up the primary key,
// regardless of whether it's defined inline (column PRIMARY KEY) or as table-level (PRIMARY KEY (cols)).
func (ct *CreateTable) getEffectivePrimaryKeyColumns() []string {
	// First check for table-level PK
	if pk := ct.getPrimaryKeyIndex(); pk != nil {
		return pk.Columns
	}

	// Check for inline PK (column-level)
	for _, col := range ct.Columns {
		if col.PrimaryKey {
			return []string{col.Name}
		}
	}

	return nil
}

// effectiveIndexes returns the table's indexes with any inline column-level
// UNIQUE declarations (`c int UNIQUE`) folded in as table-level UNIQUE
// indexes, plus the set of index names that were synthesized this way.
//
// MySQL never stores a column-level UNIQUE: it canonicalizes it into a
// table-level `UNIQUE KEY` named after the column (suffixed _2, _3, ... when
// that name is already taken — the same convention autoNameIndexes
// replicates), which is what SHOW CREATE TABLE reports. Folding the inline
// form here lets diffIndexes compare a user-written schema against the live
// canonical form symmetrically: a representation-only difference produces no
// clauses, while a real difference produces exactly one ADD/DROP.
//
// autoNameIndexes reserves these names before auto-naming unnamed table-level
// indexes (the server names inline uniques first), so the names computed here
// agree with the rest of the parsed schema; keep the two loops in sync.
func (ct *CreateTable) effectiveIndexes() ([]Index, map[string]bool) {
	hasInlineUnique := false
	for i := range ct.Columns {
		if ct.Columns[i].Unique {
			hasInlineUnique = true
			break
		}
	}
	if !hasInlineUnique {
		return ct.Indexes, nil
	}

	// Clone so the synthesized entries never leak into ct.Indexes.
	indexes := slices.Clone(ct.Indexes)
	inlineDerived := make(map[string]bool)
	usedNames := make(map[string]bool, len(indexes))
	for i := range indexes {
		usedNames[indexes[i].Name] = true
	}
	for _, col := range ct.Columns {
		if !col.Unique {
			continue
		}
		// Guess the server-assigned name: the column name, or the first free
		// _N suffix when an explicitly-declared index already claims it. If
		// the guess is wrong (the live table named it differently), the
		// content-based pairing in diffIndexes still prevents a spurious
		// DROP/ADD of an equivalent unique index.
		name := col.Name
		for suffix := 2; usedNames[name]; suffix++ {
			name = fmt.Sprintf("%s_%d", col.Name, suffix)
		}
		usedNames[name] = true
		inlineDerived[name] = true
		indexes = append(indexes, Index{
			Name:       name,
			Type:       "UNIQUE",
			Columns:    []string{col.Name},
			ColumnList: []IndexColumn{{Name: col.Name}},
		})
	}
	return indexes, inlineDerived
}

// diffIndexes compares indexes and returns ALTER clauses for differences.
//
// Most index changes are emitted into the combined ALTER (the returned
// []string). However, an index whose column list is identical but whose
// options differ (e.g. WITH PARSER or KEY_BLOCK_SIZE) cannot be changed by a
// combined `DROP INDEX x, ADD INDEX x (<same cols>)` in a single ALTER: MySQL
// pairs the two clauses up and keeps the existing index, silently ignoring the
// option change. To make such a change actually take effect, the DROP and ADD
// must run as two separate ALTER statements. Those are returned via the second
// value as standalone clause-lists (each becomes its own ALTER statement).
func (ct *CreateTable) diffIndexes(target *CreateTable) (clauses []string, separateStatements [][]string) {
	// Fold inline column-level UNIQUEs into both index sets so that
	// `c int UNIQUE` participates in index diffing like the table-level
	// `UNIQUE KEY c (c)` MySQL canonicalizes it into.
	sourceIdxList, sourceInlineDerived := ct.effectiveIndexes()
	targetIdxList, targetInlineDerived := target.effectiveIndexes()

	// Build maps for easier lookup
	sourceIndexes := make(map[string]*Index)
	for i := range sourceIdxList {
		sourceIndexes[sourceIdxList[i].Name] = &sourceIdxList[i]
	}

	targetIndexes := make(map[string]*Index)
	for i := range targetIdxList {
		targetIndexes[targetIdxList[i].Name] = &targetIdxList[i]
	}

	// Safety net for inline-derived names: the synthesized name is only a
	// guess at what the server assigned. Pair unique indexes that cover the
	// same column set but carry different names whenever at least one side's
	// name came from an inline declaration, so we never DROP a live unique
	// index (or ADD a duplicate) that the other side's inline UNIQUE already
	// expresses. Two explicitly named unique indexes that differ only in name
	// are NOT paired — that is a real rename (DROP + ADD).
	matchedSourceUnique := make(map[string]bool) // source name -> matched
	matchedTargetUnique := make(map[string]bool) // target name -> matched
	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		if sourceIdx.Type != "UNIQUE" {
			continue
		}
		if _, exactMatch := targetIndexes[sourceIdx.Name]; exactMatch {
			continue // handled by the normal name-based path
		}
		for j := range targetIdxList {
			targetIdx := &targetIdxList[j]
			if targetIdx.Type != "UNIQUE" {
				continue
			}
			if _, exactMatch := sourceIndexes[targetIdx.Name]; exactMatch {
				continue // this target index already has a name match in source
			}
			if matchedTargetUnique[targetIdx.Name] {
				continue // already paired with another source index
			}
			if !sourceInlineDerived[sourceIdx.Name] && !targetInlineDerived[targetIdx.Name] {
				continue // both explicitly named: a genuine rename
			}
			if indexColumnsIdenticalIgnoreName(sourceIdx, targetIdx) {
				matchedSourceUnique[sourceIdx.Name] = true
				matchedTargetUnique[targetIdx.Name] = true
				break
			}
		}
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string

	// Handle inline PK <-> table-level PK transitions
	targetPKIndex := target.getPrimaryKeyIndex()

	// Track if we've already added a DROP PRIMARY KEY
	pkDropAdded := false

	// Check if source has inline PK (column-level PRIMARY KEY)
	sourceHasInlinePK := false
	for _, col := range ct.Columns {
		if col.PrimaryKey {
			sourceHasInlinePK = true
			break
		}
	}

	// Check if target has inline PK
	targetHasInlinePK := false
	for _, col := range target.Columns {
		if col.PrimaryKey {
			targetHasInlinePK = true
			break
		}
	}

	// Get effective PK columns for both source and target
	sourcePKColumns := ct.getEffectivePrimaryKeyColumns()
	targetPKColumns := target.getEffectivePrimaryKeyColumns()

	// Only handle inline PK <-> table-level PK transitions if the PK columns actually changed.
	// If the columns are the same, the representations are semantically equivalent.
	// Compare case-insensitively, matching MySQL's column-name semantics.
	pkColumnsEqual := slices.EqualFunc(sourcePKColumns, targetPKColumns, strings.EqualFold)

	if sourceHasInlinePK && targetPKIndex != nil && !pkColumnsEqual {
		// Inline PK -> table-level PK with different columns: drop the inline PK
		dropClauses = append(dropClauses, "DROP PRIMARY KEY")
		pkDropAdded = true
	}
	// Note: Table-level PK -> inline PK is handled in diffColumns to ensure correct order
	// (DROP PRIMARY KEY must come before ADD PRIMARY KEY)

	// optionOnlyChanged tracks index names whose column list is unchanged but
	// whose options differ (e.g. WITH PARSER / KEY_BLOCK_SIZE). These must be
	// emitted as separate DROP + ADD statements rather than combined into one
	// ALTER, because MySQL no-ops a combined DROP+ADD of the same name and
	// column list. Such indexes are routed out of dropClauses/addClauses below.
	optionOnlyChanged := make(map[string]bool)

	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		if matchedSourceUnique[sourceIdx.Name] {
			continue // equivalent unique index exists in target under an inline-derived pairing
		}
		targetIdx, existsInTarget := targetIndexes[sourceIdx.Name]

		if !existsInTarget {
			// Index removed completely
			if sourceIdx.Type == "PRIMARY KEY" {
				// Skip if target has inline PK (handled in diffColumns)
				if !targetHasInlinePK && !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			} else {
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		} else if !indexesEqual(sourceIdx, targetIdx) && !indexesEqualIgnoreVisibility(sourceIdx, targetIdx) {
			// Index exists but changed (and not just visibility) - need to drop and re-add
			switch {
			case sourceIdx.Type == "PRIMARY KEY":
				// Only add if not already added above
				if !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			case indexNeedsSeparateRebuild(sourceIdx, targetIdx):
				// A no-op-prone option (WITH PARSER / KEY_BLOCK_SIZE) changed on
				// an unchanged column list. A combined DROP+ADD in one ALTER
				// would be a MySQL no-op, so emit two separate statements that
				// MySQL will actually apply.
				optionOnlyChanged[sourceIdx.Name] = true
				separateStatements = append(separateStatements,
					[]string{fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name))},
					[]string{formatAddIndex(targetIdx)},
				)
			default:
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetIdx := range targetIdxList {
		if matchedTargetUnique[targetIdx.Name] {
			continue // equivalent unique index exists in source under an inline-derived pairing
		}
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if !existsInSource {
			// New index - add it, but skip PRIMARY KEY if source has equivalent inline PK
			if targetIdx.Type == "PRIMARY KEY" && pkColumnsEqual {
				// Source has inline PK with same columns - skip adding table-level PK
				continue
			}
			addClauses = append(addClauses, formatAddIndex(&targetIdx))
		} else if !indexesEqual(sourceIdx, &targetIdx) {
			// Index exists but changed - check if only visibility changed
			if indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
				// Only visibility changed - skip for now, handle in ALTER INDEX section
				continue
			}
			// Option-only changes are emitted as separate statements above.
			if optionOnlyChanged[targetIdx.Name] {
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
	for _, targetIdx := range targetIdxList {
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if existsInSource && !indexesEqual(sourceIdx, &targetIdx) && indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
			// Only visibility changed
			targetVisible := targetIdx.Invisible == nil || !*targetIdx.Invisible
			if targetVisible {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX %s VISIBLE", sqlescape.EscapeIdentifier(targetIdx.Name)))
			} else {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX %s INVISIBLE", sqlescape.EscapeIdentifier(targetIdx.Name)))
			}
		}
	}
	slices.Sort(alterClauses)
	clauses = append(clauses, alterClauses...)

	return clauses, separateStatements
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

	// Build a set of source constraint names that have an equivalent match in the
	// target under a different name. This handles the case where MySQL generates
	// different auto-names for CHECK constraints whose original expression text
	// differs only cosmetically (e.g. charset introducers like _utf8mb3 that are
	// stripped during parsing). Without this, such constraints would produce a
	// spurious DROP + ADD.
	matchedSourceByExpression := make(map[string]bool) // source name -> matched
	matchedTargetByExpression := make(map[string]bool) // target name -> matched
	for i := range ct.Constraints {
		sourceConstr := &ct.Constraints[i]
		if _, exactMatch := targetConstraints[sourceConstr.Name]; exactMatch {
			continue // will be handled by the normal name-based path
		}
		// No exact name match — look for an expression-equivalent target constraint
		for j := range target.Constraints {
			targetConstr := &target.Constraints[j]
			if _, exactMatch := sourceConstraints[targetConstr.Name]; exactMatch {
				continue // this target constraint already has a name match in source
			}
			if matchedTargetByExpression[targetConstr.Name] {
				continue // already paired with another source constraint
			}
			if constraintsEqualIgnoreName(sourceConstr, targetConstr) {
				matchedSourceByExpression[sourceConstr.Name] = true
				matchedTargetByExpression[targetConstr.Name] = true
				break
			}
		}
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string
	for i := range ct.Constraints {
		sourceConstr := &ct.Constraints[i]
		if matchedSourceByExpression[sourceConstr.Name] {
			continue // equivalent constraint exists in target under a different name
		}
		targetConstr, exists := targetConstraints[sourceConstr.Name]

		// Drop if constraint doesn't exist in target OR if it changed
		if !exists || !constraintsEqual(sourceConstr, targetConstr) {
			switch sourceConstr.Type {
			case "FOREIGN KEY":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP FOREIGN KEY %s", sqlescape.EscapeIdentifier(sourceConstr.Name)))
			case "CHECK":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP CHECK %s", sqlescape.EscapeIdentifier(sourceConstr.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetConstr := range target.Constraints {
		if matchedTargetByExpression[targetConstr.Name] {
			continue // equivalent constraint exists in source under a different name
		}
		sourceConstr, existsInSource := sourceConstraints[targetConstr.Name]

		if !existsInSource || !constraintsEqual(sourceConstr, &targetConstr) {
			addClauses = append(addClauses, formatAddConstraint(&targetConstr))
		}
	}
	slices.Sort(addClauses)
	clauses = append(clauses, addClauses...)

	return clauses
}

// diffTableOptions compares table options and returns ALTER clauses for differences.
// The opts parameter controls which table options are compared.
func (ct *CreateTable) diffTableOptions(target *CreateTable, opts *DiffOptions) []string {
	var clauses []string

	// Compare ENGINE
	if !opts.IgnoreEngine {
		if !ptrEqual(ct.TableOptions.getEngine(), target.TableOptions.getEngine()) {
			if engine := target.TableOptions.getEngine(); engine != nil {
				clauses = append(clauses, fmt.Sprintf("ENGINE=%s", *engine))
			}
		}
	}

	// Compare CHARSET and COLLATION
	if !opts.IgnoreCharsetCollation {
		if !ptrEqual(ct.TableOptions.getCharset(), target.TableOptions.getCharset()) {
			if charset := target.TableOptions.getCharset(); charset != nil {
				clauses = append(clauses, fmt.Sprintf("DEFAULT CHARSET=%s", *charset))
			}
		}

		if !ptrEqual(ct.TableOptions.getCollation(), target.TableOptions.getCollation()) {
			if collation := target.TableOptions.getCollation(); collation != nil {
				clauses = append(clauses, fmt.Sprintf("COLLATE=%s", *collation))
			}
		}
	}

	// Compare COMMENT (always compared — not controlled by an ignore option)
	if !ptrEqual(ct.TableOptions.getComment(), target.TableOptions.getComment()) {
		if comment := target.TableOptions.getComment(); comment != nil {
			clauses = append(clauses, fmt.Sprintf("COMMENT='%s'", sqlescape.EscapeString(*comment)))
		} else {
			// The source has a comment but the target does not: emit an
			// explicit empty comment to clear it. Without this clause the
			// difference would be detected but silently dropped.
			clauses = append(clauses, "COMMENT=''")
		}
	}

	// Compare ROW_FORMAT
	if !opts.IgnoreRowFormat {
		if !ptrEqual(ct.TableOptions.getRowFormat(), target.TableOptions.getRowFormat()) {
			if rowFormat := target.TableOptions.getRowFormat(); rowFormat != nil {
				clauses = append(clauses, fmt.Sprintf("ROW_FORMAT=%s", *rowFormat))
			}
		}
	}

	// Compare AUTO_INCREMENT
	// Note: AUTO_INCREMENT is ignored by default (like vitess schemadiff)
	if !opts.IgnoreAutoIncrement {
		if !ptrEqual(ct.TableOptions.getAutoIncrement(), target.TableOptions.getAutoIncrement()) {
			if autoInc := target.TableOptions.getAutoIncrement(); autoInc != nil {
				clauses = append(clauses, fmt.Sprintf("AUTO_INCREMENT=%s", *autoInc))
			}
		}
	}

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

func (to *TableOptions) getAutoIncrement() *string {
	if to == nil || to.AutoIncrement == nil {
		return nil
	}
	s := strconv.FormatUint(*to.AutoIncrement, 10)
	return &s
}

// columnsEqualWithContext checks if two columns are equal, considering table context for charset/collation
// If a column's charset is nil, it inherits from the table. If it's explicitly set to the same as the table,
// it's considered equal to nil (no explicit charset needed).
func (ct *CreateTable) columnsEqualWithContext(a, b *Column, target *CreateTable, opts *DiffOptions) bool {
	// Column names are case-insensitive in MySQL, so `id` and `ID` refer
	// to the same column.
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !ptrEqual(a.Length, b.Length) {
		return false
	}
	if !ptrEqual(a.Precision, b.Precision) {
		return false
	}
	if !ptrEqual(a.Scale, b.Scale) {
		return false
	}
	if !ptrEqual(a.Unsigned, b.Unsigned) {
		return false
	}
	if !ptrEqual(a.Zerofill, b.Zerofill) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	// Normalize default values for nullable columns:
	// For nullable columns, nil and the NULL *keyword* are semantically
	// equivalent. User might write `VARCHAR(255) NULL` but MySQL outputs
	// `VARCHAR(255) DEFAULT NULL`. A quoted string literal 'NULL'
	// (DefaultIsString) is NOT the keyword and must not collapse — it is a
	// real default that differs from no-default.
	sourceDefault := a.Default
	targetDefault := b.Default
	if a.Nullable {
		if sourceDefault != nil && *sourceDefault == "NULL" && !a.DefaultIsString {
			sourceDefault = nil
		}
		if targetDefault != nil && *targetDefault == "NULL" && !b.DefaultIsString {
			targetDefault = nil
		}
	}
	if !ptrEqual(sourceDefault, targetDefault) {
		return false
	}
	if a.DefaultIsExpr != b.DefaultIsExpr {
		return false
	}
	if !columnExtendedAttributesEqual(a, b) {
		return false
	}
	// On a string column a quoted string literal default ('TRUE') is a
	// different value than the same text as a keyword/number default (TRUE),
	// so quotedness is part of column identity. On a numeric column it is not:
	// MySQL always renders the default quoted, so `DEFAULT 0` (bare) and
	// `DEFAULT '0'` (the SHOW CREATE TABLE form) are the same default and must
	// compare equal — the value itself is already compared above.
	if a.DefaultIsString != b.DefaultIsString && !isNumericColumnType(a.Type) {
		return false
	}
	if !opts.IgnoreColumnAutoIncrement && a.AutoInc != b.AutoInc {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	// Unique is intentionally NOT compared: a column-level UNIQUE is
	// representation, not state. MySQL canonicalizes `c int UNIQUE` into a
	// table-level UNIQUE KEY (that is what SHOW CREATE TABLE reports), and a
	// MODIFY COLUMN cannot express uniqueness anyway (formatColumnDefinition
	// never emits UNIQUE). Inline uniques are folded into index-level diffing
	// instead — see effectiveIndexes / diffIndexes.
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}

	// Charset comparison with table context
	// If column charset is nil, it inherits from table
	// If column charset equals table charset, it's redundant (same as nil)
	sourceCharset := a.Charset
	targetCharset := b.Charset

	// Normalize: if charset equals table charset, treat as nil
	if sourceCharset != nil && ct.TableOptions != nil && ct.TableOptions.Charset != nil && *sourceCharset == *ct.TableOptions.Charset {
		sourceCharset = nil
	}
	if targetCharset != nil && target.TableOptions != nil && target.TableOptions.Charset != nil && *targetCharset == *target.TableOptions.Charset {
		targetCharset = nil
	}

	if !ptrEqual(sourceCharset, targetCharset) {
		return false
	}

	// Collation comparison with table context
	sourceCollation := a.Collation
	targetCollation := b.Collation

	// Normalize: if collation equals table collation, treat as nil
	if sourceCollation != nil && ct.TableOptions != nil && ct.TableOptions.Collation != nil && *sourceCollation == *ct.TableOptions.Collation {
		sourceCollation = nil
	}
	if targetCollation != nil && target.TableOptions != nil && target.TableOptions.Collation != nil && *targetCollation == *target.TableOptions.Collation {
		targetCollation = nil
	}

	if !ptrEqual(sourceCollation, targetCollation) {
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
func columnsEqualIgnorePK(a, b *Column, opts *DiffOptions) bool {
	// Column names are case-insensitive in MySQL.
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !ptrEqual(a.Length, b.Length) {
		return false
	}
	if !ptrEqual(a.Precision, b.Precision) {
		return false
	}
	if !ptrEqual(a.Scale, b.Scale) {
		return false
	}
	if !ptrEqual(a.Unsigned, b.Unsigned) {
		return false
	}
	if !ptrEqual(a.Zerofill, b.Zerofill) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	// Normalize default values for nullable columns:
	// For nullable columns, nil and the NULL *keyword* are semantically
	// equivalent. A quoted string literal 'NULL' (DefaultIsString) is NOT
	// the keyword and must not collapse.
	sourceDefault := a.Default
	targetDefault := b.Default
	if a.Nullable {
		if sourceDefault != nil && *sourceDefault == "NULL" && !a.DefaultIsString {
			sourceDefault = nil
		}
		if targetDefault != nil && *targetDefault == "NULL" && !b.DefaultIsString {
			targetDefault = nil
		}
	}
	if !ptrEqual(sourceDefault, targetDefault) {
		return false
	}
	if a.DefaultIsExpr != b.DefaultIsExpr {
		return false
	}
	if !columnExtendedAttributesEqual(a, b) {
		return false
	}
	// Numeric defaults are always rendered quoted by MySQL, so quotedness is
	// not meaningful for them; for string columns it is. See the equivalent
	// guard in columnsEqualWithContext.
	if a.DefaultIsString != b.DefaultIsString && !isNumericColumnType(a.Type) {
		return false
	}
	if !opts.IgnoreColumnAutoIncrement && a.AutoInc != b.AutoInc {
		return false
	}
	// Skip PrimaryKey comparison
	// Unique is intentionally not compared — inline UNIQUE is folded into
	// index-level diffing (see effectiveIndexes / diffIndexes and the
	// matching comment in columnsEqualWithContext).
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}
	if !ptrEqual(a.Charset, b.Charset) {
		return false
	}
	if !ptrEqual(a.Collation, b.Collation) {
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

// columnExtendedAttributesEqual compares the column attributes beyond the
// basic type/nullability/default set: ON UPDATE (TIMESTAMP/DATETIME
// auto-update), GENERATED ALWAYS AS expressions (including STORED vs
// VIRTUAL), and SRID. These are semantically critical — omitting them from a
// MODIFY COLUMN silently removes the behavior from the live table.
//
// Column-level CHECK constraints are intentionally NOT compared here: the
// parser hoists them into table-level CreateTable.Constraints (see
// normalizeColumnChecks), so they are diffed by diffConstraints instead. This
// matches MySQL's SHOW CREATE TABLE, which always reports CHECKs at table
// level, and keeps a re-diff convergent.
func columnExtendedAttributesEqual(a, b *Column) bool {
	if !ptrEqual(a.OnUpdate, b.OnUpdate) {
		return false
	}
	if !ptrEqual(a.GeneratedExpr, b.GeneratedExpr) {
		return false
	}
	if a.GeneratedExpr != nil && a.GeneratedStored != b.GeneratedStored {
		return false
	}
	if !ptrEqual(a.SRID, b.SRID) {
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
	// Compare using ColumnList if available, otherwise fall back to Columns.
	// Referenced column names are matched case-insensitively to mirror
	// MySQL's column-identifier semantics.
	if len(a.ColumnList) > 0 && len(b.ColumnList) > 0 {
		if !indexColumnListsEqual(a.ColumnList, b.ColumnList) {
			return false
		}
	} else if !slices.EqualFunc(a.Columns, b.Columns, strings.EqualFold) {
		return false
	}
	if !ptrEqual(a.Invisible, b.Invisible) {
		return false
	}
	if !ptrEqual(a.Using, b.Using) {
		return false
	}
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}
	if !ptrEqual(a.KeyBlockSize, b.KeyBlockSize) {
		return false
	}
	if !ptrEqual(a.ParserName, b.ParserName) {
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
	// Compare using ColumnList if available, otherwise fall back to Columns.
	// Referenced column names are matched case-insensitively.
	if len(a.ColumnList) > 0 && len(b.ColumnList) > 0 {
		if !indexColumnListsEqual(a.ColumnList, b.ColumnList) {
			return false
		}
	} else if !slices.EqualFunc(a.Columns, b.Columns, strings.EqualFold) {
		return false
	}
	// Skip Invisible comparison
	if !ptrEqual(a.Using, b.Using) {
		return false
	}
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}
	if !ptrEqual(a.KeyBlockSize, b.KeyBlockSize) {
		return false
	}
	if !ptrEqual(a.ParserName, b.ParserName) {
		return false
	}
	return true
}

// indexColumnListIdentical reports whether two indexes have the same name,
// type, and column list — regardless of their options.
func indexColumnListIdentical(a, b *Index) bool {
	if a.Name != b.Name {
		return false
	}
	return indexColumnsIdenticalIgnoreName(a, b)
}

// indexColumnsIdenticalIgnoreName reports whether two indexes have the same
// type and column list, regardless of their names or options. Used to pair a
// unique index synthesized from an inline column-level UNIQUE (whose name is
// only a guess at the server-assigned one) with an equivalent live index.
func indexColumnsIdenticalIgnoreName(a, b *Index) bool {
	if a.Type != b.Type {
		return false
	}
	if len(a.ColumnList) > 0 && len(b.ColumnList) > 0 {
		return indexColumnListsEqual(a.ColumnList, b.ColumnList)
	}
	return slices.EqualFunc(a.Columns, b.Columns, strings.EqualFold)
}

// indexNeedsSeparateRebuild reports whether a changed index must be emitted as
// two separate ALTER statements (DROP then ADD) rather than combined into one.
//
// When the column list is unchanged, MySQL pairs a combined `DROP INDEX x, ADD
// INDEX x (<same cols>)` up and keeps the existing index — but only some
// options are silently ignored this way. Verified against MySQL 8.0:
//   - WITH PARSER  → ignored by the combined ALTER (must split)
//   - KEY_BLOCK_SIZE → ignored by the combined ALTER (must split)
//   - COMMENT      → applied by the combined ALTER (no split needed)
//   - visibility   → never reaches here; handled via ALTER INDEX VISIBLE/INVISIBLE
//
// If the column list itself changes, MySQL really rebuilds the index, so a
// combined ALTER is fine and this returns false.
func indexNeedsSeparateRebuild(source, target *Index) bool {
	if !indexColumnListIdentical(source, target) {
		return false
	}
	if !ptrEqual(source.ParserName, target.ParserName) {
		return true
	}
	if !ptrEqual(source.KeyBlockSize, target.KeyBlockSize) {
		return true
	}
	return false
}

// indexColumnListsEqual checks if two index column lists are equal
func indexColumnListsEqual(a, b []IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !indexColumnsEqual(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

// indexColumnsEqual checks if two index columns are equal
func indexColumnsEqual(a, b *IndexColumn) bool {
	// Column identifiers are case-insensitive in MySQL.
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}
	if !ptrEqual(a.Expression, b.Expression) {
		return false
	}
	if !ptrEqual(a.Length, b.Length) {
		return false
	}
	// KEY (a) and KEY (a DESC) are physically different indexes.
	if a.Desc != b.Desc {
		return false
	}
	return true
}

// constraintsEqual checks if two constraints are equal
func constraintsEqual(a, b *Constraint) bool {
	if a.Name != b.Name {
		return false
	}
	return constraintsEqualIgnoreName(a, b)
}

// constraintsEqualIgnoreName compares two constraints on everything except their name.
// This is used to detect constraints that are logically identical but have different
// auto-generated names (e.g., MySQL generates different CHECK constraint names when
// the original expression text differs only in charset introducers like _utf8mb3).
func constraintsEqualIgnoreName(a, b *Constraint) bool {
	if a.Type != b.Type {
		return false
	}
	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}
	if !ptrEqual(a.Expression, b.Expression) {
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
		// Compare ON DELETE and ON UPDATE actions
		if !ptrEqual(a.References.OnDelete, b.References.OnDelete) {
			return false
		}
		if !ptrEqual(a.References.OnUpdate, b.References.OnUpdate) {
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

	// Determine the full type definition including length/precision/values
	switch {
	case col.Type == "enum" && len(col.EnumValues) > 0:
		var values []string
		for _, v := range col.EnumValues {
			values = append(values, fmt.Sprintf("'%s'", sqlescape.EscapeString(v)))
		}
		typeDef = fmt.Sprintf("enum(%s)", strings.Join(values, ","))
	case col.Type == "set" && len(col.SetValues) > 0:
		var values []string
		for _, v := range col.SetValues {
			values = append(values, fmt.Sprintf("'%s'", sqlescape.EscapeString(v)))
		}
		typeDef = fmt.Sprintf("set(%s)", strings.Join(values, ","))
	case col.Precision != nil && col.Scale != nil:
		// DECIMAL(precision, scale) - check Precision/Scale BEFORE Length!
		// DECIMAL columns have both Length and Precision/Scale set, but we want Precision/Scale
		typeDef = fmt.Sprintf("%s(%d,%d)", col.Type, *col.Precision, *col.Scale)
	case col.Precision != nil:
		// DECIMAL(precision) or other numeric types with precision only
		typeDef = fmt.Sprintf("%s(%d)", col.Type, *col.Precision)
	case col.Length != nil:
		// VARCHAR, CHAR, etc.
		typeDef = fmt.Sprintf("%s(%d)", col.Type, *col.Length)
	}

	if col.Unsigned != nil && *col.Unsigned {
		typeDef += " unsigned"
	}

	if col.Zerofill != nil && *col.Zerofill {
		typeDef += " zerofill"
	}

	parts = append(parts, fmt.Sprintf("%s %s", sqlescape.EscapeIdentifier(col.Name), typeDef))

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

	// Generated column clause — comes directly after the data type (and any
	// charset/collation), before the NULL/NOT NULL attribute:
	// col_name type GENERATED ALWAYS AS (expr) STORED|VIRTUAL [NOT NULL|NULL] ...
	if col.GeneratedExpr != nil {
		genClause := fmt.Sprintf("GENERATED ALWAYS AS (%s)", *col.GeneratedExpr)
		if col.GeneratedStored {
			genClause += " STORED"
		} else {
			genClause += " VIRTUAL"
		}
		parts = append(parts, genClause)
	}

	// Nullable
	if !col.Nullable {
		parts = append(parts, "NOT NULL")
	} else {
		parts = append(parts, "NULL")
	}

	// SRID attribute for spatial columns. MySQL's SHOW CREATE TABLE places
	// this after the NULL/NOT NULL attribute (as /*!80003 SRID n */).
	if col.SRID != nil {
		parts = append(parts, fmt.Sprintf("SRID %d", *col.SRID))
	}

	// Default value (not permitted on generated columns)
	if col.Default != nil && col.GeneratedExpr == nil {
		defaultVal := *col.Default
		switch {
		case col.DefaultIsExpr:
			// Expression defaults must be wrapped in parentheses, e.g. DEFAULT (json_object())
			parts = append(parts, fmt.Sprintf("DEFAULT (%s)", defaultVal))
		case col.DefaultIsString:
			// Quoted string literal. The stored value is the true raw value
			// (unescaped at parse time), so quote+escape exactly once. This
			// must bypass the needsQuotes heuristic: a literal 'TRUE' or
			// 'NULL' or '2020' has to stay quoted, otherwise MySQL would
			// store the keyword/number instead of the string.
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", sqlescape.EscapeString(defaultVal)))
		case needsQuotes(defaultVal):
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", sqlescape.EscapeString(defaultVal)))
		default:
			parts = append(parts, fmt.Sprintf("DEFAULT %s", defaultVal))
		}
	}

	// ON UPDATE (TIMESTAMP/DATETIME auto-update) — comes after DEFAULT
	if col.OnUpdate != nil {
		parts = append(parts, fmt.Sprintf("ON UPDATE %s", *col.OnUpdate))
	}

	// Auto increment
	if col.AutoInc {
		parts = append(parts, "AUTO_INCREMENT")
	}

	// Comment
	if col.Comment != nil {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", sqlescape.EscapeString(*col.Comment)))
	}

	// NOTE: column-level CHECK constraints are deliberately not emitted here.
	// The parser hoists them into table-level constraints (see
	// normalizeColumnChecks), so they are emitted as ADD CONSTRAINT ... CHECK
	// by diffConstraints. Emitting them inline in a MODIFY/ADD COLUMN would
	// make MySQL hoist them to a table-level CHECK with an auto-name, breaking
	// re-diff convergence and risking a spurious DROP CHECK.

	return strings.Join(parts, " ")
}

// formatAddIndex formats an ADD INDEX clause
func formatAddIndex(idx *Index) string {
	var parts []string

	// Build the ADD clause: keyword + optional name.
	// The name is omitted when empty (safety net; autoNameIndexes should
	// have already assigned one during parsing).
	var keyword string
	switch idx.Type {
	case "PRIMARY KEY":
		keyword = "ADD PRIMARY KEY"
	case "UNIQUE":
		keyword = "ADD UNIQUE INDEX"
	case "FULLTEXT":
		keyword = "ADD FULLTEXT INDEX"
	case "SPATIAL":
		keyword = "ADD SPATIAL INDEX"
	default:
		keyword = "ADD INDEX"
	}
	if idx.Type != "PRIMARY KEY" && idx.Name != "" {
		keyword += " " + sqlescape.EscapeIdentifier(idx.Name)
	}
	parts = append(parts, keyword)

	// Columns - use ColumnList if available for full details (prefix, expressions)
	var columns []string
	if len(idx.ColumnList) > 0 {
		for _, col := range idx.ColumnList {
			var colStr string
			if col.Expression != nil {
				// Expression index (functional index) - needs double parentheses
				colStr = fmt.Sprintf("(%s)", *col.Expression)
			} else {
				// Regular column reference
				colStr = sqlescape.EscapeIdentifier(col.Name)
				if col.Length != nil {
					colStr += fmt.Sprintf("(%d)", *col.Length)
				}
			}
			// Descending key part (MySQL 8.0+). ASC is MySQL's canonical
			// default and is never emitted explicitly.
			if col.Desc {
				colStr += " DESC"
			}
			columns = append(columns, colStr)
		}
	} else {
		// Fall back to simple column names
		for _, col := range idx.Columns {
			columns = append(columns, sqlescape.EscapeIdentifier(col))
		}
	}
	parts = append(parts, fmt.Sprintf("(%s)", strings.Join(columns, ", ")))

	// USING clause
	if idx.Using != nil {
		parts = append(parts, fmt.Sprintf("USING %s", *idx.Using))
	}

	// KEY_BLOCK_SIZE
	if idx.KeyBlockSize != nil {
		parts = append(parts, fmt.Sprintf("KEY_BLOCK_SIZE=%d", *idx.KeyBlockSize))
	}

	// WITH PARSER (FULLTEXT indexes)
	if idx.ParserName != nil {
		parts = append(parts, fmt.Sprintf("WITH PARSER %s", *idx.ParserName))
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
			parts = append(parts, fmt.Sprintf("ADD CONSTRAINT %s CHECK (%s)", sqlescape.EscapeIdentifier(constr.Name), *constr.Expression))
		} else {
			parts = append(parts, fmt.Sprintf("ADD CHECK (%s)", *constr.Expression))
		}
	case "FOREIGN KEY":
		var columns []string
		for _, col := range constr.Columns {
			columns = append(columns, sqlescape.EscapeIdentifier(col))
		}
		var refColumns []string
		for _, col := range constr.References.Columns {
			refColumns = append(refColumns, sqlescape.EscapeIdentifier(col))
		}

		var fkClause string
		if constr.Name != "" {
			fkClause = fmt.Sprintf("ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				sqlescape.EscapeIdentifier(constr.Name),
				strings.Join(columns, ", "),
				sqlescape.EscapeIdentifier(constr.References.Table),
				strings.Join(refColumns, ", "))
		} else {
			fkClause = fmt.Sprintf("ADD FOREIGN KEY (%s) REFERENCES %s (%s)",
				strings.Join(columns, ", "),
				sqlescape.EscapeIdentifier(constr.References.Table),
				strings.Join(refColumns, ", "))
		}

		// Add ON DELETE clause if present
		if constr.References.OnDelete != nil {
			fkClause += fmt.Sprintf(" ON DELETE %s", *constr.References.OnDelete)
		}

		// Add ON UPDATE clause if present
		if constr.References.OnUpdate != nil {
			fkClause += fmt.Sprintf(" ON UPDATE %s", *constr.References.OnUpdate)
		}

		parts = append(parts, fkClause)
	}

	return strings.Join(parts, " ")
}

// getPrimaryKeyIndex returns the PRIMARY KEY index if it exists (table-level PK), nil otherwise
func (ct *CreateTable) getPrimaryKeyIndex() *Index {
	for i := range ct.Indexes {
		if ct.Indexes[i].Type == "PRIMARY KEY" {
			return &ct.Indexes[i]
		}
	}
	return nil
}

// diffPartitionOptions compares partition options and returns ALTER clauses for differences.
// The first return value contains clauses for the primary ALTER statement.
// The second return value contains clause sets for additional ALTER statements needed
// when a change cannot be expressed in a single statement (e.g. changing partition type
// requires REMOVE PARTITIONING followed by a separate PARTITION BY).
func (ct *CreateTable) diffPartitionOptions(target *CreateTable) ([]string, [][]string) {
	sourcePartition := ct.Partition
	targetPartition := target.Partition

	// Case 1: No partitioning in either table - no changes
	if sourcePartition == nil && targetPartition == nil {
		return nil, nil
	}

	// Case 2: Remove partitioning (source has partitioning, target doesn't)
	if sourcePartition != nil && targetPartition == nil {
		return []string{"REMOVE PARTITIONING"}, nil
	}

	// Case 3: Add partitioning (source doesn't have partitioning, target does)
	if sourcePartition == nil && targetPartition != nil {
		return []string{formatPartitionOptions(targetPartition)}, nil
	}

	// Case 4: Both have partitioning - check if they're different
	if !partitionOptionsEqual(sourcePartition, targetPartition) {
		// Special case: For HASH/KEY partitions where only the partition count changed
		// (no explicit definitions), we can use ADD PARTITION or COALESCE PARTITION
		if isCountOnly, countDiff := isPartitionCountOnlyChange(sourcePartition, targetPartition); isCountOnly {
			if countDiff > 0 {
				return []string{fmt.Sprintf("ADD PARTITION PARTITIONS %d", countDiff)}, nil
			}
			return []string{fmt.Sprintf("COALESCE PARTITION %d", -countDiff)}, nil
		}

		// For all other partition changes (e.g. changing partition type from HASH to RANGE),
		// MySQL requires two separate ALTER TABLE statements:
		// 1. REMOVE PARTITIONING
		// 2. PARTITION BY ...
		// The first goes into the primary statement, the second is returned as an additional statement.
		return []string{"REMOVE PARTITIONING"}, [][]string{{formatPartitionOptions(targetPartition)}}
	}

	return nil, nil
}

// isPartitionCountOnlyChange checks if only the partition count changed for HASH/KEY partitions
// Returns true if this is a count-only change, along with the difference in count
func isPartitionCountOnlyChange(source, target *PartitionOptions) (bool, int) {
	// Must be same partition type
	if source.Type != target.Type {
		return false, 0
	}

	// Only applies to HASH and KEY partitions
	if source.Type != "HASH" && source.Type != "KEY" {
		return false, 0
	}

	// Must have same expression/columns
	if !ptrEqual(source.Expression, target.Expression) {
		return false, 0
	}
	if !slices.Equal(source.Columns, target.Columns) {
		return false, 0
	}

	// Must have same linear flag
	if source.Linear != target.Linear {
		return false, 0
	}

	// Both must not have explicit definitions (just partition count)
	if len(source.Definitions) > 0 || len(target.Definitions) > 0 {
		return false, 0
	}

	// Only the partition count differs
	if source.Partitions == target.Partitions {
		return false, 0
	}

	return true, int(target.Partitions) - int(source.Partitions)
}

// partitionOptionsEqual checks if two partition options are equal
func partitionOptionsEqual(a, b *PartitionOptions) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Compare partition type
	if a.Type != b.Type {
		return false
	}

	// Compare expression (for HASH and RANGE)
	if !ptrEqual(a.Expression, b.Expression) {
		return false
	}

	// Compare columns (for KEY, RANGE COLUMNS, LIST COLUMNS)
	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}

	// Compare linear flag
	if a.Linear != b.Linear {
		return false
	}

	// Compare number of partitions (for HASH/KEY without explicit definitions)
	if a.Partitions != b.Partitions {
		return false
	}

	// Compare partition definitions
	if len(a.Definitions) != len(b.Definitions) {
		return false
	}

	for i := range a.Definitions {
		if !partitionDefinitionEqual(&a.Definitions[i], &b.Definitions[i]) {
			return false
		}
	}

	// Compare subpartitioning
	if !subPartitionOptionsEqual(a.SubPartition, b.SubPartition) {
		return false
	}

	return true
}

// partitionDefinitionEqual checks if two partition definitions are equal
func partitionDefinitionEqual(a, b *PartitionDefinition) bool {
	if a.Name != b.Name {
		return false
	}

	// Compare partition values
	if !partitionValuesEqual(a.Values, b.Values) {
		return false
	}

	// Compare comment
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}

	// Compare engine
	if !ptrEqual(a.Engine, b.Engine) {
		return false
	}

	return true
}

// partitionValuesEqual checks if two partition values are equal
func partitionValuesEqual(a, b *PartitionValues) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if a.Type != b.Type {
		return false
	}

	if len(a.Values) != len(b.Values) {
		return false
	}

	// Today both sides come from the same parsePartitionClause path and
	// are always Go strings, so reflect.DeepEqual and the old
	// fmt.Sprintf("%v") string compare are equivalent. The change is
	// forward-compatibility: if parsePartitionClause ever preserves the
	// AST literal kind (so e.g. an int literal stays an int rather than
	// being Restored to its string form), DeepEqual will distinguish
	// "5" from 5 where %v would collapse them. No behaviour change today.
	for i := range a.Values {
		if !reflect.DeepEqual(a.Values[i], b.Values[i]) {
			return false
		}
	}

	return true
}

// subPartitionOptionsEqual checks if two subpartition options are equal
func subPartitionOptionsEqual(a, b *SubPartitionOptions) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if a.Type != b.Type {
		return false
	}

	if !ptrEqual(a.Expression, b.Expression) {
		return false
	}

	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}

	if a.Linear != b.Linear {
		return false
	}

	if a.Count != b.Count {
		return false
	}

	return true
}

// formatPartitionOptions formats partition options for ALTER TABLE
func formatPartitionOptions(partOpts *PartitionOptions) string {
	var parts []string

	// Start with PARTITION BY
	parts = append(parts, "PARTITION BY")

	// Add LINEAR keyword if applicable
	if partOpts.Linear {
		parts = append(parts, "LINEAR")
	}

	// Add partition type
	parts = append(parts, partOpts.Type)

	// Add expression or columns based on partition type
	switch partOpts.Type {
	case "HASH":
		if partOpts.Expression != nil {
			parts = append(parts, fmt.Sprintf("(%s)", *partOpts.Expression))
		} else if len(partOpts.Columns) > 0 {
			// HASH can also use column names directly
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		}
	case "KEY":
		if len(partOpts.Columns) > 0 {
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		} else {
			// KEY() with empty columns uses primary key
			parts = append(parts, "()")
		}
	case "RANGE":
		if partOpts.Expression != nil {
			parts = append(parts, fmt.Sprintf("(%s)", *partOpts.Expression))
		} else if len(partOpts.Columns) > 0 {
			// RANGE COLUMNS
			parts[len(parts)-1] = "RANGE COLUMNS"
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		}
	case "LIST":
		if len(partOpts.Columns) > 0 {
			// LIST COLUMNS
			parts[len(parts)-1] = "LIST COLUMNS"
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		}
	}

	// Add number of partitions if specified and no explicit definitions
	if partOpts.Partitions > 0 && len(partOpts.Definitions) == 0 {
		parts = append(parts, fmt.Sprintf("PARTITIONS %d", partOpts.Partitions))
	}

	// Add partition definitions if present
	if len(partOpts.Definitions) > 0 {
		var defParts []string
		for _, def := range partOpts.Definitions {
			defParts = append(defParts, formatPartitionDefinition(&def))
		}
		parts = append(parts, fmt.Sprintf("(%s)", strings.Join(defParts, ", ")))
	}

	return strings.Join(parts, " ")
}

// formatPartitionDefinition formats a single partition definition
func formatPartitionDefinition(def *PartitionDefinition) string {
	var parts []string

	// Partition name
	parts = append(parts, "PARTITION "+sqlescape.EscapeIdentifier(def.Name))

	// Values clause
	if def.Values != nil {
		switch def.Values.Type {
		case "LESS_THAN":
			values := make([]string, len(def.Values.Values))
			for i, v := range def.Values.Values {
				values[i] = formatPartitionValue(v)
			}
			parts = append(parts, fmt.Sprintf("VALUES LESS THAN (%s)", strings.Join(values, ", ")))
		case "IN":
			values := make([]string, len(def.Values.Values))
			for i, v := range def.Values.Values {
				values[i] = formatPartitionValue(v)
			}
			parts = append(parts, fmt.Sprintf("VALUES IN (%s)", strings.Join(values, ", ")))
		case "MAXVALUE":
			parts = append(parts, "VALUES LESS THAN MAXVALUE")
		}
	}

	return strings.Join(parts, " ")
}
