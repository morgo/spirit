package table

import (
	"strings"
)

// ColumnMapping represents the column relationship between a source and target table,
// including any column renames. It is computed once and shared across chunker,
// copier, applier, checksum, and repl subscription.
//
// A nil *ColumnMapping is safe to use — all methods return sensible defaults
// (empty columns, nil renames).
type ColumnMapping struct {
	sourceTable *TableInfo
	targetTable *TableInfo
	renames     map[string]string // old→new, may be nil

	// Pre-computed intersection results
	sourceColumns []string // non-generated source columns that exist in target
	targetColumns []string // corresponding target column names (renamed where applicable)
}

// NewColumnMapping creates a ColumnMapping between source and target tables,
// with an optional rename map (old→new). The column intersection is computed
// immediately. If target is nil, source is used as the target.
func NewColumnMapping(source, target *TableInfo, renames map[string]string) *ColumnMapping {
	if target == nil {
		target = source
	}
	m := &ColumnMapping{
		sourceTable: source,
		targetTable: target,
		renames:     renames,
	}
	m.sourceColumns, m.targetColumns = m.computeIntersection()
	return m
}

// computeIntersection calculates the column intersection between source and target.
func (m *ColumnMapping) computeIntersection() ([]string, []string) {
	// Build a set of target column names for fast lookup
	t2Set := make(map[string]struct{}, len(m.targetTable.NonGeneratedColumns))
	for _, col := range m.targetTable.NonGeneratedColumns {
		t2Set[col] = struct{}{}
	}

	// Build a set of target columns that are "claimed" by renames.
	// These cannot be used for identity matching by other source columns.
	claimedTargets := make(map[string]struct{}, len(m.renames))
	for _, newName := range m.renames {
		claimedTargets[newName] = struct{}{}
	}

	var srcCols, tgtCols []string
	for _, srcCol := range m.sourceTable.NonGeneratedColumns {
		// Check if this column was renamed
		if newName, ok := m.renames[srcCol]; ok {
			if _, exists := t2Set[newName]; exists {
				srcCols = append(srcCols, srcCol)
				tgtCols = append(tgtCols, newName)
				continue
			}
		}
		// Identity match (same name in both tables), but only if the target
		// column is not already claimed by a rename from another source column.
		if _, exists := t2Set[srcCol]; exists {
			if _, claimed := claimedTargets[srcCol]; !claimed {
				srcCols = append(srcCols, srcCol)
				tgtCols = append(tgtCols, srcCol)
			}
		}
	}
	return srcCols, tgtCols
}

// Columns returns two comma-separated, backtick-quoted column lists
// for source and target. When there are no renames, both strings are identical.
func (m *ColumnMapping) Columns() (source, target string) {
	srcQuoted := make([]string, len(m.sourceColumns))
	tgtQuoted := make([]string, len(m.targetColumns))
	for i := range m.sourceColumns {
		srcQuoted[i] = "`" + m.sourceColumns[i] + "`"
		tgtQuoted[i] = "`" + m.targetColumns[i] + "`"
	}
	return strings.Join(srcQuoted, ", "), strings.Join(tgtQuoted, ", ")
}

// ColumnsSlice returns parallel slices of source and target column names.
// sourceColumns[i] corresponds to targetColumns[i].
func (m *ColumnMapping) ColumnsSlice() (sourceColumns, targetColumns []string) {
	return m.sourceColumns, m.targetColumns
}

// ChecksumExprs returns two comma-separated checksum column expressions for
// source and target, wrapping each column in IFNULL(), ISNULL() and CAST.
// The CAST type always comes from the target table's type definition.
// When there are no renames, both expressions are identical.
func (m *ColumnMapping) ChecksumExprs() (source, target string, err error) {
	sourceExprs := make([]string, len(m.sourceColumns))
	targetExprs := make([]string, len(m.targetColumns))
	for i := range m.sourceColumns {
		// CAST type comes from the target table for both source and target
		// so that type conversions (e.g. INT→BIGINT) are applied consistently.
		// For source: SQL references the old column name, type from target's new column name.
		// For target: both SQL reference and type lookup use the new column name.
		srcCast, err := m.targetTable.wrapCastTypeAs(m.sourceColumns[i], m.targetColumns[i])
		if err != nil {
			return "", "", err
		}
		tgtCast, err := m.targetTable.wrapCastType(m.targetColumns[i])
		if err != nil {
			return "", "", err
		}
		sourceExprs[i] = "IFNULL(" + srcCast + ",''), ISNULL(`" + m.sourceColumns[i] + "`)"
		targetExprs[i] = "IFNULL(" + tgtCast + ",''), ISNULL(`" + m.targetColumns[i] + "`)"
	}
	return strings.Join(sourceExprs, ", "), strings.Join(targetExprs, ", "), nil
}

// SourceColumnIndices returns the indices into sourceTable.NonGeneratedColumns
// for each intersected column. This is used when row data only contains
// non-generated columns (e.g., from SELECT statements).
func (m *ColumnMapping) SourceColumnIndices() []int {
	indexMap := make(map[string]int, len(m.sourceTable.NonGeneratedColumns))
	for i, col := range m.sourceTable.NonGeneratedColumns {
		indexMap[col] = i
	}
	indices := make([]int, len(m.sourceColumns))
	for i, col := range m.sourceColumns {
		indices[i] = indexMap[col]
	}
	return indices
}

// SourceOrdinalIndices returns the indices into sourceTable.Columns (all columns,
// including generated) for each intersected column. This is needed when working
// with binlog row images, which contain ALL columns including generated ones.
func (m *ColumnMapping) SourceOrdinalIndices() []int {
	indexMap := make(map[string]int, len(m.sourceTable.Columns))
	for i, col := range m.sourceTable.Columns {
		indexMap[col] = i
	}
	indices := make([]int, len(m.sourceColumns))
	for i, col := range m.sourceColumns {
		indices[i] = indexMap[col]
	}
	return indices
}

// Renames returns the column rename mapping (old→new), or nil if there are none.
func (m *ColumnMapping) Renames() map[string]string {
	return m.renames
}

// TargetTable returns the target table.
func (m *ColumnMapping) TargetTable() *TableInfo {
	return m.targetTable
}

// SourceTable returns the source table.
func (m *ColumnMapping) SourceTable() *TableInfo {
	return m.sourceTable
}
