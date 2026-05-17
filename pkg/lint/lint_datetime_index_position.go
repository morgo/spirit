package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type DatetimeIndexPositionLinter struct{}

func init() {
	Register(&DatetimeIndexPositionLinter{})
}

func (l *DatetimeIndexPositionLinter) String() string {
	return Stringer(l)
}

func (l *DatetimeIndexPositionLinter) Name() string {
	return "datetime_index_position"
}

func (l *DatetimeIndexPositionLinter) Description() string {
	return "Detects composite indexes where a DATETIME, TIMESTAMP, or DATE column is not the last column"
}

// Lint operates on a post-state view of the schema. For each composite index
// (>=2 columns) on each table, it flags any DATETIME, TIMESTAMP, or DATE column
// that appears in a non-last position.
//
// Rationale: date/time columns are overwhelmingly queried with range predicates
// (>, >=, <, <=, BETWEEN). Once the MySQL optimizer hits a range predicate on a
// column inside a composite index, the columns that follow can no longer be
// used for sorted index access — they're only available for index condition
// pushdown filtering. So a composite index that places a date/time column
// anywhere but last is usually carrying dead weight in its trailing columns.
//
// This is a heuristic with no visibility into the actual query workload, so it
// always emits SeverityWarning. FULLTEXT and SPATIAL indexes are skipped — they
// don't behave like B-tree indexes for range scans.
func (l *DatetimeIndexPositionLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for _, ct := range PostState(existingTables, changes) {
		colTypes := datetimeColumnTypes(ct)
		if len(colTypes) == 0 {
			continue
		}
		for _, idx := range ct.GetIndexes() {
			if !indexUsesBTreeSemantics(idx) {
				continue
			}
			if len(idx.Columns) < 2 {
				continue
			}
			for pos, colName := range idx.Columns[:len(idx.Columns)-1] {
				typeStr, ok := colTypes[strings.ToLower(colName)]
				if !ok {
					continue
				}
				violations = append(violations, l.violation(ct.TableName, idx, colName, typeStr, pos))
			}
		}
	}
	return violations
}

// datetimeColumnTypes returns a map from lowercased column name to the
// canonical type label (DATETIME / TIMESTAMP / DATE) for every column of one of
// those types in the table.
func datetimeColumnTypes(ct *statement.CreateTable) map[string]string {
	out := make(map[string]string)
	for _, col := range ct.Columns {
		if col.Raw == nil || col.Raw.Tp == nil {
			continue
		}
		switch col.Raw.Tp.GetType() {
		case mysql.TypeDatetime:
			out[strings.ToLower(col.Name)] = "DATETIME"
		case mysql.TypeTimestamp:
			out[strings.ToLower(col.Name)] = "TIMESTAMP"
		case mysql.TypeDate:
			out[strings.ToLower(col.Name)] = "DATE"
		}
	}
	return out
}

// indexUsesBTreeSemantics reports whether the optimizer treats the index as a
// B-tree (ordered) index where range-cuts-off-following-columns applies.
// FULLTEXT and SPATIAL indexes don't.
func indexUsesBTreeSemantics(idx statement.Index) bool {
	return idx.Type != "FULLTEXT" && idx.Type != "SPATIAL"
}

func (l *DatetimeIndexPositionLinter) violation(tableName string, idx statement.Index, colName, typeStr string, pos int) Violation {
	indexName := idx.Name
	colCopy := colName
	suggestion := fmt.Sprintf(
		"Consider moving %q to the last position in index %q. This is a heuristic — "+
			"the current order may be intentional if %q is only queried with equality "+
			"predicates, or if the trailing columns exist to make this a covering index.",
		colName, indexName, colName,
	)
	return Violation{
		Linter:   l,
		Severity: SeverityWarning,
		Message: fmt.Sprintf(
			"Index %q has %s column %q in position %d of %d. %s columns are typically "+
				"queried with range predicates (>, >=, <, <=, BETWEEN), and a range on a "+
				"non-last index column prevents the optimizer from using subsequent columns "+
				"for sorted access.",
			indexName, typeStr, colName, pos+1, len(idx.Columns), typeStr,
		),
		Location: &Location{
			Table:  tableName,
			Column: &colCopy,
			Index:  &indexName,
		},
		Suggestion: &suggestion,
		Context: map[string]any{
			"index_name":    indexName,
			"column_name":   colName,
			"column_type":   typeStr,
			"position":      pos + 1,
			"column_count":  len(idx.Columns),
			"index_columns": idx.Columns,
		},
	}
}
