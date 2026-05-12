package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type HasFloatLinter struct{}

func init() {
	Register(&HasFloatLinter{})
}

func (l *HasFloatLinter) String() string {
	return Stringer(l)
}

func (l *HasFloatLinter) Name() string {
	return "has_float"
}

func (l *HasFloatLinter) Description() string {
	return "Detects usage of FLOAT or DOUBLE data types in table definitions"
}

// Lint operates on a post-state view of the schema, so a column being
// converted away from FLOAT/DOUBLE in an ALTER doesn't generate a false
// positive. The violation message distinguishes pre-existing columns from
// columns added/modified by the changeset to preserve actionability.
func (l *HasFloatLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	addedOrModified := columnsAddedOrModifiedInChanges(changes)
	createdInChanges := newTablesInChanges(changes)

	for _, ct := range PostState(existingTables, changes) {
		tKey := strings.ToLower(ct.TableName)
		for _, col := range ct.Columns {
			if col.Raw == nil || col.Raw.Tp == nil {
				continue
			}
			tp := col.Raw.Tp.GetType()
			if tp != mysql.TypeFloat && tp != mysql.TypeDouble {
				continue
			}
			colName := col.Name
			colKey := strings.ToLower(colName)
			modified := !createdInChanges[tKey] && addedOrModified[tKey][colKey]
			var message string
			switch {
			case modified:
				message = fmt.Sprintf("Column %q in table %q modified to use floating-point data type", colName, ct.TableName)
			default:
				message = fmt.Sprintf("Column %q in table %q uses %s data type", colName, ct.TableName, col.Raw.Tp.String())
			}
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table:  ct.TableName,
					Column: &colName,
				},
				Message:  message,
				Severity: SeverityWarning,
			})
		}
	}
	return violations
}
