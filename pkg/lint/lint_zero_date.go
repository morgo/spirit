package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type ZeroDateLinter struct{}

func init() {
	Register(&ZeroDateLinter{})
}

func (l *ZeroDateLinter) Name() string {
	return "zero_date"
}

func (l *ZeroDateLinter) Description() string {
	return "Check for columns with zero-date default values"
}

func (l *ZeroDateLinter) String() string {
	return Stringer(l)
}

func (l *ZeroDateLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for ct := range CreateTableStatements(existingTables, changes) {
		for _, column := range ct.Columns {
			tp := column.Raw.Tp.GetType()
			if tp == mysql.TypeDatetime || tp == mysql.TypeTimestamp || tp == mysql.TypeDate {
				if column.Nullable && column.Default != nil && strings.EqualFold(*column.Default, "NULL") {
					// If the column is nullable and the default value is NULL, we're good
					continue
				}
				if !column.Nullable && column.Default == nil {
					// If the column is *not* nullable and has no default, MySQL will try to assign a zero date default value
					// *unless* strict mode and NO_ZERO_DATE are enabled
					violations = append(violations, Violation{
						Linter: l,
						Location: &Location{
							Table:  ct.TableName,
							Column: &column.Name,
						},
						Severity: SeverityWarning,
						Message:  fmt.Sprintf("date column %s is not nullable and has no default", column.Name),
					})
				}
				if column.Default != nil {
					if *column.Default == l.getZeroDate(tp) {
						violations = append(violations, Violation{
							Linter: l,
							Location: &Location{
								Table:  ct.TableName,
								Column: &column.Name,
							},
							Severity: SeverityWarning,
							Message:  fmt.Sprintf("column %s with type %q has a zero default value", column.Name, column.Type),
						})
					}
				}
			}
		}
	}
	return violations
}

func (l *ZeroDateLinter) getZeroDate(tp byte) string {
	switch tp {
	case mysql.TypeDate:
		return "0000-00-00"
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return "0000-00-00 00:00:00"
	default:
		return ""
	}
}
