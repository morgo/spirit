package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
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

func (l *HasFloatLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation
	for _, table := range existingTables {
		for _, col := range table.Columns {
			if strings.EqualFold(col.Type, "FLOAT") || strings.EqualFold(col.Type, "DOUBLE") {
				violations = append(violations, Violation{
					Linter: l,
					Location: &Location{
						Table:  table.TableName,
						Column: &col.Name,
					},
					Message:  fmt.Sprintf("Column %q in table %q uses %s data type", col.Name, table.TableName, col.Type),
					Severity: SeverityWarning,
				})
			}
		}
	}
	return violations
}
