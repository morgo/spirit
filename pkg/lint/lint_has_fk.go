package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
)

type HasFKLinter struct{}

func init() {
	Register(&HasFKLinter{})
}

func (l *HasFKLinter) String() string {
	return Stringer(l)
}

func (l *HasFKLinter) Name() string {
	return "has_foreign_key"
}

func (l *HasFKLinter) Description() string {
	return "Detects usage of FOREIGN KEY constraints in table definitions"
}

// Lint walks the post-state of the schema, so an ALTER DROP FOREIGN KEY that
// fixes a legacy FK does not produce a false positive, and an ALTER ADD
// FOREIGN KEY surfaces the violation against the table's final shape.
func (l *HasFKLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for _, ct := range PostState(existingTables, changes) {
		for _, constraint := range ct.Constraints {
			if constraint.Type != "FOREIGN KEY" {
				continue
			}
			name := constraint.Name
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table:      ct.TableName,
					Constraint: &name,
				},
				Message:  fmt.Sprintf("Table %q has FOREIGN KEY constraint %q", ct.TableName, constraint.Name),
				Severity: SeverityWarning,
			})
		}
	}
	return violations
}
