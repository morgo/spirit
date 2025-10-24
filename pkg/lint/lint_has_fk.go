package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

func (l *HasFKLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for _, change := range changes {
		if change.IsCreateTable() {
			createTable, err := change.ParseCreateTable()
			if err != nil {
				violations = append(violations, Violation{
					Linter:   l,
					Message:  fmt.Sprintf("Failed to parse CREATE TABLE statement for %q for foreign key linting", change.Table),
					Severity: SeverityError,
				})
				continue
			}
			for _, constraint := range createTable.Constraints {
				if constraint.Raw.Tp == ast.ConstraintForeignKey {
					violations = append(violations, Violation{
						Linter: l,
						Location: &Location{
							Table:      createTable.TableName,
							Constraint: &constraint.Name,
						},
						Message:  fmt.Sprintf("Table %q has FOREIGN KEY constraint %q", createTable.TableName, constraint.Name),
						Severity: SeverityWarning,
					})
				}
			}
		}
	}
	return violations
}
