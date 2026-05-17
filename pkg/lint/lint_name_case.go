package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

type NameCaseLinter struct{}

func init() {
	Register(&NameCaseLinter{})
}

func (*NameCaseLinter) Name() string {
	return "name_case"
}

func (l *NameCaseLinter) String() string {
	return Stringer(l)
}

func (l *NameCaseLinter) Description() string {
	return "ensure that table names are all lowercase"
}

// Lint walks the post-state of the schema so an ALTER RENAME that fixes a
// non-lowercase name does not produce a false positive, and a rename that
// introduces a non-lowercase name surfaces against the final table name.
func (l *NameCaseLinter) Lint(createTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for _, ct := range PostState(createTables, changes) {
		if ct.TableName != strings.ToLower(ct.TableName) {
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table: ct.TableName,
				},
				Message:  fmt.Sprintf("table name %q is not lowercase", ct.TableName),
				Severity: SeverityWarning,
			})
		}
	}
	return violations
}
