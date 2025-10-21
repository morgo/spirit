package lint

import (
	"errors"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func init() {
	Register(&InvisibleIndexBeforeDropLinter{})
}

// InvisibleIndexBeforeDropLinter checks that indexes are made invisible before dropping.
// This is a safety practice to ensure the index is not needed before permanently removing it.
type InvisibleIndexBeforeDropLinter struct {
	raiseError bool
}

func (l *InvisibleIndexBeforeDropLinter) String() string {
	return Stringer(l)
}

func (l *InvisibleIndexBeforeDropLinter) Name() string {
	return "invisible_index_before_drop"
}

func (l *InvisibleIndexBeforeDropLinter) Description() string {
	return "Requires indexes to be made invisible before dropping them as a safety measure"
}

func (l *InvisibleIndexBeforeDropLinter) Configure(a any) error {
	c, ok := a.(map[string]string)
	if !ok {
		return errors.New(l.Name() + " config must be a map[string]string")
	}
	for k, v := range c {
		switch k {
		case "raiseError":
			if strings.EqualFold(v, "true") {
				l.raiseError = true
				break
			}
			if strings.EqualFold(v, "false") {
				l.raiseError = false
				break
			}
			return fmt.Errorf("invalid value for %s: %s", k, v)
		default:
			return fmt.Errorf("unknown config key for %s: %s", l.Name(), k)
		}
	}
	return nil
}
func (l *InvisibleIndexBeforeDropLinter) DefaultConfig() any {
	return map[string]string{
		"raiseError": "true",
	}
}

var _ ConfigurableLinter = &InvisibleIndexBeforeDropLinter{}

func (l *InvisibleIndexBeforeDropLinter) Lint(createTables []*statement.CreateTable, statements []*statement.AbstractStatement) []Violation {
	severity := SeverityWarning
	if l.raiseError {
		severity = SeverityError
	}
	var violations []Violation

	for _, stmt := range statements {
		// Only check ALTER TABLE statements
		if !stmt.IsAlterTable() {
			continue
		}

		alterStmt, ok := (*stmt.StmtNode).(*ast.AlterTableStmt)
		if !ok {
			continue
		}

		tableName := stmt.Table

		// Check each ALTER specification
		for _, spec := range alterStmt.Specs {
			if spec.Tp != ast.AlterTableDropIndex {
				continue
			}

			indexName := spec.Name

			madeInvisible := false
			// If not made invisible in this ALTER, check if it's invisible in the CREATE TABLE
			if len(createTables) > 0 {
				for _, ct := range createTables {
					if ct.GetTableName() == tableName {
						for _, idx := range ct.GetIndexes() {
							if idx.Name == indexName && idx.Invisible != nil && *idx.Invisible {
								madeInvisible = true
								break
							}
						}
					}
				}
			}

			if !madeInvisible {
				suggestion := fmt.Sprintf("First make the index invisible: ALTER TABLE %s ALTER INDEX %s INVISIBLE", tableName, indexName)
				violations = append(violations, Violation{
					Linter:   l,
					Severity: severity,
					Message:  fmt.Sprintf("Index '%s' should be made invisible before dropping to ensure it's not needed", indexName),
					Location: &Location{
						Table: tableName,
						Index: &indexName,
					},
					Suggestion: &suggestion,
				})
			}
		}
	}

	return violations
}
