package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type HasTimestampLinter struct{}

func init() {
	Register(&HasTimestampLinter{})
}

func (l *HasTimestampLinter) String() string {
	return Stringer(l)
}

func (l *HasTimestampLinter) Name() string {
	return "has_timestamp"
}

func (l *HasTimestampLinter) Description() string {
	return "Detects usage of TIMESTAMP data type which overflows on 2038-01-19"
}

func (l *HasTimestampLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// Check all CREATE TABLE statements (from both existingTables and changes).
	// CREATE TABLE with TIMESTAMP is an error — don't introduce new ones.
	for ct := range CreateTableStatements(existingTables, changes) {
		violations = append(violations, l.checkCreateTable(ct)...)
	}

	// Check ALTER TABLE statements
	violations = append(violations, l.checkAlterStatements(existingTables, changes)...)

	return violations
}

// checkCreateTable checks a CREATE TABLE for TIMESTAMP columns and returns Error-level violations.
func (l *HasTimestampLinter) checkCreateTable(table *statement.CreateTable) (violations []Violation) {
	for _, col := range table.Columns {
		if col.Raw.Tp.GetType() == mysql.TypeTimestamp {
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table:  table.TableName,
					Column: &col.Name,
				},
				Message:  fmt.Sprintf("Column %q uses TIMESTAMP which overflows on 2038-01-19. Please use DATETIME instead.", col.Name),
				Severity: SeverityError,
			})
		}
	}
	return violations
}

// checkAlterStatements checks ALTER TABLE statements for TIMESTAMP usage.
// Adding a TIMESTAMP column is an Error. Modifying a table that already has
// TIMESTAMP columns (but not adding new ones) is a Warning.
func (l *HasTimestampLinter) checkAlterStatements(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// Build a map of existing tables for quick lookup
	existingTableMap := make(map[string]*statement.CreateTable)
	for _, table := range existingTables {
		existingTableMap[table.GetTableName()] = table
	}

	for _, change := range changes {
		alter, ok := change.AsAlterTable()
		if !ok {
			continue
		}

		// Check if the ALTER itself adds/modifies columns to TIMESTAMP (Error)
		addingTimestamp := false
		for _, spec := range alter.Specs {
			var message string
			switch spec.Tp { //nolint:exhaustive
			case ast.AlterTableAddColumns:
				message = "Column %q uses TIMESTAMP which overflows on 2038-01-19. Please use DATETIME instead."
			case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				message = "Column %q uses TIMESTAMP which overflows on 2038-01-19. Please use DATETIME instead."
			default:
				continue
			}

			for _, col := range spec.NewColumns {
				if col.Tp.GetType() == mysql.TypeTimestamp {
					addingTimestamp = true
					violations = append(violations, Violation{
						Linter: l,
						Location: &Location{
							Table:  change.Table,
							Column: &col.Name.Name.O,
						},
						Message:  fmt.Sprintf(message, col.Name.Name.O),
						Severity: SeverityError,
					})
				}
			}
		}

		// If the ALTER is not itself introducing TIMESTAMP columns, check whether
		// the existing table already has TIMESTAMP columns (Warning).
		if !addingTimestamp {
			if existing, ok := existingTableMap[change.Table]; ok {
				for _, col := range existing.Columns {
					if col.Raw.Tp.GetType() == mysql.TypeTimestamp {
						violations = append(violations, Violation{
							Linter: l,
							Location: &Location{
								Table:  change.Table,
								Column: &col.Name,
							},
							Message:  fmt.Sprintf("Column %q uses TIMESTAMP which overflows on 2038-01-19. Please use DATETIME instead.", col.Name),
							Severity: SeverityWarning,
						})
					}
				}
			}
		}
	}

	return violations
}
