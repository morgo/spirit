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
	// Existing tables with TIMESTAMP get Warning — don't boil the ocean on legacy schemas.
	for _, ct := range existingTables {
		for _, col := range ct.Columns {
			if col.Raw.Tp.GetType() == mysql.TypeTimestamp {
				violations = append(violations, Violation{
					Linter: l,
					Location: &Location{
						Table:  ct.TableName,
						Column: &col.Name,
					},
					Message:  fmt.Sprintf("Column %q uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.", col.Name),
					Severity: SeverityWarning,
				})
			}
		}
	}

	// CREATE TABLE statements in changes get Error — don't introduce new TIMESTAMP.
	for _, change := range changes {
		if change.IsCreateTable() {
			ct, err := change.ParseCreateTable()
			if err != nil {
				continue
			}
			for _, col := range ct.Columns {
				if col.Raw.Tp.GetType() == mysql.TypeTimestamp {
					violations = append(violations, Violation{
						Linter: l,
						Location: &Location{
							Table:  ct.TableName,
							Column: &col.Name,
						},
						Message:  fmt.Sprintf("Column %q uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.", col.Name),
						Severity: SeverityError,
					})
				}
			}
		}
	}

	// Check ALTER TABLE statements
	violations = append(violations, l.checkAlterStatements(existingTables, changes)...)

	return violations
}

// checkAlterStatements checks ALTER TABLE statements for TIMESTAMP usage.
// Adding or modifying a column to TIMESTAMP is an Error. Modifying a table that
// already has TIMESTAMP columns (but not adding new ones) is a Warning — unless
// the ALTER is actively removing/converting those TIMESTAMP columns.
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

		// Track whether the ALTER is adding/modifying columns to TIMESTAMP (Error),
		// and collect the set of columns being dropped or converted away from TIMESTAMP.
		addingTimestamp := false
		columnsBeingFixed := make(map[string]bool)

		for _, spec := range alter.Specs {
			switch spec.Tp { //nolint:exhaustive
			case ast.AlterTableAddColumns:
				for _, col := range spec.NewColumns {
					if col.Tp.GetType() == mysql.TypeTimestamp {
						addingTimestamp = true
						violations = append(violations, Violation{
							Linter: l,
							Location: &Location{
								Table:  change.Table,
								Column: &col.Name.Name.O,
							},
							Message:  fmt.Sprintf("Column %q uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.", col.Name.Name.O),
							Severity: SeverityError,
						})
					}
				}
			case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				for _, col := range spec.NewColumns {
					if col.Tp.GetType() == mysql.TypeTimestamp {
						addingTimestamp = true
						violations = append(violations, Violation{
							Linter: l,
							Location: &Location{
								Table:  change.Table,
								Column: &col.Name.Name.O,
							},
							Message:  fmt.Sprintf("Column %q uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.", col.Name.Name.O),
							Severity: SeverityError,
						})
					} else {
						// Column is being changed to a non-TIMESTAMP type — it's being fixed.
						// For CHANGE COLUMN, OldColumnName is the original name.
						if spec.OldColumnName != nil {
							columnsBeingFixed[spec.OldColumnName.Name.O] = true
						}
						columnsBeingFixed[col.Name.Name.O] = true
					}
				}
			case ast.AlterTableDropColumn:
				if spec.OldColumnName != nil {
					columnsBeingFixed[spec.OldColumnName.Name.O] = true
				}
			}
		}

		// If the ALTER is not itself introducing TIMESTAMP columns, check whether
		// the existing table already has TIMESTAMP columns (Warning).
		// Exclude columns that are being dropped or converted in this ALTER.
		if !addingTimestamp {
			if existing, ok := existingTableMap[change.Table]; ok {
				for _, col := range existing.Columns {
					if col.Raw.Tp.GetType() == mysql.TypeTimestamp && !columnsBeingFixed[col.Name] {
						violations = append(violations, Violation{
							Linter: l,
							Location: &Location{
								Table:  change.Table,
								Column: &col.Name,
							},
							Message:  fmt.Sprintf("Column %q uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.", col.Name),
							Severity: SeverityWarning,
						})
					}
				}
			}
		}
	}

	return violations
}
