package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("rename", renameCheck, ScopePreflight)
}

// renameCheck validates rename operations in ALTER TABLE statements.
// Table renames are always blocked. Column renames are allowed for non-PK columns
// in non-buffered mode, but blocked for PK columns and in buffered mode.
// Additionally, it blocks dangerous patterns where a rename's old or new name
// overlaps with an added column name, which could cause data corruption.
func renameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}

	// Build set of primary key columns for quick lookup (if table info is available)
	pkColumns := make(map[string]struct{})
	if r.Table != nil {
		for _, col := range r.Table.KeyColumns {
			pkColumns[col] = struct{}{}
		}
	}

	// Collect all renames and added columns to check for dangerous overlaps.
	// renamedFrom maps old_name → new_name for all renames in this ALTER.
	renamedFrom := make(map[string]string)
	// renamedTo maps new_name → old_name (the reverse) for overlap detection.
	renamedTo := make(map[string]string)
	// addedColumns tracks column names being added in this ALTER.
	addedColumns := make(map[string]struct{})

	for _, spec := range alterStmt.Specs {
		// Table renames are never supported
		if spec.Tp == ast.AlterTableRenameTable {
			return errors.New("table renames are not supported")
		}

		if spec.Tp == ast.AlterTableRenameColumn {
			if r.Buffered {
				return errors.New("column renames are not supported in buffered mode")
			}
			if spec.OldColumnName != nil {
				if _, isPK := pkColumns[spec.OldColumnName.Name.O]; isPK {
					return fmt.Errorf("renaming primary key column %q is not supported", spec.OldColumnName.Name.O)
				}
				if spec.NewColumnName != nil && spec.OldColumnName.Name.O != spec.NewColumnName.Name.O {
					renamedFrom[spec.OldColumnName.Name.O] = spec.NewColumnName.Name.O
					renamedTo[spec.NewColumnName.Name.O] = spec.OldColumnName.Name.O
				}
			}
			continue
		}

		if spec.Tp == ast.AlterTableChangeColumn {
			if spec.OldColumnName != nil && len(spec.NewColumns) > 0 {
				oldName := spec.OldColumnName.Name.O
				newName := spec.NewColumns[0].Name.Name.O
				if oldName != newName {
					if r.Buffered {
						return errors.New("column renames are not supported in buffered mode")
					}
					if _, isPK := pkColumns[oldName]; isPK {
						return fmt.Errorf("renaming primary key column %q is not supported", oldName)
					}
					renamedFrom[oldName] = newName
					renamedTo[newName] = oldName
				}
			}
			continue
		}

		if spec.Tp == ast.AlterTableAddColumns {
			for _, col := range spec.NewColumns {
				addedColumns[col.Name.Name.O] = struct{}{}
			}
		}
	}

	// Check for dangerous overlaps between renames and added columns.
	// Pattern 1: RENAME COLUMN c1 TO n1, ADD COLUMN c1 ...
	//   The old name "c1" is reused by a new column. The intersection logic
	//   could identity-match source.c1 to the new target.c1 instead of
	//   following the rename to target.n1.
	for oldName, newName := range renamedFrom {
		if _, added := addedColumns[oldName]; added {
			return fmt.Errorf("column rename %q to %q conflicts with added column %q: "+
				"the old column name is reused, which could cause data corruption", oldName, newName, oldName)
		}
	}

	// Pattern 2: RENAME COLUMN a TO c, ADD COLUMN ... (where c already exists)
	//   or more generally, a rename target name that collides with another
	//   source column. This is caught by MySQL itself (duplicate column name),
	//   but we check it defensively.
	for newName, oldName := range renamedTo {
		if _, added := addedColumns[newName]; added {
			return fmt.Errorf("column rename %q to %q conflicts with added column %q: "+
				"the new column name collides with an added column", oldName, newName, newName)
		}
	}

	return nil // no unsupported renames
}
