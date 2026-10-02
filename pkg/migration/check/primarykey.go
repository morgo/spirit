package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
)

func init() {
	registerCheck("primarykey", primaryKeyCheck, ScopePreflight|ScopeStatement)
}

// primaryKeyCheck refuses a statement that drops the primary key. Spirit
// chunks the copy and identifies the rows it replays by the primary key, so
// the new table must keep it. The one DROP PRIMARY KEY accepted is one the
// same statement adds back on the same columns, in the same order, with no
// prefix length or expression: the key is unchanged, and MySQL applies the
// pair in the one rebuild the statement does anyway. That is the plan
// statement.CreateTable.Diff emits for a table KEY_BLOCK_SIZE change, where
// the primary key has to be re-created to take the new size.
//
// The columns are compared against the table's current key when the check
// has the table, as the runner always does. At statement scope without one
// the statement alone decides, as for the other checks that read the current
// definition; outside that scope a missing table fails the check rather than
// passing it quietly.
func primaryKeyCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	dropped := false
	var added [][]string // one column list per ADD PRIMARY KEY
	for _, spec := range alterStmt.Specs {
		switch {
		case spec.Tp == ast.AlterTableDropPrimaryKey:
			dropped = true
		case spec.Tp == ast.AlterTableAddConstraint && spec.Constraint != nil && spec.Constraint.Tp == ast.ConstraintPrimaryKey:
			added = append(added, wholeColumnKey(spec.Constraint))
		}
	}
	if !dropped {
		return nil
	}
	if len(added) != 1 || added[0] == nil {
		return errors.New("dropping primary key is not supported")
	}
	if r.Table == nil {
		if r.scope&ScopeStatement != 0 {
			logger.Debug("no table metadata supplied: accepting a DROP PRIMARY KEY that the statement adds back, without comparing the columns to the table's",
				"check", "primarykey")
			return nil
		}
		return errors.New("dropping primary key is not supported: the table's primary key is not available to confirm the statement adds it back unchanged")
	}
	if !sameColumnList(added[0], r.Table.KeyColumns) {
		return fmt.Errorf("dropping primary key is not supported: the primary key added back is on (%s), the table's is on (%s)",
			strings.Join(added[0], ", "), strings.Join(r.Table.KeyColumns, ", "))
	}
	return nil
}

// wholeColumnKey returns the column names of a key made of whole columns, or
// nil when a part has a prefix length or is an expression, which the table's
// key columns do not record and so cannot be confirmed unchanged.
func wholeColumnKey(c *ast.Constraint) []string {
	cols := make([]string, 0, len(c.Keys))
	for _, k := range c.Keys {
		if k.Column == nil || k.Expr != nil || k.Length > 0 {
			return nil
		}
		cols = append(cols, k.Column.Name.O)
	}
	return cols
}

// sameColumnList reports whether two column lists name the same columns in
// the same order. Column names compare case-insensitively, as in MySQL.
func sameColumnList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}
