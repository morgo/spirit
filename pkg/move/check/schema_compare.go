package check

import (
	"context"
	"database/sql"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/statement"
)

// showCreateTable returns the SHOW CREATE TABLE statement for schema.table.
func showCreateTable(ctx context.Context, db *sql.DB, schema, table string) (string, error) {
	// Build the query with sqlescape's %n identifier verb so schema/table names
	// containing backticks (or other identifier characters) are quoted safely,
	// consistent with the rest of the codebase's identifier handling.
	query, err := sqlescape.EscapeSQL("SHOW CREATE TABLE %n.%n", schema, table)
	if err != nil {
		return "", err
	}
	var name, createStmt string
	row := db.QueryRowContext(ctx, query)
	if err := row.Scan(&name, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

// schemaDiff compares two CREATE TABLE statements and returns a runnable
// ALTER TABLE statement describing how they differ, or an empty string if they
// are equivalent. See statement.DiffCreateTables for the canonicalization
// rules; move adds one relaxation of its own:
//   - the column-level AUTO_INCREMENT attribute is ignored: an unsharded source
//     legitimately differs from a sharded target that drops AUTO_INCREMENT in
//     favor of a Vitess sequence; the difference does not affect copy
//     correctness, so it must not block a move into a pre-created target.
//
// "want" is treated as the source-of-truth (e.g. sources[0] or the move source);
// "got" is the schema being validated (another source, or a pre-created target).
func schemaDiff(table, wantCreate, gotCreate string) (string, error) {
	diffOpts := statement.NewDiffOptions()
	diffOpts.IgnoreColumnAutoIncrement = true
	return statement.DiffCreateTables(table, wantCreate, gotCreate, diffOpts)
}
