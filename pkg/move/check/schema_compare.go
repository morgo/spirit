package check

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// showCreateTable returns the SHOW CREATE TABLE statement for schema.table.
func showCreateTable(ctx context.Context, db *sql.DB, schema, table string) (string, error) {
	var name, createStmt string
	row := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table))
	if err := row.Scan(&name, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

// schemaDiff compares two CREATE TABLE statements and returns a human-readable
// description of how they differ, or an empty string if they are equivalent.
//
// The comparison is performed by parsing both statements with the TiDB parser
// and diffing the structured form via statement.CreateTable.Diff. This canonical
// comparison:
//   - ignores AUTO_INCREMENT counter values (instance-specific noise),
//   - ignores ENGINE and ROW_FORMAT cosmetic defaults,
//   - DOES compare column types, nullability, defaults, and per-column /
//     per-table CHARACTER SET and COLLATE,
//   - DOES compare indexes (including the primary key) and constraints.
//
// "want" is treated as the source-of-truth (e.g. sources[0] or the move source);
// "got" is the schema being validated (another source, or a pre-created target).
// The returned ALTER clauses describe the transformation that would be required
// to turn "got" into "want", which is what makes the message actionable.
func schemaDiff(wantCreate, gotCreate string) (string, error) {
	want, err := statement.ParseCreateTable(wantCreate)
	if err != nil {
		return "", fmt.Errorf("failed to parse reference CREATE TABLE: %w", err)
	}
	got, err := statement.ParseCreateTable(gotCreate)
	if err != nil {
		return "", fmt.Errorf("failed to parse CREATE TABLE under validation: %w", err)
	}
	// Diff requires both tables to have the same name. The two CREATE TABLE
	// statements come from tables with the same logical name on different
	// instances, but rewrite both names to a fixed token so the comparison is
	// purely structural and never trips on the name guard.
	want.TableName = "t"
	got.TableName = "t"

	// Diff(got -> want): the returned clauses are the ALTER that would morph the
	// validated schema ("got") into the reference schema ("want"). If nil, the
	// two schemas are equivalent under the canonicalization rules above.
	stmts, err := got.Diff(want, statement.NewDiffOptions())
	if err != nil {
		return "", fmt.Errorf("failed to diff CREATE TABLE statements: %w", err)
	}
	if len(stmts) == 0 {
		return "", nil
	}
	clauses := make([]string, 0, len(stmts))
	for _, s := range stmts {
		if s.Alter != "" {
			clauses = append(clauses, s.Alter)
		}
	}
	return strings.Join(clauses, "; "), nil
}
