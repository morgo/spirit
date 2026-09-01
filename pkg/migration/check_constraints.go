package migration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/statement"
)

// newTableAlter returns the ALTER clauses to apply to the _new table.
//
// This is the user's ALTER, except for the names of check constraints. Those
// names are unique per schema and not per table (MySQL: "The CONSTRAINT symbol
// value, if defined, must be unique in the database"), so the _new table cannot
// hold them while the user's table still exists: CREATE TABLE .. LIKE gives its
// copies server-generated names instead. Replaying the ALTER verbatim would
// then fail on a name that is not there ("Check constraint 'x' is not found in
// the table", error 3821), or - for an ALTER that drops a constraint and re-adds
// it under the same name - on one that is still owned by the user's table
// ("Duplicate check constraint name 'x'", error 3822).
//
// So DROP CHECK / ALTER CHECK are retargeted at the _new table's names for the
// same constraints, and a re-added name is dropped so MySQL generates one. See
// AlterWithRenamedCheckConstraints for the rewriting rules.
func (c *tableChange) newTableAlter(ctx context.Context) (string, error) {
	// Only an ALTER that names a check constraint needs any of this, which
	// keeps the two SHOW CREATE TABLE round trips off the common path.
	if len(c.stmt.CheckConstraintsReferenced()) == 0 {
		return c.stmt.TrimAlter(), nil
	}
	renames, err := c.checkConstraintRenames(ctx)
	if err != nil {
		return "", err
	}
	alter, unnamed, err := c.stmt.AlterWithRenamedCheckConstraints(renames)
	if err != nil {
		return "", err
	}
	for _, name := range unnamed {
		c.runner.logger.Warn("a CHECK constraint cannot be re-added under the same name by a copy migration, because the name is still held by the table being replaced: MySQL will generate a name for it instead",
			"table", c.table.TableName,
			"constraint", name)
	}
	if alter != c.stmt.TrimAlter() {
		c.runner.logger.Info("rewrote CHECK constraint names for the new table",
			"table", c.newTable.TableName,
			"alter", alter)
	}
	return alter, nil
}

// checkConstraintRenames maps each check constraint name on the table being
// altered (lower-cased, because MySQL matches these names case-insensitively)
// to the name the same constraint has on the _new table.
//
// CREATE TABLE .. LIKE copies check constraints in declaration order, so the
// two tables' constraints correspond positionally. Their expressions are
// compared to confirm that, rather than trusting the ordering and retargeting a
// DROP CHECK at some other constraint.
func (c *tableChange) checkConstraintRenames(ctx context.Context) (map[string]string, error) {
	source, err := checkConstraints(ctx, c.runner.db, c.table.TableName)
	if err != nil {
		return nil, err
	}
	newTable, err := checkConstraints(ctx, c.runner.db, c.newTable.TableName)
	if err != nil {
		return nil, err
	}
	if len(source) != len(newTable) {
		return nil, fmt.Errorf("table %s has %d CHECK constraint(s) but its copy %s has %d",
			c.table.TableName, len(source), c.newTable.TableName, len(newTable))
	}
	renames := make(map[string]string, len(source))
	for i := range source {
		if !expressionsEqual(source[i], newTable[i]) {
			return nil, fmt.Errorf("CHECK constraint %s on %s does not match %s on its copy %s",
				source[i].Name, c.table.TableName, newTable[i].Name, c.newTable.TableName)
		}
		renames[strings.ToLower(source[i].Name)] = newTable[i].Name
	}
	return renames, nil
}

// checkConstraints returns the table's CHECK constraints in declaration order.
func checkConstraints(ctx context.Context, db *sql.DB, tableName string) (statement.Constraints, error) {
	var name, createTable string
	if err := db.QueryRowContext(ctx,
		sqlescape.MustEscapeSQL("SHOW CREATE TABLE %n", tableName)).Scan(&name, &createTable); err != nil {
		return nil, fmt.Errorf("could not read the definition of table %s: %w", tableName, err)
	}
	stmts, err := statement.New(createTable)
	if err != nil {
		return nil, fmt.Errorf("could not parse the definition of table %s: %w", tableName, err)
	}
	createStmt, err := stmts[0].ParseCreateTable()
	if err != nil {
		return nil, fmt.Errorf("could not parse the definition of table %s: %w", tableName, err)
	}
	var constraints statement.Constraints
	for _, constraint := range createStmt.GetConstraints() {
		if constraint.Type == "CHECK" {
			constraints = append(constraints, constraint)
		}
	}
	return constraints, nil
}

// expressionsEqual reports whether two CHECK constraints have the same
// expression. Both sides come from SHOW CREATE TABLE and are parsed the same
// way, so MySQL's own canonical rendering makes the comparison textual.
func expressionsEqual(a, b statement.Constraint) bool {
	if a.Expression == nil || b.Expression == nil {
		return a.Expression == b.Expression
	}
	return *a.Expression == *b.Expression
}
