package migration

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

type change struct {
	stmt     *statement.AbstractStatement
	table    *table.TableInfo
	newTable *table.TableInfo

	// Store a pointer back to the migration runner
	// (for compatibility, we want to eventually remove this)
	runner *Runner
}

func (c *change) createNewTable(ctx context.Context) error {
	newName := fmt.Sprintf(check.NameFormatNew, c.table.TableName)
	// drop the newName if we've decided to call this func.
	if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n.%n", c.table.SchemaName, newName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, c.runner.db, "CREATE TABLE %n.%n LIKE %n.%n",
		c.table.SchemaName, newName, c.table.SchemaName, c.table.TableName); err != nil {
		return err
	}
	c.newTable = table.NewTableInfo(c.runner.db, c.stmt.Schema, newName)
	if err := c.newTable.SetInfo(ctx); err != nil {
		return err
	}
	return nil
}

// alterNewTable applies the ALTER to the new table.
// It has been pre-checked it is not a rename, or modifying the PRIMARY KEY.
// We first attempt to do this using ALGORITHM=COPY so we don't burn
// an INSTANT version. But surprisingly this is not supported for all DDLs (issue #277)
func (c *change) alterNewTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n "+c.stmt.TrimAlter()+", ALGORITHM=COPY",
		c.newTable.SchemaName, c.newTable.TableName); err != nil {
		// Retry without the ALGORITHM=COPY. If there is a second error, then the DDL itself
		// is not supported. It could be a syntax error, in which case we return the second error,
		// which will probably be easier to read because it is unaltered.
		if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n "+c.stmt.Alter, c.newTable.SchemaName, c.newTable.TableName); err != nil {
			return err
		}
	}
	// Call GetInfo on the table again, since the columns
	// might have changed and this will affect the row copiers intersect func.
	if err := c.newTable.SetInfo(ctx); err != nil {
		return err
	}

	// Preserve AUTO_INCREMENT value from the original table AFTER the ALTER.
	// CREATE TABLE LIKE doesn't copy AUTO_INCREMENT, and ALTER with ALGORITHM=COPY
	// can reset it. For empty tables, INSERT SELECT won't trigger MySQL's automatic
	// adjustment, so we explicitly set it to prevent new inserts from restarting at 1.
	return c.preserveAutoIncrement(ctx)
}

func (c *change) preserveAutoIncrement(ctx context.Context) error {
	// Get AUTO_INCREMENT from the original table.
	var originalAutoInc sql.NullInt64
	err := c.runner.db.QueryRowContext(ctx,
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		c.table.SchemaName, c.table.TableName).Scan(&originalAutoInc)
	if err != nil {
		return fmt.Errorf("failed to get AUTO_INCREMENT value from original table: %w", err)
	}

	// If the original table doesn't have a meaningful AUTO_INCREMENT, nothing to preserve.
	if !originalAutoInc.Valid || originalAutoInc.Int64 <= 1 {
		return nil
	}

	// Get AUTO_INCREMENT from the new table to detect if it was explicitly set by the ALTER.
	var newTableAutoInc sql.NullInt64
	err = c.runner.db.QueryRowContext(ctx,
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		c.newTable.SchemaName, c.newTable.TableName).Scan(&newTableAutoInc)
	if err != nil {
		return fmt.Errorf("failed to get AUTO_INCREMENT value from new table: %w", err)
	}

	// Only override AUTO_INCREMENT on the new table if it doesn't appear to have been explicitly set
	// (that is, it's NULL or <= 1). This avoids clobbering a user-specified AUTO_INCREMENT in the ALTER.
	if newTableAutoInc.Valid && newTableAutoInc.Int64 > 1 {
		// Respect the explicitly configured AUTO_INCREMENT on the new table.
		return nil
	}

	if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n AUTO_INCREMENT = %?",
		c.newTable.SchemaName, c.newTable.TableName, originalAutoInc.Int64); err != nil {
		return fmt.Errorf("failed to set AUTO_INCREMENT on new table: %w", err)
	}
	c.runner.logger.Info("preserved AUTO_INCREMENT value",
		"table", c.table.TableName,
		"auto_increment", originalAutoInc.Int64)
	return nil
}

func (c *change) dropOldTable(ctx context.Context) error {
	return dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n.%n", c.table.SchemaName, c.oldTableName())
}

func (c *change) oldTableName() string {
	// By default we just set the old table name to _<table>_old
	// but if they've enabled SkipDropAfterCutover, we add a timestamp
	if !c.runner.migration.SkipDropAfterCutover {
		return fmt.Sprintf(check.NameFormatOld, c.table.TableName)
	}
	return fmt.Sprintf(check.NameFormatOldTimeStamp, c.table.TableName, c.runner.startTime.UTC().Format(check.NameFormatTimestamp))
}

func (c *change) attemptInstantDDL(ctx context.Context) error {
	if c.runner.migration.ForceKill {
		return dbconn.ForceExec(
			ctx,
			c.runner.db,
			[]*table.TableInfo{c.table},
			c.runner.dbConfig,
			c.runner.logger,
			"ALTER TABLE %n.%n ALGORITHM=INSTANT, "+c.stmt.Alter,
			c.table.SchemaName,
			c.table.TableName,
		)
	}
	return dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n ALGORITHM=INSTANT, "+c.stmt.Alter, c.table.SchemaName, c.table.TableName)
}

func (c *change) attemptInplaceDDL(ctx context.Context) error {
	if c.runner.migration.ForceKill {
		return dbconn.ForceExec(
			ctx,
			c.runner.db,
			[]*table.TableInfo{c.table},
			c.runner.dbConfig,
			c.runner.logger,
			"ALTER TABLE %n.%n ALGORITHM=INPLACE, LOCK=NONE, "+c.stmt.Alter,
			c.table.SchemaName,
			c.table.TableName,
		)
	}
	return dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n ALGORITHM=INPLACE, LOCK=NONE, "+c.stmt.Alter, c.table.SchemaName, c.table.TableName)
}

func (c *change) cleanup(ctx context.Context) error {
	if c.newTable != nil {
		if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n.%n", c.newTable.SchemaName, c.newTable.TableName); err != nil {
			return err
		}
	}
	return nil
}

// attemptMySQLDDL "attempts" to use DDL directly on MySQL with an assertion
// such as ALGORITHM=INSTANT. If MySQL is able to use the INSTANT algorithm,
// it will perform the operation without error. If it can't, it will return
// an error. It is important to let MySQL decide if it can handle the DDL
// operation, because keeping track of which operations are "INSTANT"
// is incredibly difficult. It will depend on MySQL minor version,
// and could possibly be specific to the table.
func (c *change) attemptMySQLDDL(ctx context.Context) error {
	err := c.attemptInstantDDL(ctx)
	if err == nil {
		c.runner.usedInstantDDL = true // success
		return nil
	}

	// Many "inplace" operations (such as adding an index)
	// are only online-safe to do in Aurora GLOBAL
	// because replicas do not use the binlog. Some, however,
	// only modify the table metadata and are safe.
	//
	// Spirit automatically detects safe operations that can use
	// the INPLACE algorithm without blocking read replicas.
	err = c.stmt.AlgorithmInplaceConsideredSafe()
	if err == nil {
		err = c.attemptInplaceDDL(ctx)
		if err == nil {
			c.runner.usedInplaceDDL = true // success
			return nil
		}
	}
	c.runner.logger.Info("unable to use INPLACE", "error", err)

	// Failure is expected, since MySQL DDL only applies in limited scenarios
	// Return the error, which will be ignored by the caller.
	// Proceed with regular copy algorithm.
	return err
}

func (c *change) Close() error {
	if c.table != nil {
		return c.table.Close()
	}
	return nil
}
