//nolint:dupword
package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// are still supported. From https://github.com/block/spirit/issues/277
// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00 can still be migrated.
func TestSkipDropAfterCutoverLongTableName(t *testing.T) {
	t.Parallel()

	// A table name at the normal max (56 chars) should work with SkipDropAfterCutover.
	// Previously this would have been rejected because the timestamp format exceeds 64 chars,
	// but now we truncate the table name portion in the old table name.
	tableName := "a_fifty_six_character_table_name_that_fits_normal_limits"
	assert.Equal(t, 56, len(tableName))

	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	testutils.RunSQL(t, fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL AUTO_INCREMENT,
		PRIMARY KEY(pk)
	)`, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: true,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	// Verify the old table exists (with truncated name + timestamp)
	oldName := m.changes[0].oldTableName()
	assert.LessOrEqual(t, len(oldName), 64, "old table name should fit within 64 chars")

	var tableCount int
	err = m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, oldName)).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount, "old table should exist after SkipDropAfterCutover")
	assert.NoError(t, m.Close())
}

func TestBadOptions(t *testing.T) {
	// N.B. Because host, user, password and database have defaults enforced, we expect to
	// fail in the same way when they're not provided.
	t.Parallel()
	_, err := NewRunner(&Migration{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	_, err = NewRunner(&Migration{
		Host: cfg.Addr,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mydatabase",
		Table:    "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadAlter(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS bot1, bot2`)
	table := `CREATE TABLE bot1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	table = `CREATE TABLE bot2 (
		id int(11) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "badalter",
	})
	assert.Error(t, err) // parses and fails.
	assert.Nil(t, m)

	// Column renames are now supported, but this ALTER is invalid because
	// it references the old column name in the ADD INDEX after renaming it.
	// MySQL rejects this when Spirit applies the ALTER to the shadow table.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "RENAME COLUMN name TO name2, ADD INDEX(name)", // ADD INDEX references old name after rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err) // alter is invalid (MySQL rejects it)
	assert.NoError(t, m.Close())

	// Same issue via CHANGE COLUMN syntax
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "CHANGE name name2 VARCHAR(255), ADD INDEX(name)", // ADD INDEX references old name after rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err) // alter is invalid (MySQL rejects it)
	assert.NoError(t, m.Close())

	// But this is supported (no rename)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "CHANGE name name VARCHAR(200), ADD INDEX(name)", //nolint: dupword
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.NoError(t, err) // its valid, no rename
	assert.NoError(t, m.Close())

	// Test DROP PRIMARY KEY, change primary key.
	// The REPLACE statement likely relies on the same PRIMARY KEY on the new table,
	// so things get a lot more complicated if the primary key changes.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot2",
		Alter:    "DROP PRIMARY KEY",
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "dropping primary key")
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyNoAutoInc is a good test of the how much the
// chunker will boil the ocean:
//   - There is a MIN(key)=1 and a MAX(key)=8589934592
//   - There is no auto-increment so the chunker is allowed to expand each chunk
//     based on estimated rows (which is low).
//
// Only the key=8589934592 will fail to be converted. On my system this test
// currently runs in 0.4 seconds which is "acceptable" for chunker performance.
// The generated number of chunks should also be very low because of prefetching.
func TestForRemainingTableArtifacts(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS remainingtbl, _remainingtbl_new, _remainingtbl_old, _remainingtbl_chkpnt`)
	table := `CREATE TABLE remainingtbl (
		id INT NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "remainingtbl",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified.
	assert.NoError(t, m.Run(t.Context())) // it's an accepted type.
	assert.NoError(t, m.Close())

	// Now we should have a _remainingtbl_old table and a remainingtbl table
	// but no _remainingtbl_new table or _remainingtbl_chkpnt table.
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	stmt := `SELECT GROUP_CONCAT(table_name) FROM information_schema.tables where table_schema='test' and table_name LIKE '%remainingtbl%' ORDER BY table_name;`
	var tables string
	assert.NoError(t, db.QueryRowContext(t.Context(), stmt).Scan(&tables))
	assert.Equal(t, "remainingtbl", tables)
}

func TestDefaultPort(t *testing.T) {
	t.Parallel()
	m, err := NewRunner(&Migration{
		Host:     "localhost",
		Username: "root",
		Password: mkPtr("mypassword"),
		Database: "test",
		Threads:  2,
		Table:    "t1",
		Alter:    "DROP COLUMN b, ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	assert.Equal(t, "localhost:3306", m.migration.Host)
	m.SetLogger(slog.Default())
}

func TestSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	tableName := `skipdrop_test`

	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	table := fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName)

	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "skipdrop_test",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: true,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = m.db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount)
	assert.NoError(t, m.Close())
}

func TestDropAfterCutover(t *testing.T) {
	t.Parallel()
	tableName := `drop_test`
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	table := fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName)

	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "drop_test",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = m.db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOver(t *testing.T) {
	t.Skip("skipping: this test waits for sentinelWaitLimit to expire, which is too slow with the current 48 hour limit")
	t.Parallel()

	// Create unique database for this test
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	tableName := `deferred_cutover`
	newName := fmt.Sprintf("_%s_new", tableName)
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "deferred_cutover",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Go(func() {
		err = m.Run(t.Context())
		assert.Error(t, err)
		assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	})

	// While it's waiting, check the Progress.
	waitForStatus(t, m, status.WaitingOnSentinelTable)
	wg.Wait()

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, newName)
	var tableCount int
	err = m.db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOverE2E(t *testing.T) {
	t.Parallel()

	// Create unique database for this test
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e`
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	// Add cleanup handler to guarantee table cleanup even on failure/timeout
	t.Cleanup(func() {
		db, _ := sql.Open("mysql", testutils.DSNForDatabase(dbName))
		defer func() { _ = db.Close() }()
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(
			"DROP TABLE IF EXISTS %s, _%s_new, _%s_old, _%s_chkpnt, %s",
			tableName, tableName, tableName, tableName, sentinelTableName))
	})

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              1,
		Table:                "deferred_cutover_e2e",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	go func() {
		err := m.Run(t.Context())
		assert.NoError(t, err)
		c <- err
	}()

	// wait until the sentinel table exists
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	for {
		var rowCount int
		sql := fmt.Sprintf(
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'`, dbName, sentinelTableName)
		err = db.QueryRowContext(t.Context(), sql).Scan(&rowCount)
		assert.NoError(t, err)
		if rowCount > 0 {
			break
		}
	}
	assert.NoError(t, err)

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c // wait for the migration to finish
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOverE2EBinlogAdvance(t *testing.T) {
	t.Parallel()
	// Create unique database for this test
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e_stage`
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	// Add cleanup handler to guarantee table cleanup even on failure/timeout
	t.Cleanup(func() {
		db, _ := sql.Open("mysql", testutils.DSNForDatabase(dbName))
		defer func() { _ = db.Close() }()
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(
			"DROP TABLE IF EXISTS %s, _%s_new, _%s_old, _%s_chkpnt, %s",
			tableName, tableName, tableName, tableName, sentinelTableName))
	})

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              1,
		Table:                "deferred_cutover_e2e_stage",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	go func() {
		err := m.Run(t.Context())
		assert.NoError(t, err)
		c <- err
	}()

	// wait until the sentinel table exists
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	waitForStatus(t, m, status.WaitingOnSentinelTable)

	binlogPos := m.replClient.GetBinlogApplyPosition()
	for range 4 {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))
		assert.NoError(t, m.replClient.BlockWait(t.Context()))
		assert.NoError(t, m.replClient.Flush(t.Context()))
		newBinlogPos := m.replClient.GetBinlogApplyPosition()
		assert.Equal(t, 1, newBinlogPos.Compare(binlogPos))
		binlogPos = newBinlogPos
	}

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c // wait for the migration to finish
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

// From https://github.com/block/spirit/issues/241
// If an ALTER qualifies as instant, but an instant can't apply, don't burn an instant version.
func TestForNonInstantBurn(t *testing.T) {
	t.Parallel()
	// We skip this test in MySQL 8.0.28. It uses INSTANT_COLS instead of total_row_versions
	// and it supports instant add col, but not instant drop col.
	// It's safe to skip, but we need 8.0.28 in tests because it's the minor version
	// used by Aurora's LTS.
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	err = db.QueryRowContext(t.Context(), `SELECT version()`).Scan(&version)
	assert.NoError(t, err)
	if version == "8.0.28" {
		t.Skip("Skipping this test for MySQL 8.0.28")
	}
	if strings.HasPrefix(version, "9.") {
		t.Skip("Skipping this test for MySQL 9.x: total_row_versions limit was raised beyond 64")
	}
	// Continue with the test.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS instantburn`)
	table := `CREATE TABLE instantburn (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	rowVersions := func() int {
		// Check that the number of total_row_versions is Zero (i'e doesn't burn)
		db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		defer utils.CloseAndLog(db)
		var rowVersions int
		err = db.QueryRowContext(t.Context(), `SELECT total_row_versions FROM INFORMATION_SCHEMA.INNODB_TABLES where name='test/instantburn'`).Scan(&rowVersions)
		assert.NoError(t, err)
		return rowVersions
	}

	testutils.RunSQL(t, table)
	for range 32 { // requires 64 instants
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, ADD newcol INT")
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, DROP newcol")
	}
	assert.Equal(t, 64, rowVersions()) // confirm all 64 are used.
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "instantburn",
		Alter:    "add newcol2 int",
	})
	assert.NoError(t, err)
	defer utils.CloseAndLog(m)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.False(t, m.usedInstantDDL) // it would have had to apply a copy.
	assert.Equal(t, 0, rowVersions()) // confirm we reset to zero, not 1 (no burn)
}

// From https://github.com/block/spirit/issues/283
// ALTER INDEX .. VISIBLE is INPLACE which is really weird.
// it only makes sense to be instant, so we attempt it as a "safe inplace".
// If it's not with a set of safe changes, then we error.
// This means the user is expected to split their DDL into two separate ALTERs.
//
// There is a partial workaround for users to use --force-inplace, which would
// help only if the other included changes are also INPLACE and not copy.
// We *do* document this under --force-inplace docs, but it's
// really not a typical use case to ever mix invisible with any other change.
// i.e. if anything it's more a side-effect than a workaround.
func TestIndexVisibility(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS indexvisibility`)
	table := `CREATE TABLE indexvisibility (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`
	testutils.RunSQL(t, table)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b INVISIBLE",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.True(t, m.usedInplaceDDL) // expected to count as safe.
	assert.NoError(t, m.Close())

	// Test again with visible
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m.usedInplaceDDL) // expected to count as safe.
	assert.NoError(t, m.Close())

	// Test again but include an unsafe INPLACE change at the same time.
	// This won't work by default.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE, ADD INDEX (c)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close()) // it's errored, we don't need to try again. We can close.

	// The above should now fail with enhanced automatic detection.
	// Index visibility mixed with table-rebuilding operations should be rejected.

	// Index visibility mixed with table-rebuilding operations should fail.
	// This is important because invisible should never be mixed with copy
	// (the semantics are weird since it's for experiments).
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE, CHANGE c cc BIGINT NOT NULL",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close()) // it's errored, we don't need to try again. We can close.
}

// TestPreventConcurrentRuns ensures that metadata locking
// prevents two concurrent migrations on the same table.
// We use DeferCutOver=true option to force the first migration
// to stay running.
func TestStatementWorkflowStillInstant(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS stmtworkflow`)
	table := `CREATE TABLE stmtworkflow (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`
	testutils.RunSQL(t, table)
	m, err := NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "ALTER TABLE stmtworkflow ADD newcol INT",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.True(t, m.usedInstantDDL) // expected to count as instant.
	assert.NoError(t, m.Close())
}

func TestTrailingSemicolon(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS multiSecondary`)
	testutils.RunSQL(t, `CREATE TABLE multiSecondary (
			  id int unsigned NOT NULL AUTO_INCREMENT,
			  v varchar(32) DEFAULT NULL,
			  PRIMARY KEY (id),
			  KEY idx5 (v),
			  KEY idx1 (v),
			  KEY idx2 (v),
			  KEY idx3 (v),
			  KEY idx4 (v)
			)`)
	dropIndexesAlter := "drop index idx1, drop index idx2, drop index idx3, drop index idx4"
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "multiSecondary",
		Alter:    dropIndexesAlter,
		Threads:  1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	assert.True(t, m.usedInplaceDDL) // DROP INDEX operations now use INPLACE for better performance
	assert.NoError(t, m.Close())

	m, err = NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Statement: "alter table multiSecondary add index idx1(v), add index idx2(v), add index idx3(v), add index idx4(v);",
		Threads:   1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	require.False(t, m.usedInplaceDDL) // ADD INDEX operations now use copy process for replica safety
	require.NoError(t, m.Close())

	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "multiSecondary",
		// https://github.com/block/spirit/issues/384
		Alter:   dropIndexesAlter + "; ",
		Threads: 1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
}
func TestAlterExtendVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1extendvarchar, _t1extendvarchar_new`)
	table := `CREATE TABLE t1extendvarchar (
		id int not null primary key auto_increment,
		col1 varchar(10),
		col2 varchar(10)
	) character set utf8mb4`
	testutils.RunSQL(t, table)

	type alterAttempt struct {
		Statement string
		Error     bool
		InPlace   bool
	}
	alters := []alterAttempt{
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(20)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar CHANGE col1 col1 varchar(21)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(22), CHANGE col2 col2 varchar(22) `, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(23), CHANGE col2 col2 varchar(200) `, InPlace: false},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(200)`, InPlace: false},
	}

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	for _, attempt := range alters {
		m, err := NewRunner(&Migration{
			Host:      cfg.Addr,
			Username:  cfg.User,
			Password:  &cfg.Passwd,
			Database:  cfg.DBName,
			Threads:   1,
			Statement: attempt.Statement,
		})
		require.NoError(t, err)
		err = m.Run(t.Context())
		require.NoError(t, err)
		assert.Equal(t, attempt.InPlace, m.usedInplaceDDL)

		// go test howls about resource leaks if we don't close all these things
		err = m.Close()
		assert.NoError(t, err)
	}
}

func TestPasswordMasking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "basic DSN with password",
			input:    "user:password@tcp(localhost:3306)/database",
			expected: "user:***@tcp(localhost:3306)/database",
		},
		{
			name:     "DSN with complex password",
			input:    "myuser:c0mplex!Pa$$w0rd@tcp(db.example.com:3306)/mydb",
			expected: "myuser:***@tcp(db.example.com:3306)/mydb",
		},
		{
			name:     "DSN without password",
			input:    "user@tcp(localhost:3306)/database",
			expected: "user@tcp(localhost:3306)/database",
		},
		{
			name:     "DSN with empty password",
			input:    "user:@tcp(localhost:3306)/database",
			expected: "user:***@tcp(localhost:3306)/database",
		},
		{
			name:     "empty DSN",
			input:    "",
			expected: "",
		},
		{
			name:     "malformed DSN without @",
			input:    "user:password",
			expected: "user:password",
		},
		{
			name:     "DSN with colon in password",
			input:    "user:pass:word@tcp(localhost:3306)/database",
			expected: "user:***@tcp(localhost:3306)/database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := maskPasswordInDSN(tt.input)
			assert.Equal(t, tt.expected, result, "Password masking failed for input: %s", tt.input)
		})
	}
}

func TestDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		user     string
		password string
		host     string
		schema   string
	}{
		{
			name:     "simple password",
			user:     "root",
			password: "secret",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "password with @",
			user:     "root",
			password: "p@ssword",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "password with multiple @",
			user:     "root",
			password: "p@ss@word",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "password with special characters",
			user:     "root",
			password: "p@ss:word/with#special!chars",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "empty password",
			user:     "root",
			password: "",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "AWS IAM-style token",
			user:     "iam_user",
			password: "aaa@bbb.ccc.us-east-1.rds.amazonaws.com:3306/?Action=connect&DBUser=iam_user",
			host:     "mydb.cluster-xyz.us-east-1.rds.amazonaws.com:3306",
			schema:   "production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pw := tt.password
			r := &Runner{
				migration: &Migration{
					Username: tt.user,
					Password: &pw,
					Host:     tt.host,
				},
				changes: []*change{
					{
						stmt: &statement.AbstractStatement{
							Schema: tt.schema,
						},
					},
				},
			}

			dsn := r.dsn()

			// Parse the DSN back and verify all fields round-trip correctly.
			cfg, err := mysql.ParseDSN(dsn)
			require.NoError(t, err)
			assert.Equal(t, tt.user, cfg.User)
			assert.Equal(t, tt.password, cfg.Passwd)
			assert.Equal(t, tt.host, cfg.Addr)
			assert.Equal(t, tt.schema, cfg.DBName)
			assert.Equal(t, "tcp", cfg.Net)
		})
	}
}

// TestEnumReorder tests that reordering ENUM values in an ALTER TABLE
// produces correct data after migration.
//
// This test only works correctly in unbuffered mode because of the way
// ENUM values are represented in the binlog. We test *both* unbuffered and buffered modes
// though and we accept a pre-flight failure as a "pass", since it's not corruption.
// i.e. it's OK to refuse changes you can't handle.
//
// The unbuffered path uses
// REPLACE INTO ... SELECT (SQL-level string operations) which handles
// ENUM reordering correctly. The buffered path uses UpsertRows with
// raw binlog values, where ENUM values are represented as int64 ordinals.
// If the ENUM is reordered, the ordinals map to different string values
// in the target table, causing data corruption.
//
// This test exercises both the copier path (initial data) and the binlog
// replay path (concurrent DML during migration) to verify correctness.
func TestMigrationWithSQLCommentsInStatement(t *testing.T) {
	// This test verifies that Spirit correctly handles SQL comments
	// prepended to ALTER TABLE statements when using the Statement field
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS t1_comment_test")
	testutils.RunSQL(t, "CREATE TABLE t1_comment_test (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT)")
	testutils.RunSQL(t, "INSERT INTO t1_comment_test (a) VALUES (1), (2), (3)")

	// Statement with multiple SQL comments before the ALTER — this is exactly
	// what our tool passes when users include comments in their .sql files.
	statementWithComments := `-- Migration for JIRA-1234
-- Author: someone@block.xyz
-- Date: 2025-07-01
-- This migration adds an index on column a
-- for improved query performance on the dashboard.
ALTER TABLE t1_comment_test ADD INDEX idx_a (a)`

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Statement: statementWithComments,
	}

	// Use NewRunner so we can inspect the parsed changes before running.
	r, err := NewRunner(migration)
	require.NoError(t, err)
	require.Len(t, r.changes, 1)
	assert.Equal(t, "ADD INDEX `idx_a`(`a`)", r.changes[0].stmt.Alter)

	err = migration.Run()
	assert.NoError(t, err)

	// Verify the index was actually created
	var indexName string
	err = db.QueryRowContext(t.Context(), "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t1_comment_test' AND INDEX_NAME='idx_a'").Scan(&indexName)
	assert.NoError(t, err)
	assert.Equal(t, "idx_a", indexName)
	testutils.RunSQL(t, "DROP TABLE IF EXISTS t1_comment_test")
}
