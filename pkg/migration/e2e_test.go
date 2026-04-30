//nolint:dupword
package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVarcharNonBinaryComparable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "nonbinarycompatt1",
		`CREATE TABLE nonbinarycompatt1 (
			uuid varchar(40) NOT NULL,
			name varchar(255) NOT NULL,
			PRIMARY KEY (uuid)
		)`)

	m := NewTestRunner(t, "nonbinarycompatt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context())) // it's a non-binary comparable type (varchar)
	assert.NoError(t, m.Close())
}

// TestPartitioningSyntax tests that ALTERs that don't support ALGORITHM assertion
// are still supported. From https://github.com/block/spirit/issues/277
func TestPartitioningSyntax(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "partt1",
		`CREATE TABLE partt1 (
			id INT NOT NULL PRIMARY KEY auto_increment,
			name varchar(255) NOT NULL
		)`)

	m := NewTestRunner(t, "partt1", "PARTITION BY KEY() PARTITIONS 8")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestVarbinary(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "varbinaryt1",
		`CREATE TABLE varbinaryt1 (
			uuid varbinary(40) NOT NULL,
			name varchar(255) NOT NULL,
			PRIMARY KEY (uuid)
		)`)
	testutils.RunSQL(t, "INSERT INTO varbinaryt1 (uuid, name) VALUES (UUID(), REPEAT('a', 200))")

	m := NewTestRunner(t, "varbinaryt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context())) // varbinary is compatible.
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())
}

// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00 can still be migrated.
func TestDataFromBadSqlMode(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "badsqlt1",
		`CREATE TABLE badsqlt1 (
			id int not null primary key auto_increment,
			d date NOT NULL,
			t timestamp NOT NULL
		)`)
	testutils.RunSQL(t, "INSERT IGNORE INTO badsqlt1 (d, t) VALUES ('0000-00-00', '0000-00-00 00:00:00'),('2020-02-00', '2020-02-30 00:00:00')")

	m := NewTestRunner(t, "badsqlt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())
}

func TestOnline(t *testing.T) {
	t.Parallel()

	// CHANGE COLUMN varchar→int requires copy (not inplace).
	testutils.NewTestTable(t, "testonline",
		`CREATE TABLE testonline (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	m := NewTestRunner(t, "testonline", "CHANGE COLUMN b b int(11) NOT NULL") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	// ADD COLUMN uses instant DDL.
	testutils.NewTestTable(t, "testonline2",
		`CREATE TABLE testonline2 (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	m = NewTestRunner(t, "testonline2", "ADD c int(11) NOT NULL")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInplaceDDL)
	assert.True(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// ADD INDEX now always requires copy.
	testutils.NewTestTable(t, "testonline3",
		`CREATE TABLE testonline3 (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	m = NewTestRunner(t, "testonline3", "ADD INDEX(b)")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.False(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	// DROP INDEX uses inplace.
	testutils.NewTestTable(t, "testonline4",
		`CREATE TABLE testonline4 (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			key name (name),
			key b (b),
			PRIMARY KEY (id)
		)`)
	m = NewTestRunner(t, "testonline4", "drop index name, drop index b")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	assert.True(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	// Mixing INSTANT and INPLACE operations falls back to copy.
	testutils.NewTestTable(t, "testonline5",
		`CREATE TABLE testonline5 (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			key name (name),
			key b (b),
			PRIMARY KEY (id)
		)`)
	m = NewTestRunner(t, "testonline5", "drop index name, add column c int")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	assert.False(t, m.usedInplaceDDL) // unfortunately false, since it combines INSTANT and INPLACE operations
	assert.NoError(t, m.Close())

	// Hash-partitioned tables require a lock (not inplace).
	testutils.NewTestTable(t, "testonline6",
		`CREATE TABLE testonline6 (
			id int(11) NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) PARTITION BY HASH (id) PARTITIONS 4`)
	m = NewTestRunner(t, "testonline6", "add partition partitions 4")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.False(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	// Range-partitioned tables can run inplace without a lock.
	testutils.NewTestTable(t, "testonline7",
		`CREATE TABLE testonline7 (
			id int(11) NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) PARTITION BY RANGE (id) (
			PARTITION p0 VALUES LESS THAN (100000),
			PARTITION p1 VALUES LESS THAN (200000)
		)`)
	m = NewTestRunner(t, "testonline7", "add partition (partition p2 values less than (300000))")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.True(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())
}

func TestTableLength(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "thisisareallylongtablenamethisisareallylongtablename60charac",
		`CREATE TABLE thisisareallylongtablenamethisisareallylongtablename60charac (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	m := NewTestRunner(t, "thisisareallylongtablenamethisisareallylongtablename60charac", "ENGINE=InnoDB")
	err := m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name must be less than 56 characters")
	assert.NoError(t, m.Close())

	// There is another condition where the error will be in dropping the _old table first
	// if the character limit is exceeded in that query.
	m = NewTestRunner(t, "thisisareallylongtablenamethisisareallylongtablename60charac", "ENGINE=InnoDB")
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name must be less than 56 characters")
	assert.NoError(t, m.Close())
}

// TestAddUniqueIndexChecksumEnabled tests that adding a unique index with
// duplicate errors from a resume, and a constraint violation. So what we do is:
// 0) *FORCE* checksum to be enabled (regardless now, its always on)
func TestAddUniqueIndexChecksumEnabled(t *testing.T) {
	testutils.NewTestTable(t, "uniqmytable",
		`CREATE TABLE uniqmytable (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('b', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('c', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))") // duplicate

	m := NewTestRunner(t, "uniqmytable", "ADD UNIQUE INDEX b (b)")
	err := m.Run(t.Context())
	assert.Error(t, err)         // not unique
	assert.NoError(t, m.Close()) // need to close now otherwise we'll get an error on re-opening it.

	testutils.RunSQL(t, "DELETE FROM uniqmytable WHERE b = REPEAT('a', 200) LIMIT 1") // make unique
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_chkpnt`)                   // make sure no checkpoint exists, we need to start again.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_new`)                      // cleanup temp table from first run

	m2 := NewTestRunner(t, "uniqmytable", "ADD UNIQUE INDEX b (b)")
	require.NoError(t, m2.Run(t.Context())) // works fine.
	assert.NoError(t, m2.Close())
}

func TestChangeNonIntPK(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "nonintpk",
		`CREATE TABLE nonintpk (
			pk varbinary(36) NOT NULL PRIMARY KEY,
			name varchar(255) NOT NULL,
			b varchar(10) NOT NULL
		)`)
	testutils.RunSQL(t, "INSERT INTO nonintpk (pk, name, b) VALUES (UUID(), 'a', REPEAT('a', 5))")

	m := NewTestRunner(t, "nonintpk", "CHANGE COLUMN b b VARCHAR(255) NOT NULL") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestForRemainingTableArtifacts(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "remainingtbl",
		`CREATE TABLE remainingtbl (
			id INT NOT NULL PRIMARY KEY,
			name varchar(255) NOT NULL
		)`)

	m := NewTestRunner(t, "remainingtbl", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())

	// After migration, only the base table should remain — no _new, _old, or _chkpnt artifacts.
	stmt := `SELECT GROUP_CONCAT(table_name) FROM information_schema.tables where table_schema='test' and table_name LIKE '%remainingtbl%' ORDER BY table_name;`
	var tables string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), stmt).Scan(&tables))
	assert.Equal(t, "remainingtbl", tables)
}

func TestDropColumn(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "dropcol",
		`CREATE TABLE dropcol (
			id int(11) NOT NULL AUTO_INCREMENT,
			a varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			c varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	tt.SeedRows(t, "INSERT INTO dropcol (a, b, c) SELECT 'a', 'b', 'c'", 1)

	m := NewTestRunner(t, "dropcol", "DROP COLUMN b, ENGINE=InnoDB") // need both to ensure it is not instant!
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL) // need to ensure it uses full process.
	assert.NoError(t, m.Close())
}

func TestPartitionedTable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "part1",
		`CREATE TABLE part1 (
			id bigint(20) NOT NULL AUTO_INCREMENT,
			partition_id smallint(6) NOT NULL,
			created_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			initiated_at timestamp(3) NULL DEFAULT NULL,
			version int(11) NOT NULL DEFAULT '0',
			type varchar(50) DEFAULT NULL,
			token varchar(255) DEFAULT NULL,
			PRIMARY KEY (id,partition_id),
			UNIQUE KEY idx_token (token,partition_id)
		  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC
		  /*!50100 PARTITION BY LIST (partition_id)
		  (PARTITION p0 VALUES IN (0) ENGINE = InnoDB,
		   PARTITION p1 VALUES IN (1) ENGINE = InnoDB,
		   PARTITION p2 VALUES IN (2) ENGINE = InnoDB,
		   PARTITION p3 VALUES IN (3) ENGINE = InnoDB,
		   PARTITION p4 VALUES IN (4) ENGINE = InnoDB,
		   PARTITION p5 VALUES IN (5) ENGINE = InnoDB,
		   PARTITION p6 VALUES IN (6) ENGINE = InnoDB,
		   PARTITION p7 VALUES IN (7) ENGINE = InnoDB) */`)
	testutils.RunSQL(t, `insert into part1 values (1, 1, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 2, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 3, NOW(), NOW(), NOW(), 1, 'type', 'token2')`) //nolint: dupword

	m := NewTestRunner(t, "part1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestVarcharE2E(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "varchart1",
		`CREATE TABLE varchart1 (
			pk varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (pk)
		)`)
	// UUID() generates unique PKs so we can't use simple SeedRows doubling.
	// Use cubic JOIN growth for fast seeding with unique keys.
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM dual")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	_ = tt

	m := NewTestRunner(t, "varchart1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestPreRunChecksE2E(t *testing.T) {
	t.Parallel()
	// We test the checks in tests for that package, but we also want to test
	// that the checks run correctly when instantiating a migration.
	m := NewTestRunner(t, "test_checks_e2e", "engine=innodb", WithThreads(1))
	err := m.runChecks(t.Context(), check.ScopePreRun)
	assert.NoError(t, err)
}

func TestPreventConcurrentRuns(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `prevent_concurrent_runs`

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	defer utils.CloseAndLog(m)
	wg := sync.WaitGroup{}
	wg.Go(func() {
		// Shadow err to avoid a data race
		err := m.Run(t.Context())
		assert.Error(t, err)
		// The error can either be context cancelled or timed out.
		// Both are acceptable
		if !errors.Is(err, context.Canceled) {
			assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
		}
	})

	// Wait until m has reached the sentinel wait phase before starting m2.
	waitForStatus(t, m, status.WaitingOnSentinelTable)

	m2, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
	})
	assert.NoError(t, err)
	err = m2.Run(t.Context())
	defer utils.CloseAndLog(m2)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not acquire metadata lock")

	// Cancel the first migration rather than waiting for the sentinel timeout
	// (which could take up to sentinelWaitLimit).
	m.Cancel()
	wg.Wait()
}

func TestMigrationCancelledFromTableModification(t *testing.T) {
	t.Parallel()
	// This test covers the case where a migration is running
	// and the user modifies the table (e.g. with another ALTER).
	// The migration should detect this and cancel itself.
	// We use a long-running copy phase to give us time to do the modification.
	tt := testutils.NewTestTable(t, "t1modification",
		`CREATE TABLE t1modification (
			id int not null primary key auto_increment,
			col1 varbinary(1024),
			col2 varbinary(1024)
		) character set utf8mb4`)
	tt.SeedRows(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024)", 100000)

	m := NewTestRunner(t, "t1modification", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond)) // weak performance at copying.

	// Start the migration in a goroutine
	var wg sync.WaitGroup
	var gErr error
	wg.Go(func() {
		gErr = m.Run(t.Context())
	})

	// Wait until the copy phase has started.
	waitForStatus(t, m, status.CopyRows)

	// Now modify the table
	// instant DDL (applies quickly and will cause the migration to cancel)
	testutils.RunSQL(t, "ALTER TABLE t1modification ADD col3 INT")

	wg.Wait() // wait for the error to occur.

	require.Error(t, gErr)
	require.NoError(t, m.Close())
}

func TestCreateTableNameLength(t *testing.T) {
	t.Parallel()

	// A CREATE TABLE with a table name exceeding Spirit's manageable limit (56 chars)
	// should be rejected, since Spirit needs room for metadata suffixes like _<table>_chkpnt.
	longName := strings.Repeat("z", 57)
	assert.Greater(t, len(longName), check.MaxMigratableTableNameLength)

	m := NewTestRunnerFromStatement(t,
		fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL PRIMARY KEY)", longName))
	err := m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf("exceeds the maximum length of %d characters that Spirit can manage", check.MaxMigratableTableNameLength))
	assert.NoError(t, m.Close())

	// A CREATE TABLE with a table name at exactly the max manageable length (56 chars)
	// should be allowed.
	exactName := strings.Repeat("x", check.MaxMigratableTableNameLength)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", exactName))

	m = NewTestRunnerFromStatement(t,
		fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL PRIMARY KEY)", exactName))
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}
