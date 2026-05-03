package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestVarcharNonBinaryComparable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "nonbinarycompatt1", `CREATE TABLE nonbinarycompatt1 (
		uuid varchar(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`)

	m := NewTestRunner(t, "nonbinarycompatt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestPartitioningSyntax tests that ALTERs that don't support ALGORITHM assertion
// (such as PARTITION BY) still work.
func TestPartitioningSyntax(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "partt1", `CREATE TABLE partt1 (
		id INT NOT NULL PRIMARY KEY auto_increment,
		name varchar(255) NOT NULL
	)`)

	m := NewTestRunner(t, "partt1", "PARTITION BY KEY() PARTITIONS 8")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

func TestVarbinary(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "varbinaryt1", `CREATE TABLE varbinaryt1 (
		uuid varbinary(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`)
	tt.SeedRows(t, "INSERT INTO varbinaryt1 (uuid, name) SELECT UUID(), REPEAT('a', 200)", 1)

	m := NewTestRunner(t, "varbinaryt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00
// can still be migrated. From https://github.com/block/spirit/issues/277
func TestDataFromBadSqlMode(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "badsqlt1", `CREATE TABLE badsqlt1 (
		id int not null primary key auto_increment,
		d date NOT NULL,
		t timestamp NOT NULL
	)`)
	testutils.RunSQL(t, "INSERT IGNORE INTO badsqlt1 (d, t) VALUES ('0000-00-00', '0000-00-00 00:00:00'),('2020-02-00', '2020-02-30 00:00:00')")

	m := NewTestRunner(t, "badsqlt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

// TestOnline tests the DDL algorithm detection: instant, inplace, and copy.
func TestOnline(t *testing.T) {
	t.Parallel()

	// Test 1: CHANGE COLUMN type requires copy (not inplace)
	testutils.NewTestTable(t, "testonline", `CREATE TABLE testonline (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestRunner(t, "testonline", "CHANGE COLUMN b b int(11) NOT NULL") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// Test 2: ADD COLUMN uses instant DDL
	testutils.NewTestTable(t, "testonline2", `CREATE TABLE testonline2 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline2", "ADD c int(11) NOT NULL")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInplaceDDL)
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	// Test 3: ADD INDEX requires copy (not instant or inplace)
	testutils.NewTestTable(t, "testonline3", `CREATE TABLE testonline3 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline3", "ADD INDEX(b)")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL) // ADD INDEX operations now always require copy
	require.NoError(t, m.Close())

	// Test 4: DROP INDEX uses inplace DDL
	testutils.NewTestTable(t, "testonline4", `CREATE TABLE testonline4 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline4", "drop index name, drop index b")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// Test 5: DROP INDEX + ADD COLUMN combines instant and inplace — neither applies alone
	testutils.NewTestTable(t, "testonline5", `CREATE TABLE testonline5 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline5", "drop index name, add column c int")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL) // combines INSTANT and INPLACE operations
	require.NoError(t, m.Close())

	// Test 6: ADD PARTITION (hash) — requires lock, not inplace
	testutils.NewTestTable(t, "testonline6", `CREATE TABLE testonline6 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) PARTITION BY HASH (id) PARTITIONS 4`)
	m = NewTestRunner(t, "testonline6", "add partition partitions 4")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL) // hash/key partitioned tables require a lock
	require.NoError(t, m.Close())

	// Test 7: ADD PARTITION (range) — inplace without lock
	testutils.NewTestTable(t, "testonline7", `CREATE TABLE testonline7 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (100000),
		PARTITION p1 VALUES LESS THAN (200000)
	)`)
	m = NewTestRunner(t, "testonline7", "add partition (partition p2 values less than (300000))")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.True(t, m.usedInplaceDDL) // range/list partitioned tables can run inplace without a lock
	require.NoError(t, m.Close())
}

// TestTableLength tests that Spirit rejects table names exceeding 56 characters.
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
	require.Error(t, err)
	require.ErrorContains(t, err, "table name must be less than 56 characters")
	require.NoError(t, m.Close())

	// Run again — same error (tests that first run didn't leave artifacts that block second run)
	m = NewTestRunner(t, "thisisareallylongtablenamethisisareallylongtablename60charac", "ENGINE=InnoDB")
	err = m.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "table name must be less than 56 characters")
	require.NoError(t, m.Close())
}

// TestCreateTableNameLength tests that CREATE TABLE statements with long names are rejected.
func TestCreateTableNameLength(t *testing.T) {
	t.Parallel()

	// A CREATE TABLE with a table name exceeding Spirit's manageable limit (56 chars)
	// should be rejected.
	longName := strings.Repeat("z", 57)
	require.Greater(t, len(longName), check.MaxMigratableTableNameLength)

	m := NewTestMigration(t, WithStatement(fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL PRIMARY KEY)", longName)))
	err := m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("exceeds the maximum length of %d characters that Spirit can manage", check.MaxMigratableTableNameLength))

	// A CREATE TABLE with a table name at exactly the max manageable length (56 chars)
	// should be allowed.
	exactName := strings.Repeat("x", check.MaxMigratableTableNameLength)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", exactName))
	defer func() {
		// Use background context — t.Context() is canceled during cleanup.
		db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		if err == nil {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", exactName))
			_ = db.Close()
		}
	}()

	m = NewTestMigration(t, WithStatement(fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL PRIMARY KEY)", exactName)))
	err = m.Run()
	require.NoError(t, err)
}

// TestAddUniqueIndexChecksumEnabled tests that adding a UNIQUE index on non-unique data
// fails with a checksum error, and succeeds after the duplicate is removed.
func TestAddUniqueIndexChecksumEnabled(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "uniqmytable", `CREATE TABLE uniqmytable (
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
	require.Error(t, err) // not unique
	require.NoError(t, m.Close())

	// Fix the data and retry
	testutils.RunSQL(t, "DELETE FROM uniqmytable WHERE b = REPEAT('a', 200) LIMIT 1")
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_chkpnt`) // clear checkpoint
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_new`)    // cleanup temp table

	m2 := NewTestRunner(t, "uniqmytable", "ADD UNIQUE INDEX b (b)")
	err = m2.Run(t.Context())
	require.NoError(t, err)
	require.NoError(t, m2.Close())

	// Verify the index exists
	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='uniqmytable' AND index_name='b'").Scan(&count))
	require.Equal(t, 1, count)
}

func TestChangeNonIntPK(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "nonintpk", `CREATE TABLE nonintpk (
		pk varbinary(36) NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL,
		b varchar(10) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO nonintpk (pk, name, b) SELECT UUID(), 'a', REPEAT('a', 5)", 1)

	m := NewTestRunner(t, "nonintpk", "CHANGE COLUMN b b VARCHAR(255) NOT NULL") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

func TestDropColumn(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "dropcol", `CREATE TABLE dropcol (
		id int(11) NOT NULL AUTO_INCREMENT,
		a varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		c varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO dropcol (id, a, b, c) VALUES (1, 'a', 'b', 'c')`)

	m := NewTestRunner(t, "dropcol", "DROP COLUMN b, ENGINE=InnoDB") // need both to ensure it is not instant
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

func TestPartitionedTable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "part1", `CREATE TABLE part1 (
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
	testutils.RunSQL(t, `INSERT INTO part1 VALUES (1, 1, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 2, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 3, NOW(), NOW(), NOW(), 1, 'type', 'token2')`) //nolint: dupword

	m := NewTestRunner(t, "part1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestVarcharE2E tests migration with a large table using varchar primary keys.
func TestVarcharE2E(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "varchart1", `CREATE TABLE varchart1 (
		pk varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (pk)
	)`)
	tt.SeedRows(t, "INSERT INTO varchart1 (pk, b) SELECT UUID(), 'abcd'", 100000)

	m := NewTestRunner(t, "varchart1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestChunkerPrefetching tests that the chunker handles large ID gaps correctly.
func TestChunkerPrefetching(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "prefetchtest", `CREATE TABLE prefetchtest (
		id BIGINT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`)
	// Insert about 11K rows, then add large ID gaps to test prefetching.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) VALUES (NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 10000`)

	// Insert far-off IDs to create large gaps that test the prefetcher.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (300000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (600000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (900000000000, NULL)`)

	m := NewTestRunner(t, "prefetchtest", "engine=innodb")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestPreRunChecksE2E tests that pre-run checks execute correctly during migration setup.
func TestPreRunChecksE2E(t *testing.T) {
	t.Parallel()
	m := NewTestRunner(t, "test_checks_e2e", "engine=innodb", WithThreads(1))
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	err = m.runChecks(t.Context(), check.ScopePreRun)
	require.NoError(t, err)
	require.NoError(t, m.Close())
}

// TestPreventConcurrentRuns tests that two migrations on the same table cannot run concurrently.
func TestPreventConcurrentRuns(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `prevent_concurrent_runs`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, checkpointTableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())
	defer utils.CloseAndLog(m)

	wg := sync.WaitGroup{}
	wg.Go(func() {
		err := m.Run(t.Context())
		require.Error(t, err)
		if !errors.Is(err, context.Canceled) {
			require.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
		}
	})

	// Wait until m has reached the sentinel wait phase before starting m2.
	waitForStatus(t, m, status.WaitingOnSentinelTable)

	m2 := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithThreads(4))
	err := m2.Run(t.Context())
	defer utils.CloseAndLog(m2)
	require.Error(t, err)
	require.ErrorContains(t, err, "could not acquire metadata lock")

	m.Cancel()
	wg.Wait()
}

// TestMigrationCancelledFromTableModification tests that a migration detects
// concurrent DDL on the source table and cancels itself.
func TestMigrationCancelledFromTableModification(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1modification", `CREATE TABLE t1modification (
		id int not null primary key auto_increment,
		col1 varbinary(1024),
		col2 varbinary(1024)
	) character set utf8mb4`)
	tt.SeedRows(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024)", 100000)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE t1modification ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond))

	wg := sync.WaitGroup{}
	var gErr error
	wg.Go(func() {
		gErr = m.Run(t.Context())
	})

	waitForStatus(t, m, status.CopyRows)

	// Apply instant DDL — migration should detect this and cancel itself.
	testutils.RunSQL(t, "ALTER TABLE t1modification ADD col3 INT")

	wg.Wait()
	require.Error(t, gErr)
	require.NoError(t, m.Close())
}
