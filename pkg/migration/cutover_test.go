package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCutOver(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS cutovert1, _cutovert1_new, _cutovert1_old, _cutovert1_chkpnt`)
	tbl := `CREATE TABLE cutovert1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	tbl = `CREATE TABLE _cutovert1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `CREATE TABLE _cutovert1_chkpnt (a int)`) // for binlog advancement

	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	testutils.RunSQL(t, `INSERT INTO cutovert1 VALUES (1, 2), (2,2)`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	assert.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "cutovert1")
	assert.NoError(t, t1.SetInfo(t.Context())) // required to extract PK.
	t1new := table.NewTableInfo(db, "test", "_cutovert1_new")
	t1old := "_cutovert1_old"
	logger := slog.Default()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	assert.NoError(t, err)
	assert.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	// the feed must be started.
	assert.NoError(t, feed.Run(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, dbconn.NewDBConfig(), logger)
	assert.NoError(t, err)

	err = cutover.Run(t.Context())
	assert.NoError(t, err)

	assert.Equal(t, 0, db.Stats().InUse) // all connections are returned

	// Verify that t1 has no rows (its lost because we only did cutover, not copy-rows)
	// and t1_old has 2 row.
	// Verify that t2 has one row.
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cutovert1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _cutovert1_old").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestMDLLockFails(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS mdllocks, _mdllocks_new, _mdllocks_old, _mdllocks_chkpnt`)
	tbl := `CREATE TABLE mdllocks (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	tbl = `CREATE TABLE _mdllocks_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `CREATE TABLE _mdllocks_chkpnt (a int)`) // for binlog advancement
	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	testutils.RunSQL(t, `INSERT INTO mdllocks VALUES (1, 2), (2,2)`)

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.LockWaitTimeout = 1

	db, err := dbconn.New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "mdllocks")
	assert.NoError(t, t1.SetInfo(t.Context())) // required to extract PK.
	t1new := table.NewTableInfo(db, "test", "_mdllocks_new")
	t1old := "test_old"
	logger := slog.Default()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	assert.NoError(t, err)
	assert.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	// the feed must be started.
	assert.NoError(t, feed.Run(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, config, logger)
	assert.NoError(t, err)

	// Before we cutover, we READ LOCK the table.
	// This will not fail the table lock but it will fail the rename.
	trx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	assert.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "LOCK TABLES mdllocks READ")
	assert.NoError(t, err)

	// Start the cutover. It will retry in a loop and fail
	// after about 15 seconds (3 sec timeout * 5 retries)
	// or in 5.7 it might fail because it can't find the RENAME in the processlist.
	err = cutover.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, trx.Rollback())
}

func TestInvalidOptions(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	logger := slog.Default()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS invalid_t1, _invalid_t1_new`)
	tbl := `CREATE TABLE invalid_t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	tbl = `CREATE TABLE _invalid_t1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	// Invalid options
	_, err = NewCutOver(db, []*cutoverConfig{{}}, nil, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
	t1 := table.NewTableInfo(db, "test", "invalid_t1")
	assert.NoError(t, t1.SetInfo(t.Context())) // required to extract PK.
	t1new := table.NewTableInfo(db, "test", "_invalid_t1_new")
	t1old := "test_old"
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	assert.NoError(t, err)
	assert.NoError(t, feed.AddSubscription(t1, t1new, chunker))

	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: t1old,
	}}, feed, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: "",
	}}, feed, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
}

// TestCutoverAtomicityWithConcurrentWrites tests that if we modify the cutover
// so that it never renames the new table into the place of the existing table,
// we have consistency between the _old and _new tables.
//
// Since rename is atomic this is an injected failure to be able to consistency
// check. It's not an actual failure mode. But because the checksum runs before
// we rename there is a window where inconsistency could technically be introduced.
//
// This test proves that the window does not introduce inconsistency, even when
// there are concurrent writes happening that are trying to introduce it.
func TestCutoverAtomicityWithConcurrentWrites(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1concurrent, _t1concurrent_new, _t1concurrent_old`)

	table := `CREATE TABLE t1concurrent (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		x_token VARCHAR(36) NOT NULL,
		cents INT NOT NULL,
		currency VARCHAR(3) NOT NULL,
		s_token VARCHAR(36) NOT NULL,
		r_token VARCHAR(36) NOT NULL,
		version INT NOT NULL DEFAULT 1,
		c1 VARCHAR(20),
		c2 VARCHAR(200),
		c3 VARCHAR(10),
		t1 DATETIME,
		t2 DATETIME,
		t3 DATETIME,
		b1 TINYINT,
		b2 TINYINT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		UNIQUE KEY idx_x_token (x_token),
		KEY idx_s_token (s_token),
		KEY idx_r_token (r_token)
	)`
	testutils.RunSQL(t, table)

	// Insert some initial data
	testutils.RunSQL(t, `INSERT INTO t1concurrent (x_token, cents, currency, s_token, r_token, version, created_at, updated_at)
		VALUES ('initial-1', 100, 'USD', 'sender-1', 'receiver-1', 1, NOW(), NOW())`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Open connection for concurrent writes
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Start concurrent write load
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var errorCount atomic.Int64
	numThreads := 4

	// Start write threads
	for range numThreads {
		wg.Go(func() {
			migrationConcurrentWriteThread(ctx, db, &writeCount, &errorCount)
		})
	}

	// Give the writers a moment to start
	time.Sleep(100 * time.Millisecond)

	// Create and configure the migration with a custom cutover algorithm
	// that intentionally fails after renaming the original table
	migration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		Table:           "t1concurrent",
		Alter:           "ENGINE=InnoDB",
		TargetChunkTime: 100 * time.Millisecond,
		useTestCutover:  true, // indicates we want the test cutover
	}

	// Run the migration - we expect it to fail with our intentional error
	err = migration.Run()

	// Stop the write threads
	// When the migration "fails" they won't be able to insert anyway,
	// because the table will no longer exist.
	cancel()
	wg.Wait()

	// Report statistics
	t.Logf("Completed %d writes with %d errors during migration",
		writeCount.Load(), errorCount.Load())

	// The migration should fail - either with our intentional error or because
	// the table doesn't exist after the partial rename
	assert.Error(t, err, "Migration should fail")

	// The partial cutover should have renamed t1concurrent to _t1concurrent_old
	// Let's verify both tables exist
	var oldTableExists, newTableExists bool
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_t1concurrent_old'").Scan(&oldTableExists)
	assert.NoError(t, err)
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_t1concurrent_new'").Scan(&newTableExists)
	assert.NoError(t, err)

	assert.True(t, oldTableExists, "The _old table should exist after partial cutover")
	assert.True(t, newTableExists, "The _new table should exist after partial cutover")

	// Verify that the old table (_old) and the new table (_new) have identical checksums.
	// This proves that all changes were captured correctly up to the point of cutover.
	// We use assert.Eventually to allow a brief window for any residual replication to
	// settle. The cutover flushes binlog under lock, but in CI environments the Docker
	// MySQL containers can have variable I/O latency that delays final event delivery.
	checksumQuery := "SELECT BIT_XOR(CRC32(CONCAT_WS(',', id, x_token, cents, currency, s_token, r_token, version, IFNULL(c1,''), IFNULL(c2,''), IFNULL(c3,''), IFNULL(t1,''), IFNULL(t2,''), IFNULL(t3,''), IFNULL(b1,''), IFNULL(b2,''), created_at, updated_at))) FROM %s"

	var oldChecksum, newChecksum string
	assert.Eventually(t, func() bool {
		err1 := db.QueryRowContext(t.Context(), fmt.Sprintf(checksumQuery, "_t1concurrent_old")).Scan(&oldChecksum)
		err2 := db.QueryRowContext(t.Context(), fmt.Sprintf(checksumQuery, "_t1concurrent_new")).Scan(&newChecksum)
		return err1 == nil && err2 == nil && oldChecksum == newChecksum
	}, 5*time.Second, 250*time.Millisecond, "Checksums should eventually match between old and new tables")

	t.Logf("Old table checksum: %s, New table checksum: %s", oldChecksum, newChecksum)

	// Also verify row counts match
	var oldCount, newCount int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _t1concurrent_old").Scan(&oldCount)
	assert.NoError(t, err)
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _t1concurrent_new").Scan(&newCount)
	assert.NoError(t, err)

	t.Logf("Old table count: %d, New table count: %d", oldCount, newCount)
	assert.Equal(t, oldCount, newCount, "Row counts should match")
}

// migrationConcurrentWriteThread simulates concurrent write load during migration
func migrationConcurrentWriteThread(ctx context.Context, db *sql.DB, writeCount, errorCount *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := doOneMigrationWriteLoop(ctx, db); err != nil {
				errorCount.Add(1)
				// Continue on error - some errors are expected during cutover
			} else {
				writeCount.Add(1)
			}
		}
	}
}

// doOneMigrationWriteLoop performs one iteration of insert + update + reads
func doOneMigrationWriteLoop(ctx context.Context, db *sql.DB) error {
	trx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = trx.Rollback()
		}
	}()

	xtoken := fmt.Sprintf("x-%d", time.Now().UnixNano())
	stoken := fmt.Sprintf("s-%d", time.Now().UnixNano())
	rtoken := fmt.Sprintf("r-%d", time.Now().UnixNano())

	//nolint: dupword
	_, err = trx.ExecContext(ctx, `INSERT INTO t1concurrent (x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at)
		VALUES (?, 100, 'USD', ?, ?, 1, HEX(RANDOM_BYTES(10)), HEX(RANDOM_BYTES(100)), HEX(RANDOM_BYTES(5)), NOW(), NOW(), NOW(), 1, 2, NOW(), NOW())`,
		xtoken, stoken, rtoken)
	if err != nil {
		return err
	}

	// Update
	_, err = trx.ExecContext(ctx, `UPDATE t1concurrent SET version = 2, updated_at = NOW() WHERE x_token = ?`, xtoken)
	if err != nil {
		return err
	}

	// Do some cached reads
	var rows *sql.Rows
	for range 10 {
		rows, err = trx.QueryContext(ctx, `SELECT id, x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at FROM t1concurrent WHERE x_token = ?`, xtoken)
		if err != nil {
			return err
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	return trx.Commit()
}

// --- Cutover lifecycle tests (skip/drop, deferred cutover) ---

func TestSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "skipdrop_test",
		`CREATE TABLE skipdrop_test (
			pk int UNSIGNED NOT NULL,
			PRIMARY KEY(pk)
		)`)

	m := NewTestRunner(t, "skipdrop_test", "ENGINE=InnoDB",
		WithThreads(4), WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))

	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err := m.db.QueryRowContext(t.Context(), query).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount)
	assert.NoError(t, m.Close())
}

func TestDropAfterCutover(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "drop_test",
		`CREATE TABLE drop_test (
			pk int UNSIGNED NOT NULL,
			PRIMARY KEY(pk)
		)`)

	m := NewTestRunner(t, "drop_test", "ENGINE=InnoDB", WithThreads(4))
	require.NoError(t, m.Run(t.Context()))

	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err := m.db.QueryRowContext(t.Context(), query).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOver(t *testing.T) {
	t.Skip("skipping: this test waits for sentinelWaitLimit to expire, which is too slow with the current 48 hour limit")
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `deferred_cutover`
	newName := fmt.Sprintf("_%s_new", tableName)
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
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
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
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

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c
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
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e_stage`

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

	err = <-c
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

func TestSkipDropAfterCutoverLongTableName(t *testing.T) {
	t.Parallel()

	// A table name at the normal max (56 chars) should work with SkipDropAfterCutover.
	// Previously this would have been rejected because the timestamp format exceeds 64 chars,
	// but now we truncate the table name portion in the old table name.
	tableName := "a_fifty_six_character_table_name_that_fits_normal_limits"
	assert.Equal(t, 56, len(tableName))

	testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL AUTO_INCREMENT,
		PRIMARY KEY(pk)
	)`, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithThreads(4), WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))

	// Verify the old table exists (with truncated name + timestamp)
	oldName := m.changes[0].oldTableName()
	assert.LessOrEqual(t, len(oldName), 64, "old table name should fit within 64 chars")

	var tableCount int
	err := m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, oldName)).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount, "old table should exist after SkipDropAfterCutover")
	assert.NoError(t, m.Close())
}
