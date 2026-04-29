package migration

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Wait until we are at least copying rows
// before we dump a checkpoint, then wait for first
// successful checkpoint.
func waitForCheckpoint(t *testing.T, runner *Runner) {
	t.Helper()
	for runner.status.Get() < status.CopyRows {
		time.Sleep(time.Millisecond)
	}
	for {
		err := runner.DumpCheckpoint(t.Context())
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Test int to bigint primary key while resuming from checkpoint.
func TestChangeIntToBigIntPKResumeFromChkPt(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "bigintpk",
		`CREATE TABLE bigintpk (
			pk int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(255) NOT NULL,
			b varchar(10) NOT NULL,
			version bigint unsigned NOT NULL DEFAULT '1' COMMENT 'Used for optimistic concurrency.'
		)`)
	// Insert initial data, there needs to be enough that it doesn't just finish
	// the full copy before the first checkpoint can be written.
	tt.SeedRows(t, "INSERT INTO bigintpk (name, b) SELECT 'a', 'a'", 1000)

	m := NewTestRunner(t, "bigintpk", "modify column pk bigint unsigned not null auto_increment",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := m.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	waitForCheckpoint(t, m)

	// Between cancel and Close() every resource is freed.
	assert.NoError(t, m.Close())
	cancel()

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO bigintpk (name,b) VALUES('t', 't')")
	// Start a new migration with the same parameters. Let it complete.
	m2 := NewTestRunner(t, "bigintpk", "modify column pk bigint unsigned not null auto_increment")
	require.NoError(t, m2.Run(t.Context()))
	assert.True(t, m2.usedResumeFromCheckpoint)
	assert.NoError(t, m2.Close())
}

func TestCheckpoint(t *testing.T) {
	testutils.NewTestTable(t, "cpt1",
		`CREATE TABLE cpt1 (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			id2 INT NOT NULL,
			pad VARCHAR(100) NOT NULL default 0)`)
	// Exact row count matters for hardcoded progress assertions below.
	testutils.RunSQL(t, `INSERT INTO cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQL(t, `INSERT INTO cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1`)
	testutils.RunSQL(t, `INSERT INTO cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `INSERT INTO cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `INSERT INTO cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 LIMIT 10000`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	preSetup := func() *Runner {
		r, err := NewRunner(&Migration{
			Host:             cfg.Addr,
			Username:         cfg.User,
			Password:         &cfg.Passwd,
			Database:         cfg.DBName,
			Threads:          1,
			TargetChunkTime:  100 * time.Millisecond,
			Table:            "cpt1",
			Alter:            "ENGINE=InnoDB",
			useTestThrottler: true,
		})
		require.NoError(t, err)
		assert.Equal(t, "initial", r.status.Get().String())
		// Usually we would call r.Run() but we want to step through
		// the migration process manually.
		r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		require.NoError(t, err)
		r.dbConfig = dbconn.NewDBConfig()

		// Get Table Info
		r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
		err = r.changes[0].table.SetInfo(t.Context())
		require.NoError(t, err)
		require.NoError(t, r.changes[0].dropOldTable(t.Context()))
		return r
	}
	r := preSetup()
	// migrationRunner.Run usually calls r.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.
	assert.Error(t, r.resumeFromCheckpoint(t.Context()))
	// So we proceed with the initial steps.
	assert.NoError(t, r.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling r.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	r.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", r.status.Get().String())

	assert.Contains(t, r.Status(), `migration status: state=copyRows copy-progress=0/11040 0.00% binlog-deltas=0`)

	// first chunk.
	chunk1, err := r.copyChunker.Next()
	assert.NoError(t, err)

	chunk2, err := r.copyChunker.Next()
	assert.NoError(t, err)

	chunk3, err := r.copyChunker.Next()
	assert.NoError(t, err)

	// Assert there is no watermark yet, because we've not finished
	// copying any of the chunks.
	_, err = r.copyChunker.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, r.DumpCheckpoint(t.Context()))

	ccopier, ok := r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	// Let's complete them in the order of 2, 1, 3. When 2 phones home first
	// it should be queued. Then when 1 phones home it should apply and de-queue 2.
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk2))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk1))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk3))

	time.Sleep(time.Second) // wait for status to be updated.
	assert.Contains(t, r.Status(), `migration status: state=copyRows copy-progress=3000/11040 27.17% binlog-deltas=0`)

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.
	watermark, err := r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, r.DumpCheckpoint(t.Context()))

	// Clean up first runner
	assert.NoError(t, r.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	r = preSetup()
	defer utils.CloseAndLog(r)
	// Start the binary log feed just before copy rows starts.
	// replClient.Run() is already called in resumeFromCheckpoint.
	assert.NoError(t, r.resumeFromCheckpoint(t.Context()))
	// This opens the table at the checkpoint (table.OpenAtWatermark())
	// which sets the chunkPtr at the LowerBound. It also has to position
	// the watermark to this point so new watermarks "align" correctly.
	// So lets now call NextChunk to verify.

	ccopier, ok = r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	chunk, err := r.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "1001", chunk.LowerBound.Value[0].String())
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// It's ideally not typical but you can still dump checkpoint from
	// a restored checkpoint state. We won't have advanced anywhere from
	// the last checkpoint because on restore, the LowerBound is taken.
	watermark, err = r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, r.DumpCheckpoint(t.Context()))

	// Let's confirm we do advance the watermark.
	for range 10 {
		chunk, err = r.copyChunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	watermark, err = r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"11001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"12001\"],\"Inclusive\":false}}", watermark)
}

func TestCheckpointRestore(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cpt2",
		`CREATE TABLE cpt2 (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			id2 INT NOT NULL,
			pad VARCHAR(100) NOT NULL default 0)`)

	r := NewTestRunner(t, "cpt2", "ENGINE=InnoDB")
	assert.Equal(t, "initial", r.status.Get().String())
	// Usually we would call r.Run() but we want to step through
	// the migration process manually.
	var err error
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	// Get Table Info
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	err = r.changes[0].table.SetInfo(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, r.changes[0].dropOldTable(t.Context()))

	// Proceed with the initial steps.
	assert.NoError(t, r.newMigration(t.Context()))

	// Now insert a fake checkpoint, this uses a known bad value
	// from issue #125
	watermark := "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"53926425\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"53926425\"],\"Inclusive\":false}}"
	binlog := r.replClient.GetBinlogApplyPosition()
	err = dbconn.Exec(t.Context(), r.db, `INSERT INTO %n.%n
	(copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement)
	VALUES
	(%?,  %?, %?, %?, %?)`,
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		watermark,
		"",
		binlog.Name,
		binlog.Pos,
		r.migration.Statement,
	)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	r2 := NewTestRunner(t, "cpt2", "ENGINE=InnoDB")
	require.NoError(t, r2.Run(t.Context()))
	assert.True(t, r2.usedResumeFromCheckpoint)
	assert.NoError(t, r2.Close())
}

// https://github.com/block/spirit/issues/381
func TestCheckpointRestoreBinaryPK(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	tt := testutils.NewTestTable(t, "binarypk",
		`CREATE TABLE binarypk (
			main_id varbinary(16) NOT NULL,
			sub_id varchar(36) CHARACTER SET latin1 COLLATE latin1_swedish_ci GENERATED ALWAYS AS (jsonbody->>'$._id') STORED NOT NULL,
			jsonbody json NOT NULL,
			PRIMARY KEY (main_id,sub_id)
		)`)
	tt.SeedRows(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024)))`, 10000)

	r := NewTestRunner(t, "binarypk", "ENGINE=InnoDB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())
	assert.Equal(t, "initial", r.status.Get().String())
	// Usually we would call r.Run() but we want to step through
	// the migration process manually.
	var err error
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	// Get Table Info
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	err = r.changes[0].table.SetInfo(ctx)
	assert.NoError(t, err)
	assert.NoError(t, r.changes[0].dropOldTable(ctx))

	assert.NoError(t, r.newMigration(t.Context()))

	ccopier, ok := r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	for range 3 {
		chunk, err := r.copyChunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, ccopier.CopyChunk(ctx, chunk))
	}
	// Dump checkpoint and close runner.
	assert.NoError(t, r.DumpCheckpoint(t.Context()))
	assert.NoError(t, r.Close())
	// Try and resume and then check if we used a checkpoint for resuming.
	r2 := NewTestRunner(t, "binarypk", "ENGINE=InnoDB")
	require.NoError(t, r2.Run(t.Context()))
	assert.True(t, r2.usedResumeFromCheckpoint) // managed to resume.
	assert.NoError(t, r2.Close())
}

func TestCheckpointResumeDuringChecksum(t *testing.T) {
	t.Parallel()
	// Create unique database for this test — needed for sentinel table isolation.
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE cptresume (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _spirit_sentinel (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume a JOIN cptresume b JOIN cptresume c`)

	r := NewTestRunner(t, "cptresume", "ENGINE=InnoDB",
		WithDBName(dbName), WithThreads(4), WithTargetChunkTime(100*time.Millisecond), WithRespectSentinel())

	// Call r.Run() with our context in a go-routine.
	// When we see that we are waiting on the sentinel table,
	// we then manually start the first bits of checksum, and then close()
	// We should be able to resume from the checkpoint into the checksum state.
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := r.Run(ctx)
		assert.Error(t, err) // context cancelled
	}()
	for r.status.Get() < status.WaitingOnSentinelTable {
		// Wait for the sentinel table.
		time.Sleep(time.Millisecond)
	}

	assert.NoError(t, r.checksum(t.Context()))       // run the checksum, the original Run is blocked on sentinel.
	assert.NoError(t, r.DumpCheckpoint(t.Context())) // dump a checkpoint with the watermark.
	assert.NoError(t, r.Close())                     // close the run first to avoid race conditions.
	cancel()                                         // unblock the original waiting on sentinel.

	// drop the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE _spirit_sentinel`)

	// insert a couple more rows (should not change anything)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('b', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('c', 100) FROM dual`)

	// Start again as a new runner — should resume from checkpoint.
	r2 := NewTestRunner(t, "cptresume", "ENGINE=InnoDB",
		WithDBName(dbName), WithThreads(4), WithTargetChunkTime(100*time.Millisecond))
	require.NoError(t, r2.Run(t.Context()))
	assert.True(t, r2.usedResumeFromCheckpoint)
	assert.NoError(t, r2.Close())
}

func TestCheckpointDifferentRestoreOptions(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "cpt1difft1",
		`CREATE TABLE cpt1difft1 (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			id2 INT NOT NULL,
			pad VARCHAR(100) NOT NULL default 0)`)
	tt.SeedRows(t, "INSERT INTO cpt1difft1 (id2, pad) SELECT 1, REPEAT('a', 100)", 1000)

	preSetup := func(alter string) *Runner {
		m := NewTestRunner(t, "cpt1difft1", alter,
			WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())
		assert.Equal(t, "initial", m.status.Get().String())
		// Usually we would call m.Run() but we want to step through
		// the migration process manually.
		var err error
		m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		m.dbConfig = dbconn.NewDBConfig()
		// Get Table Info
		m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
		err = m.changes[0].table.SetInfo(t.Context())
		assert.NoError(t, err)
		assert.NoError(t, m.changes[0].dropOldTable(t.Context()))
		return m
	}

	m := preSetup("ADD COLUMN id3 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	// migrationRunner.Run usually calls m.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.

	assert.Error(t, m.resumeFromCheckpoint(t.Context()))

	assert.NoError(t, m.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	// m.copier.StartTime = time.Now()
	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// first chunk.
	chunk1, err := m.copyChunker.Next()
	assert.NoError(t, err)

	chunk2, err := m.copyChunker.Next()
	assert.NoError(t, err)

	chunk3, err := m.copyChunker.Next()
	assert.NoError(t, err)

	// There is no watermark yet.
	_, err = m.copyChunker.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, m.DumpCheckpoint(t.Context()))

	ccopier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk2))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk1))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk3))

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.

	watermark, err := m.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, m.DumpCheckpoint(t.Context()))

	// Close m
	assert.NoError(t, m.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	m = preSetup("ADD COLUMN id4 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	assert.Error(t, m.resumeFromCheckpoint(t.Context())) // it should error because the ALTER does not match.
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2E(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chkpresumetest",
		`CREATE TABLE chkpresumetest (
			id int(11) NOT NULL AUTO_INCREMENT,
			pad varbinary(1024) NOT NULL,
			PRIMARY KEY (id)
		)`)
	tt.SeedRows(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024)", 100000)

	// use as slow as possible here: we want the copy to be still running
	// when we kill it once we have a checkpoint saved.
	runner := NewTestRunner(t, "chkpresumetest", "ADD INDEX(pad);",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())

	go func() {
		err := runner.Run(t.Context())
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	waitForCheckpoint(t, runner)

	// Close() before cancelFunc() to avoid race conditions.
	assert.NoError(t, runner.Close())
	runner.cancelFunc()

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest LIMIT 1000")
	// Start a new migration with the same parameters. Let it complete.
	m := NewTestRunner(t, "chkpresumetest", "ADD INDEX(pad);",
		WithThreads(4), WithTargetChunkTime(5*time.Second))
	require.NoError(t, m.Run(t.Context()))
	assert.True(t, m.usedResumeFromCheckpoint)
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2ECompositeVarcharPK(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "compositevarcharpk",
		`CREATE TABLE compositevarcharpk (
			token varchar(128) NOT NULL,
			version varchar(255) NOT NULL,
			state varchar(255) NOT NULL,
			source varchar(128) NOT NULL,
			created_at datetime(3) NOT NULL,
			updated_at datetime(3) NOT NULL,
			PRIMARY KEY (token,version)
		)`)
	tt.SeedRows(t, `INSERT INTO compositevarcharpk (token, version, state, source, created_at, updated_at)
		SELECT HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)`, 10000)
	// Add a second version for each token.
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
		a.token, '2', 'active', 'test', NOW(3), NOW(3)
		FROM compositevarcharpk a WHERE version='1'`)

	runner := NewTestRunner(t, "compositevarcharpk", "ENGINE=InnoDB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	waitForCheckpoint(t, runner)

	// Close() before cancel() to avoid race conditions.
	assert.NoError(t, runner.Close())
	cancel()

	m2 := NewTestRunner(t, "compositevarcharpk", "ENGINE=InnoDB",
		WithThreads(2), WithTargetChunkTime(5*time.Second))
	require.NoError(t, m2.Run(t.Context()))
	assert.True(t, m2.usedResumeFromCheckpoint)
	assert.NoError(t, m2.Close())
}

func TestResumeFromCheckpointStrict(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "resumestricttest",
		`CREATE TABLE resumestricttest (
			id int(11) NOT NULL AUTO_INCREMENT,
			pad varbinary(1024) NOT NULL,
			PRIMARY KEY (id)
		)`)
	tt.SeedRows(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024)", 100000)

	// Kick off a migration with --strict enabled and let it run until the first checkpoint is available
	ctx, cancel := context.WithCancel(t.Context())
	runner := NewTestRunner(t, "resumestricttest", "ADD INDEX(pad);",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithStrict(), WithTestThrottler())

	done := make(chan struct{})
	go func() {
		defer close(done)
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	waitForCheckpoint(t, runner)

	// Cancel context first to signal goroutines to stop, then Close() to clean up resources.
	cancel()
	assert.NoError(t, runner.Close())
	<-done // Wait for the goroutine to finish

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest LIMIT 1000")

	// Start a _different_ migration on the same table. We don't expect this to work when --strict is enabled
	// since the --alter doesn't match what is recorded in the checkpoint table
	runner2 := NewTestRunner(t, "resumestricttest", "ENGINE=INNODB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithStrict())
	err := runner2.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorIs(t, err, status.ErrMismatchedAlter)
	assert.NoError(t, runner2.Close())

	// We should be able to force the migration to run even though there's a mismatched --alter
	// by disabling --strict
	runner3 := NewTestRunner(t, "resumestricttest", "ENGINE=INNODB", WithThreads(4))
	require.NoError(t, runner3.Run(t.Context()))
	assert.False(t, runner3.usedResumeFromCheckpoint)
	assert.NoError(t, runner3.Close())
}

// TestResumeFromCheckpointPhantom tests that there is not a phantom row issue
// when resuming from checkpoint. i.e. consider the following scenario:
// 1) A new row is inserted at the end of the table, and the copier copies it.. but the low watermark never advances past this point
// 2) The row is then deleted after it’s been copied (but the binary log doesn't get to this point)
// 3) A resume occurs
// 4) The insert and delete tracking ignore the row because it’s above the high watermark.
// 5) The INSERT..SELECT only inserts new rows, it doesn't delete non-conflicting existing rows.
// This leaves a broken state because the _new table has a row that should have been deleted.
//
// The fix for this is simple:
// - When resuming from checkpoint, we need to initialize the high watermark from a SELECT MAX(key) FROM the _new table.
// - If this is done correctly, then on resume the DELETE will no longer be ignored.
func TestResumeFromCheckpointPhantom(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "phantomtest",
		`CREATE TABLE phantomtest (
			id int(11) NOT NULL AUTO_INCREMENT,
			pad varbinary(1024) NOT NULL,
			PRIMARY KEY (id)
		)`)
	// Exactly 10 rows needed — the test asserts MaxValue() == "10".
	// Can't use SeedRows (doubling produces 16, not 10).
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")

	m := NewTestRunner(t, "phantomtest", "ENGINE=InnoDB",
		WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())
	ctx, cancel := context.WithCancel(t.Context())

	// Do the initial setup.
	var err error
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	assert.NoError(t, m.changes[0].table.SetInfo(ctx))

	assert.NoError(t, m.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// We step through this manually using the unbuffered copier, since we want
	// to checkpoint after a few chunks.

	copier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// first chunk.
	chunk, err := m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` < 1", chunk.String())
	err = copier.CopyChunk(ctx, chunk)
	assert.NoError(t, err)

	// second chunk
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String())
	err = copier.CopyChunk(ctx, chunk)
	assert.NoError(t, err)

	// now we insert a row in the range of the third chunk
	testutils.RunSQL(t, "INSERT INTO phantomtest (id, pad) VALUES (1002, RANDOM_BYTES(1024))")

	// we copy it but we don't feedback it (a hack)
	testutils.RunSQL(t, "INSERT INTO _phantomtest_new (id, pad) SELECT * FROM phantomtest WHERE id = 1002")

	// delete the row (but not from the _new table)
	// when it gets to recopy it will not be there.
	testutils.RunSQL(t, "DELETE FROM phantomtest WHERE id = 1002")

	// then we save the checkpoint without the feedback.
	assert.NoError(t, m.DumpCheckpoint(ctx))
	// assert there is a checkpoint
	var rowCount int
	err = m.db.QueryRowContext(ctx, `SELECT count(*) from _phantomtest_chkpnt`).Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowCount)

	// kill it.
	cancel()
	assert.NoError(t, m.Close())

	// Resume the migration using and apply all of the replication
	// changes before starting the copier.
	ctx = t.Context()
	m = NewTestRunner(t, "phantomtest", "ENGINE=InnoDB",
		WithTargetChunkTime(100*time.Millisecond))
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	assert.NoError(t, m.changes[0].table.SetInfo(ctx))
	// check we can resume from checkpoint
	// this is normally done in m.setup() but we want to call it in isolation.
	assert.NoError(t, m.resumeFromCheckpoint(ctx))
	// This is normally done in m.setup()
	m.replClient.SetWatermarkOptimization(true)
	// doublecheck that the highPtr is 1002 in the _new table and not in the original table.
	assert.Equal(t, "10", m.changes[0].table.MaxValue().String())
	assert.Equal(t, "1002", m.changes[0].newTable.MaxValue().String())

	// flush the replication changes
	// if the bug exists, this would cause the breakage.
	assert.NoError(t, m.replClient.Flush(ctx))
	// start the copier.
	assert.NoError(t, m.copier.Run(ctx))
	// the checksum runs in prepare for cutover.
	// previously it would fail, but it should work as long as the resumeFromCheckpoint()
	// correctly finds the high watermark.
	err = m.checksum(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2EWithManualSentinel(t *testing.T) {
	t.Parallel()
	// This test is similar to TestResumeFromCheckpointE2E but it adds a sentinel table
	// created after the migration begins and is interrupted.
	// The migration itself runs with DeferCutOver=false
	// so we test to make sure a sentinel table created manually by the operator
	// blocks cutover.

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `resume_checkpoint_e2e_w_sentinel`
	tableInfo := table.TableInfo{SchemaName: dbName, TableName: tableName}
	lockTables := []*table.TableInfo{&tableInfo}

	// CreateUniqueTestDatabase registers t.Cleanup to drop the entire database.
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`, tableName))
	// Insert enough rows to ensure the first migration is still copying when we kill it.
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM dual", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 50000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 50000", tableName, tableName, tableName, tableName))

	alterSQL := "ADD INDEX(pad);"
	runner := NewTestRunner(t, tableName, alterSQL,
		WithDBName(dbName), WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithRespectSentinel())

	ctx, cancel := context.WithCancel(t.Context())

	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	waitForCheckpoint(t, runner)

	// Test that it's not possible to acquire metadata lock with name
	// as tablename while the migration is running.
	lock, err := dbconn.NewMetadataLock(ctx, testutils.DSN(), lockTables, dbconn.NewDBConfig(), slog.Default())
	assert.Error(t, err)
	assert.Nil(t, lock)

	// Close() before cancel() to avoid race conditions.
	assert.NoError(t, runner.Close())
	cancel()

	// Manually create the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE _spirit_sentinel (id int unsigned primary key)")

	// Insert some more dummy data
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s LIMIT 1000", tableName, tableName))
	// Start a new migration with the same parameters. It should resume from checkpoint.
	m := NewTestRunner(t, tableName, alterSQL,
		WithDBName(dbName), WithThreads(4), WithTargetChunkTime(5*time.Second), WithRespectSentinel())

	// Run the resumed migration in a goroutine. It should block on the
	// manually-created sentinel table.
	c := make(chan error, 1)
	go func() {
		c <- m.Run(t.Context())
	}()

	// Wait until the migration is blocked on the sentinel table, confirming
	// that the manually-created sentinel is respected on resume.
	require.Eventually(t, func() bool {
		return m.status.Get() == status.WaitingOnSentinelTable
	}, 30*time.Second, 100*time.Millisecond, "migration did not reach WaitingOnSentinelTable")

	// Cancel instead of waiting for the full sentinelWaitLimit timeout.
	m.Cancel()
	err = <-c
	assert.Error(t, err)
	assert.True(t, m.usedResumeFromCheckpoint)
	assert.NoError(t, m.Close())
}

// TestResumeFromCheckpointCleanupOnFailure tests that when a checkpoint's binlog
// position is no longer available on the server (e.g., purged), resumeFromCheckpoint
// detects this early — before creating the replClient — and falls back to newMigration.
//
// This validates the fix for a bug where volume changes (stop/start cycle) during a
// migration could cause stale binlog positions, leading to "subscription already exists"
// errors when the fallback to newMigration tried to re-create subscriptions.
func TestResumeFromCheckpointCleanupOnFailure(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "cleanup_test",
		`CREATE TABLE cleanup_test (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO cleanup_test (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint that we can manipulate
	r := NewTestRunner(t, "cleanup_test", "ENGINE=InnoDB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		_ = r.Run(ctx)
	}()

	// Wait for checkpoint to be created
	waitForCheckpoint(t, r)

	// Verify the _new table exists (required for the resume path we want to test)
	var tableName string
	err := tt.DB.QueryRowContext(t.Context(), "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = '_cleanup_test_new'").Scan(&tableName)
	assert.NoError(t, err, "_cleanup_test_new table should exist after checkpoint")

	// Close() before cancel() to avoid race conditions (see other tests)
	assert.NoError(t, r.Close())
	cancel()

	// Now corrupt the checkpoint by setting an invalid binlog position.
	// This simulates binlog expiry between stop and start.
	testutils.RunSQL(t, `UPDATE _cleanup_test_chkpnt SET binlog_name = 'nonexistent-bin.999999', binlog_pos = 999999999`)

	// Without strict mode: falls back to newMigration and completes successfully.
	r2 := NewTestRunner(t, "cleanup_test", "ENGINE=InnoDB")
	require.NoError(t, r2.Run(t.Context()))
	assert.False(t, r2.usedResumeFromCheckpoint) // Should NOT have resumed because binlog was invalid
	assert.NoError(t, r2.Close())
}

func TestResumeFromCheckpointStrictBinlogExpired(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "strictbinlogtest",
		`CREATE TABLE strictbinlogtest (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			pad varbinary(1024) NOT NULL,
			PRIMARY KEY (id)
		)`)
	tt.SeedRows(t, "INSERT INTO strictbinlogtest (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint
	r := NewTestRunner(t, "strictbinlogtest", "ENGINE=InnoDB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		_ = r.Run(ctx)
	}()

	waitForCheckpoint(t, r)
	assert.NoError(t, r.Close())
	cancel()

	// Corrupt binlog name to simulate expiry
	testutils.RunSQL(t, `UPDATE _strictbinlogtest_chkpnt SET binlog_name = 'nonexistent-bin.999999', binlog_pos = 999999999`)

	// With strict mode: should error with ErrBinlogNotFound instead of silently restarting
	r2 := NewTestRunner(t, "strictbinlogtest", "ENGINE=InnoDB", WithStrict())
	err := r2.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorIs(t, err, status.ErrBinlogNotFound)
	assert.NoError(t, r2.Close())
}

// TestResumeFromCheckpointTooOld tests that when a checkpoint's created_at timestamp
// exceeds CheckpointMaxAge, the migration falls back to a fresh start instead of
// resuming from the stale checkpoint. This prevents the slow replay of many days
// of binary logs when starting fresh would be faster.
func TestResumeFromCheckpointTooOld(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chkpttooold",
		`CREATE TABLE chkpttooold (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO chkpttooold (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint
	r := NewTestRunner(t, "chkpttooold", "ENGINE=InnoDB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = r.Run(ctx) }()
	waitForCheckpoint(t, r)
	assert.NoError(t, r.Close())
	cancel()

	// Backdate the checkpoint's created_at to simulate an old checkpoint (8 days ago).
	testutils.RunSQL(t, `UPDATE _chkpttooold_chkpnt SET created_at = DATE_SUB(NOW(), INTERVAL 8 DAY)`)

	// Without strict mode: falls back to newMigration and completes successfully.
	r2 := NewTestRunner(t, "chkpttooold", "ENGINE=InnoDB")
	require.NoError(t, r2.Run(t.Context()))
	assert.False(t, r2.usedResumeFromCheckpoint) // Should NOT have resumed because checkpoint was too old
	assert.NoError(t, r2.Close())
}

// TestResumeFromCheckpointStrictTooOld tests that when strict mode is enabled
// and a checkpoint exceeds CheckpointMaxAge, the migration fails with
// ErrCheckpointTooOld rather than silently starting fresh.
func TestResumeFromCheckpointStrictTooOld(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "strictoldtest",
		`CREATE TABLE strictoldtest (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO strictoldtest (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint
	r := NewTestRunner(t, "strictoldtest", "ENGINE=InnoDB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = r.Run(ctx) }()
	waitForCheckpoint(t, r)
	assert.NoError(t, r.Close())
	cancel()

	// Backdate the checkpoint's created_at to simulate an old checkpoint (8 days ago).
	testutils.RunSQL(t, `UPDATE _strictoldtest_chkpnt SET created_at = DATE_SUB(NOW(), INTERVAL 8 DAY)`)

	// With strict mode: should error with ErrCheckpointTooOld instead of silently restarting.
	r2 := NewTestRunner(t, "strictoldtest", "ENGINE=InnoDB", WithStrict())
	err := r2.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorIs(t, err, status.ErrCheckpointTooOld)
	assert.NoError(t, r2.Close())
}

// TestResumeFromCheckpointNotTooOld tests that a recent checkpoint (within
// CheckpointMaxAge) is still used for resume as expected.
func TestResumeFromCheckpointNotTooOld(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chkptnotold",
		`CREATE TABLE chkptnotold (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO chkptnotold (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint
	r := NewTestRunner(t, "chkptnotold", "ENGINE=InnoDB",
		WithThreads(1), WithTargetChunkTime(100*time.Millisecond), WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = r.Run(ctx) }()
	waitForCheckpoint(t, r)
	assert.NoError(t, r.Close())
	cancel()

	// Do NOT backdate the checkpoint - it was just created, so it's fresh.
	// The migration should resume from checkpoint successfully.
	r2 := NewTestRunner(t, "chkptnotold", "ENGINE=InnoDB")
	require.NoError(t, r2.Run(t.Context()))
	assert.True(t, r2.usedResumeFromCheckpoint) // Should have resumed because checkpoint is fresh
	assert.NoError(t, r2.Close())
}
