package datasync

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// Tick the status logger fast. Continuous-replication latency is set
	// per-test via Sync.FlushInterval.
	status.StatusInterval = 100 * time.Millisecond
	goleak.VerifyTestMain(m)
}

func TestNewRunnerValidation(t *testing.T) {
	// Defaults are applied for zero-valued knobs.
	r, err := NewRunner(&Sync{})
	require.NoError(t, err)
	require.Equal(t, 4, r.sync.Threads)
	require.Equal(t, 4, r.sync.WriteThreads)
	require.Equal(t, 5*time.Second, r.sync.TargetChunkTime)
	require.Positive(t, r.sync.FlushInterval)
}

// TestSyncE2E drives the full sync lifecycle against a local MySQL using
// the built-in binlog change source: initial copy, then continuous
// replication of an INSERT, an UPDATE and a DELETE, then a clean
// cancellation.
func TestSyncE2E(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_src"
	dest := cfg.Clone()
	dest.DBName = "sync_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_dest`)
	testutils.RunSQL(t, `CREATE DATABASE sync_dest`)

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)

	countRows := func() int {
		var n int
		if err := tgt.QueryRow(`SELECT COUNT(*) FROM t1`).Scan(&n); err != nil {
			return -1 // table may not exist yet
		}
		return n
	}
	valOf := func(id int) string {
		var v string
		if err := tgt.QueryRow(`SELECT val FROM t1 WHERE id = ?`, id).Scan(&v); err != nil {
			return ""
		}
		return v
	}

	s := &Sync{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		FlushInterval:   100 * time.Millisecond,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Initial copy lands all three rows.
	require.Eventually(t, func() bool { return countRows() == 3 },
		30*time.Second, 100*time.Millisecond, "initial copy should replicate 3 rows")

	// Continuous: an INSERT replicates.
	testutils.RunSQL(t, `INSERT INTO sync_src.t1 VALUES (4,'four')`)
	require.Eventually(t, func() bool { return countRows() == 4 },
		30*time.Second, 100*time.Millisecond, "continuous sync should replicate the INSERT")

	// Continuous: an UPDATE and a DELETE replicate.
	testutils.RunSQL(t, `UPDATE sync_src.t1 SET val='ONE' WHERE id=1`)
	testutils.RunSQL(t, `DELETE FROM sync_src.t1 WHERE id=2`)
	require.Eventually(t, func() bool { return countRows() == 3 && valOf(1) == "ONE" },
		30*time.Second, 100*time.Millisecond, "continuous sync should replicate the UPDATE + DELETE")

	// Cancellation drains and returns cleanly.
	cancel()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(30 * time.Second):
		t.Fatal("sync did not stop within 30s of cancellation")
	}
	require.NoError(t, runner.Close())
}

// TestSyncCopyOnly verifies copy-only mode: the initial copy runs and the
// runner returns on its own — no change source, no continuous phase, no
// cancellation needed.
func TestSyncCopyOnly(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_copyonly_src"
	dest := cfg.Clone()
	dest.DBName = "sync_copyonly_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_copyonly_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_copyonly_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_copyonly_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_copyonly_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	// Leave the target database absent — copy-only must auto-create it.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_copyonly_dest`)

	s := &Sync{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		CopyOnly:        true,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	// Copy-only Run returns on its own once the copy completes (no cancel).
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, runner.Run(ctx))
	require.NoError(t, runner.Close())

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	var n int
	require.NoError(t, tgt.QueryRow("SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n)
	var v string
	require.NoError(t, tgt.QueryRow("SELECT val FROM t1 WHERE id = 2").Scan(&v))
	require.Equal(t, "two", v)
}

// TestRunnerStatusTask exercises the status.Task surface (Progress, Status,
// DumpCheckpoint, Cancel) concurrently with Run, validating both the values
// reported and the locking around the progress fields (run with -race).
func TestRunnerStatusTask(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_statustask_src"
	dest := cfg.Clone()
	dest.DBName = "sync_statustask_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_statustask_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_statustask_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_statustask_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_statustask_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_statustask_dest`)

	s := &Sync{
		SourceDSN:       src.FormatDSN(),
		TargetDSN:       dest.FormatDSN(),
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		CopyOnly:        true,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	// Safe to poll before Run starts: reports the Initial state, no panic, and
	// the copy-only checkpoint is a no-op.
	require.Equal(t, status.Initial, runner.Progress().CurrentState)
	require.NotEmpty(t, runner.Status())
	require.NoError(t, runner.DumpCheckpoint(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Hammer the status.Task accessors from another goroutine while Run runs,
	// so the race detector covers the locking around copier/copyChunker/etc.
	done := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-done:
				return
			default:
				_ = runner.Progress()
				_ = runner.Status()
				_ = runner.DumpCheckpoint(ctx)
			}
		}
	}()

	runErr := runner.Run(ctx)
	close(done)
	<-pollDone
	require.NoError(t, runErr)
	runner.Cancel() // post-run cancel must be harmless
	require.NoError(t, runner.Close())

	// After a completed copy, Progress reports the table as fully copied.
	p := runner.Progress()
	require.Len(t, p.Tables, 1)
	require.Equal(t, "t1", p.Tables[0].TableName)
	require.True(t, p.Tables[0].IsComplete)
	require.Equal(t, uint64(3), p.Tables[0].RowsCopied)
}

// TestSyncResume verifies that a copy-only sync writes a copier-watermark
// checkpoint and that a second run against the same (non-empty) target detects
// it and resumes — opening the chunker at the saved watermark instead of
// tripping the fresh-sync target-empty check — leaving the data intact. Uses
// two tables so the copier is a multi-chunker (as a real per-shard keyspace
// import is), which always records a checkpoint.
func TestSyncResume(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_resume_src"
	dest := cfg.Clone()
	dest.DBName = "sync_resume_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_resume_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_resume_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_resume_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_resume_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `CREATE TABLE sync_resume_src.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_resume_src.t2 VALUES (10,'ten'),(20,'twenty')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_resume_dest`)

	newSync := func() *Sync {
		return &Sync{
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
			CopyOnly:        true,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// First run: copies both tables and records a checkpoint.
	r1, err := NewRunner(newSync())
	require.NoError(t, err)
	require.NoError(t, r1.Run(ctx))
	require.NoError(t, r1.Close())

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)

	countRows := func(tbl string) int {
		var n int
		require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+tbl).Scan(&n))
		return n
	}
	require.Equal(t, 3, countRows("t1"))
	require.Equal(t, 2, countRows("t2"))

	// A copy must persist a checkpoint with a copier watermark.
	var wm string
	require.NoError(t, tgt.QueryRowContext(context.Background(),
		"SELECT IFNULL(copier_watermark, '') FROM _spirit_sync_checkpoint WHERE id = 1").Scan(&wm))
	require.NotEmpty(t, wm, "a copy should record a copier watermark")

	// Second run with the target left intact: it must detect the checkpoint and
	// resume (open the chunker at the watermark) rather than fail the fresh
	// target-empty check, and the data must be unchanged.
	r2, err := NewRunner(newSync())
	require.NoError(t, err)
	require.NoError(t, r2.Run(ctx))
	require.NoError(t, r2.Close())

	require.Equal(t, 3, countRows("t1"), "resume must not duplicate or drop rows")
	require.Equal(t, 2, countRows("t2"), "resume must not duplicate or drop rows")
}
