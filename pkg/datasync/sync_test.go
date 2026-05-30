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
