package migration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// failingChecker is a checksum.Checker that always returns the configured
// error from Run. Used to drive the runner.checksum() error path without
// needing to fabricate a real source/target divergence.
type failingChecker struct {
	err error
}

func (f *failingChecker) Run(ctx context.Context) error { return f.err }
func (f *failingChecker) GetProgress() string           { return "failing" }
func (f *failingChecker) StartTime() time.Time          { return time.Now() }
func (f *failingChecker) ExecTime() time.Duration       { return 0 }
func (f *failingChecker) DifferencesFound() uint64      { return 0 }

// setupRunnerForChecksumTest creates a real table, runs the runner setup as
// far as creating the checkpoint table on disk, and returns a Runner that can
// have its checker swapped and r.checksum() called directly. It deliberately
// stops short of starting the binlog feed (replClient.Run) — these tests
// short-circuit the checker, so the binlog is unnecessary.
func setupRunnerForChecksumTest(t *testing.T, tableName string) *Runner {
	t.Helper()
	dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s, %s, %s",
		tableName,
		utils.NewTableName(tableName),
		utils.CheckpointTableName(tableName),
	)
	testutils.RunSQL(t, dropStmt)
	t.Cleanup(func() { testutils.RunSQL(t, dropStmt) })
	testutils.RunSQL(t, "CREATE TABLE "+tableName+" (id INT NOT NULL PRIMARY KEY)")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    tableName,
		Alter:    "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(r) })

	// Reuse one DBConfig so the *sql.DB pool sizing and the runner's view of
	// it agree if their defaults ever drift.
	dbCfg := dbconn.NewDBConfig()
	r.db, err = dbconn.New(testutils.DSN(), dbCfg)
	require.NoError(t, err)
	r.dbConfig = dbCfg
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.changes[0].dropOldTable(t.Context()))
	require.NoError(t, r.changes[0].createNewTable(t.Context()))
	require.NoError(t, r.changes[0].alterNewTable(t.Context()))
	require.NoError(t, r.createCheckpointTable(t.Context()))
	r.checkpointTable = table.NewTableInfo(r.db, r.changes[0].table.SchemaName, r.checkpointTableName())

	require.True(t, checkpointTableExists(t, r),
		"checkpoint table must exist after setup")
	return r
}

// TestChecksumFailureInvalidatesCheckpoint pins the invariant that when the
// initial checksum exhausts retries, the runner invalidates the checkpoint
// table before returning. Without this, a supervisor restart would resume
// at the partial checksum_watermark persisted by the periodic dumper, skip
// any chunk the failed run could not reconcile, and — if the remaining
// range happens to pass — cut over on top of a corrupt _new table.
//
// See the silent-data-loss story documented at the call site of
// fatalError in (*Runner).checksum.
func TestChecksumFailureInvalidatesCheckpoint(t *testing.T) {
	t.Parallel()
	r := setupRunnerForChecksumTest(t, "chkpt_invalidation")

	var cancelled bool
	ctx, cancel := context.WithCancel(t.Context())
	r.cancelFunc = func() { cancelled = true; cancel() }
	r.checker = &failingChecker{err: errors.New("simulated checksum failure")}

	checksumErr := r.checksum(ctx)
	require.Error(t, checksumErr, "r.checksum must propagate the checker's error")
	require.ErrorContains(t, checksumErr, "checksum failed")
	require.True(t, cancelled, "fatalError must cancel the run context")
	require.Equal(t, status.ErrCleanup, r.status.Get(),
		"fatalError must transition status to ErrCleanup")
	require.False(t, checkpointTableExists(t, r),
		"checkpoint table must be dropped after a permanent checksum failure")
}

// TestChecksumCancellationPreservesCheckpoint pins the complementary
// invariant: a checker error returned because the parent context was
// cancelled (operator Ctrl-C, deadline, supervisor stop) must NOT
// invalidate the checkpoint. The persisted checksum_watermark is the
// legitimate progress of a partial pass and the next run should be free
// to resume from it.
func TestChecksumCancellationPreservesCheckpoint(t *testing.T) {
	t.Parallel()
	r := setupRunnerForChecksumTest(t, "chkpt_cancel_preserves")

	var cancelled bool
	ctx, cancel := context.WithCancel(t.Context())
	r.cancelFunc = func() { cancelled = true; cancel() }
	// Mimic the checker returning ctx.Err() after the parent was cancelled.
	r.checker = &failingChecker{err: context.Canceled}
	cancel()

	checksumErr := r.checksum(ctx)
	require.Error(t, checksumErr, "r.checksum must propagate the cancellation error")
	require.False(t, cancelled,
		"runner cancelFunc must NOT fire on a parent-context cancellation")
	require.NotEqual(t, status.ErrCleanup, r.status.Get(),
		"status must not transition to ErrCleanup on cancellation")
	require.True(t, checkpointTableExists(t, r),
		"checkpoint table must be preserved when checksum exits due to cancellation")
}

func checkpointTableExists(t *testing.T, r *Runner) bool {
	t.Helper()
	var n int
	err := r.db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		r.checkpointTable.SchemaName, r.checkpointTable.TableName).Scan(&n)
	require.NoError(t, err)
	return n > 0
}
