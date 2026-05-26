package migration

import (
	"context"
	"errors"
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
	tableName := "chkpt_invalidation"
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName+", _"+tableName+"_new, _"+tableName+"_chkpnt")
	t.Cleanup(func() {
		testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName+", _"+tableName+"_new, _"+tableName+"_chkpnt")
	})
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
	defer utils.CloseAndLog(r)

	// Step through the runner's setup far enough to have a real checkpoint
	// table on disk. We deliberately stop short of starting the binlog feed
	// (replClient.Run) because we don't need it — we'll short-circuit the
	// checker.
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.changes[0].dropOldTable(t.Context()))
	require.NoError(t, r.changes[0].createNewTable(t.Context()))
	require.NoError(t, r.changes[0].alterNewTable(t.Context()))
	require.NoError(t, r.createCheckpointTable(t.Context()))
	r.checkpointTable = table.NewTableInfo(r.db, r.changes[0].table.SchemaName, r.checkpointTableName())

	// Confirm the checkpoint table is actually present before we run the
	// failing checksum, so the post-condition is meaningful.
	require.True(t, checkpointTableExists(t, r),
		"checkpoint table must exist before invoking the failing checksum")

	// Install a runner-cancel hook so we can assert fatalError fired.
	var cancelled bool
	ctx, cancel := context.WithCancel(t.Context())
	r.cancelFunc = func() { cancelled = true; cancel() }

	// Substitute the real checker with one that simulates max-retries-exhausted.
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

func checkpointTableExists(t *testing.T, r *Runner) bool {
	t.Helper()
	var n int
	err := r.db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		r.checkpointTable.SchemaName, r.checkpointTable.TableName).Scan(&n)
	require.NoError(t, err)
	return n > 0
}
