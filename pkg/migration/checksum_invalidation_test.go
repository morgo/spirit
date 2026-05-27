package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
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

// fakeChecker is a checksum.Checker stand-in whose Run and DifferencesFound
// are scripted by the test. It lets a test drive (*Runner).checksum and
// (*Runner).DumpCheckpoint without standing up a real checker / source-target
// divergence.
type fakeChecker struct {
	runErr           error
	differencesFound atomic.Uint64
}

func (f *fakeChecker) Run(ctx context.Context) error { return f.runErr }
func (f *fakeChecker) GetProgress() string           { return "fake" }
func (f *fakeChecker) StartTime() time.Time          { return time.Now() }
func (f *fakeChecker) ExecTime() time.Duration       { return 0 }
func (f *fakeChecker) DifferencesFound() uint64      { return f.differencesFound.Load() }

// setupRunnerForChecksumTest creates a real table, runs the runner setup as
// far as creating the checkpoint table on disk, and returns a Runner that can
// have its checker swapped and r.checksum() / r.DumpCheckpoint() called
// directly. It deliberately stops short of starting the binlog feed
// (replClient.Run) — these tests short-circuit the checker, so the binlog is
// unnecessary.
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

	// Share one DBConfig so the *sql.DB pool sizing and the runner's view of
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

// TestChecksumErrorPreservesCheckpoint pins the new contract: any error
// returned by the checksum (retry exhaustion, operator cancellation, or
// anything else) must NOT drop the checkpoint table. Resume safety is
// instead enforced by the DumpCheckpoint invariant — see
// TestDumpCheckpointSuppressesWatermarkWithDifferences.
//
// The old fix invalidated the checkpoint on retry exhaustion via
// fatalError(), which also forced an unwanted full restart on Ctrl-C /
// deadline. The data-invariant fix lets a clean cancellation resume
// naturally from whatever verified-clean watermark was last persisted.
func TestChecksumErrorPreservesCheckpoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "RetryExhaustion", err: errors.New("simulated retry exhaustion")},
		{name: "ContextCancelled", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := setupRunnerForChecksumTest(t, "chkpt_preserve_"+tc.name)

			var cancelled bool
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			r.cancelFunc = func() { cancelled = true; cancel() }
			r.checker = &fakeChecker{runErr: tc.err}

			err := r.checksum(ctx)
			require.Error(t, err, "the checker's error must propagate")
			require.False(t, cancelled,
				"fatalError must not fire on a checksum error path")
			require.NotEqual(t, status.ErrCleanup, r.status.Get(),
				"status must not transition to ErrCleanup")
			require.True(t, checkpointTableExists(t, r),
				"checkpoint table must be preserved regardless of error shape")
		})
	}
}

// TestDumpCheckpointSuppressesWatermarkWithDifferences pins the data
// invariant that closes the silent-cutover hole: while the current
// checksum pass has had any chunks repaired (DifferencesFound > 0), the
// persisted row's checksum_watermark must be empty so a resumed run will
// re-verify the table from the start of the checksum phase. Once the
// counter clears (next pass starts, no repairs yet), the real watermark
// is persisted again.
func TestDumpCheckpointSuppressesWatermarkWithDifferences(t *testing.T) {
	t.Parallel()
	r := setupRunnerForChecksumTest(t, "chkpt_invariant")

	// Seed the table with enough rows that the chunker can carve out
	// multiple chunks. The composite chunker won't report a low-watermark
	// until at least one fully-processed chunk has had Feedback, which in
	// practice requires more than one chunk on the runway.
	seedRows(t, r.db, r.migration.Table, 4096)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.initChunkers())
	require.NoError(t, r.copyChunker.Open())
	require.NoError(t, r.checksumChunker.Open())
	disableDynamicChunking(t, r.copyChunker)
	disableDynamicChunking(t, r.checksumChunker)
	require.NoError(t, r.setupCopierCheckerAndReplClient(t.Context()))
	require.NoError(t, r.replClient.Run(t.Context()))
	t.Cleanup(func() { r.replClient.Close() })

	advanceUntilWatermark(t, r.copyChunker)
	advanceUntilWatermark(t, r.checksumChunker)

	// Swap in a fake checker whose DifferencesFound() we control. The
	// invariant only looks at this value (and the chunker's watermark);
	// it never calls Run, so the runErr doesn't matter.
	fake := &fakeChecker{}
	r.checker = fake
	r.status.Set(status.Checksum)

	// --- Case 1: current pass has had differences. Watermark must be "". ---
	fake.differencesFound.Store(1)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark should always be persisted")
	require.Empty(t, checksumWM,
		"checksum_watermark must be empty while DifferencesFound > 0")

	// --- Case 2: counter reset (next pass starts clean). Watermark restored. ---
	fake.differencesFound.Store(0)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM)
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted again once DifferencesFound clears")
}

// seedRows populates the named table by doubling until it has at least
// targetRows rows. The table is expected to have a single AUTO_INCREMENT
// PK column named id.
func seedRows(t *testing.T, db *sql.DB, tableName string, targetRows int) {
	t.Helper()
	testutils.RunSQL(t, "INSERT INTO "+tableName+" (id) VALUES (1)")
	for {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM `"+tableName+"`").Scan(&count))
		if count >= targetRows {
			return
		}
		testutils.RunSQL(t,
			"INSERT INTO "+tableName+" (id) SELECT id + (SELECT MAX(id) FROM `"+tableName+"`) FROM `"+tableName+"`")
	}
}

// advanceUntilWatermark pulls and reports chunks until the chunker's
// GetLowWatermark returns a non-error answer. Tests that need a meaningful
// watermark without running a real copy / checksum use this to bring the
// chunker into a state DumpCheckpoint will accept.
func advanceUntilWatermark(t *testing.T, c table.Chunker) {
	t.Helper()
	for range 16 {
		chunk, err := c.Next()
		require.NoError(t, err)
		c.Feedback(chunk, time.Millisecond, 0)
		if _, err := c.GetLowWatermark(); err == nil {
			return
		}
	}
	t.Fatalf("chunker did not produce a low-watermark after processing 16 chunks")
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

// latestCheckpointWatermarks returns the most recently inserted
// copier_watermark / checksum_watermark from the checkpoint table.
func latestCheckpointWatermarks(t *testing.T, r *Runner) (string, string) {
	t.Helper()
	var copierWM, checksumWM string
	err := r.db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT copier_watermark, checksum_watermark FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
			r.checkpointTable.SchemaName, r.checkpointTable.TableName)).Scan(&copierWM, &checksumWM)
	require.NoError(t, err)
	return copierWM, checksumWM
}
