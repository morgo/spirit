package move

// End-to-end tests for the reverse-window move driven through the real Runner
// (forward copy + cutover + reverse window + terminal action). 1 source → 1
// target keeps the harness simple; the N:1 reverse feed itself is covered by
// reversefeed_test.go. Uses the :8033 test MySQL (binlog=ROW).

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func setupReverseWindowMove(t *testing.T, srcDBName, dstDBName string) (sourceDSN, targetDSN string, ctl *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = srcDBName
	dst := cfg.Clone()
	dst.DBName = dstDBName

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDBName)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDBName)
	testutils.RunSQL(t, "CREATE TABLE "+srcDBName+".t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "INSERT INTO "+srcDBName+".t1 (id, val) VALUES (1,'one'),(2,'two'),(3,'three')")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDBName)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDBName)

	ctl, err = sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(ctl) })
	return src.FormatDSN(), dst.FormatDSN(), ctl
}

// shortenReverseWindowPolling makes the window loop responsive in tests.
func shortenReverseWindowPolling(t *testing.T) {
	t.Helper()
	old := reverseWindowPollInterval
	reverseWindowPollInterval = 100 * time.Millisecond
	t.Cleanup(func() { reverseWindowPollInterval = old })
}

func tableExists(t *testing.T, db *sql.DB, schema, name string) bool {
	t.Helper()
	var one int
	err := db.QueryRowContext(t.Context(),
		"SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?", schema, name).Scan(&one)
	if err == sql.ErrNoRows {
		return false
	}
	require.NoError(t, err)
	return true
}

// waitForReverseWindow polls the checkpoint until the move has entered its
// reverse window (move_phase = reverse_window), i.e. cutover has happened.
func waitForReverseWindow(t *testing.T, db *sql.DB, dstDBName string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var phase string
		err := db.QueryRowContext(t.Context(),
			"SELECT move_phase FROM "+dstDBName+"."+checkpointTableName+" WHERE id=1").Scan(&phase)
		if err == nil && phase == phaseReverseWindow {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the reverse window to open")
}

// TestMoveReverseWindowCompleteForward: with a reverse window and no revert, the
// move holds the window then finalizes forward — source retired to _old, target
// serving, checkpoint dropped.
func TestMoveReverseWindowCompleteForward(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwcf_src", "rwcf_dst")

	m := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: time.Second,
		Threads:         1,
		WriteThreads:    1,
		ReverseWindow:   2 * time.Second,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)

	var cutoverCalled, reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	require.NoError(t, runner.Run(t.Context()))

	require.True(t, cutoverCalled, "forward cutover func must run")
	require.False(t, reverseCutoverCalled, "reverse cutover func must NOT run when the window elapses")
	// Source retired to _old; target serving.
	require.True(t, tableExists(t, ctl, "rwcf_src", "t1_old"), "source table should be retired to _old")
	require.False(t, tableExists(t, ctl, "rwcf_src", "t1"), "source real table should be gone after retire")
	require.True(t, tableExists(t, ctl, "rwcf_dst", "t1"), "target table should be serving")
	require.False(t, tableExists(t, ctl, "rwcf_dst", checkpointTableName), "checkpoint should be dropped")
}

// TestMoveReverseWindowRevert: a revert requested during the window rolls the
// move back — writes that landed on the target during the window flow back to
// the source, the source is un-retired and serving, the target is retired.
func TestMoveReverseWindowRevert(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwrv_src", "rwrv_dst")

	m := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: time.Second,
		Threads:         1,
		WriteThreads:    1,
		ReverseWindow:   30 * time.Second, // long; the revert ends it early
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)

	var reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(context.Background()) }()

	// Once the window is open, a write to the target and then a revert request.
	// The revert is triggered the way the operator's revert command will:
	// create the revert marker on targets[0] (here, rwrv_dst).
	waitForReverseWindow(t, ctl, "rwrv_dst")
	testutils.RunSQL(t, "INSERT INTO rwrv_dst.t1 (id, val) VALUES (99,'late')")
	testutils.RunSQL(t, "CREATE TABLE rwrv_dst."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the reverse cutover to complete")
	}

	require.True(t, reverseCutoverCalled, "reverse cutover func must run on revert")
	// Source un-retired and serving, with the window's write flowed back.
	require.True(t, tableExists(t, ctl, "rwrv_src", "t1"), "source should be un-retired")
	require.False(t, tableExists(t, ctl, "rwrv_src", "t1_old"), "source _old should be gone after un-retire")
	var val string
	require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT val FROM rwrv_src.t1 WHERE id=99").Scan(&val))
	require.Equal(t, "late", val, "a write made on the target during the window must flow back to the source")
	// Target retired to its _revert form.
	require.True(t, tableExists(t, ctl, "rwrv_dst", "t1_revert"), "target should be retired to _revert")
	require.False(t, tableExists(t, ctl, "rwrv_dst", "t1"), "target real table should be gone after retire")
	require.False(t, tableExists(t, ctl, "rwrv_dst", "t1_old"), "target must not use the _old (forward) suffix")
	require.False(t, tableExists(t, ctl, "rwrv_dst", checkpointTableName), "checkpoint should be dropped")
}

// runRevertingMove runs one reverse-window move against the given DSNs and, once
// the window opens, requests a revert (creates the marker), returning when the
// reverse cutover has completed. Fails the test on any error.
func runRevertingMove(t *testing.T, sourceDSN, targetDSN string, ctl *sql.DB, dstDBName string, gtid bool) {
	t.Helper()
	m := &Move{
		SourceDSN:              sourceDSN,
		TargetDSN:              targetDSN,
		TargetChunkTime:        time.Second,
		Threads:                1,
		WriteThreads:           1,
		ReverseWindow:          30 * time.Second, // long; the revert ends it early
		EnableExperimentalGTID: gtid,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(context.Background()) }()

	waitForReverseWindow(t, ctl, dstDBName)
	testutils.RunSQL(t, "CREATE TABLE "+dstDBName+"."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the reverse cutover to complete")
	}
}

// TestMoveReverseWindowRevertIdempotentAcrossRetries: running move+revert twice
// against the same source/target must not collide with the first revert's
// retired (_revert) target tables — the fresh-start cleanup drops them.
func TestMoveReverseWindowRevertIdempotentAcrossRetries(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwidem_src", "rwidem_dst")

	for attempt := 1; attempt <= 2; attempt++ {
		runRevertingMove(t, sourceDSN, targetDSN, ctl, "rwidem_dst", false)
		require.True(t, tableExists(t, ctl, "rwidem_src", "t1"), "attempt %d: source should be serving", attempt)
		require.True(t, tableExists(t, ctl, "rwidem_dst", "t1_revert"), "attempt %d: target retired to _revert", attempt)
		require.False(t, tableExists(t, ctl, "rwidem_dst", "t1"), "attempt %d: target real table gone", attempt)
	}
}

// TestMoveReverseWindowRevertGTID: the reverse window works end-to-end with the
// GTID change source (--enable-experimental-gtid), exercising the GTID path of
// Source.CurrentPosition (position capture at cutover) and the GTID reverse feed
// — proving the feature is not binlog-only.
func TestMoveReverseWindowRevertGTID(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwgtid_src", "rwgtid_dst")
	runRevertingMove(t, sourceDSN, targetDSN, ctl, "rwgtid_dst", true)
	require.True(t, tableExists(t, ctl, "rwgtid_src", "t1"), "source should be serving after rollback")
	require.True(t, tableExists(t, ctl, "rwgtid_dst", "t1_revert"), "target retired to _revert")
	require.False(t, tableExists(t, ctl, "rwgtid_dst", "t1"), "target real table gone")
}

// TestMoveReverseWindowResumesAfterKill: killing a move while it is in the
// reverse window and re-running it must RESUME the window (from the checkpoint)
// — not re-discover/re-copy the source's now-_old tables. This is the reported
// failure mode.
func TestMoveReverseWindowResumesAfterKill(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwrk_src", "rwrk_dst")

	// Run 1: reach the reverse window, then simulate a kill (cancel the context)
	// without completing it. The checkpoint (phase=reverse_window) survives.
	run1, err := NewRunner(&Move{
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		TargetChunkTime: time.Second, Threads: 1, WriteThreads: 1,
		ReverseWindow: 30 * time.Second,
	})
	require.NoError(t, err)
	run1.SetCutover(func(context.Context) error { return nil })
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1Done := make(chan struct{})
	go func() { _ = run1.Run(ctx1); close(run1Done) }()

	waitForReverseWindow(t, ctl, "rwrk_dst")
	cancel1() // "kill" the process mid-window
	<-run1Done
	utils.CloseAndLog(run1)

	// The interrupted state: source retired to _old, target serving, checkpoint present.
	require.True(t, tableExists(t, ctl, "rwrk_src", "t1_old"), "source should be retired to _old mid-window")
	require.False(t, tableExists(t, ctl, "rwrk_src", "t1"), "source real table renamed away at cutover")
	require.True(t, tableExists(t, ctl, "rwrk_dst", "t1"), "target should be serving")
	require.True(t, tableExists(t, ctl, "rwrk_dst", checkpointTableName), "checkpoint must survive the kill")

	// Run 2: re-run the move. It must RESUME the window, not re-copy. Prove it by
	// failing if the forward cutover runs again, then request a revert and confirm
	// the rollback completes.
	run2, err := NewRunner(&Move{
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		TargetChunkTime: time.Second, Threads: 1, WriteThreads: 1,
		ReverseWindow: 30 * time.Second,
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(run2)
	run2.SetCutover(func(context.Context) error {
		t.Error("resume must NOT run the forward cutover again (it re-copied instead of resuming)")
		return nil
	})
	var reverseCutoverCalled bool
	run2.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- run2.Run(context.Background()) }()

	waitForReverseWindow(t, ctl, "rwrk_dst") // already reverse_window from run 1
	testutils.RunSQL(t, "CREATE TABLE rwrk_dst."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the resumed reverse window to roll back")
	}

	require.True(t, reverseCutoverCalled, "resumed window must be able to roll back")
	require.True(t, tableExists(t, ctl, "rwrk_src", "t1"), "source un-retired after rollback")
	require.True(t, tableExists(t, ctl, "rwrk_dst", "t1_revert"), "target retired to _revert after rollback")
	require.False(t, tableExists(t, ctl, "rwrk_dst", "t1"), "target real table gone after rollback")
}

// TestMoveReverseWindowRefusesStaleRevertMarker: a leftover revert marker on
// targets[0] (a prior reverse-window move that didn't complete) must make the
// move refuse at pre-flight rather than start on an unknown-state target.
func TestMoveReverseWindowRefusesStaleRevertMarker(t *testing.T) {
	sourceDSN, targetDSN, _ := setupReverseWindowMove(t, "rwsm_src", "rwsm_dst")
	testutils.RunSQL(t, "CREATE TABLE rwsm_dst."+revertMarkerName+" (id INT)")

	m := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: time.Second,
		Threads:         1,
		WriteThreads:    1,
		ReverseWindow:   2 * time.Second,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	runner.SetCutover(func(context.Context) error { return nil })

	err = runner.Run(t.Context())
	require.Error(t, err, "move must refuse to start when a revert marker is present")
	require.Contains(t, err.Error(), "revert marker")
}
