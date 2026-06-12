package move

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// These tests mirror pkg/migration/runner_close_test.go: the move runner
// forked from the migration runner and its fatalError/Close paths must
// uphold the same contracts. The exposure is larger here — move wires one
// repl client per source to the same fatalError callback, so concurrent
// invocation is realistic, not just theoretical.

// TestFatalErrorIsIdempotent pins the invariant that fatalError's side
// effects (status transition + context cancel) run at most once, even
// under a burst of fatal events. After the first call the status sits at
// ErrCleanup, which is numerically greater than CutOver, so the
// early-return path swallows subsequent calls — but if a second caller
// races the first past the status check, fatalOnce still prevents the
// side effects from running twice.
//
// The test uses a minimal Runner constructed by hand: targets and
// checkpointTable are left empty/nil so the (now-guarded) checkpoint-drop
// path is a no-op. cancelFunc is a counter we observe.
func TestFatalErrorIsIdempotent(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func() { cancelCalls.Add(1) },
	}

	require.True(t, r.fatalError(), "first call must return true")
	require.Equal(t, int32(1), cancelCalls.Load(), "first call must cancel once")
	require.Equal(t, status.ErrCleanup, r.status.Get(), "status must transition to ErrCleanup")

	// Subsequent calls: status (ErrCleanup) > CutOver, so the early
	// return kicks in. cancel must not be re-invoked.
	require.False(t, r.fatalError(), "subsequent call returns false via the past-cutover guard")
	require.False(t, r.fatalError(), "and again")
	require.Equal(t, int32(1), cancelCalls.Load(), "cancel must not be re-invoked")
}

// TestFatalErrorConcurrentRace exercises the fatalOnce guard directly:
// many goroutines call fatalError in parallel before any has finished
// setting status, simulating N repl clients (one per source) hitting a
// fatal stream error at the same time. The Once ensures cancelFunc fires
// exactly once even when the racing callers all pass the pre-CutOver
// status check. Run with -race.
func TestFatalErrorConcurrentRace(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func() { cancelCalls.Add(1) },
	}

	const goroutines = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			<-start
			r.fatalError()
		})
	}
	close(start)
	wg.Wait()

	require.Equal(t, int32(1), cancelCalls.Load(),
		"cancelFunc must fire exactly once regardless of concurrent fatalError calls")
}

// TestFatalErrorPastCutoverIsNoop pins the existing contract that
// fatalError is a no-op once the move is at or past cutover: Spirit's own
// RENAME TABLE DDL is expected at that point and must not invalidate the
// (already-irrelevant) checkpoint.
func TestFatalErrorPastCutoverIsNoop(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func() { cancelCalls.Add(1) },
	}
	r.status.Set(status.CutOver)

	require.False(t, r.fatalError(), "fatalError at/past cutover must return false")
	require.Equal(t, int32(0), cancelCalls.Load(), "must not cancel at/past cutover")
	require.Equal(t, status.CutOver, r.status.Get(), "status must not transition")
}

// TestFatalErrorSafeWithoutCancelFunc verifies fatalError tolerates a nil
// cancelFunc (early setup / test paths that bypass Run). Without the
// nil-check it nil-derefs.
func TestFatalErrorSafeWithoutCancelFunc(t *testing.T) {
	r := &Runner{
		logger: slog.Default(),
		// cancelFunc intentionally nil
	}
	require.NotPanics(t, func() {
		require.True(t, r.fatalError())
	})
}

// fakeChangeSource is a minimal change.Source used to observe that
// Close() reaches every repl client even when an earlier cleanup step
// failed.
type fakeChangeSource struct {
	closed atomic.Bool
}

func (f *fakeChangeSource) AddSubscription(_, _ *table.TableInfo, _ table.MappedChunker) error {
	return nil
}
func (f *fakeChangeSource) Start(_ context.Context) error                       { return nil }
func (f *fakeChangeSource) StartFromPosition(_ context.Context, _ string) error { return nil }
func (f *fakeChangeSource) Position() string                                    { return "" }
func (f *fakeChangeSource) Flush(_ context.Context) error                       { return nil }
func (f *fakeChangeSource) FlushUnderTableLock(_ context.Context, _ []*dbconn.TableLock) error {
	return nil
}
func (f *fakeChangeSource) BlockWait(_ context.Context) error { return nil }
func (f *fakeChangeSource) GetDeltaLen() int                  { return 0 }
func (f *fakeChangeSource) SetWatermarkOptimization(_ context.Context, _ bool) error {
	return nil
}
func (f *fakeChangeSource) StartPeriodicFlush(_ context.Context, _ time.Duration) {}
func (f *fakeChangeSource) StopPeriodicFlush()                                    {}
func (f *fakeChangeSource) AllChangesFlushed() bool                               { return true }
func (f *fakeChangeSource) Close()                                                { f.closed.Store(true) }

// TestCloseRunsAllClosersOnError pins the Close() aggregation contract:
// every cleanup step runs even when an early one fails. Previously the
// first failing step short-circuited the rest, leaking the remaining repl
// clients' binlog reader goroutines and the target DB handles. The
// chunker is rigged to fail Close(); the repl clients and target DBs must
// still be closed, and the chunker's error must surface in the joined
// result.
func TestCloseRunsAllClosersOnError(t *testing.T) {
	chunkerErr := errors.New("chunker close failed")
	mockChunker := table.NewMockChunker("t1", 100)
	require.NoError(t, mockChunker.Open())
	mockChunker.SetCloseError(chunkerErr)

	repl1 := &fakeChangeSource{}
	repl2 := &fakeChangeSource{}

	db1, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	db2, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)

	r := &Runner{
		logger:      slog.Default(),
		copyChunker: mockChunker,
		sources: []sourceInfo{
			{replClient: repl1},
			{replClient: repl2},
		},
		targets: []applier.Target{
			{KeyRange: "0", DB: db1},
			{KeyRange: "1", DB: db2},
		},
	}

	err = r.Close()
	require.ErrorIs(t, err, chunkerErr, "the failing step's error must surface")

	require.True(t, repl1.closed.Load(), "repl client 1 must be closed despite the chunker error")
	require.True(t, repl2.closed.Load(), "repl client 2 must be closed despite the chunker error")
	require.ErrorContains(t, db1.PingContext(t.Context()), "database is closed",
		"target DB 1 must be closed despite the chunker error")
	require.ErrorContains(t, db2.PingContext(t.Context()), "database is closed",
		"target DB 2 must be closed despite the chunker error")
}
