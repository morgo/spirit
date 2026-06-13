package throttler

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func newTestThreadsRunning(t *testing.T, vCPUs int64) *ThreadsRunning {
	t.Helper()
	return &ThreadsRunning{
		vCPUs:  vCPUs,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestThreadsRunning_BelowVCPUs(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	a.applySample(4)
	require.False(t, a.IsThrottled())
	require.Equal(t, int64(4), a.lastThreadsRunning.Load())
}

func TestThreadsRunning_AtVCPUsIsNotThrottled(t *testing.T) {
	// Threshold is strictly greater than vCPUs — sitting exactly at vCPUs is
	// not over-subscribed, so we should not throttle.
	a := newTestThreadsRunning(t, 8)
	a.applySample(8)
	require.False(t, a.IsThrottled())
}

func TestThreadsRunning_AboveVCPUsThrottles(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	a.applySample(9)
	require.True(t, a.IsThrottled())
	require.Equal(t, int64(9), a.lastThreadsRunning.Load())
}

func TestThreadsRunning_RecoversBelowVCPUs(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	a.applySample(20)
	require.True(t, a.IsThrottled())
	a.applySample(3)
	require.False(t, a.IsThrottled())
}

func TestThreadsRunning_Utilization(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	// The first sample seeds the EWMA directly.
	a.applySample(4)
	require.InDelta(t, 0.5, a.Utilization(), 1e-9)
	// Subsequent samples blend at threadsRunningEWMAAlpha:
	// 4 + 0.3*(8-4) = 5.2 running → 0.65.
	a.applySample(8)
	require.InDelta(t, 5.2/8, a.Utilization(), 1e-9)
	// 5.2 + 0.3*(12-5.2) = 7.24 running → 0.905.
	a.applySample(12)
	require.InDelta(t, 7.24/8, a.Utilization(), 1e-9)
	// A sustained level converges the average onto it: utilization can
	// exceed 1.0 (oversubscribed) once the load is persistent, not noise.
	for range 50 {
		a.applySample(12)
	}
	require.InDelta(t, 1.5, a.Utilization(), 1e-6)
}

func TestThreadsRunning_HardStopIsRawWhileUtilizationIsSmoothed(t *testing.T) {
	// A single spike must trip the binary hard-stop within one sample — it
	// protects the production workload — while the autoscaler's utilization
	// view absorbs it, so one noisy reading cannot trigger scaling.
	a := newTestThreadsRunning(t, 8)
	a.applySample(2) // seed: util 0.25
	a.applySample(20)
	require.True(t, a.IsThrottled(), "hard-stop must react to the raw sample")
	// EWMA: 2 + 0.3*(20-2) = 7.4 running → util 0.925.
	require.InDelta(t, 7.4/8, a.Utilization(), 1e-9)
	// The spike clearing releases the hard-stop just as quickly.
	a.applySample(1)
	require.False(t, a.IsThrottled())
}

func TestThreadsRunning_UtilizationZeroVCPUs(t *testing.T) {
	// vCPUs unknown (Open not yet called) must not divide by zero.
	a := newTestThreadsRunning(t, 0)
	a.applySample(4)
	require.Zero(t, a.Utilization())
}

func TestThreadsRunning_StaleSignalReportsHold(t *testing.T) {
	a := newTestThreadsRunning(t, 8)

	a.applySample(2) // util 0.25
	require.InDelta(t, 0.25, a.Utilization(), 1e-9)

	// Persistent sampling failure: the cached count freezes at 2 while the
	// last successful sample ages out. Utilization must hold in the dead
	// band; the binary hard-stop state is unchanged.
	ageLastSample(&a.stale, staleSignalThreshold+time.Second)
	require.InDelta(t, StaleUtilizationHold, a.Utilization(), 1e-9)
	require.False(t, a.IsThrottled())

	// A fresh sample recovers the live signal.
	a.applySample(2)
	require.InDelta(t, 0.25, a.Utilization(), 1e-9)
}

func TestThreadsRunning_EWMAReseedsAfterStaleGap(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	a.applySample(2) // seed: util 0.25

	// Samples stop arriving for longer than the staleness threshold. The
	// first sample after the gap must reseed the average, not blend into
	// pre-outage history — a blend would report 2 + 0.3*(6-2) = 3.2 (0.4), a
	// fiction made of dead data.
	ageLastSample(&a.stale, staleSignalThreshold+time.Second)
	a.applySample(6)
	require.InDelta(t, 0.75, a.Utilization(), 1e-9)
}

func TestThreadsRunning_StaleSignalKeepsThrottledHardStop(t *testing.T) {
	a := newTestThreadsRunning(t, 8)

	a.applySample(20) // over vCPUs → throttled
	require.True(t, a.IsThrottled())

	ageLastSample(&a.stale, staleSignalThreshold+time.Second)
	require.True(t, a.IsThrottled(), "staleness must not clear the hard-stop")
	require.InDelta(t, StaleUtilizationHold, a.Utilization(), 1e-9)
}

func TestThreadsRunning_NeverSampledIsNotStale(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	require.Zero(t, a.Utilization(), "pre-Open must read as idle, not as a stale hold")
}

func TestThreadsRunning_BlockWaitReturnsImmediatelyWhenUnthrottled(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	start := time.Now()
	a.BlockWait(t.Context())
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestThreadsRunning_BlockWaitRespectsContext(t *testing.T) {
	a := newTestThreadsRunning(t, 8)
	a.isThrottled.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	a.BlockWait(ctx)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestThreadsRunning_BlockWaitReturnsWhenThrottlingClears(t *testing.T) {
	prev := blockWaitInterval
	blockWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { blockWaitInterval = prev })

	a := newTestThreadsRunning(t, 8)
	a.isThrottled.Store(true)

	go func() {
		time.Sleep(30 * time.Millisecond)
		a.isThrottled.Store(false)
	}()

	start := time.Now()
	a.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestNewThreadsRunningThrottler_RejectsNilDB(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := NewThreadsRunningThrottler(nil, logger)
	require.ErrorContains(t, err, "non-nil DB")
}

func TestThreadsRunningQuery_LocalMySQL(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Threads_running is a stock status variable, so the sampling query must
	// run cleanly and scan into an int on any healthy MySQL — it needs only
	// the global_status access that IsAurora already proves, no perf-schema
	// table grants. A non-negative result confirms the query and its CAST.
	var running int64
	require.NoError(t, db.QueryRowContext(t.Context(), threadsRunningQuery).Scan(&running))
	require.GreaterOrEqual(t, running, int64(1), "the sampling query itself is a running thread")
}

func TestResolveWriteThreads_PassThroughPositive(t *testing.T) {
	// A positive value is returned unchanged without touching the DB, so a
	// nil DB is safe here.
	n, err := ResolveWriteThreads(t.Context(), nil, 8, discardLogger())
	require.NoError(t, err)
	require.Equal(t, 8, n)
}

func TestResolveWriteThreads_RejectsNegative(t *testing.T) {
	_, err := ResolveWriteThreads(t.Context(), nil, -1, discardLogger())
	require.ErrorContains(t, err, "non-negative")
}

func TestResolveWriteThreads_NilDBWhenAutoSizing(t *testing.T) {
	// Auto-sizing (requested==0) must reject a nil DB rather than panic.
	_, err := ResolveWriteThreads(t.Context(), nil, 0, discardLogger())
	require.ErrorContains(t, err, "no database connection")
}

func TestResolveWriteThreads_NonAuroraUsesDefault_LocalMySQL(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// 0 means "auto-size". On a stock (non-Aurora) MySQL there is no reliable
	// vCPU signal, so auto-sizing falls back to the default rather than guess
	// or fail.
	n, err := ResolveWriteThreads(t.Context(), db, 0, discardLogger())
	require.NoError(t, err)
	require.Equal(t, DefaultWriteThreads, n)
}

func TestResolveMaxWriteThreads(t *testing.T) {
	// Disabled: cap equals start so the count cannot move.
	require.Equal(t, 4, ResolveMaxWriteThreads(4, false))

	// Enabled: the cap is fixed at 2x the start value (not configurable).
	require.Equal(t, 8, ResolveMaxWriteThreads(4, true))
	require.Equal(t, 10, ResolveMaxWriteThreads(5, true))
}

func TestAuroraVCPUs_LocalMySQL(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// @@innodb_buffer_pool_instances exists on stock MySQL too (it just isn't
	// pinned to the vCPU count there). The query and positive-value check should
	// still succeed — this guards the shared helper used by both the
	// threads-running throttler and write-thread auto-sizing.
	vCPUs, err := AuroraVCPUs(t.Context(), db)
	require.NoError(t, err)
	require.Positive(t, vCPUs)
}
