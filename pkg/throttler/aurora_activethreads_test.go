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

func newTestActiveThreads(t *testing.T, vCPUs int64) *ActiveThreads {
	t.Helper()
	return &ActiveThreads{
		vCPUs:  vCPUs,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestActiveThreads_BelowVCPUs(t *testing.T) {
	a := newTestActiveThreads(t, 8)
	a.applySample(4)
	require.False(t, a.IsThrottled())
	require.Equal(t, int64(4), a.lastActiveThreads.Load())
}

func TestActiveThreads_AtVCPUsIsNotThrottled(t *testing.T) {
	// Threshold is strictly greater than vCPUs — sitting exactly at vCPUs is
	// not over-subscribed, so we should not throttle.
	a := newTestActiveThreads(t, 8)
	a.applySample(8)
	require.False(t, a.IsThrottled())
}

func TestActiveThreads_AboveVCPUsThrottles(t *testing.T) {
	a := newTestActiveThreads(t, 8)
	a.applySample(9)
	require.True(t, a.IsThrottled())
	require.Equal(t, int64(9), a.lastActiveThreads.Load())
}

func TestActiveThreads_RecoversBelowVCPUs(t *testing.T) {
	a := newTestActiveThreads(t, 8)
	a.applySample(20)
	require.True(t, a.IsThrottled())
	a.applySample(3)
	require.False(t, a.IsThrottled())
}

func TestActiveThreads_Utilization(t *testing.T) {
	a := newTestActiveThreads(t, 8)
	a.applySample(4)
	require.InDelta(t, 0.5, a.Utilization(), 1e-9)
	a.applySample(8) // exactly at vCPUs => 1.0
	require.InDelta(t, 1.0, a.Utilization(), 1e-9)
	a.applySample(12) // oversubscribed => >1.0
	require.InDelta(t, 1.5, a.Utilization(), 1e-9)
}

func TestActiveThreads_UtilizationZeroVCPUs(t *testing.T) {
	// vCPUs unknown (Open not yet called) must not divide by zero.
	a := newTestActiveThreads(t, 0)
	a.applySample(4)
	require.Zero(t, a.Utilization())
}

func TestActiveThreads_BlockWaitReturnsImmediatelyWhenUnthrottled(t *testing.T) {
	a := newTestActiveThreads(t, 8)
	start := time.Now()
	a.BlockWait(t.Context())
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestActiveThreads_BlockWaitRespectsContext(t *testing.T) {
	a := newTestActiveThreads(t, 8)
	a.isThrottled.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	a.BlockWait(ctx)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestActiveThreads_BlockWaitReturnsWhenThrottlingClears(t *testing.T) {
	prev := blockWaitInterval
	blockWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { blockWaitInterval = prev })

	a := newTestActiveThreads(t, 8)
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

func TestNewActiveThreadsThrottler_RejectsNilDB(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := NewActiveThreadsThrottler(nil, logger)
	require.ErrorContains(t, err, "non-nil DB")
}

func TestCanReadActiveThreads_LocalMySQL(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// On a stock MySQL with perf_schema enabled the query should run cleanly
	// even though there's no Aurora-specific data — it's a vanilla perf-
	// schema join. If this ever errors on a healthy MySQL we want to know,
	// since runner.setupThrottler depends on it as the gate.
	require.NoError(t, CanReadActiveThreads(t.Context(), db))
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

	// @@innodb_purge_threads exists on stock MySQL too (it just isn't pinned
	// to the vCPU count there). The query and positive-value check should
	// still succeed — this guards the shared helper used by both the
	// active-threads throttler and write-thread auto-sizing.
	vCPUs, err := auroraVCPUs(t.Context(), db)
	require.NoError(t, err)
	require.Positive(t, vCPUs)
}
