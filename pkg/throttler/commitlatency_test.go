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

func newTestCommitLatency(t *testing.T, threshold time.Duration) *CommitLatency {
	t.Helper()
	return &CommitLatency{
		threshold: threshold,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestCommitLatency_FirstSampleEstablishesBaseline(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	// First sample with cumulative-since-boot values that would otherwise
	// look like 200ms avg latency. We must NOT throttle on a single sample
	// because we have no Δ window yet.
	c.applySample(1_000_000, 200_000_000_000) // 200_000us avg if interpreted as a delta
	require.False(t, c.IsThrottled())
}

func TestCommitLatency_BelowThreshold(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000) // baseline
	// 1000 commits added over the window, +2_000_000us latency → 2ms avg
	c.applySample(1_001_000, 2_002_000_000)
	require.False(t, c.IsThrottled())
	require.Equal(t, int64(2_000), c.avgLatencyUs.Load())
}

func TestCommitLatency_AtThresholdIsThrottled(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	// 1000 commits, +100_000_000us latency → 100ms avg, exactly at threshold
	c.applySample(1_001_000, 2_100_000_000)
	require.True(t, c.IsThrottled())
	require.Equal(t, int64(100_000), c.avgLatencyUs.Load())
}

func TestCommitLatency_AboveThreshold(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	// 100 commits, +20_000_000us latency → 200ms avg
	c.applySample(1_000_100, 2_020_000_000)
	require.True(t, c.IsThrottled())
	require.Equal(t, int64(200_000), c.avgLatencyUs.Load())
}

func TestCommitLatency_RecoversBelowThreshold(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_000_100, 2_020_000_000) // 200ms avg → throttled
	require.True(t, c.IsThrottled())

	// Next window: 1000 commits at 5ms avg → unthrottled.
	c.applySample(1_001_100, 2_025_000_000)
	require.False(t, c.IsThrottled())
}

func TestCommitLatency_NoNewCommitsClearsThrottle(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_000_100, 2_020_000_000) // throttled
	require.True(t, c.IsThrottled())

	// Idle window: no new commits. Treat as "no signal" rather than carrying
	// the previous throttled state forward — the workload has gone quiet.
	c.applySample(1_000_100, 2_020_000_000)
	require.False(t, c.IsThrottled())
}

func TestCommitLatency_CounterResetClearsThrottle(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_000_100, 2_020_000_000) // throttled
	require.True(t, c.IsThrottled())

	// Server restart / failover: counters go backwards. Don't report a
	// nonsense huge negative latency — just drop the sample.
	c.applySample(50, 1_000)
	require.False(t, c.IsThrottled())
	require.Equal(t, int64(0), c.avgLatencyUs.Load())
}

func TestCommitLatency_BlockWaitReturnsImmediatelyWhenUnthrottled(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)
	start := time.Now()
	c.BlockWait(t.Context())
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestCommitLatency_BlockWaitRespectsContext(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)
	c.isThrottled.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	c.BlockWait(ctx)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestCommitLatency_BlockWaitReturnsWhenThrottlingClears(t *testing.T) {
	// Use a short blockWaitInterval so the test doesn't have to wait a full
	// second per loop iteration. Save and restore.
	prev := blockWaitInterval
	blockWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { blockWaitInterval = prev })

	c := newTestCommitLatency(t, 100*time.Millisecond)
	c.isThrottled.Store(true)

	go func() {
		time.Sleep(30 * time.Millisecond)
		c.isThrottled.Store(false)
	}()

	start := time.Now()
	c.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestIsAurora_LocalMySQLReturnsFalse(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	got, err := IsAurora(t.Context(), db)
	require.NoError(t, err)
	// Local MySQL in CI/dev does not expose AuroraDb_* status variables.
	// If this ever fires on a real Aurora MYSQL_DSN, the test should be
	// updated to check the cluster type rather than asserting false.
	require.False(t, got, "IsAurora returned true against local MySQL — set MYSQL_DSN to a non-Aurora server")
}

func TestNewCommitLatencyThrottler_RejectsBadInputs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	_, err := NewCommitLatencyThrottler(nil, 100*time.Millisecond, logger)
	require.ErrorContains(t, err, "non-nil DB")

	// Threshold <= 0 is rejected — setupThrottler is responsible for not
	// constructing the throttler at all when MaxCommitLatency is 0. Use a
	// non-nil *sql.DB so the error must come from the threshold check, not
	// the nil-DB guard. sql.Open is lazy and does not actually connect.
	db, openErr := sql.Open("mysql", "user:pass@tcp(127.0.0.1:0)/db")
	require.NoError(t, openErr)
	defer utils.CloseAndLog(db)

	_, err = NewCommitLatencyThrottler(db, 0, logger)
	require.ErrorContains(t, err, "positive threshold")
	_, err = NewCommitLatencyThrottler(db, -1*time.Millisecond, logger)
	require.ErrorContains(t, err, "positive threshold")
}
