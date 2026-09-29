package throttler

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

const (
	replicaBlockWaitTestInterval = 10 * time.Millisecond
	replicaBlockWaitRecoverAfter = 3 * replicaBlockWaitTestInterval
	replicaBlockWaitMinDuration  = 2 * replicaBlockWaitTestInterval
	replicaBlockWaitMaxDuration  = 50 * replicaBlockWaitTestInterval
)

func newTestReplica(t *testing.T, tolerance time.Duration) *Replica {
	t.Helper()
	return &Replica{
		lagTolerance: tolerance,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func useReplicaBlockWaitTestInterval(t *testing.T) {
	t.Helper()
	prev := blockWaitInterval
	blockWaitInterval = replicaBlockWaitTestInterval
	t.Cleanup(func() { blockWaitInterval = prev })
}

func TestReplica_LagBasedThrottling(t *testing.T) {
	l := newTestReplica(t, 120*time.Second)

	l.applyLag(5) // 5ms, healthy
	require.False(t, l.IsThrottled())

	l.applyLag(120_000) // exactly at the tolerance
	require.True(t, l.IsThrottled())

	l.applyLag(30_000) // recovered below the tolerance
	require.False(t, l.IsThrottled())
}

func TestReplica_FailsClosedWhenLagUnobservable(t *testing.T) {
	l := newTestReplica(t, 120*time.Second)

	// Healthy poll: low lag, not throttled.
	l.applyLag(5)
	require.False(t, l.IsThrottled())

	// Lag polling starts failing. The poll loop only logs, so the cached lag
	// freezes at the last healthy value (5ms). Once the last successful poll
	// is older than staleSignalThreshold the throttler must fail closed:
	// nobody is measuring the lag budget anymore, so the copy pauses rather
	// than run at full speed. (Pre-fix this stayed false indefinitely.)
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)
	require.True(t, l.IsThrottled())

	// A successful poll resumes normal lag-based behavior...
	l.applyLag(5)
	require.False(t, l.IsThrottled())

	// ...including throttling on real lag as before.
	l.applyLag(999_999)
	require.True(t, l.IsThrottled())
}

func TestReplica_NeverPolledIsNotStale(t *testing.T) {
	// Open() fails if the very first poll fails, so "no poll yet" means the
	// throttler isn't open — not that a working signal died. It must not
	// report throttled before the first observation.
	l := newTestReplica(t, 120*time.Second)
	require.False(t, l.IsThrottled())
}

func TestReplica_BlockWaitReturnsImmediatelyWhenUnthrottled(t *testing.T) {
	l := newTestReplica(t, 60*time.Second)
	start := time.Now()
	l.BlockWait(t.Context())
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestReplica_BlockWaitRespectsContext(t *testing.T) {
	l := newTestReplica(t, 60*time.Second)
	l.applyLag(60_000)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	l.BlockWait(ctx)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestReplica_BlockWaitFailsClosedOnStaleSignal(t *testing.T) {
	useReplicaBlockWaitTestInterval(t)

	l := newTestReplica(t, 120*time.Second)
	l.applyLag(5) // healthy baseline, well under the tolerance
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)

	// Recover the signal shortly after BlockWait starts blocking: BlockWait
	// must hold while lag is unobservable (the copier's enforcement path is
	// BlockWait, not IsThrottled) and release once polling recovers.
	go func() {
		time.Sleep(replicaBlockWaitRecoverAfter)
		l.applyLag(5)
	}()

	start := time.Now()
	l.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, replicaBlockWaitMinDuration, "BlockWait must block while lag is unobservable")
	require.Less(t, elapsed, replicaBlockWaitMaxDuration)
}

func TestReplica_BlockWaitReturnsWhenLagRecovers(t *testing.T) {
	useReplicaBlockWaitTestInterval(t)

	l := newTestReplica(t, 60*time.Second)
	l.applyLag(60_000) // exactly at the tolerance, so BlockWait must hold

	go func() {
		time.Sleep(replicaBlockWaitRecoverAfter)
		l.applyLag(59_999)
	}()

	start := time.Now()
	l.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, replicaBlockWaitMinDuration, "BlockWait must block while lag is over budget")
	require.Less(t, elapsed, replicaBlockWaitMaxDuration)
}

func TestReplica_BlockWaitReturnsAfterGiveUpBudget(t *testing.T) {
	useReplicaBlockWaitTestInterval(t)

	l := newTestReplica(t, 60*time.Second)
	l.applyLag(60_000)

	start := time.Now()
	l.BlockWait(t.Context())
	elapsed := time.Since(start)

	// BlockWait deliberately gives the caller a chance to make progress after
	// 60 intervals even if the replica remains over budget. Pin that safety
	// tradeoff so shortening the loop cannot silently weaken throttling.
	require.GreaterOrEqual(t, elapsed, 60*replicaBlockWaitTestInterval)
	require.True(t, l.IsThrottled())
}

func TestReplica_UpdateLagWrapsCause(t *testing.T) {
	// sql.Open is lazy and does not connect. A pre-canceled context makes
	// QueryRowContext fail deterministically with context.Canceled before any
	// dial, letting us assert the cause survives UpdateLag's wrapping.
	db, err := sql.Open("block-mysql", "user:pass@tcp(127.0.0.1:0)/test")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	l := newTestReplica(t, 120*time.Second)
	l.replica = db

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = l.UpdateLag(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled, "UpdateLag must wrap the underlying cause, not replace it")
	require.ErrorContains(t, err, "could not check replication lag")
}

// TestMySQL8LagQueryClampsAtZero runs MySQL8LagQuery against stand-in tables
// for the two performance_schema tables it reads, so the arithmetic can be
// checked on the primary without depending on the state of a live replica.
// A commit timestamp that is ahead of the replica's NOW(6) (clock skew, or a
// lower-precision NOW() truncating the current second) must report 0, not a
// negative lag. See https://github.com/block/spirit/issues/1326.
func TestMySQL8LagQueryClampsAtZero(t *testing.T) {
	worker := testutils.NewTestTable(t, "lagq_worker",
		`CREATE TABLE lagq_worker (
			channel_name VARCHAR(64) NOT NULL PRIMARY KEY,
			APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP DATETIME(6) NOT NULL,
			LAST_APPLIED_TRANSACTION VARCHAR(64) NOT NULL,
			LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP DATETIME(6) NOT NULL
		)`)
	testutils.NewTestTable(t, "lagq_conn",
		`CREATE TABLE lagq_conn (
			channel_name VARCHAR(64) NOT NULL PRIMARY KEY,
			LAST_QUEUED_TRANSACTION VARCHAR(64) NOT NULL,
			LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP DATETIME(6) NOT NULL
		)`)
	query := strings.ReplaceAll(MySQL8LagQuery, "performance_schema.replication_applier_status_by_worker", "lagq_worker")
	query = strings.ReplaceAll(query, "performance_schema.replication_connection_status", "lagq_conn")
	require.NotContains(t, query, "performance_schema")

	// setCommitOffset places both commit timestamps offsetMs from the server's
	// NOW(6), with a queued transaction not yet applied so the queue branch
	// computes a latency instead of returning 0.
	setCommitOffset := func(offsetMs int) {
		t.Helper()
		ctx := t.Context()
		_, err := worker.DB.ExecContext(ctx, "REPLACE INTO lagq_worker VALUES ('', NOW(6) + INTERVAL ? MICROSECOND, 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:5', NOW(6) + INTERVAL ? MICROSECOND)", offsetMs*1000, offsetMs*1000)
		require.NoError(t, err)
		_, err = worker.DB.ExecContext(ctx, "REPLACE INTO lagq_conn VALUES ('', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:10', NOW(6))")
		require.NoError(t, err)
	}
	lagMs := func() int64 {
		t.Helper()
		var lag int64
		require.NoError(t, worker.DB.QueryRowContext(t.Context(), query).Scan(&lag))
		return lag
	}

	// Committed in the future relative to the replica clock: clamped to 0.
	setCommitOffset(500)
	require.Equal(t, int64(0), lagMs())

	// Committed 5s ago: the lag is reported, so the clamp does not hide it.
	setCommitOffset(-5000)
	lag := lagMs()
	require.GreaterOrEqual(t, lag, int64(5000))
	require.Less(t, lag, int64(10000))
}
