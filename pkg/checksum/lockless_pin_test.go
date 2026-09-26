package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

// pinTestChecker builds the smallest checker that can run the settle path: a
// source, a target, and a feed. No chunker and no recopier — settleHotSnapshot
// uses neither.
func pinTestChecker(t *testing.T, db *sql.DB, feed *fakeFeed) *LocklessChecker {
	t.Helper()
	cfg := CheckerConfig{}
	applySharedDefaults(&cfg)
	// A typed nil would satisfy the interface and defeat the no-feed case.
	var source change.Source
	if feed != nil {
		source = feed
	}
	return newLocklessChecker(db, db, nil, source, nil, &cfg)
}

// pendingSnapshot captures a snapshot and asserts the fallback really did leave
// obligations outstanding, so a settle verdict is about the settle path rather
// than about an empty work set.
func pendingSnapshot(t *testing.T, db *sql.DB, chunk *table.Chunk, wantPending int) *hotSnapshot {
	t.Helper()
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	passed, err := snapshot.check(t.Context())
	require.NoError(t, err)
	require.False(t, passed)
	require.Len(t, snapshot.pending, wantPending)
	return snapshot
}

// TestSettleHotSnapshotClean is the case the escalation exists for: the target
// was genuinely behind when the snapshot froze, and has since caught up. Polling
// would have found that too — what matters here is that pinning reaches the same
// verdict in one shot rather than depending on catching a quiet moment.
func TestSettleHotSnapshotClean(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	snapshotExec(t, db, "INSERT INTO dst VALUES (2,20)")
	c := pinTestChecker(t, db, &fakeFeed{})
	verdict, err := c.settleHotSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, pinnedClean, verdict)
}

// TestSettleHotSnapshotDiverged is the verdict polling can never reach. The
// target holds a value that no amount of waiting will correct, and with the
// source pinned and the feed drained there is nothing left that could explain it.
func TestSettleHotSnapshotDiverged(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	c := pinTestChecker(t, db, &fakeFeed{})
	verdict, err := c.settleHotSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, pinnedDiverged, verdict)
}

// TestSettleHotSnapshotMissingTargetRowDiverges covers the other divergence
// shape: the row is on the source and absent from the target. The pinned read
// found it, so it cannot be a row the source deleted while we looked.
func TestSettleHotSnapshotMissingTargetRowDiverges(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	c := pinTestChecker(t, db, &fakeFeed{})
	verdict, err := c.settleHotSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, pinnedDiverged, verdict)
}

// TestSettleHotSnapshotAbsenceIsUnavailable is the honesty constraint. A row
// that must NOT be on the target cannot be settled: READ COMMITTED has no lock
// to take on a row that is not there, so an INSERT can still land between the
// drain and the read. The range defers exactly as it did before.
func TestSettleHotSnapshotAbsenceIsUnavailable(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)
	for _, row := range snapshot.pending {
		require.False(t, row.present, "the outstanding obligation should be an absence")
	}

	c := pinTestChecker(t, db, &fakeFeed{})
	verdict, err := c.settleHotSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, pinnedUnavailable, verdict)
}

// TestSettleHotSnapshotWithoutFeedIsUnavailable: without a feed there is no way
// to know the target has seen everything the source has done, so the drain step
// is meaningless and no verdict is honest. Library callers may have no feed.
func TestSettleHotSnapshotWithoutFeedIsUnavailable(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	c := pinTestChecker(t, db, nil)
	verdict, err := c.settleHotSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, pinnedUnavailable, verdict)
}

// TestSettleHotSnapshotYieldsToWriters is the blast-radius guarantee. A hot row
// is by definition one other transactions are holding; the settle path must lose
// that race quickly and defer rather than queue up behind application writes.
func TestSettleHotSnapshotYieldsToWriters(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	writer, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer writer.Close() //nolint:errcheck // test cleanup
	_, err = writer.ExecContext(t.Context(), "BEGIN")
	require.NoError(t, err)
	_, err = writer.ExecContext(t.Context(), "UPDATE src SET value=21 WHERE id=2")
	require.NoError(t, err)

	c := pinTestChecker(t, db, &fakeFeed{})
	verdict, err := c.settleHotSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, pinnedUnavailable, verdict, "an exclusively locked row must defer, not block")

	_, err = writer.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, err)
}

// TestSettleHotSnapshotHoldsSourceStill is the property the whole escalation
// rests on: while the window is open, no transaction can change the rows being
// settled. The feed's drain runs inside that window, so this asserts from there.
func TestSettleHotSnapshotHoldsSourceStill(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
	snapshot := pendingSnapshot(t, db, chunk, 1)
	snapshotExec(t, db, "INSERT INTO dst VALUES (2,20)")

	var blocked bool
	feed := &fakeFeed{flushFn: func(ctx context.Context) error {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck // test cleanup
		_, err = conn.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 1")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "UPDATE src SET value=999 WHERE id=2")
		blocked = err != nil
		return nil
	}}

	c := pinTestChecker(t, db, feed)
	verdict, err := c.settleHotSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.True(t, blocked, "a writer must not be able to move a pinned row mid-settle")
	require.Equal(t, pinnedClean, verdict)
}

// TestCheckHotSnapshotEscalatesOnlyWhenExhausted: settling takes locks, so it
// must be the terminal step and not something every poll does. Below
// MaxHotAttempts the range keeps polling; at the limit it settles instead of
// deferring.
func TestCheckHotSnapshotEscalatesOnlyWhenExhausted(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	cfg := CheckerConfig{MaxHotAttempts: 3}
	applySharedDefaults(&cfg)
	c := newLocklessChecker(db, db, nil, &fakeFeed{}, nil, &cfg)
	settles := 0
	c.pinSourceRows = func(ctx context.Context, s *hotSnapshot, keys [][]table.Datum) (*pinnedRows, error) {
		settles++
		return pinSourceRows(ctx, db, s, keys)
	}

	res := &workResult{item: &workItem{chunk: chunk}}
	// snapshot.check already consumed one attempt in pendingSnapshot.
	for snapshot.attempts < cfg.MaxHotAttempts-1 {
		c.checkHotSnapshot(t.Context(), res, snapshot)
		require.NoError(t, res.err)
		require.False(t, res.passed)
		require.False(t, res.deferHot)
		require.Zero(t, settles, "must not take locks while ordinary polling is still allowed")
	}

	c.checkHotSnapshot(t.Context(), res, snapshot)
	require.NoError(t, res.err)
	require.Equal(t, 1, settles)
	require.True(t, snapshot.settled)
	require.False(t, res.deferHot, "a settled divergence is a verdict, not a deferral")
	require.True(t, res.permanent, "no recopier configured, so a settled divergence is fatal")
	require.Equal(t, uint64(1), c.hotChunksSettledThisPass.Load())
}

// TestLocklessSettlesHotChunkEndToEnd is the whole point of the escalation,
// driven through RunUntilClean rather than by calling settleHotSnapshot.
//
// The chunk's aggregate never matches — the source CRC changes on every read,
// which is what a continuously written range looks like — so the checker falls
// back to the row snapshot, and the snapshot never converges by polling either,
// because polling is passive: it reads the target and waits. Before settling,
// that combination had exactly one outcome regardless of whether the data was
// actually wrong. Now it reaches a verdict, and which verdict depends on the
// data:
//
//   - converge: the missing row is buffered in the feed and lands when the
//     settle path flushes it, so the range verifies and the pass goes clean.
//   - diverge: the target holds a value nothing will ever correct, so the
//     range is reported rather than deferred for the rest of the run.
func TestLocklessSettlesHotChunkEndToEnd(t *testing.T) {
	for _, converge := range []bool{true, false} {
		t.Run(fmt.Sprint(converge), func(t *testing.T) {
			db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
			snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
			snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
			if !converge {
				// Present but wrong, which no flush can fix.
				snapshotExec(t, db, "INSERT INTO dst VALUES (2,99)")
			}

			chunker := newTestChunker(1)
			chunker.chunks[0] = chunk
			cfg := fastConfig()
			cfg.RetryDelay = time.Millisecond
			cfg.MaxHotAttempts = 3
			cfg.MinPassInterval = time.Hour
			// No recopier: a settled divergence must be reported, which is the
			// clearest way to see that a verdict was reached at all.
			c := newTestChecker(t, chunker, cfg, func(_ context.Context, _ *table.Chunk, attempt int) (int64, int64, uint64, error) {
				return int64(attempt), 0, 1, nil // the source never stops moving
			})
			c.snapshotChunk = func(ctx context.Context, chunk *table.Chunk) (*hotSnapshot, error) {
				return captureHotSnapshot(ctx, db, db, chunk)
			}
			c.pinSourceRows = func(ctx context.Context, s *hotSnapshot, keys [][]table.Datum) (*pinnedRows, error) {
				return pinSourceRows(ctx, db, s, keys)
			}
			// The feed holds the row the target is missing. Flushing it inside
			// the settle window is what the drain step is for.
			c.feed = &fakeFeed{flushFn: func(ctx context.Context) error {
				if converge {
					_, err := db.ExecContext(ctx, "REPLACE INTO dst SELECT * FROM src")
					return err
				}
				return nil
			}}

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			err := c.RunUntilClean(ctx)

			stats := c.Stats()
			require.Equal(t, uint64(1), stats.HotChunksSettledThisPass, "the range must reach a verdict, not defer")
			require.Zero(t, stats.HotChunksDeferredThisPass)
			if converge {
				require.NoError(t, err)
				require.False(t, stats.FirstCleanPassAt.IsZero())
				var diffs int
				require.NoError(t, db.QueryRowContext(t.Context(),
					"SELECT COUNT(*) FROM src LEFT JOIN dst USING (id, value) WHERE dst.id IS NULL").Scan(&diffs))
				require.Zero(t, diffs, "the settle path's drain must have landed the missing row")
				return
			}
			require.ErrorIs(t, err, ErrPermanentDivergence,
				"a settled divergence is reported; before settling it was invisible")
		})
	}
}
