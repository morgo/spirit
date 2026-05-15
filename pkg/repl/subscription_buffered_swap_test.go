package repl

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestBufferedMapSwapPairRecoversViaQueueMode is a deterministic,
// single-threaded regression test for the fix to block/spirit#847.
//
// Background: `bufferedMap.flushMapLocked` iterates `s.changes` (a Go
// map, randomized iteration) and hands the rows to the applier as a
// single `INSERT ... ON DUPLICATE KEY UPDATE` with a multi-row VALUES
// list. MySQL processes that list in array order. When a swap pair
// lands in the same batch — row A's deactivation and row B's activation,
// both from the same source-side transaction — the random map order can
// place B's activation before A's deactivation, and MySQL rejects the
// first row with Error 1062 because A still holds the unique value.
//
// The fix is detect-and-switch (`handleFlushError`): when Flush sees a
// 1062, the subscription discards its pending map, flips into
// forceQueueMode, and asks the client to rewind the binlog reader to
// position 4 of the flushedPos file. The events that were lost when
// the map was cleared are re-read FIFO and applied via flushQueueLocked,
// which preserves binlog order in the resulting batch — no collision.
//
// This test exercises the subscription-level half of that fix: we
// drive a swap pair into the map for 50 independent buckets (so the
// probability that map iteration happens to land *every* pair in the
// safe order is 2^-50, effectively zero), call Flush once, and assert:
//
//   - Flush returns no error (we recovered).
//   - allChangesFlushed=false (so flushedPos won't advance).
//   - The subscription is now in queue mode.
//   - The map is empty (state was discarded).
//
// The end-to-end half of the fix — the client actually rewinding the
// binlog reader and the queue mode applying the re-read events — is
// covered by TestSwapPairFullRecoveryViaQueueMode below.
func TestBufferedMapSwapPairRecoversViaQueueMode(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Seed the pre-swap state in the destination table directly. The
	// source table doesn't need to be populated — we drive HasChanged
	// ourselves instead of relying on the binlog reader.
	//
	// For each bucket i: PK = 2i+1 holds slot_id='S<i>'; PK = 2i+2 holds NULL.
	const buckets = 50
	seed := make([]string, 0, buckets*2)
	for i := 0; i < buckets; i++ {
		seed = append(seed,
			fmt.Sprintf("(%d, 'S%d')", 2*i+1, i),
			fmt.Sprintf("(%d, NULL)", 2*i+2),
		)
	}
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, slot_id) VALUES %s",
		dstTable.QuotedTableName, strings.Join(seed, ", ")))

	mockChunker := table.NewMockChunker(srcTable.TableName, uint64(buckets*2))
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: false, // accept every event; no chunker-progress gating
		pkIsMemoryComparable:  true,  // INT PK → map mode (the failing path)
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Inject the swap pair for every bucket. In source these two
	// UPDATEs are committed inside one transaction:
	//   UPDATE id=active SET slot_id=NULL;
	//   UPDATE id=passive SET slot_id='S<i>';
	// We bypass the binlog reader and call HasChanged with the
	// post-transaction row image for each row.
	for i := 0; i < buckets; i++ {
		activePK := 2*i + 1
		passivePK := 2*i + 2
		sub.HasChanged([]any{activePK}, []any{activePK, nil}, false)
		sub.HasChanged([]any{passivePK}, []any{passivePK, fmt.Sprintf("S%d", i)}, false)
	}
	require.Equal(t, buckets*2, sub.Length())

	// With the buggy map-iteration order, the batched
	// INSERT ... ON DUPLICATE KEY UPDATE will, with probability
	// 1 - 2^-buckets, place at least one bucket's activation before its
	// deactivation and trigger Error 1062 on uq_slot inside flushBatch.
	// `handleFlushError` recognizes that, clears the map, flips
	// forceQueueMode, asks the client to rewind, and returns the flush
	// as a no-op (allChangesFlushed=false, no error).
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "recovery path should swallow the 1062")
	require.False(t, allFlushed,
		"allChangesFlushed must be false so flushedPos stays put — the events still need to be re-read FIFO")

	// State assertions: the recovery path discarded pending changes and
	// flipped the subscription into queue mode.
	sub.Lock()
	require.True(t, sub.forceQueueMode, "forceQueueMode should be set after a 1062 recovery")
	require.True(t, sub.queueModeActive(), "queueModeActive should return true once forceQueueMode is on")
	require.Empty(t, sub.changes, "pending map should be cleared")
	require.Zero(t, sub.sizeBytes, "sizeBytes should be reset to 0")
	sub.Unlock()

	// The recovery also asks the client to rewind the binlog reader.
	// (We constructed the Client directly without a syncer, so the
	// close-the-syncer half of requestRewind is a no-op; the flag flip
	// is the observable signal here.)
	require.True(t, client.rewindRequested.Load(),
		"client should have been asked to rewind the binlog reader")

	// A subsequent HasChanged should route to the queue, not the map.
	sub.HasChanged([]any{42}, []any{42, "after-recovery"}, false)
	sub.Lock()
	require.Empty(t, sub.changes, "new events must not land back in the map")
	require.Len(t, sub.queue, 1, "new events must land in the FIFO queue")
	sub.Unlock()
}

// TestFlushDoesNotRegressFlushedPosAfterRewind covers the defensive
// guard in `Client.flush` against a backwards checkpoint move when
// a rewind has snapped `bufferedPos` back. Without the guard, a flush
// that happened to run between the rewind (which sets bufferedPos =
// {flushedPos.Name, 4}) and readStream catching up past the old
// flushedPos.Pos would publish the rewound bufferedPos as the new
// flushedPos — silently rewinding checkpoints.
//
// We don't run a real binlog reader here. We exercise `flush` directly
// on a client that has no subscriptions (so allChangesFlushed=true)
// and assert that flushedPos does not move backwards when bufferedPos
// is set behind it.
func TestFlushDoesNotRegressFlushedPosAfterRewind(t *testing.T) {
	client := &Client{
		db:              nil, // unused: no subscriptions to flush
		logger:          slog.Default(),
		concurrency:     1,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
		subscriptions:   map[string]Subscription{},
	}

	// Pretend we'd flushed up to (file, 5000) and then a rewind dropped
	// bufferedPos back to the start of the file.
	advanced := mysql.Position{Name: "binlog.000042", Pos: 5000}
	rewound := mysql.Position{Name: "binlog.000042", Pos: 4}
	client.SetFlushedPos(advanced)
	client.setBufferedPos(rewound)

	require.NoError(t, client.flush(t.Context(), false, nil))

	require.Equal(t, advanced, client.GetBinlogApplyPosition(),
		"flushedPos must not regress when a flush captures a rewound bufferedPos")

	// Sanity: once bufferedPos advances back past the old flushedPos, a
	// subsequent flush *does* move the checkpoint forward.
	forward := mysql.Position{Name: "binlog.000042", Pos: 6000}
	client.setBufferedPos(forward)
	require.NoError(t, client.flush(t.Context(), false, nil))
	require.Equal(t, forward, client.GetBinlogApplyPosition(),
		"flushedPos should advance once bufferedPos is ahead again")
}

// TestSwapPairFullRecoveryViaQueueMode drives the full end-to-end
// rewind path: real Client.Run, real binlog reader, real swap
// transactions on source, real Flush calls, real recovery, real
// destination state.
//
// The flow:
//
//  1. Source and destination tables are seeded with the pre-swap state.
//     The seed inserts happen *before* Run starts so they are outside
//     the subscription's binlog window (the rewind will still re-read
//     them from the start of the file, but applying them is idempotent
//     because their values match what's already in dest).
//  2. Run starts the binlog reader at the current server position.
//  3. The test executes N legal swap transactions on source.
//  4. BlockWait waits for the subscription to receive every event.
//  5. The first Flush hits the swap-pair 1062 inside flushMapLocked,
//     recovers via handleFlushError, asks the client to rewind, and
//     returns no error. The subscription is now in forceQueueMode.
//  6. readStream notices the closed syncer, sees rewindRequested, and
//     recreates the streamer at position 4 of flushedPos.Name. As it
//     re-reads events they accumulate in the FIFO queue.
//  7. Subsequent Flush loops drain the queue via flushQueueLocked,
//     which preserves binlog order — no in-batch reordering.
//  8. The destination ends up byte-equal to the source.
func TestSwapPairFullRecoveryViaQueueMode(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applierInstance, &ClientConfig{
		Logger:          slog.Default(),
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))

	// Seed both tables with identical pre-swap state BEFORE Run starts,
	// so these inserts aren't in the subscription's window. (They will
	// still be re-read once the rewind kicks in, but applying them via
	// the FIFO queue is idempotent — they match what's already in dst.)
	const buckets = 50
	var seed []string
	for i := 0; i < buckets; i++ {
		seed = append(seed,
			fmt.Sprintf("(%d, 'S%d')", 2*i+1, i),
			fmt.Sprintf("(%d, NULL)", 2*i+2),
		)
	}
	insertVals := strings.Join(seed, ", ")
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, slot_id) VALUES %s",
		srcTable.QuotedTableName, insertVals))
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, slot_id) VALUES %s",
		dstTable.QuotedTableName, insertVals))

	require.NoError(t, client.Run(t.Context()))
	defer client.Close()

	sub := client.subscriptions[srcTable.SchemaName+"."+srcTable.TableName].(*bufferedMap)
	// Disable the watermark optimization so every binlog event is
	// admitted to the map (in production this is what happens after
	// the copy phase finishes — the post-copy applier sees every event).
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))

	// Run swap transactions on source. Each is a legal
	// deactivate-then-activate inside a single transaction.
	for i := 0; i < buckets; i++ {
		activePK := 2*i + 1
		passivePK := 2*i + 2
		slot := fmt.Sprintf("S%d", i)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(),
			fmt.Sprintf("UPDATE %s SET slot_id=NULL WHERE id=?", srcTable.QuotedTableName), activePK)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(),
			fmt.Sprintf("UPDATE %s SET slot_id=? WHERE id=?", srcTable.QuotedTableName), slot, passivePK)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	// Drive client.Flush through to completion. Internally it loops
	// flush -> BlockWait -> flush until the delta is trivial; the
	// recovery+rewind path is wholly contained inside that loop.
	require.NoError(t, client.Flush(t.Context()))

	// The subscription must have detected the swap-pair collision and
	// switched modes — otherwise the bug would still be present.
	sub.Lock()
	require.True(t, sub.forceQueueMode,
		"the swap-pair collision should have triggered the recovery path")
	sub.Unlock()

	// End-state assertion: dst must match src bucket-for-bucket. Active
	// rows have moved from PK=2i+1 to PK=2i+2.
	for i := 0; i < buckets; i++ {
		activePK := 2*i + 1
		passivePK := 2*i + 2
		var srcActive, dstActive sql.NullString
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", srcTable.QuotedTableName), activePK).
			Scan(&srcActive))
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", dstTable.QuotedTableName), activePK).
			Scan(&dstActive))
		require.False(t, srcActive.Valid, "source bucket %d previously-active row should now be NULL", i)
		require.Equal(t, srcActive, dstActive,
			"bucket %d previously-active row (PK=%d) state must match between src and dst", i, activePK)

		var srcPassive, dstPassive sql.NullString
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", srcTable.QuotedTableName), passivePK).
			Scan(&srcPassive))
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", dstTable.QuotedTableName), passivePK).
			Scan(&dstPassive))
		require.True(t, srcPassive.Valid, "source bucket %d previously-passive row should now hold the slot", i)
		require.Equal(t, srcPassive, dstPassive,
			"bucket %d previously-passive row (PK=%d) state must match between src and dst", i, passivePK)
	}
}
