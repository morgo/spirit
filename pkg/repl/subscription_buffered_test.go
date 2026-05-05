package repl

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestBufferedMap(t *testing.T) {
	db, client, srcTable, dstTable := setupBufferedTest(t)
	defer client.Close()
	defer utils.CloseAndLog(db)

	// Insert into srcTable.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'test')", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	require.Equal(t, 1, client.GetDeltaLen())

	// Inspect the subscription directly.
	sub, ok := client.subscriptions[srcTable.SchemaName+"."+srcTable.TableName].(*bufferedMap)
	require.True(t, ok)
	require.Equal(t, 1, sub.Length())

	// Check the logical row structure
	require.False(t, sub.changes["1"].logicalRow.IsDeleted)
	require.Equal(t, []any{int32(1), "test"}, sub.changes["1"].logicalRow.RowImage)

	// Now delete the row.
	testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s WHERE id = 1", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))

	require.True(t, sub.changes["1"].logicalRow.IsDeleted)
	require.Equal(t, []any(nil), sub.changes["1"].logicalRow.RowImage)

	// Now insert 2 more rows:
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'test2'), (3, 'test3')", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))

	require.Equal(t, 3, sub.Length())
	require.False(t, sub.changes["2"].logicalRow.IsDeleted)
	require.False(t, sub.changes["3"].logicalRow.IsDeleted)

	// Now flush the changes.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	// The destination table should now have the 2 rows.
	var name string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 2", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "test2", name)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 3", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "test3", name)
}

// TestBufferedMapVariableColumns tests the buffered map with a newTable
// That doesn't have all the columns of the source table.
func TestBufferedMapVariableColumns(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		extracol JSON,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		newcol INT,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Run(t.Context()))

	defer client.Close()
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (id, name, extracol) VALUES (1, 'whatever', JSON_ARRAY(1,2,3))", srcTable.QuotedTableName))
	require.NoError(t, err)
	require.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))
}

// TestBufferedMapIllegalValues tests the buffered map with values that
// need escaping (e.g. quotes, backslashes, nulls).
func TestBufferedMapIllegalValues(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		extracol JSON,
		dt DATETIME,
		ts TIMESTAMP,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		newcol INT,
		dt DATETIME,
		ts TIMESTAMP,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Run(t.Context()))

	defer client.Close()
	defer utils.CloseAndLog(db)
	// Insert into srcTable various illegal values.
	// This includes quotes, backslashes, and nulls.
	// Also test with a string that includes a null byte.

	_, err = db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (id, name, dt, ts) VALUES (1, 'test''s', '2025-10-06 09:09:46 +02:00', '2025-10-06 09:09:46 +02:00'), (2, 'back\\slash', NOW(), NOW()), (3, NULL, NOW(), NOW()), (4, 'null\000byte', NOW(), NOW())", srcTable.QuotedTableName))
	require.NoError(t, err)
	require.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	require.Equal(t, 4, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	// Now we want to check that the tables match,
	// using an adhoc checksum.
	var checksumSrc, checksumDst string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT BIT_XOR(CRC32(name)) as checksum FROM %s", srcTable.QuotedTableName)).Scan(&checksumSrc)
	require.NoError(t, err)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT BIT_XOR(CRC32(name)) as checksum FROM %s", dstTable.QuotedTableName)).Scan(&checksumDst)
	require.NoError(t, err)
	require.Equal(t, checksumSrc, checksumDst, "Checksums do not match between source and destination tables")
}

// TestBufferedMapFlushUnderLockBypassesWatermark is a regression test for the bug where
// watermark optimization was incorrectly applied during underLock=true flush.
// This caused data loss during cutover because some changes were skipped.
//
// The test verifies that when underLock=true:
// 1. ALL changes are flushed, regardless of watermark position
// 2. Changes that would normally be skipped (below low watermark) are still flushed
//
// With the bug (old code): This test would fail because changes below the low watermark
// would be skipped even during underLock=true flush, leaving them in the subscription map.
//
// With the fix (new code): This test passes because underLock=true bypasses the
// watermark optimization check, ensuring all changes are flushed.
func TestBufferedMapFlushUnderLockBypassesWatermark(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	logger := slog.Default()
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
		logger:          logger,
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with current position at 5
	// This means:
	// - Keys < 5 are below the low watermark (copier has passed them, safe to flush)
	// - Keys >= 5 are NOT below the low watermark (copier is at or hasn't reached them)
	// - Keys > 5 are above the high watermark (copier hasn't reached them, don't track)
	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.SimulateProgress(0.005) // Current position at 5
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true, // Enable watermark optimization
		pkIsMemoryComparable:  true, // INT PK -> map mode
	}

	// Insert test data into source table - data must exist before we lock
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES
		(1, 'below_watermark'),
		(3, 'below_watermark_2'),
		(5, 'at_watermark'),
		(4, 'below_watermark_3')`, srcTable.QuotedTableName))

	// Add changes for keys both below and at/above the low watermark
	// Keys 1, 3, 4 are below watermark (< 5) - these would NOT be flushed normally
	// because bufferedMap has inverted logic: KeyBelowLowWatermark returns true for keys
	// that are still being copied, so we skip flushing them
	sub.HasChanged([]any{1}, []any{1, "below_watermark"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3"}, false)
	// Key 5 is at the watermark (== 5) - this would be flushed normally
	// because KeyBelowLowWatermark(5) = (5 < 5) = false
	sub.HasChanged([]any{5}, []any{5, "at_watermark"}, false)

	require.Equal(t, 4, sub.Length(), "Should have 4 pending changes")

	// Verify watermark behavior before flush
	require.True(t, mockChunker.KeyBelowLowWatermark(1), "Key 1 should be below watermark")
	require.True(t, mockChunker.KeyBelowLowWatermark(3), "Key 3 should be below watermark")
	require.True(t, mockChunker.KeyBelowLowWatermark(4), "Key 4 should be below watermark")
	require.False(t, mockChunker.KeyBelowLowWatermark(5), "Key 5 should NOT be below watermark (at current position)")

	// First, disable watermark optimization and do a normal flush to populate the new table
	// This simulates the state after the copier has finished
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())

	// Verify all rows were copied
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 4, count, "All 4 rows should be in the new table")

	// Now re-enable watermark optimization and add the same changes again
	// This simulates changes that came in during the final stages of migration
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), true))
	sub.HasChanged([]any{1}, []any{1, "below_watermark_updated"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2_updated"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3_updated"}, false)
	sub.HasChanged([]any{5}, []any{5, "at_watermark_updated"}, false)
	require.Equal(t, 4, sub.Length(), "Should have 4 pending changes again")

	// Create a table lock for underLock=true flush
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	defer utils.CloseAndLogWithContext(t.Context(), lock)

	// Flush with underLock=true
	// This is the critical test: ALL changes should be flushed, including keys 1, 3, 4 which are below watermark
	// The key assertion is that sub.Length() becomes 0, meaning all changes were flushed
	allFlushed, err = sub.Flush(t.Context(), true, lock)

	// THE KEY ASSERTION: With the fix, allFlushed should be true and sub.Length() should be 0
	// With the bug (before the fix), allFlushed would be false and sub.Length() would be 3
	// because keys 1, 3, 4 (below watermark) would not be flushed
	require.NoError(t, err)
	require.True(t, allFlushed, "All changes should be flushed when underLock=true - THIS IS THE KEY TEST")
	require.Equal(t, 0, sub.Length(), "All changes should be removed from the map after flush - THIS IS THE KEY TEST")

	// Note: We don't verify the database state here because the applier operations
	// are executed under the table lock. The important test is that sub.Length() == 0,
	// which proves all changes were processed and removed from the subscription map.
}

// TestBufferedMapFlushWithoutLockRespectsWatermark verifies that when underLock=false,
// the watermark optimization is applied: keys below the watermark (already copied) are
// flushed, while keys at or above the watermark (copier hasn't passed them) are skipped.
// This matches the deltaMap behavior.
func TestBufferedMapFlushWithoutLockRespectsWatermark(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	logger := slog.Default()
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
		logger:          logger,
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with current position at 5
	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.SimulateProgress(0.005) // Current position at 5
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true, // Enable watermark optimization
		pkIsMemoryComparable:  true, // INT PK -> map mode
	}

	// Insert test data into source table
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES
		(1, 'below_watermark'),
		(3, 'below_watermark_2'),
		(5, 'at_watermark'),
		(4, 'below_watermark_3')`, srcTable.QuotedTableName))

	// Add changes for keys both below and at the low watermark
	sub.HasChanged([]any{1}, []any{1, "below_watermark"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3"}, false)
	sub.HasChanged([]any{5}, []any{5, "at_watermark"}, false) // At watermark, not below

	require.Equal(t, 4, sub.Length(), "Should have 4 pending changes")

	// Flush WITHOUT lock (underLock=false)
	// This should respect the watermark optimization:
	// Keys 1, 3, 4 are below watermark (copier already passed them) → flushed
	// Key 5 is at watermark (copier hasn't passed it) → NOT flushed
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.False(t, allFlushed, "Not all changes should be flushed when watermark optimization is active")
	require.Equal(t, 1, sub.Length(), "Key 5 (at watermark) should remain in the map")
	require.Equal(t, int64(4), sub.keysAdded.Load(), "all four HasChanged calls should have incremented keysAdded")
	require.Equal(t, int64(1), sub.keysSkippedBelow.Load(), "key 5 should be counted as skipped-below-low-watermark")

	// Verify that 3 rows (below watermark) were copied to the new table
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count, "Only 3 rows below watermark should be in the new table")

	// Verify that rows below watermark were copied
	var name string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 1", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "below_watermark", name)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 3", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "below_watermark_2", name)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 4", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "below_watermark_3", name)

	// Verify that the row at watermark was NOT copied
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 5", dstTable.QuotedTableName)).Scan(&name)
	require.Error(t, err, "Row with id=5 (at watermark) should NOT exist yet")
}

// TestBufferedMapQueueModeRouting verifies the routing under
// forceEnableBufferedMap=true (the optimization): map during copy,
// queue post-copy. See subscription_buffered.go for the full state table.
func TestBufferedMapQueueModeRouting(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	require.Error(t, srcTable.PrimaryKeyIsMemoryComparable(),
		"VARCHAR PK must be non-memory-comparable for this test")

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                      client,
		table:                  srcTable,
		newTable:               dstTable,
		changes:                make(map[string]bufferedChange),
		chunker:                mockChunker,
		pkIsMemoryComparable:   false,
		forceEnableBufferedMap: true, // exercise the map-during-copy optimization
	}

	// Watermark optimization on => map mode under the optimization.
	sub.watermarkOptimization = true
	sub.HasChanged([]any{"abc"}, []any{"abc", "value"}, false)
	require.Len(t, sub.changes, 1, "watermark on: event must go to the map")
	require.Empty(t, sub.queue, "watermark on: queue must remain empty")

	// Watermark optimization off + non-memory-comparable PK => queue mode.
	// Direct field assignment (rather than SetWatermarkOptimization) so the
	// existing map entry is preserved — the test asserts the *routing* of
	// new events, not the drain-on-toggle behaviour.
	sub.watermarkOptimization = false
	sub.HasChanged([]any{"def"}, []any{"def", "v1"}, false)
	sub.HasChanged([]any{"ghi"}, nil, true)
	require.Len(t, sub.changes, 1, "queue mode: pre-existing map entry preserved (direct field assignment, no drain)")
	require.Len(t, sub.queue, 2, "queue mode: new events appended to queue in order")
	require.Equal(t, utils.HashKey([]any{"def"}), sub.queue[0].key)
	require.False(t, sub.queue[0].logicalRow.IsDeleted)
	require.Equal(t, []any{"def", "v1"}, sub.queue[0].logicalRow.RowImage,
		"queue must preserve the row image so the applier can write without re-reading source")
	require.Equal(t, utils.HashKey([]any{"ghi"}), sub.queue[1].key)
	require.True(t, sub.queue[1].logicalRow.IsDeleted)
	require.Equal(t, 3, sub.Length(), "Length reports map+queue combined")
}

// TestBufferedMapQueueFullTimeDefault verifies that with the default
// forceEnableBufferedMap=false, non-memory-comparable PKs use the FIFO
// queue at all times — even during the copy phase. This is the safety
// default that keeps the queue path warm in CI until we trust the
// optimization enough to flip the default.
func TestBufferedMapQueueFullTimeDefault(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.SimulateProgress(0.5) // make in-range keys below-high-watermark

	client := &Client{
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                      client,
		table:                  srcTable,
		newTable:               dstTable,
		changes:                make(map[string]bufferedChange),
		chunker:                mockChunker,
		watermarkOptimization:  true,  // copy phase
		pkIsMemoryComparable:   false, // VARCHAR PK
		forceEnableBufferedMap: false, // default
	}

	// Even with watermark on (copy phase), the default routes to the queue.
	sub.HasChanged([]any{"a"}, []any{"a", "v"}, false)
	require.Empty(t, sub.changes, "default + non-memory-comparable PK: events must not land in the map")
	require.Len(t, sub.queue, 1, "default + non-memory-comparable PK: events must land in the queue")

	// And when watermark is toggled off, we stay in queue mode (no transition).
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))
	require.Len(t, sub.queue, 1, "no mode change: queue contents preserved across the toggle")
	sub.HasChanged([]any{"b"}, []any{"b", "v2"}, false)
	require.Len(t, sub.queue, 2, "queue keeps accepting events post-copy")
	require.Empty(t, sub.changes)
}

// TestBufferedMapQueueModeFlush exercises the queue-mode flush path end-to-end.
// Each event carries its row image so the applier writes via UpsertRows /
// DeleteKeys (no REPLACE INTO ... SELECT). Consecutive same-type ops are
// coalesced into a single applier call to amortize round-trips, but order
// across type boundaries is preserved.
func TestBufferedMapQueueModeFlush(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		applier:              applierInstance,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
		// watermarkOptimization left false so queue mode is active.
	}

	// Seed target with c so the DELETE has something to remove.
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES ('c', 'stale')`,
		dstTable.QuotedTableName))

	// UPSERT<a,b>, DELETE<c>, UPSERT<d,e> -- exercises segment coalescing.
	sub.HasChanged([]any{"a"}, []any{"a", "alpha"}, false)
	sub.HasChanged([]any{"b"}, []any{"b", "beta"}, false)
	sub.HasChanged([]any{"c"}, nil, true)
	sub.HasChanged([]any{"d"}, []any{"d", "delta"}, false)
	sub.HasChanged([]any{"e"}, []any{"e", "epsilon"}, false)
	require.Equal(t, 5, sub.Length())
	require.Empty(t, sub.changes, "queue-mode events must not leak into the map")

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())

	// a, b, d, e present in target with the row images we provided; c absent.
	rows, err := db.QueryContext(t.Context(),
		fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", dstTable.QuotedTableName))
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var got []string
	for rows.Next() {
		var id, name string
		require.NoError(t, rows.Scan(&id, &name))
		got = append(got, id+"="+name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"a=alpha", "b=beta", "d=delta", "e=epsilon"}, got)
}

// TestBufferedMapQueueModeFIFOOrder verifies that when a single logical row
// is touched multiple times in queue mode, the events apply in binlog order.
// The map path applies them in non-deterministic order — this is what the
// queue is here to fix.
func TestBufferedMapQueueModeFIFOOrder(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		applier:              applierInstance,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
	}

	// INSERT, DELETE, INSERT for the same logical row. End state must reflect
	// the *final* INSERT (FIFO replay, target's PK uniqueness collapses).
	sub.HasChanged([]any{"k"}, []any{"k", "first"}, false)
	sub.HasChanged([]any{"k"}, nil, true)
	sub.HasChanged([]any{"k"}, []any{"k", "final"}, false)
	require.Len(t, sub.queue, 3)

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	var name string
	err = db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT name FROM %s WHERE id = 'k'", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "final", name, "FIFO replay must leave the last-INSERT value as the end state")
}

// TestBufferedMapTransitionDrainsOutgoing verifies that SetWatermarkOptimization
// drains the outgoing store inline whenever the toggle changes mode. This
// is the invariant that lets HasChanged write only to the active store and
// lets Flush reason about a single store in the normal path.
func TestBufferedMapTransitionDrainsOutgoing(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.MarkAsComplete()

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                      client,
		applier:                applierInstance,
		table:                  srcTable,
		newTable:               dstTable,
		changes:                make(map[string]bufferedChange),
		chunker:                mockChunker,
		watermarkOptimization:  true,
		pkIsMemoryComparable:   false,
		forceEnableBufferedMap: true, // opt into the optimization so transitions actually flip mode
	}

	// Copy-phase events land in the map (watermark on, queue-mode inactive).
	sub.HasChanged([]any{"map-1"}, []any{"map-1", "from-map"}, false)
	sub.HasChanged([]any{"map-2"}, []any{"map-2", "from-map-2"}, false)
	require.Len(t, sub.changes, 2)
	require.Empty(t, sub.queue)

	// Flip to queue mode. The toggle must drain the map inline so we leave
	// with map empty and queue empty (queue gets new events from here on).
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))
	require.Empty(t, sub.changes, "outgoing map must be drained inline by the toggle")
	require.Empty(t, sub.queue)

	// Verify the drain actually wrote to the target.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, 2, count, "drain on toggle must apply through the applier")

	// New events go to the queue.
	sub.HasChanged([]any{"queue-1"}, []any{"queue-1", "from-queue"}, false)
	require.Empty(t, sub.changes)
	require.Len(t, sub.queue, 1)

	// Flip back to map mode. The toggle must drain the queue inline.
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), true))
	require.Empty(t, sub.queue, "outgoing queue must be drained inline by the toggle")
	require.Empty(t, sub.changes)

	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, 3, count, "drain on toggle must apply queue contents through the applier")
}

// TestBufferedMapTogglePassthrough verifies that for memory-comparable PKs
// SetWatermarkOptimization is a no-op other than flipping the flag — the
// active store is the map in both states, so there is nothing to drain.
func TestBufferedMapTogglePassthrough(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	// Advance so key=1 is below-high-watermark (so HasChanged accepts it)
	// but at-or-above the low watermark (so flushMapLocked would skip it
	// while the watermark is on — the property we're testing here is that
	// a non-mode-changing toggle doesn't drain the map at all).
	mockChunker.SimulateProgress(0.001)

	client := &Client{
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: true,
	}

	sub.watermarkOptimization = true
	sub.HasChanged([]any{1}, []any{1, "v"}, false)
	require.Len(t, sub.changes, 1)

	// Toggle to false. With pkIsMemoryComparable=true the active store is
	// still the map, so the map keeps its entry — no drain happens.
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))
	require.Len(t, sub.changes, 1, "memory-comparable toggle must not drain the map")
	require.Empty(t, sub.queue)
}
