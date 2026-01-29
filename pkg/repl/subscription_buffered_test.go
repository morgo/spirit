package repl

import (
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestBufferedMap(t *testing.T) {
	db, client := setupBufferedTest(t)
	defer client.Close()
	defer utils.CloseAndLog(db)

	// Insert into srcTable.
	testutils.RunSQL(t, "INSERT INTO subscription_test (id, name) VALUES (1, 'test')")
	assert.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	assert.Equal(t, 1, client.GetDeltaLen())

	// Inspect the subscription directly.
	sub, ok := client.subscriptions["test.subscription_test"].(*bufferedMap)
	assert.True(t, ok)
	assert.Equal(t, 1, sub.Length())

	// Check the logical row structure
	assert.False(t, sub.changes["1"].logicalRow.IsDeleted)
	assert.Equal(t, []any{int32(1), "test"}, sub.changes["1"].logicalRow.RowImage)

	// Now delete the row.
	testutils.RunSQL(t, "DELETE FROM subscription_test WHERE id = 1")
	assert.NoError(t, client.BlockWait(t.Context()))

	assert.True(t, sub.changes["1"].logicalRow.IsDeleted)
	assert.Equal(t, []any(nil), sub.changes["1"].logicalRow.RowImage)

	// Now insert 2 more rows:
	testutils.RunSQL(t, "INSERT INTO subscription_test (id, name) VALUES (2, 'test2'), (3, 'test3')")
	assert.NoError(t, client.BlockWait(t.Context()))

	assert.Equal(t, 3, sub.Length())
	assert.False(t, sub.changes["2"].logicalRow.IsDeleted)
	assert.False(t, sub.changes["3"].logicalRow.IsDeleted)

	// Now flush the changes.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)
	assert.True(t, allFlushed)

	// The destination table should now have the 2 rows.
	var name string
	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 2").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "test2", name)

	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 3").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "test3", name)
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
	assert.NoError(t, err)
	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:                     logger,
		Concurrency:                4,
		TargetBatchTime:            time.Second,
		ServerID:                   NewServerID(),
		UseExperimentalBufferedMap: true,
		Applier:                    applier,
	})
	assert.NoError(t, client.AddSubscription(srcTable, dstTable, nil))
	assert.NoError(t, client.Run(t.Context()))

	defer client.Close()
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "INSERT INTO subscription_test (id, name, extracol) VALUES (1, 'whatever', JSON_ARRAY(1,2,3))")
	assert.NoError(t, err)
	assert.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	assert.Equal(t, 1, client.GetDeltaLen())
	assert.NoError(t, client.Flush(t.Context()))
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
	assert.NoError(t, err)
	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:                     logger,
		Concurrency:                4,
		TargetBatchTime:            time.Second,
		ServerID:                   NewServerID(),
		UseExperimentalBufferedMap: true,
		Applier:                    applier,
	})
	assert.NoError(t, client.AddSubscription(srcTable, dstTable, nil))
	assert.NoError(t, client.Run(t.Context()))

	defer client.Close()
	defer utils.CloseAndLog(db)
	// Insert into srcTable various illegal values.
	// This includes quotes, backslashes, and nulls.
	// Also test with a string that includes a null byte.

	_, err = db.ExecContext(t.Context(), "INSERT INTO subscription_test (id, name, dt, ts) VALUES (1, 'test''s', '2025-10-06 09:09:46 +02:00', '2025-10-06 09:09:46 +02:00'), (2, 'back\\slash', NOW(), NOW()), (3, NULL, NOW(), NOW()), (4, 'null\000byte', NOW(), NOW())")
	assert.NoError(t, err)
	assert.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	assert.Equal(t, 4, client.GetDeltaLen())
	assert.NoError(t, client.Flush(t.Context()))

	// Now we want to check that the tables match,
	// using an adhoc checksum.
	var checksumSrc, checksumDst string
	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(name)) as checksum FROM subscription_test").Scan(&checksumSrc)
	assert.NoError(t, err)

	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(name)) as checksum FROM _subscription_test_new").Scan(&checksumDst)
	assert.NoError(t, err)
	assert.Equal(t, checksumSrc, checksumDst, "Checksums do not match between source and destination tables")
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
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	assert.NoError(t, err)

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
	mockChunker := table.NewMockChunker("subscription_test", 1000)
	mockChunker.SimulateProgress(0.005) // Current position at 5

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true, // Enable watermark optimization
	}

	// Insert test data into source table - data must exist before we lock
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES 
		(1, 'below_watermark'),
		(3, 'below_watermark_2'),
		(5, 'at_watermark'),
		(4, 'below_watermark_3')`)

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

	assert.Equal(t, 4, sub.Length(), "Should have 4 pending changes")

	// Verify watermark behavior before flush
	assert.True(t, mockChunker.KeyBelowLowWatermark(1), "Key 1 should be below watermark")
	assert.True(t, mockChunker.KeyBelowLowWatermark(3), "Key 3 should be below watermark")
	assert.True(t, mockChunker.KeyBelowLowWatermark(4), "Key 4 should be below watermark")
	assert.False(t, mockChunker.KeyBelowLowWatermark(5), "Key 5 should NOT be below watermark (at current position)")

	// First, disable watermark optimization and do a normal flush to populate the new table
	// This simulates the state after the copier has finished
	sub.SetWatermarkOptimization(false)
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)
	assert.True(t, allFlushed)
	assert.Equal(t, 0, sub.Length())

	// Verify all rows were copied
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 4, count, "All 4 rows should be in the new table")

	// Now re-enable watermark optimization and add the same changes again
	// This simulates changes that came in during the final stages of migration
	sub.SetWatermarkOptimization(true)
	sub.HasChanged([]any{1}, []any{1, "below_watermark_updated"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2_updated"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3_updated"}, false)
	sub.HasChanged([]any{5}, []any{5, "at_watermark_updated"}, false)
	assert.Equal(t, 4, sub.Length(), "Should have 4 pending changes again")

	// Create a table lock for underLock=true flush
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), slog.Default())
	assert.NoError(t, err)
	defer utils.CloseAndLogWithContext(t.Context(), lock)

	// Flush with underLock=true
	// This is the critical test: ALL changes should be flushed, including keys 1, 3, 4 which are below watermark
	// The key assertion is that sub.Length() becomes 0, meaning all changes were flushed
	allFlushed, err = sub.Flush(t.Context(), true, lock)

	// THE KEY ASSERTION: With the fix, allFlushed should be true and sub.Length() should be 0
	// With the bug (before the fix), allFlushed would be false and sub.Length() would be 3
	// because keys 1, 3, 4 (below watermark) would not be flushed
	assert.NoError(t, err)
	assert.True(t, allFlushed, "All changes should be flushed when underLock=true - THIS IS THE KEY TEST")
	assert.Equal(t, 0, sub.Length(), "All changes should be removed from the map after flush - THIS IS THE KEY TEST")

	// Note: We don't verify the database state here because the applier operations
	// are executed under the table lock. The important test is that sub.Length() == 0,
	// which proves all changes were processed and removed from the subscription map.
}

// TestBufferedMapFlushWithoutLockRespectsWatermark verifies that when underLock=false,
// the watermark optimization is still applied (this is the normal behavior).
// Note: bufferedMap has inverted logic compared to deltaMap - KeyBelowLowWatermark returns true
// for keys that are still being copied, so we skip flushing them.
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
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	assert.NoError(t, err)

	client := &Client{
		db:              db,
		logger:          logger,
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with current position at 5
	mockChunker := table.NewMockChunker("subscription_test", 1000)
	mockChunker.SimulateProgress(0.005) // Current position at 5

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true, // Enable watermark optimization
	}

	// Insert test data into source table
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES 
		(1, 'below_watermark'),
		(3, 'below_watermark_2'),
		(5, 'at_watermark'),
		(4, 'below_watermark_3')`)

	// Add changes for keys both below and at the low watermark
	sub.HasChanged([]any{1}, []any{1, "below_watermark"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3"}, false)
	sub.HasChanged([]any{5}, []any{5, "at_watermark"}, false) // At watermark, not below

	assert.Equal(t, 4, sub.Length(), "Should have 4 pending changes")

	// Flush WITHOUT lock (underLock=false)
	// This should respect the watermark optimization
	// Keys 1, 3, 4 are below watermark and should NOT be flushed (inverted logic)
	// Key 5 is at watermark (not below) and should be flushed
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)
	assert.False(t, allFlushed, "Not all changes should be flushed when watermark optimization is active")
	assert.Equal(t, 3, sub.Length(), "Keys 1, 3, 4 (below watermark) should remain in the map")

	// Verify that only 1 row (at/above watermark) was copied to the new table
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "Only 1 row at/above watermark should be in the new table")

	// Verify that the row at watermark was copied
	var name string
	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 5").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "at_watermark", name)

	// Verify that rows below watermark were NOT copied
	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 1").Scan(&name)
	assert.Error(t, err, "Row with id=1 (below watermark) should NOT exist yet")

	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 3").Scan(&name)
	assert.Error(t, err, "Row with id=3 (below watermark) should NOT exist yet")

	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 4").Scan(&name)
	assert.Error(t, err, "Row with id=4 (below watermark) should NOT exist yet")
}
