package repl

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestSubscriptionDeltaMap(t *testing.T) {
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

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Test initial state
	assert.Equal(t, 0, sub.Length())

	// Test key changes
	sub.HasChanged([]any{1}, nil, false) // Insert/Replace
	assert.Equal(t, 1, sub.Length())

	sub.HasChanged([]any{2}, nil, true) // Delete
	assert.Equal(t, 2, sub.Length())

	// Test statement generation
	deleteStmt := sub.createDeleteStmt([]string{"'1'"})
	assert.Contains(t, deleteStmt.stmt, "DELETE FROM")
	assert.Contains(t, deleteStmt.stmt, "WHERE")
	assert.Equal(t, 1, deleteStmt.numKeys)

	replaceStmt := sub.createReplaceStmt([]string{"'1'"})
	assert.Contains(t, replaceStmt.stmt, "REPLACE INTO")
	assert.Contains(t, replaceStmt.stmt, "SELECT")
	assert.Equal(t, 1, replaceStmt.numKeys)
}

func TestFlushWithLock(t *testing.T) {
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

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES (1, 'test1'), (2, 'test2')`)

	// Add some changes
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{2}, nil, true)

	// Create a table lock
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), slog.Default())
	assert.NoError(t, err)

	// Test flush with lock
	allFlushed, err := sub.Flush(t.Context(), true, lock)
	assert.NoError(t, err)
	assert.True(t, allFlushed)

	assert.NoError(t, lock.Close(t.Context()))

	// Verify the changes were applied
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count) // Only ID 1 should be present, ID 2 was deleted
}

func TestFlushWithoutLock(t *testing.T) {
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

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 2,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
		(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')`)

	// Add multiple changes to test batch processing
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{2}, nil, false)
	sub.HasChanged([]any{3}, nil, true)
	sub.HasChanged([]any{4}, nil, true)

	// Test flush without lock
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)
	assert.True(t, allFlushed)

	// Verify the changes were applied
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // IDs 1 and 2 should be present, 3 and 4 were deleted
}

func TestConcurrentKeyChanges(t *testing.T) {
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

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Run concurrent key changes
	done := make(chan bool)
	go func() {
		for i := range 100 {
			sub.HasChanged([]any{i}, nil, false)
		}
		done <- true
	}()

	go func() {
		for i := 100; i < 200; i++ {
			sub.HasChanged([]any{i}, nil, true)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	assert.Equal(t, 200, sub.Length())
}

func TestKeyChangedOverwrite(t *testing.T) {
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

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Test with deltaMap
	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Test overwriting the same key multiple times
	sub.HasChanged([]any{1}, nil, false) // Insert
	sub.HasChanged([]any{1}, nil, true)  // Delete
	sub.HasChanged([]any{1}, nil, false) // Insert again
	assert.Equal(t, 1, sub.Length())     // Should only count once in the map

	// Test with deltaQueue
	subQueue := &deltaQueue{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make([]queuedChange, 0),
	}

	// Same operations with queue should maintain history
	subQueue.HasChanged([]any{1}, nil, false)
	subQueue.HasChanged([]any{1}, nil, true)
	subQueue.HasChanged([]any{1}, nil, false)
	assert.Equal(t, 3, subQueue.Length()) // Queue maintains all changes
}

func TestKeyChangedEdgeCases(t *testing.T) {
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

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with watermark behavior
	mockChunker := table.NewMockChunker("test_table", 1000)
	// Set current position to 5, so keys above 5 will be above watermark
	mockChunker.SimulateProgress(0.005) // 5/1000 = 0.005

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
		chunker:  mockChunker,
	}

	// Test with string keys
	sub.HasChanged([]any{"key1"}, nil, false)
	assert.Equal(t, 1, sub.Length())

	// Test with composite keys
	sub.HasChanged([]any{"prefix", 123}, nil, false)
	assert.Equal(t, 2, sub.Length())

	sub.SetWatermarkOptimization(true)

	// Test exactly at watermark (position 5)
	sub.HasChanged([]any{5}, nil, false)
	assert.Equal(t, 3, sub.Length())

	// Test one above watermark (position 6 > 5)
	sub.HasChanged([]any{6}, nil, false)
	assert.Equal(t, 3, sub.Length()) // Should not increase as it's above watermark

	// Test with string key when watermark is enabled
	sub.HasChanged([]any{"key2"}, nil, false)
	assert.Equal(t, 4, sub.Length()) // Should still process string keys
}

func TestKeyChangedNilAndEmpty(t *testing.T) {
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
	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Test with empty string key
	sub.HasChanged([]any{""}, nil, false)
	assert.Equal(t, 1, sub.Length())

	// Test with empty array as part of composite key
	sub.HasChanged([]any{"prefix", []string{}}, nil, false)
	assert.Equal(t, 2, sub.Length())

	// Test with zero values
	sub.HasChanged([]any{0}, nil, false)
	assert.Equal(t, 3, sub.Length())
}

func TestKeyAboveWatermark(t *testing.T) {
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

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	mockChunker := table.NewMockChunker("test_table", 1000)
	// Set current position to 5, so keys above 5 will be above watermark
	mockChunker.SimulateProgress(0.005) // 5/1000 = 0.005

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
		chunker:  mockChunker,
	}

	// Test with watermark optimization disabled
	sub.HasChanged([]any{1}, nil, false)
	assert.Equal(t, 1, sub.Length())

	// Enable watermark optimization
	sub.SetWatermarkOptimization(true)

	// Test key below watermark (3 < 5)
	sub.HasChanged([]any{3}, nil, false)
	assert.Equal(t, 2, sub.Length())

	// Test key above watermark (10 > 5)
	sub.HasChanged([]any{10}, nil, false)
	assert.Equal(t, 2, sub.Length()) // Should not increase as key is above watermark
}

func TestKeyBelowWatermarkMock(t *testing.T) {
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

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with low watermark behavior
	mockChunker := table.NewMockChunker("test_table", 1000)
	// Set current position to 5, so keys below 5 will be below watermark
	mockChunker.SimulateProgress(0.005) // 5/1000 = 0.005

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
		chunker:  mockChunker,
	}

	// Add some changes first
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{3}, nil, false)
	sub.HasChanged([]any{7}, nil, false)
	assert.Equal(t, 3, sub.Length())

	// Test that keys below watermark are identified correctly
	// Key 3 < 5 (current position), so it should be below watermark
	assert.True(t, mockChunker.KeyBelowLowWatermark(3))
	// Key 7 > 5 (current position), so it should not be below watermark
	assert.False(t, mockChunker.KeyBelowLowWatermark(7))

	// Test with string keys (should return true to allow processing)
	assert.True(t, mockChunker.KeyBelowLowWatermark("string_key"))
}

// TestFlushUnderLockBypassesWatermark is a regression test for the bug where
// watermark optimization was incorrectly applied during underLock=true flush.
// This caused data loss during cutover because some changes were skipped.
//
// The test verifies that when underLock=true:
// 1. ALL changes are flushed, regardless of watermark position
// 2. Changes that would normally be skipped (not below low watermark) are still flushed
//
// With the bug (old code): This test would fail because changes not below the low watermark
// would be skipped even during underLock=true flush, leaving them in the subscription map.
//
// With the fix (new code): This test passes because underLock=true bypasses the
// watermark optimization check, ensuring all changes are flushed.
func TestFlushUnderLockBypassesWatermark(t *testing.T) {
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

	client := &Client{
		db:              db,
		logger:          slog.Default(),
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

	sub := &deltaMap{
		c:                     client,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]mapChange),
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
	// Keys 1, 3, 4 are below watermark (< 5) - these would be flushed normally
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{3}, nil, false)
	sub.HasChanged([]any{4}, nil, false)
	// Key 5 is at the watermark (== 5) - this would NOT be flushed normally
	// because KeyBelowLowWatermark(5) = (5 < 5) = false
	sub.HasChanged([]any{5}, nil, false)

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

	// Now re-enable watermark optimization and add the same changes again
	// This simulates changes that came in during the final stages of migration
	sub.SetWatermarkOptimization(true)
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{3}, nil, false)
	sub.HasChanged([]any{4}, nil, false)
	sub.HasChanged([]any{5}, nil, false)
	assert.Equal(t, 4, sub.Length(), "Should have 4 pending changes again")

	// Create a table lock for underLock=true flush
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), slog.Default())
	assert.NoError(t, err)
	defer utils.CloseAndLogWithContext(t.Context(), lock)

	// Flush with underLock=true
	// This is the critical test: ALL changes should be flushed, including key 5 which is not below watermark
	// The key assertion is that sub.Length() becomes 0, meaning all changes were flushed
	allFlushed, err = sub.Flush(t.Context(), true, lock)

	// THE KEY ASSERTION: With the fix, allFlushed should be true and sub.Length() should be 0
	// With the bug (before the fix), allFlushed would be false and sub.Length() would be 1
	// because key 5 (at watermark) would not be flushed
	assert.NoError(t, err)
	assert.True(t, allFlushed, "All changes should be flushed when underLock=true - THIS IS THE KEY TEST")
	assert.Equal(t, 0, sub.Length(), "All changes should be removed from the map after flush - THIS IS THE KEY TEST")

	// Note: We don't verify the database state here because the REPLACE INTO ... SELECT
	// statement would timeout trying to read from the locked source table.
	// The important test is that sub.Length() == 0, which proves all changes were processed.
}

// TestFlushWithoutLockRespectsWatermark verifies that when underLock=false,
// the watermark optimization is still applied (this is the normal behavior).
func TestFlushWithoutLockRespectsWatermark(t *testing.T) {
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

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with current position at 5
	mockChunker := table.NewMockChunker("subscription_test", 1000)
	mockChunker.SimulateProgress(0.005) // Current position at 5

	sub := &deltaMap{
		c:                     client,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]mapChange),
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
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{3}, nil, false)
	sub.HasChanged([]any{4}, nil, false)
	sub.HasChanged([]any{5}, nil, false) // At watermark, not below

	assert.Equal(t, 4, sub.Length(), "Should have 4 pending changes")

	// Flush WITHOUT lock (underLock=false)
	// This should respect the watermark optimization
	// Keys 1, 3, 4 are below watermark and should be flushed
	// Key 5 is at watermark (not below) and should NOT be flushed
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)
	assert.False(t, allFlushed, "Not all changes should be flushed when watermark optimization is active")
	assert.Equal(t, 1, sub.Length(), "Key 5 (at watermark) should remain in the map")

	// Verify that only 3 rows (below watermark) were copied to the new table
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count, "Only 3 rows below watermark should be in the new table")

	// Verify that the row at watermark was NOT copied
	var name string
	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 5").Scan(&name)
	assert.Error(t, err, "Row with id=5 (at watermark) should NOT exist yet")

	// Verify that rows below watermark were copied
	err = db.QueryRowContext(t.Context(), "SELECT name FROM _subscription_test_new WHERE id = 1").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "below_watermark", name)
}
