package repl

import (
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestSubscriptionDeltaMap(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Test initial state
	assert.Equal(t, 0, sub.Length())

	// Test key changes
	sub.KeyHasChanged([]any{1}, false) // Insert/Replace
	assert.Equal(t, 1, sub.Length())

	sub.KeyHasChanged([]any{2}, true) // Delete
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
	srcTable, dstTable := setupTestTables(t)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	client := &Client{
		db:              db,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES (1, 'test1'), (2, 'test2')`)

	// Add some changes
	sub.KeyHasChanged([]any{1}, false)
	sub.KeyHasChanged([]any{2}, true)

	// Create a table lock
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), logrus.New())
	assert.NoError(t, err)

	// Test flush with lock
	err = sub.Flush(t.Context(), true, lock)
	assert.NoError(t, err)

	lock.Close()

	// Verify the changes were applied
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count) // Only ID 1 should be present, ID 2 was deleted
}

func TestFlushWithoutLock(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	client := &Client{
		db:              db,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 2,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES 
		(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')`)

	// Add multiple changes to test batch processing
	sub.KeyHasChanged([]any{1}, false)
	sub.KeyHasChanged([]any{2}, false)
	sub.KeyHasChanged([]any{3}, true)
	sub.KeyHasChanged([]any{4}, true)

	// Test flush without lock
	err = sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)

	// Verify the changes were applied
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // IDs 1 and 2 should be present, 3 and 4 were deleted
}

func TestConcurrentKeyChanges(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Run concurrent key changes
	done := make(chan bool)
	go func() {
		for i := range 100 {
			sub.KeyHasChanged([]any{i}, false)
		}
		done <- true
	}()

	go func() {
		for i := 100; i < 200; i++ {
			sub.KeyHasChanged([]any{i}, true)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	assert.Equal(t, 200, sub.Length())
}

func TestKeyChangedOverwrite(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Test with deltaMap
	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Test overwriting the same key multiple times
	sub.KeyHasChanged([]any{1}, false) // Insert
	sub.KeyHasChanged([]any{1}, true)  // Delete
	sub.KeyHasChanged([]any{1}, false) // Insert again
	assert.Equal(t, 1, sub.Length())   // Should only count once in the map

	// Test with deltaQueue
	subQueue := &deltaQueue{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make([]queuedChange, 0),
	}

	// Same operations with queue should maintain history
	subQueue.KeyHasChanged([]any{1}, false)
	subQueue.KeyHasChanged([]any{1}, true)
	subQueue.KeyHasChanged([]any{1}, false)
	assert.Equal(t, 3, subQueue.Length()) // Queue maintains all changes
}

func TestKeyChangedEdgeCases(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Test with string keys
	sub.KeyHasChanged([]any{"key1"}, false)
	assert.Equal(t, 1, sub.Length())

	// Test with composite keys
	sub.KeyHasChanged([]any{"prefix", 123}, false)
	assert.Equal(t, 2, sub.Length())

	// Test watermark edge cases
	watermark := 5
	sub.keyAboveCopierCallback = func(key any) bool {
		// Handle different types of keys
		switch v := key.(type) {
		case int:
			return v > watermark
		case string:
			return false // strings always process
		default:
			return false
		}
	}
	sub.SetKeyAboveWatermarkOptimization(true)

	// Test exactly at watermark
	sub.KeyHasChanged([]any{5}, false)
	assert.Equal(t, 3, sub.Length())

	// Test one above watermark
	sub.KeyHasChanged([]any{6}, false)
	assert.Equal(t, 3, sub.Length()) // Should not increase as it's above watermark

	// Test with string key when watermark is enabled
	sub.KeyHasChanged([]any{"key2"}, false)
	assert.Equal(t, 4, sub.Length()) // Should still process string keys
}

func TestKeyChangedNilAndEmpty(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Test with empty string key
	sub.KeyHasChanged([]any{""}, false)
	assert.Equal(t, 1, sub.Length())

	// Test with empty array as part of composite key
	sub.KeyHasChanged([]any{"prefix", []string{}}, false)
	assert.Equal(t, 2, sub.Length())

	// Test with zero values
	sub.KeyHasChanged([]any{0}, false)
	assert.Equal(t, 3, sub.Length())
}

func TestKeyAboveWatermark(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}

	// Test with watermark optimization disabled
	sub.KeyHasChanged([]any{1}, false)
	assert.Equal(t, 1, sub.Length())

	// Setup watermark callback
	watermark := 5
	sub.keyAboveCopierCallback = func(key any) bool {
		return key.(int) > watermark
	}

	// Enable watermark optimization
	sub.SetKeyAboveWatermarkOptimization(true)

	// Test key below watermark
	sub.KeyHasChanged([]any{3}, false)
	assert.Equal(t, 2, sub.Length())

	// Test key above watermark
	sub.KeyHasChanged([]any{10}, false)
	assert.Equal(t, 2, sub.Length()) // Should not increase as key is above watermark
}
