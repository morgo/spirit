package applier

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardedApplierIntegration tests the ShardedApplier with real MySQL databases
// This test demonstrates:
// 1. Creating multiple target databases (shards)
// 2. Creating tables in each shard
// 3. Creating a ShardedApplier with a test hash function
// 4. Sending rows through the applier
// 5. Verifying rows are correctly distributed across shards based on the hash function
func TestShardedApplierIntegration(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_target1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_target2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_source")
	testutils.RunSQL(t, "CREATE DATABASE sharded_target1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_target2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "sharded_source"
	sourceDB, err := sql.Open("mysql", source.FormatDSN())
	require.NoError(t, err)
	defer sourceDB.Close()

	target1 := base.Clone()
	target1.DBName = "sharded_target1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "sharded_target2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	// Create test table in source
	createTableSQL := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			user_id INT NOT NULL,
			name VARCHAR(100),
			email VARCHAR(100),
			INDEX idx_user_id (user_id)
		)
	`

	// Create table in source
	ctx := context.Background()
	_, err = sourceDB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)

	// Create same table in both shards
	for _, db := range []*sql.DB{target1DB, target2DB} {
		_, err = db.ExecContext(ctx, createTableSQL)
		require.NoError(t, err)
	}

	// Create table info objects
	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "users")
	err = sourceTable.SetInfo(ctx)
	require.NoError(t, err)

	shard1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	err = shard1Table.SetInfo(ctx)
	require.NoError(t, err)

	// Create a test hash function that shards based on user_id
	// The hash function will return a value that we'll match against key ranges
	// For testing, we'll use a simple modulo-based hash that maps to 64-bit space
	shardFunc := func(colAny any) (uint64, error) {
		col, ok := colAny.(int64)
		if !ok {
			return 0, fmt.Errorf("expected int64 for sharding column, got %T", colAny)
		}
		// Simple hash: map even user_ids to lower half, odd to upper half
		// This simulates a hash function that distributes across the full uint64 space
		var hash uint64
		if col%2 == 0 {
			// Even user_ids map to 0x0000000000000000 - + the int
			// Use a simple formula that keeps us in the lower half
			hash = uint64(col)
		} else {
			// Odd user_ids map to 0x8000000000000000 + the int.
			// Start from the midpoint and add a small offset
			hash = 0x8000000000000000 + uint64(col)
		}
		t.Logf("Hash for user_id=%d: 0x%016x", col, hash)
		return hash, nil
	}

	// Create ShardedApplier with key ranges
	// Shard 0: "-80" (0x0000000000000000 - 0x7fffffffffffffff)
	// Shard 1: "80-" (0x8000000000000000 - 0xffffffffffffffff)
	dbConfig := dbconn.NewDBConfig()
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"}, // Lower half of key space
		{DB: target2DB, KeyRange: "80-"}, // Upper half of key space
	}
	applier, err := NewShardedApplier(
		targets,
		"user_id",
		shardFunc,
		dbConfig,
		slog.Default(),
	)
	require.NoError(t, err)

	// Start the applier
	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer applier.Close()

	// Prepare test data - 10 rows with alternating user_ids
	testRows := [][]any{
		{int64(1), int64(101), "Alice", "alice@example.com"},
		{int64(2), int64(102), "Bob", "bob@example.com"},
		{int64(3), int64(103), "Charlie", "charlie@example.com"},
		{int64(4), int64(104), "Diana", "diana@example.com"},
		{int64(5), int64(105), "Eve", "eve@example.com"},
		{int64(6), int64(106), "Frank", "frank@example.com"},
		{int64(7), int64(107), "Grace", "grace@example.com"},
		{int64(8), int64(108), "Henry", "henry@example.com"},
		{int64(9), int64(109), "Ivy", "ivy@example.com"},
		{int64(10), int64(110), "Jack", "jack@example.com"},
	}

	// Create a chunk for the apply operation
	chunk := &table.Chunk{
		Table:    sourceTable,
		NewTable: shard1Table, // Doesn't matter which, both have same structure
	}

	// Apply the rows
	callbackInvoked := false
	var callbackAffectedRows int64
	var callbackErr error

	callback := func(affectedRows int64, err error) {
		callbackInvoked = true
		callbackAffectedRows = affectedRows
		callbackErr = err
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Wait for the applier to process all rows
	err = applier.Wait(t.Context())
	require.NoError(t, err)

	// Verify callback was invoked
	assert.True(t, callbackInvoked, "Callback should have been invoked")
	assert.NoError(t, callbackErr, "Callback should not have an error")
	assert.Equal(t, int64(10), callbackAffectedRows, "Should have affected 10 rows")

	// Verify data distribution across shards
	// Based on our hash function:
	// - Even user_ids (102, 104, 106, 108, 110) map to lower half (shard 0)
	// - Odd user_ids (101, 103, 105, 107, 109) map to upper half (shard 1)

	// Check shard 0 (even user_ids)
	var shard1Count int
	err = target1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard1Count)
	require.NoError(t, err)
	t.Logf("Shard 0 has %d rows", shard1Count)

	// Check shard 1 (odd user_ids)
	var shard2Count int
	err = target2DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard2Count)
	require.NoError(t, err)
	t.Logf("Shard 1 has %d rows", shard2Count)

	// Verify the distribution
	assert.Equal(t, 5, shard1Count, "Shard 0 should have 5 rows (even user_ids)")
	assert.Equal(t, 5, shard2Count, "Shard 1 should have 5 rows (odd user_ids)")

	// Verify which rows went where
	rows, err := target1DB.QueryContext(t.Context(), "SELECT user_id FROM users ORDER BY user_id")
	require.NoError(t, err)
	defer rows.Close()
	var shard1UserIDs []int64
	for rows.Next() {
		var userID int64
		err = rows.Scan(&userID)
		require.NoError(t, err)
		shard1UserIDs = append(shard1UserIDs, userID)
	}
	require.NoError(t, rows.Err())
	t.Logf("Shard 0 user_ids: %v", shard1UserIDs)

	rows, err = target2DB.QueryContext(t.Context(), "SELECT user_id FROM users ORDER BY user_id")
	require.NoError(t, err)
	defer rows.Close()
	var shard2UserIDs []int64
	for rows.Next() {
		var userID int64
		err = rows.Scan(&userID)
		require.NoError(t, err)
		shard2UserIDs = append(shard2UserIDs, userID)
	}
	require.NoError(t, rows.Err())
	t.Logf("Shard 1 user_ids: %v", shard2UserIDs)

	// Verify even user_ids are in shard 0
	for _, uid := range shard1UserIDs {
		assert.Equal(t, int64(0), uid%2, "Shard 0 should only have even user_ids, got %d", uid)
	}

	// Verify odd user_ids are in shard 1
	for _, uid := range shard2UserIDs {
		assert.Equal(t, int64(1), uid%2, "Shard 1 should only have odd user_ids, got %d", uid)
	}

	t.Log("✓ Data correctly distributed across shards")
	t.Logf("✓ Shard 0 (even user_ids): %v", shard1UserIDs)
	t.Logf("✓ Shard 1 (odd user_ids): %v", shard2UserIDs)
}
