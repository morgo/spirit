package applier

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleTargetApplierBasic tests basic functionality of SingleTargetApplier
func TestSingleTargetApplierBasic(t *testing.T) {
	// Setup test databases
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_target")
	testutils.RunSQL(t, "CREATE DATABASE single_source")
	testutils.RunSQL(t, "CREATE DATABASE single_target")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "single_source"
	sourceDB, err := sql.Open("mysql", source.FormatDSN())
	require.NoError(t, err)
	defer sourceDB.Close()

	target := base.Clone()
	target.DBName = "single_target"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	// Create test table
	createTableSQL := `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			value INT
		)
	`

	_, err = sourceDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	// Create table info objects
	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "test_table")
	err = sourceTable.SetInfo(t.Context())
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Start the applier
	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, applier.Stop())
	}()

	// Prepare test data
	testRows := [][]any{
		{int64(1), "Alice", int64(100)},
		{int64(2), "Bob", int64(200)},
		{int64(3), "Charlie", int64(300)},
	}

	// Create a chunk
	chunk := &table.Chunk{
		Table:    sourceTable,
		NewTable: targetTable,
	}

	// Apply the rows
	var callbackInvoked atomic.Bool
	var callbackAffectedRows atomic.Int64
	var callbackErrMu sync.Mutex
	var callbackErr error

	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		callbackAffectedRows.Store(affectedRows)
		callbackErrMu.Lock()
		callbackErr = err
		callbackErrMu.Unlock()
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Wait for processing
	err = applier.Wait(t.Context())
	require.NoError(t, err)

	// Verify callback was invoked
	assert.True(t, callbackInvoked.Load(), "Callback should have been invoked")
	callbackErrMu.Lock()
	assert.NoError(t, callbackErr, "Callback should not have an error")
	callbackErrMu.Unlock()
	assert.Equal(t, int64(3), callbackAffectedRows.Load(), "Should have affected 3 rows")

	// Verify data in target
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "Should have 3 rows in target")

	// Verify specific rows
	rows, err := targetDB.QueryContext(t.Context(), "SELECT id, name, value FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	expected := []struct {
		id    int64
		name  string
		value int64
	}{
		{1, "Alice", 100},
		{2, "Bob", 200},
		{3, "Charlie", 300},
	}

	i := 0
	for rows.Next() {
		var id, value int64
		var name string
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		assert.Equal(t, expected[i].id, id)
		assert.Equal(t, expected[i].name, name)
		assert.Equal(t, expected[i].value, value)
		i++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, i, "Should have read 3 rows")
}

// TestSingleTargetApplierEmptyRows tests applying empty row set
func TestSingleTargetApplierEmptyRows(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_empty_test")
	testutils.RunSQL(t, "CREATE DATABASE single_empty_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_empty_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer applier.Stop()

	// Apply empty rows
	chunk := &table.Chunk{
		Table:    targetTable,
		NewTable: targetTable,
	}

	var callbackInvoked atomic.Bool
	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), affectedRows)
	}

	err = applier.Apply(t.Context(), chunk, [][]any{}, callback)
	require.NoError(t, err)

	// Callback should be invoked immediately for empty rows
	assert.True(t, callbackInvoked.Load(), "Callback should have been invoked immediately for empty rows")
}

// TestSingleTargetApplierLargeDataset tests applying a large dataset that spans multiple chunklets
func TestSingleTargetApplierLargeDataset(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_large_test")
	testutils.RunSQL(t, "CREATE DATABASE single_large_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_large_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, applier.Stop())
	}()

	// Create 2500 rows (will span 3 chunklets of 1000 each)
	rowCount := 2500
	testRows := make([][]any, rowCount)
	for i := range rowCount {
		testRows[i] = []any{int64(i + 1), int64(i * 10)}
	}

	chunk := &table.Chunk{
		Table:    targetTable,
		NewTable: targetTable,
	}

	var callbackInvoked atomic.Bool
	var callbackAffectedRows atomic.Int64

	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		callbackAffectedRows.Store(affectedRows)
		assert.NoError(t, err)
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	err = applier.Wait(t.Context())
	require.NoError(t, err)

	assert.True(t, callbackInvoked.Load())
	assert.Equal(t, int64(rowCount), callbackAffectedRows.Load())

	// Verify count
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, rowCount, count)
}

// TestSingleTargetApplierConcurrentApplies tests multiple concurrent Apply calls
func TestSingleTargetApplierConcurrentApplies(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_concurrent_test")
	testutils.RunSQL(t, "CREATE DATABASE single_concurrent_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_concurrent_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, batch INT, value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer applier.Stop()

	// Launch multiple concurrent Apply operations
	numBatches := 10
	rowsPerBatch := 100
	var wg sync.WaitGroup
	var totalCallbacks int32

	for batch := range numBatches {
		wg.Add(1)
		go func(batchNum int) {
			defer wg.Done()

			// Create rows for this batch
			testRows := make([][]any, rowsPerBatch)
			for i := range rowsPerBatch {
				id := batchNum*rowsPerBatch + i + 1
				testRows[i] = []any{int64(id), int64(batchNum), int64(i)}
			}

			chunk := &table.Chunk{
				Table:    targetTable,
				NewTable: targetTable,
			}

			callback := func(affectedRows int64, err error) {
				atomic.AddInt32(&totalCallbacks, 1)
				assert.NoError(t, err)
				assert.Equal(t, int64(rowsPerBatch), affectedRows)
			}

			err := applier.Apply(t.Context(), chunk, testRows, callback)
			assert.NoError(t, err)
		}(batch)
	}

	wg.Wait()

	// Wait for all work to complete
	err = applier.Wait(t.Context())
	require.NoError(t, err)

	// Verify all callbacks were invoked
	assert.Equal(t, int32(numBatches), atomic.LoadInt32(&totalCallbacks))

	// Verify total count
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, numBatches*rowsPerBatch, count)
}

// TestSingleTargetApplierDeleteKeys tests the DeleteKeys method
func TestSingleTargetApplierDeleteKeys(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_delete_test")
	testutils.RunSQL(t, "CREATE DATABASE single_delete_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_delete_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	// Insert test data
	_, err = targetDB.ExecContext(t.Context(), "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Diana'), (5, 'Eve')")
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Delete keys 2 and 4
	keysToDelete := []string{
		utils.HashKey([]any{int64(2)}),
		utils.HashKey([]any{int64(4)}),
	}

	affectedRows, err := applier.DeleteKeys(t.Context(), targetTable, targetTable, keysToDelete, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), affectedRows)

	// Verify remaining rows
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify specific rows remain
	rows, err := targetDB.QueryContext(t.Context(), "SELECT id, name FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	expected := []struct {
		id   int64
		name string
	}{
		{1, "Alice"},
		{3, "Charlie"},
		{5, "Eve"},
	}

	i := 0
	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, expected[i].id, id)
		assert.Equal(t, expected[i].name, name)
		i++
	}
	require.NoError(t, rows.Err())
}

// TestSingleTargetApplierDeleteKeysEmpty tests DeleteKeys with empty key list
func TestSingleTargetApplierDeleteKeysEmpty(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_delete_empty_test")
	testutils.RunSQL(t, "CREATE DATABASE single_delete_empty_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_delete_empty_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Delete with empty keys
	affectedRows, err := applier.DeleteKeys(t.Context(), targetTable, targetTable, []string{}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), affectedRows)
}

// TestSingleTargetApplierUpsertRows tests the UpsertRows method
func TestSingleTargetApplierUpsertRows(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100), value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	// Insert initial data
	_, err = targetDB.ExecContext(t.Context(), "INSERT INTO test_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Upsert rows: update id=1, insert id=3
	upsertRows := []LogicalRow{
		{
			RowImage:  []any{int64(1), "Alice Updated", int64(150)},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(3), "Charlie", int64(300)},
			IsDeleted: false,
		},
	}

	affectedRows, err := applier.UpsertRows(t.Context(), targetTable, targetTable, upsertRows, nil)
	require.NoError(t, err)
	// MySQL returns 1 for insert, 2 for update in ON DUPLICATE KEY UPDATE
	// So we expect 1 (update of id=1) + 1 (insert of id=3) = at least 2
	assert.GreaterOrEqual(t, affectedRows, int64(2))

	// Verify data
	rows, err := targetDB.QueryContext(t.Context(), "SELECT id, name, value FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	expected := []struct {
		id    int64
		name  string
		value int64
	}{
		{1, "Alice Updated", 150},
		{2, "Bob", 200},
		{3, "Charlie", 300},
	}

	i := 0
	for rows.Next() {
		var id, value int64
		var name string
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		assert.Equal(t, expected[i].id, id)
		assert.Equal(t, expected[i].name, name)
		assert.Equal(t, expected[i].value, value)
		i++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, i)
}

// TestSingleTargetApplierUpsertRowsSkipDeleted tests that deleted rows are skipped
func TestSingleTargetApplierUpsertRowsSkipDeleted(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_deleted_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_deleted_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_deleted_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Upsert with deleted rows mixed in
	upsertRows := []LogicalRow{
		{
			RowImage:  []any{int64(1), "Alice"},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(2), "Bob"},
			IsDeleted: true, // This should be skipped
		},
		{
			RowImage:  []any{int64(3), "Charlie"},
			IsDeleted: false,
		},
	}

	affectedRows, err := applier.UpsertRows(t.Context(), targetTable, targetTable, upsertRows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), affectedRows)

	// Verify only non-deleted rows were inserted
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// TestSingleTargetApplierUpsertRowsEmpty tests UpsertRows with empty row list
func TestSingleTargetApplierUpsertRowsEmpty(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_empty_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_empty_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_empty_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Upsert with empty rows
	affectedRows, err := applier.UpsertRows(t.Context(), targetTable, targetTable, []LogicalRow{}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), affectedRows)
}

// TestSingleTargetApplierContextCancellation tests that context cancellation stops processing
func TestSingleTargetApplierContextCancellation(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_cancel_test")
	testutils.RunSQL(t, "CREATE DATABASE single_cancel_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_cancel_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Create a cancellable context
	cancelCtx, cancel := context.WithCancel(t.Context())

	err = applier.Start(cancelCtx)
	require.NoError(t, err)
	defer applier.Stop()

	// Create a large dataset
	testRows := make([][]any, 1000)
	for i := range 1000 {
		testRows[i] = []any{int64(i + 1), int64(i)}
	}

	chunk := &table.Chunk{
		Table:    targetTable,
		NewTable: targetTable,
	}

	// Start applying but cancel immediately
	callback := func(affectedRows int64, err error) {
		// Callback may or may not be invoked depending on timing
	}

	err = applier.Apply(cancelCtx, chunk, testRows, callback)
	require.NoError(t, err)
	// Cancel the context
	cancel()

	// Wait should return context cancelled error
	err = applier.Wait(cancelCtx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestSingleTargetApplierWaitTimeout tests Wait with a timeout context
func TestSingleTargetApplierWaitTimeout(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_timeout_test")
	testutils.RunSQL(t, "CREATE DATABASE single_timeout_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_timeout_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer applier.Stop()

	// Apply some work
	testRows := [][]any{{int64(1)}, {int64(2)}}
	chunk := &table.Chunk{
		Table:    targetTable,
		NewTable: targetTable,
	}

	callback := func(affectedRows int64, err error) {}
	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Create a context with very short timeout
	timeoutCtx, cancel := context.WithTimeout(t.Context(), 1*time.Nanosecond)
	defer cancel()

	// Wait should timeout (or succeed if fast enough)
	err = applier.Wait(timeoutCtx)
	// Either succeeds or times out, both are acceptable
	if err != nil {
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

// TestSingleTargetApplierStartClose tests the lifecycle of starting and closing
func TestSingleTargetApplierStartClose(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_lifecycle_test")
	testutils.RunSQL(t, "CREATE DATABASE single_lifecycle_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_lifecycle_test"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	dbConfig := dbconn.NewDBConfig()
	applier := NewSingleTargetApplier(targetDB, dbConfig, slog.Default())

	// Start the applier
	err = applier.Start(t.Context())
	require.NoError(t, err)

	// Apply some work
	testRows := [][]any{{int64(1)}, {int64(2)}}
	chunk := &table.Chunk{
		Table:    targetTable,
		NewTable: targetTable,
	}

	var callbackInvoked atomic.Bool
	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		assert.NoError(t, err)
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Wait for work to complete
	err = applier.Wait(t.Context())
	require.NoError(t, err)
	assert.True(t, callbackInvoked.Load())

	// Close the applier
	err = applier.Stop()
	require.NoError(t, err)

	// Verify data was written
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}
