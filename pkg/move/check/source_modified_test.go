package check

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceModifiedCheck(t *testing.T) {
	// Setup test database
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_modified_test`)
	testutils.RunSQL(t, `CREATE DATABASE source_modified_test`)
	defer testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_modified_test`)
	testutils.RunSQL(t, `CREATE TABLE source_modified_test.test1 (id INT PRIMARY KEY, name VARCHAR(255))`)
	testutils.RunSQL(t, `CREATE TABLE source_modified_test.test2 (id INT PRIMARY KEY, value INT)`)

	db, err := dbconn.New(testutils.DSNForDatabase("source_modified_test"), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	logger := slog.Default()

	// Create table info for the initial tables
	table1 := table.NewTableInfo(db, "source_modified_test", "test1")
	require.NoError(t, table1.SetInfo(ctx))

	table2 := table.NewTableInfo(db, "source_modified_test", "test2")
	require.NoError(t, table2.SetInfo(ctx))

	t.Run("NoNewTables", func(t *testing.T) {
		// Test that check passes when no new tables are created
		resources := Resources{
			SourceDB:     db,
			SourceTables: []*table.TableInfo{table1, table2},
		}

		err := sourceModifiedCheck(ctx, resources, logger)
		assert.NoError(t, err, "check should pass when no new tables exist")
	})

	t.Run("NewTableDetected", func(t *testing.T) {
		// Create a new table that wasn't in the original list
		testutils.RunSQL(t, `CREATE TABLE source_modified_test.test3 (id INT PRIMARY KEY, data TEXT)`)
		defer testutils.RunSQL(t, `DROP TABLE IF EXISTS source_modified_test.test3`)

		resources := Resources{
			SourceDB:     db,
			SourceTables: []*table.TableInfo{table1, table2},
		}

		err := sourceModifiedCheck(ctx, resources, logger)
		assert.Error(t, err, "check should fail when new table is detected")
		assert.Contains(t, err.Error(), "new table(s) detected", "error should mention new tables")
		assert.Contains(t, err.Error(), "test3", "error should include the new table name")
	})

	t.Run("EmptySourceTables", func(t *testing.T) {
		// Test that check is skipped when SourceTables is empty
		resources := Resources{
			SourceDB:     db,
			SourceTables: []*table.TableInfo{},
		}

		err := sourceModifiedCheck(ctx, resources, logger)
		assert.NoError(t, err, "check should be skipped when SourceTables is empty")
	})

	t.Run("IgnoresSpiritInternalTables", func(t *testing.T) {
		// Create spirit internal tables - they should be ignored
		testutils.RunSQL(t, `DROP TABLE IF EXISTS source_modified_test._spirit_checkpoint, source_modified_test._spirit_sentinel`)
		testutils.RunSQL(t, `CREATE TABLE source_modified_test._spirit_checkpoint (id INT PRIMARY KEY)`)
		testutils.RunSQL(t, `CREATE TABLE source_modified_test._spirit_sentinel (id INT PRIMARY KEY)`)
		defer func() {
			testutils.RunSQL(t, `DROP TABLE IF EXISTS source_modified_test._spirit_checkpoint, source_modified_test._spirit_sentinel`)
		}()

		resources := Resources{
			SourceDB:     db,
			SourceTables: []*table.TableInfo{table1, table2},
		}

		err := sourceModifiedCheck(ctx, resources, logger)
		assert.NoError(t, err, "check should ignore spirit internal tables")
	})
}

func TestSourceModifiedCheckIntegration(t *testing.T) {
	// Setup test database
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS integration_test_db`)
	testutils.RunSQL(t, `CREATE DATABASE integration_test_db`)
	defer testutils.RunSQL(t, `DROP DATABASE IF EXISTS integration_test_db`)

	testutils.RunSQL(t, `CREATE TABLE integration_test_db.integration_test1 (id INT PRIMARY KEY)`)

	db, err := dbconn.New(testutils.DSNForDatabase("integration_test_db"), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	logger := slog.Default()

	table1 := table.NewTableInfo(db, "integration_test_db", "integration_test1")
	require.NoError(t, table1.SetInfo(ctx))

	resources := Resources{
		SourceDB:     db,
		SourceTables: []*table.TableInfo{table1},
	}

	// Test that check passes when no new tables
	err = sourceModifiedCheck(ctx, resources, logger)
	assert.NoError(t, err, "check should pass when no new tables")

	// Create a new table and verify check fails
	testutils.RunSQL(t, `CREATE TABLE integration_test_db.integration_test2 (id INT PRIMARY KEY)`)
	defer testutils.RunSQL(t, `DROP TABLE IF EXISTS integration_test_db.integration_test2`)

	err = sourceModifiedCheck(ctx, resources, logger)
	assert.Error(t, err, "check should fail when new table is detected")
	assert.Contains(t, err.Error(), "integration_test2", "error should mention the new table")
}

func TestSourceModifiedCheckWithNilDB(t *testing.T) {
	// Test error handling when database connection fails
	// Setup test database
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS nil_db_test`)
	testutils.RunSQL(t, `CREATE DATABASE nil_db_test`)
	defer testutils.RunSQL(t, `DROP DATABASE IF EXISTS nil_db_test`)

	ctx := context.Background()
	logger := slog.Default()

	// Create a closed database connection
	db, err := dbconn.New(testutils.DSNForDatabase("nil_db_test"), dbconn.NewDBConfig())
	require.NoError(t, err)
	db.Close() // Close it immediately

	resources := Resources{
		SourceDB:     db,
		SourceTables: []*table.TableInfo{{TableName: "test"}},
	}

	err = sourceModifiedCheck(ctx, resources, logger)
	assert.Error(t, err, "check should fail with closed database connection")
	assert.Contains(t, err.Error(), "failed to query source tables", "error should indicate query failure")
}

func TestSourceModifiedCheckMultipleNewTables(t *testing.T) {
	// Test detection of multiple new tables
	// Setup test database
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS multi_test_db`)
	testutils.RunSQL(t, `CREATE DATABASE multi_test_db`)
	defer testutils.RunSQL(t, `DROP DATABASE IF EXISTS multi_test_db`)

	testutils.RunSQL(t, `CREATE TABLE multi_test_db.multi_test1 (id INT PRIMARY KEY)`)
	testutils.RunSQL(t, `CREATE TABLE multi_test_db.multi_test2 (id INT PRIMARY KEY)`)

	db, err := dbconn.New(testutils.DSNForDatabase("multi_test_db"), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	logger := slog.Default()

	table1 := table.NewTableInfo(db, "multi_test_db", "multi_test1")
	require.NoError(t, table1.SetInfo(ctx))

	table2 := table.NewTableInfo(db, "multi_test_db", "multi_test2")
	require.NoError(t, table2.SetInfo(ctx))

	// Create two new tables
	testutils.RunSQL(t, `CREATE TABLE multi_test_db.multi_test3 (id INT PRIMARY KEY)`)
	testutils.RunSQL(t, `CREATE TABLE multi_test_db.multi_test4 (id INT PRIMARY KEY)`)
	defer func() {
		testutils.RunSQL(t, `DROP TABLE IF EXISTS multi_test_db.multi_test3, multi_test_db.multi_test4`)
	}()

	resources := Resources{
		SourceDB:     db,
		SourceTables: []*table.TableInfo{table1, table2},
	}

	err = sourceModifiedCheck(ctx, resources, logger)
	assert.Error(t, err, "check should fail when multiple new tables are detected")
	assert.Contains(t, err.Error(), "multi_test3", "error should mention first new table")
	assert.Contains(t, err.Error(), "multi_test4", "error should mention second new table")
}
