package migration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoIncrementEmptyTable tests that AUTO_INCREMENT is preserved when migrating
// an EMPTY table with a high AUTO_INCREMENT value. This is the core bug being fixed.
func TestAutoIncrementEmptyTable(t *testing.T) {
	t.Parallel()

	tableName := "test_empty_table"
	testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_new, _%s_old`, tableName, tableName, tableName))

	// Create EMPTY table with high AUTO_INCREMENT value
	testutils.RunSQL(t, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB AUTO_INCREMENT=2979716`, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	testDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer testDB.Close()

	// Verify table is empty but has AUTO_INCREMENT set
	var autoIncValue sql.NullInt64
	err = testDB.QueryRow(
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	require.True(t, autoIncValue.Valid, "AUTO_INCREMENT should be set")
	require.Equal(t, int64(2979716), autoIncValue.Int64, "AUTO_INCREMENT should be 2979716")
	t.Logf("✓ Empty table has AUTO_INCREMENT: %d", autoIncValue.Int64)

	var rowCount int
	err = testDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	require.NoError(t, err)
	require.Equal(t, 0, rowCount, "Table should be empty")
	t.Logf("✓ Table is empty: %d rows", rowCount)

	t.Log("→ Running migration on empty table...")

	// Run migration with an ALTER that forces copy algorithm
	r, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    tableName,
		Alter:    "ADD COLUMN test_col VARCHAR(255), ADD UNIQUE INDEX uk_test_col (test_col)",
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = r.Run(ctx)
	require.NoError(t, err)

	// After migration, insert rows and verify they get correct IDs
	t.Log("→ Inserting rows after migration...")
	testutils.RunSQL(t, fmt.Sprintf(`
		INSERT INTO %s (name) VALUES 
		('user1'),
		('user2'),
		('user3')`, tableName))

	// Verify the IDs are correct (should start from 2979716, not from 1)
	var insertedIDs []int64
	rows, err := testDB.Query(fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		err := rows.Scan(&id)
		require.NoError(t, err)
		insertedIDs = append(insertedIDs, id)
	}

	expectedIDs := []int64{2979716, 2979717, 2979718}
	assert.Equal(t, expectedIDs, insertedIDs, "Inserted IDs should start from 2979716, not 1")
	t.Logf("✓ New insert IDs: %v (expected: %v)", insertedIDs, expectedIDs)

	// Verify final AUTO_INCREMENT value
	err = testDB.QueryRow(
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	assert.True(t, autoIncValue.Valid)
	assert.GreaterOrEqual(t, autoIncValue.Int64, int64(2979716), "Final AUTO_INCREMENT should be >= 2979716")
	t.Logf("✓ Final AUTO_INCREMENT: %d", autoIncValue.Int64)
}

// TestAutoIncrementWithRows tests that AUTO_INCREMENT is preserved when migrating
// a table that HAS rows. This should pass even without the fix because MySQL's
// INSERT SELECT automatically adjusts AUTO_INCREMENT based on max(id).
func TestAutoIncrementWithRows(t *testing.T) {
	t.Parallel()

	tableName := "test_with_rows"
	testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_new, _%s_old`, tableName, tableName, tableName))

	// Create table with high AUTO_INCREMENT value
	testutils.RunSQL(t, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB AUTO_INCREMENT=2979716`, tableName))

	// Insert rows with IDs starting from 2979716
	testutils.RunSQL(t, fmt.Sprintf(`
		INSERT INTO %s (name) VALUES 
		('user1'),
		('user2'),
		('user3')`, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	testDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer testDB.Close()

	// Verify table has rows and AUTO_INCREMENT is set
	var autoIncValue sql.NullInt64
	err = testDB.QueryRow(
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	require.True(t, autoIncValue.Valid)
	require.Equal(t, int64(2979719), autoIncValue.Int64, "AUTO_INCREMENT should be 2979719 (3 rows inserted)")
	t.Logf("✓ Table with rows has AUTO_INCREMENT: %d", autoIncValue.Int64)

	var rowCount int
	err = testDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	require.NoError(t, err)
	require.Equal(t, 3, rowCount, "Table should have 3 rows")
	t.Logf("✓ Table has %d rows", rowCount)

	// Verify existing IDs
	var existingIDs []int64
	rows, err := testDB.Query(fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		existingIDs = append(existingIDs, id)
	}
	rows.Close()
	expectedExistingIDs := []int64{2979716, 2979717, 2979718}
	assert.Equal(t, expectedExistingIDs, existingIDs)
	t.Logf("✓ Existing IDs before migration: %v", existingIDs)

	t.Log("→ Running migration on table with rows...")

	// Run migration
	r, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    tableName,
		Alter:    "ADD COLUMN test_col VARCHAR(255), ADD UNIQUE INDEX uk_test_col (test_col)",
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = r.Run(ctx)
	require.NoError(t, err)

	// Verify existing rows are preserved
	var migratedIDs []int64
	rows, err = testDB.Query(fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		migratedIDs = append(migratedIDs, id)
	}
	rows.Close()
	assert.Equal(t, expectedExistingIDs, migratedIDs, "Existing IDs should be preserved")
	t.Logf("✓ IDs after migration: %v", migratedIDs)

	// Insert new rows after migration
	t.Log("→ Inserting new rows after migration...")
	testutils.RunSQL(t, fmt.Sprintf(`
		INSERT INTO %s (name) VALUES 
		('user4'),
		('user5'),
		('user6')`, tableName))

	// Verify all IDs (existing + new)
	var allIDs []int64
	rows, err = testDB.Query(fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		allIDs = append(allIDs, id)
	}
	rows.Close()

	expectedAllIDs := []int64{2979716, 2979717, 2979718, 2979719, 2979720, 2979721}
	assert.Equal(t, expectedAllIDs, allIDs, "New IDs should continue from 2979719")
	t.Logf("✓ All IDs after new inserts: %v", allIDs)

	// Verify final AUTO_INCREMENT
	err = testDB.QueryRow(
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	assert.True(t, autoIncValue.Valid)
	assert.GreaterOrEqual(t, autoIncValue.Int64, int64(2979719), "Final AUTO_INCREMENT should be >= 2979719")
	t.Logf("✓ Final AUTO_INCREMENT: %d", autoIncValue.Int64)
}
