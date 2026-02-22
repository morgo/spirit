package migration

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Attempt multi changes across different schemas
// This must never happen because we use the schema from
// changes[0] in a lot of places, we tie the checkpoint to
// a schema, and the sentinel etc.
func TestMultiChangesDifferentSchemas(t *testing.T) {
	t.Parallel()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS multichangedb1`)
	testutils.RunSQL(t, `CREATE DATABASE multichangedb1`)
	testutils.RunSQL(t, `CREATE TABLE multichangedb1.multichange1 (id int not null primary key auto_increment, b INT NOT NULL)`)

	// from the test schema
	testutils.RunSQL(t, `DROP TABLE IF EXISTS multichange2, multichange3`)
	testutils.RunSQL(t, `CREATE TABLE multichange2 (id int not null primary key auto_increment, b INT NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE multichange3 (id int not null primary key auto_increment, b INT NOT NULL)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         4,
		TargetChunkTime: 2 * time.Second,
		Statement:       "ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT, ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT",
		ForceKill:       true,
	}
	assert.Error(t, migration.Run())
	migration.Statement = "ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT; ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT"
	assert.Error(t, migration.Run())
	migration.Statement = "ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT"
	assert.Error(t, migration.Run())
	migration.Statement = "ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT"
	assert.Error(t, migration.Run()) // even this is an error because we have schema + explicit DB.
	migration.Statement = "ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT"
	assert.NoError(t, migration.Run())
}

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
	defer utils.CloseAndLog(testDB)

	// Verify table is empty but has AUTO_INCREMENT set
	var autoIncValue sql.NullInt64
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	require.True(t, autoIncValue.Valid, "AUTO_INCREMENT should be set")
	require.Equal(t, int64(2979716), autoIncValue.Int64, "AUTO_INCREMENT should be 2979716")

	var rowCount int
	err = testDB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	require.NoError(t, err)
	require.Equal(t, 0, rowCount, "Table should be empty")

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
	defer utils.CloseAndLog(r)

	ctx := t.Context()
	err = r.Run(ctx)
	require.NoError(t, err)

	// After migration, insert rows and verify they get correct IDs
	testutils.RunSQL(t, fmt.Sprintf(`
		INSERT INTO %s (name) VALUES 
		('user1'),
		('user2'),
		('user3')`, tableName))

	// Verify the IDs are correct (should start from 2979716, not from 1)
	var insertedIDs []int64
	rows, err := testDB.QueryContext(t.Context(), fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var id int64
		err := rows.Scan(&id)
		require.NoError(t, err)
		insertedIDs = append(insertedIDs, id)
	}

	expectedIDs := []int64{2979716, 2979717, 2979718}
	assert.Equal(t, expectedIDs, insertedIDs, "Inserted IDs should start from 2979716, not 1")

	// Verify final AUTO_INCREMENT value
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	assert.True(t, autoIncValue.Valid)
	assert.GreaterOrEqual(t, autoIncValue.Int64, int64(2979716), "Final AUTO_INCREMENT should be >= 2979716")
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
	defer utils.CloseAndLog(testDB)

	// Verify table has rows and AUTO_INCREMENT is set
	var autoIncValue sql.NullInt64
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	require.True(t, autoIncValue.Valid)
	require.Equal(t, int64(2979719), autoIncValue.Int64, "AUTO_INCREMENT should be 2979719 (3 rows inserted)")

	var rowCount int
	err = testDB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	require.NoError(t, err)
	require.Equal(t, 3, rowCount, "Table should have 3 rows")

	// Verify existing IDs
	var existingIDs []int64
	rows, err := testDB.QueryContext(t.Context(), fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		existingIDs = append(existingIDs, id)
	}
	_ = rows.Close()
	expectedExistingIDs := []int64{2979716, 2979717, 2979718}
	assert.Equal(t, expectedExistingIDs, existingIDs)

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
	defer utils.CloseAndLog(r)

	ctx := t.Context()
	err = r.Run(ctx)
	require.NoError(t, err)

	// Verify existing rows are preserved
	var migratedIDs []int64
	rows, err = testDB.QueryContext(t.Context(), fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		migratedIDs = append(migratedIDs, id)
	}
	_ = rows.Close()
	assert.Equal(t, expectedExistingIDs, migratedIDs, "Existing IDs should be preserved")

	// Insert new rows after migration
	testutils.RunSQL(t, fmt.Sprintf(`
		INSERT INTO %s (name) VALUES 
		('user4'),
		('user5'),
		('user6')`, tableName))

	// Verify all IDs (existing + new)
	var allIDs []int64
	rows, err = testDB.QueryContext(t.Context(), fmt.Sprintf("SELECT id FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		allIDs = append(allIDs, id)
	}
	_ = rows.Close()

	expectedAllIDs := []int64{2979716, 2979717, 2979718, 2979719, 2979720, 2979721}
	assert.Equal(t, expectedAllIDs, allIDs, "New IDs should continue from 2979719")

	// Verify final AUTO_INCREMENT
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	assert.True(t, autoIncValue.Valid)
	assert.GreaterOrEqual(t, autoIncValue.Int64, int64(2979719), "Final AUTO_INCREMENT should be >= 2979719")
}
