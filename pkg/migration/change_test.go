package migration

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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

	testutils.RunSQL(t, `DROP TABLE IF EXISTS multichange2, multichange3, _multichange2_new, _multichange3_new`)
	testutils.RunSQL(t, `CREATE TABLE multichange2 (id int not null primary key auto_increment, b INT NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE multichange3 (id int not null primary key auto_increment, b INT NOT NULL)`)
	t.Cleanup(func() {
		testutils.RunSQL(t, `DROP TABLE IF EXISTS multichange2, multichange3, _multichange2_new, _multichange3_new`)
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS multichangedb1`)
	})

	// Build a fresh *Migration per Run. Reusing a single *Migration across
	// multiple Run() calls is not a supported production path, and stale
	// internal state from prior failed Runs (replication subscriptions,
	// useTestCutover bookkeeping, etc.) has been observed to surface in
	// the next Run as transient checksum failures. See block/spirit#769.
	run := func(statement string) error {
		return NewTestMigration(t, WithStatement(statement)).Run()
	}
	require.Error(t, run("ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT, ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT"))
	require.Error(t, run("ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT; ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT"))
	require.Error(t, run("ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT"))
	require.Error(t, run("ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT")) // even this is an error because we have schema + explicit DB.
	require.NoError(t, run("ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT"))
}

// TestAutoIncrementEmptyTable tests that AUTO_INCREMENT is preserved when migrating
// an EMPTY table with a high AUTO_INCREMENT value. This is the core bug being fixed.
func TestAutoIncrementEmptyTable(t *testing.T) {
	t.Parallel()

	tableName := "test_empty_table"
	testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_new, _%s_old`, tableName, tableName, tableName))
	t.Cleanup(func() {
		testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_new, _%s_old`, tableName, tableName, tableName))
	})

	// Create EMPTY table with high AUTO_INCREMENT value
	testutils.RunSQL(t, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB AUTO_INCREMENT=2979716`, tableName))

	testDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(testDB)

	// Verify table is empty but has AUTO_INCREMENT set
	var autoIncValue sql.NullInt64
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	require.True(t, autoIncValue.Valid, "AUTO_INCREMENT should be set")
	require.Equal(t, int64(2979716), autoIncValue.Int64, "AUTO_INCREMENT should be 2979716")

	var rowCount int
	err = testDB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	require.NoError(t, err)
	require.Equal(t, 0, rowCount, "Table should be empty")

	// Run migration with an ALTER that forces copy algorithm
	r := NewTestRunner(t, tableName, "ADD COLUMN test_col VARCHAR(255), ADD UNIQUE INDEX uk_test_col (test_col)")
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
	require.Equal(t, expectedIDs, insertedIDs, "Inserted IDs should start from 2979716, not 1")

	// Verify final AUTO_INCREMENT value
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	require.True(t, autoIncValue.Valid)
	require.GreaterOrEqual(t, autoIncValue.Int64, int64(2979716), "Final AUTO_INCREMENT should be >= 2979716")
}

// TestAutoIncrementWithRows tests that AUTO_INCREMENT is preserved when migrating
// a table that HAS rows. This should pass even without the fix because MySQL's
// INSERT SELECT automatically adjusts AUTO_INCREMENT based on max(id).
func TestAutoIncrementWithRows(t *testing.T) {
	t.Parallel()

	tableName := "test_with_rows"
	testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_new, _%s_old`, tableName, tableName, tableName))
	t.Cleanup(func() {
		testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_new, _%s_old`, tableName, tableName, tableName))
	})

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

	testDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(testDB)

	// Verify table has rows and AUTO_INCREMENT is set
	var autoIncValue sql.NullInt64
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		tableName).Scan(&autoIncValue)
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
	require.Equal(t, expectedExistingIDs, existingIDs)

	// Run migration
	r := NewTestRunner(t, tableName, "ADD COLUMN test_col VARCHAR(255), ADD UNIQUE INDEX uk_test_col (test_col)")
	defer utils.CloseAndLog(r)

	err = r.Run(t.Context())
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
	require.Equal(t, expectedExistingIDs, migratedIDs, "Existing IDs should be preserved")

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
	require.Equal(t, expectedAllIDs, allIDs, "New IDs should continue from 2979719")

	// Verify final AUTO_INCREMENT
	err = testDB.QueryRowContext(t.Context(),
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		tableName).Scan(&autoIncValue)
	require.NoError(t, err)
	require.True(t, autoIncValue.Valid)
	require.GreaterOrEqual(t, autoIncValue.Int64, int64(2979719), "Final AUTO_INCREMENT should be >= 2979719")
}

func TestOldTableNameTruncation(t *testing.T) {
	t.Parallel()
	startTime := time.Date(2025, 6, 15, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		name                 string
		tableName            string
		skipDropAfterCutover bool
		expectMaxLen         int
	}{
		{
			name:                 "short name without skip drop",
			tableName:            "mytable",
			skipDropAfterCutover: false,
			expectMaxLen:         utils.MaxTableNameLength,
		},
		{
			name:                 "short name with skip drop",
			tableName:            "mytable",
			skipDropAfterCutover: true,
			expectMaxLen:         utils.MaxTableNameLength,
		},
		{
			name:                 "long name without skip drop",
			tableName:            strings.Repeat("a", 56),
			skipDropAfterCutover: false,
			expectMaxLen:         utils.MaxTableNameLength,
		},
		{
			name:                 "long name with skip drop - requires truncation",
			tableName:            strings.Repeat("b", 56),
			skipDropAfterCutover: true,
			expectMaxLen:         utils.MaxTableNameLength,
		},
		{
			name:                 "64-char name without skip drop - requires truncation",
			tableName:            strings.Repeat("c", utils.MaxTableNameLength),
			skipDropAfterCutover: false,
			expectMaxLen:         utils.MaxTableNameLength,
		},
		{
			name:                 "64-char name with skip drop - requires truncation",
			tableName:            strings.Repeat("d", utils.MaxTableNameLength),
			skipDropAfterCutover: true,
			expectMaxLen:         utils.MaxTableNameLength,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &change{
				table: &table.TableInfo{
					TableName: tt.tableName,
				},
				runner: &Runner{
					migration: &Migration{
						SkipDropAfterCutover: tt.skipDropAfterCutover,
					},
					startTime: startTime,
				},
			}

			result := c.oldTableName()
			require.LessOrEqual(t, len(result), tt.expectMaxLen,
				"oldTableName() result %q (len=%d) exceeds max length %d",
				result, len(result), tt.expectMaxLen)
			require.Greater(t, len(result), 0, "oldTableName() should not be empty")

			if tt.skipDropAfterCutover {
				// Should contain the timestamp
				require.Contains(t, result, "20250615_103045")
				// Should have the expected format prefix and suffix
				require.True(t, strings.HasPrefix(result, "_"))
				require.Contains(t, result, "_old_")
			} else {
				// _<name>_old, possibly with the table name portion truncated to fit.
				require.True(t, strings.HasPrefix(result, "_"))
				require.True(t, strings.HasSuffix(result, "_old"))
				// Short names should be reproduced exactly.
				if 1+len(tt.tableName)+len("_old") <= utils.MaxTableNameLength {
					require.Equal(t, "_"+tt.tableName+"_old", result)
				}
			}
		})
	}
}

func TestOldTableNameTruncationCollision(t *testing.T) {
	t.Parallel()
	// Two long table names that share a 50-char common prefix produce the
	// same _old_<timestamp> name once the suffix is truncated away. This is
	// the documented trade-off of deterministic truncation. Resume safety
	// for the checkpoint table is provided by the original_table_name column
	// (see TestResumeRejectsCheckpointFromDifferentTable); the old table is named
	// only for human archaeology when SkipDropAfterCutover is set.
	startTime := time.Date(2025, 6, 15, 10, 30, 45, 0, time.UTC)
	prefix := strings.Repeat("x", 50)

	c1 := &change{
		table:  &table.TableInfo{TableName: prefix + "_aaaaaaaaaaa"},
		runner: &Runner{migration: &Migration{SkipDropAfterCutover: true}, startTime: startTime},
	}
	c2 := &change{
		table:  &table.TableInfo{TableName: prefix + "_bbbbbbbbbbb"},
		runner: &Runner{migration: &Migration{SkipDropAfterCutover: true}, startTime: startTime},
	}

	result1 := c1.oldTableName()
	result2 := c2.oldTableName()
	require.LessOrEqual(t, len(result1), utils.MaxTableNameLength)
	require.LessOrEqual(t, len(result2), utils.MaxTableNameLength)
	require.Equal(t, result1, result2,
		"distinct long table names with a shared prefix collide after truncation")
}
