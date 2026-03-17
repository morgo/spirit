package table

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSchemaFromDB(t *testing.T) {
	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		name varchar(100) DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		user_id bigint unsigned NOT NULL,
		amount decimal(10,2) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_user_id (user_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tables, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	assert.Len(t, tables, 2)

	// Build a map for easier assertions
	byName := make(map[string]TableSchema)
	for _, ts := range tables {
		byName[ts.Name] = ts
	}

	assert.Contains(t, byName, "users")
	assert.Contains(t, byName, "orders")
	assert.Contains(t, byName["users"].Schema, "CREATE TABLE")
	assert.Contains(t, byName["users"].Schema, "`name` varchar(100)")
	assert.Contains(t, byName["orders"].Schema, "`amount` decimal(10,2)")
}

func TestLoadSchemaFromDB_EmptyDatabase(t *testing.T) {
	dbName := testutils.CreateUniqueTestDatabase(t)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tables, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	assert.Empty(t, tables)
}

func TestLoadSchemaFromDB_PreservesAutoIncrement(t *testing.T) {
	// Verify that LoadSchemaFromDB returns the raw DDL including AUTO_INCREMENT
	// values. Consumers that need to strip it (e.g. for diffing) do so themselves.
	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE counters (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4`)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tables, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "counters", tables[0].Name)
	assert.Contains(t, tables[0].Schema, "AUTO_INCREMENT=")
}

func TestLoadSchemaFromDB_MultipleOptsReturnsError(t *testing.T) {
	dbName := testutils.CreateUniqueTestDatabase(t)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = LoadSchemaFromDB(t.Context(), db, FilterOptions{}, FilterOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most one FilterOptions may be provided")
}

func TestLoadSchemaFromDB_FilterUnderscoreTables(t *testing.T) {
	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _vt_shadow (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _pending_drops (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Without filter: all 3 tables returned.
	all, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	assert.Len(t, all, 3)

	// With underscore filter: only "users" returned.
	filtered, err := LoadSchemaFromDB(t.Context(), db, FilterOptions{TablesStartingWithUnderscore: true})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, "users", filtered[0].Name)
}

func TestLoadSchemaFromDB_FilterArchiveTables(t *testing.T) {
	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users_archive_2024 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE orders_archive_2024_01 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE logs_archive_2024_01_15 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Without filter: all 4 tables returned.
	all, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	assert.Len(t, all, 4)

	// With archive filter: only "users" returned.
	filtered, err := LoadSchemaFromDB(t.Context(), db, FilterOptions{ArchiveTables: true})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, "users", filtered[0].Name)
}

func TestLoadSchemaFromDB_StripAutoIncrement(t *testing.T) {
	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE counters (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4`)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tables, err := LoadSchemaFromDB(t.Context(), db, FilterOptions{StripAutoIncrement: true})
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.NotContains(t, tables[0].Schema, "AUTO_INCREMENT=")
	// The column-level AUTO_INCREMENT keyword should still be present.
	assert.Contains(t, tables[0].Schema, "AUTO_INCREMENT")
}

func TestLoadSchemaFromDB_CombinedFilters(t *testing.T) {
	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _shadow (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users_archive_2024 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	filtered, err := LoadSchemaFromDB(t.Context(), db, FilterOptions{
		TablesStartingWithUnderscore: true,
		ArchiveTables:                true,
		StripAutoIncrement:           true,
	})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, "users", filtered[0].Name)
	assert.NotContains(t, filtered[0].Schema, "AUTO_INCREMENT=")
}
