package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestTable is a helper for creating and managing test tables.
// It handles DROP IF EXISTS before creation, registers t.Cleanup()
// to drop the table and all Spirit artifacts (_new, _old, _chkpnt)
// when the test finishes, and provides a DB connection for verification.
type TestTable struct {
	// Name is the base table name (e.g. "mytable").
	Name string

	// DB is a *sql.DB connection to the test database, ready for
	// verification queries. It is closed automatically via t.Cleanup().
	DB *sql.DB
}

// NewTestTable creates a test table with the given name and DDL.
// It drops any existing table (and Spirit artifacts) before creation,
// and registers a cleanup function to drop them when the test finishes.
//
// The createSQL must be a complete CREATE TABLE statement. Use RunSQL
// or SeedRows after creation to populate the table with data.
//
// The returned TestTable includes a DB connection for verification queries.
//
// Example:
//
//	tt := testutils.NewTestTable(t, "mytable",
//	    `CREATE TABLE mytable (
//	        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
//	        name VARCHAR(255) NOT NULL
//	    )`,
//	)
//	tt.SeedRows(t, "INSERT INTO mytable (name) SELECT 'a'", 1000)
func NewTestTable(t *testing.T, name string, createSQL string) *TestTable {
	t.Helper()

	db, err := sql.Open("mysql", DSN())
	require.NoError(t, err)

	tt := &TestTable{Name: name, DB: db}
	tt.dropArtifacts(t)
	RunSQL(t, createSQL)

	t.Cleanup(func() {
		defer utils.CloseAndLog(db)
		// Use a background context because t.Context() is cancelled after the test.
		for _, suffix := range []string{"", "_new", "_old", "_chkpnt"} {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", name+suffix))
		}
	})

	return tt
}

// dropArtifacts drops the base table and all Spirit shadow/checkpoint/old tables.
// Errors are ignored because artifact names may exceed MySQL's 64-char identifier
// limit (e.g. when testing long table names).
func (tt *TestTable) dropArtifacts(t *testing.T) {
	t.Helper()
	db, err := sql.Open("mysql", DSN())
	if err != nil {
		return
	}
	defer utils.CloseAndLog(db)
	for _, suffix := range []string{"", "_new", "_old", "_chkpnt"} {
		_, _ = db.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tt.Name+suffix))
	}
}

// SeedRows populates the table with approximately targetRows rows.
//
// The insertSelectSQL should be an INSERT ... SELECT statement without
// a FROM clause. SeedRows appends "FROM dual" for the initial insert,
// then appends "FROM <table>" and doubles repeatedly until the target
// is reached.
//
// Example:
//
//	tt.SeedRows(t, "INSERT INTO mytable (name, val) SELECT 'a', 1", 4096)
//
// This executes:
//
//	INSERT INTO mytable (name, val) SELECT 'a', 1 FROM dual       -- seed
//	INSERT INTO mytable (name, val) SELECT 'a', 1 FROM mytable    -- 2x
//	INSERT INTO mytable (name, val) SELECT 'a', 1 FROM mytable    -- 4x
//	...until ~4096 rows
func (tt *TestTable) SeedRows(t *testing.T, insertSelectSQL string, targetRows int) {
	t.Helper()
	require.Greater(t, targetRows, 0, "targetRows must be positive")

	// Run the initial seed: INSERT INTO t (...) SELECT ... FROM dual
	RunSQL(t, insertSelectSQL+" FROM dual")

	// Count how many rows the seed inserted.
	var currentRows int
	err := tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tt.Name)).Scan(&currentRows)
	require.NoError(t, err)
	require.Greater(t, currentRows, 0, "seed SQL must insert at least one row")

	// Double until we reach the target: INSERT INTO t (...) SELECT ... FROM t
	doubleSQL := fmt.Sprintf("%s FROM `%s`", insertSelectSQL, tt.Name)
	for currentRows < targetRows {
		RunSQL(t, doubleSQL)
		currentRows *= 2
	}
}
