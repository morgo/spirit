package testutils

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTable manages a test table's lifecycle: creation, cleanup, and
// provides a DB connection for verification queries after migration.
type TestTable struct {
	Name string
	DB   *sql.DB
}

// NewTestTable creates a test table and registers cleanup to drop it
// and all Spirit artifacts (_new, _old, _chkpnt) when the test finishes.
//
// Example:
//
//	tt := testutils.NewTestTable(t, "mytable",
//	    `CREATE TABLE mytable (
//	        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
//	        name VARCHAR(255) NOT NULL
//	    )`)
//
//	// Use tt.DB for verification queries after migration
//	var count int
//	tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM mytable").Scan(&count)
func NewTestTable(t *testing.T, name string, createSQL string) *TestTable {
	t.Helper()

	tt := &TestTable{Name: name}

	// Open a DB connection for this table (used for cleanup and verification).
	db, err := sql.Open("mysql", DSN())
	require.NoError(t, err)
	tt.DB = db

	// Drop any pre-existing table and Spirit artifacts.
	tt.dropArtifacts(t)

	// Create the table.
	_, err = db.ExecContext(t.Context(), createSQL)
	require.NoError(t, err)

	// Register cleanup to drop everything when the test finishes.
	t.Cleanup(func() {
		tt.dropArtifacts(t)
		defer utils.CloseAndLog(db)
	})

	return tt
}

// dropArtifacts drops the base table and all Spirit shadow/checkpoint tables.
// It silently ignores errors for artifact names that exceed MySQL's 64-char
// identifier limit (e.g., when testing long table names).
func (tt *TestTable) dropArtifacts(t *testing.T) {
	t.Helper()
	tables := []string{
		tt.Name,
		fmt.Sprintf("_%s_new", tt.Name),
		fmt.Sprintf("_%s_old", tt.Name),
		fmt.Sprintf("_%s_chkpnt", tt.Name),
	}
	for _, tbl := range tables {
		_, err := tt.DB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tbl))
		if err != nil {
			// Silently ignore errors (e.g., identifier too long for artifact tables).
			continue
		}
	}
}

// SeedRows populates the table by doubling rows until reaching approximately
// targetRows. The insertSelectSQL should be an INSERT INTO ... SELECT statement
// WITHOUT a FROM clause. SeedRows appends "FROM dual" for the initial insert,
// then "FROM <table>" for each doubling iteration.
//
// Example:
//
//	// Simple seeding — produces ~4096 identical rows (different auto-increment IDs)
//	tt.SeedRows(t, "INSERT INTO mytable (name, val) SELECT 'seed', 1", 4096)
//
//	// With SQL functions — each row gets unique random data
//	tt.SeedRows(t, "INSERT INTO mytable (pad) SELECT RANDOM_BYTES(1024)", 100000)
func (tt *TestTable) SeedRows(t *testing.T, insertSelectSQL string, targetRows int) {
	t.Helper()

	// Initial insert from dual (creates 1 row).
	_, err := tt.DB.ExecContext(t.Context(), insertSelectSQL+" FROM dual")
	require.NoError(t, err)

	// Count initial rows (may be >1 if the SELECT produces multiple rows).
	var count int
	err = tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tt.Name)).Scan(&count)
	require.NoError(t, err)

	// Double rows until we reach the target.
	for count < targetRows {
		_, err = tt.DB.ExecContext(t.Context(),
			fmt.Sprintf("%s FROM `%s`", insertSelectSQL, tt.Name))
		assert.NoError(t, err)
		count *= 2
	}
}
