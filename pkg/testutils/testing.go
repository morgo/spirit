// Package testutils contains some common utilities used exclusively
// by the test suite.
package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func DSN() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "spirit:spirit@tcp(127.0.0.1:3306)/test"
	}
	return dsn
}

// DSNForDatabase returns a DSN for a specific database name
func DSNForDatabase(dbName string) string {
	baseDSN := DSN()
	// Replace the database part of the DSN
	parts := strings.Split(baseDSN, "/")
	if len(parts) >= 2 {
		parts[len(parts)-1] = dbName
		return strings.Join(parts, "/")
	}
	return baseDSN
}

// CreateUniqueTestDatabase creates a unique database for a test and returns
// both the database name and a *sql.DB connection scoped to that database.
// The connection and database are automatically cleaned up when the test finishes.
func CreateUniqueTestDatabase(t *testing.T) (string, *sql.DB) {
	t.Helper()

	// Create a unique database name based on test name
	dbName := fmt.Sprintf("t_%s_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		os.Getpid())

	// Connect to MySQL without specifying a database
	baseDSN := DSN()
	lastSlash := strings.LastIndex(baseDSN, "/")
	if lastSlash < 0 {
		t.Fatalf("could not parse DSN: %s", baseDSN)
	}
	rootDSN := baseDSN[:lastSlash+1]

	rootDB, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	defer func() {
		_ = rootDB.Close()
	}()
	_, err = rootDB.ExecContext(t.Context(), "CREATE DATABASE IF NOT EXISTS "+dbName)
	require.NoError(t, err)

	// Open a connection scoped to the new database
	scopedDB, err := sql.Open("mysql", rootDSN+dbName)
	require.NoError(t, err)

	// Register cleanup to close the connection and drop the database
	t.Cleanup(func() {
		_ = scopedDB.Close()
		cleanupDB, err := sql.Open("mysql", rootDSN)
		assert.NoError(t, err)
		defer func() {
			_ = cleanupDB.Close()
		}()
		_, err = cleanupDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+dbName)
		assert.NoError(t, err)
	})

	return dbName, scopedDB
}

// RunSQLInDatabase runs SQL in a specific database
func RunSQLInDatabase(t *testing.T, dbName, stmt string) {
	t.Helper()
	dsn := DSNForDatabase(dbName)
	db, err := sql.Open("mysql", dsn)
	assert.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	_, err = db.ExecContext(t.Context(), stmt)
	assert.NoError(t, err)
}

func RunSQL(t *testing.T, stmt string) {
	t.Helper()
	db, err := sql.Open("mysql", DSN())
	assert.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	_, err = db.ExecContext(t.Context(), stmt)
	assert.NoError(t, err)
}

var (
	isRBRTestRunnerCached bool
	isRBRTestRunnerOnce   sync.Once
)

func IsMinimalRBRTestRunner(t *testing.T) bool {
	// Check if we are in the minimal RBR test runner.
	// we use this to skip certain tests.
	isRBRTestRunnerOnce.Do(func() {
		cfg, err := mysql.ParseDSN(DSN())
		require.NoError(t, err)
		db, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()
		var binlogRowImage, binlogRowValueOptions string
		err = db.QueryRowContext(t.Context(),
			`SELECT
		@@global.binlog_row_image,
		@@global.binlog_row_value_options`).Scan(
			&binlogRowImage,
			&binlogRowValueOptions,
		)
		require.NoError(t, err)
		if binlogRowImage != "FULL" || binlogRowValueOptions != "" {
			isRBRTestRunnerCached = true
		}
	})
	return isRBRTestRunnerCached
}

// EvenOddHasher is a test hash function that shards assuming -80 and 80- shards.
// even goes to -80, odd goes to 80-
func EvenOddHasher(colAny any) (uint64, error) {
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
	return hash, nil
}
