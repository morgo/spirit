// Package testutils contains some common utilities used exclusively
// by the test suite.
package testutils

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

// CreateUniqueTestDatabase creates a unique database for a test
func CreateUniqueTestDatabase(t *testing.T) string {
	t.Helper()

	// Create a unique database name based on test name
	dbName := fmt.Sprintf("spirit_test_%s_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		os.Getpid())

	// Connect to MySQL without specifying a database
	baseDSN := DSN()
	lastSlash := strings.LastIndex(baseDSN, "/")
	if lastSlash >= 0 {
		// Keep everything up to and including the slash, but remove the database name
		rootDSN := baseDSN[:lastSlash+1]

		db, err := sql.Open("mysql", rootDSN)
		assert.NoError(t, err)
		defer db.Close()

		// Create the database
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
		assert.NoError(t, err)

		// Register cleanup to drop the database
		t.Cleanup(func() {
			db, err := sql.Open("mysql", rootDSN)
			if err == nil {
				db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
				db.Close()
			}
		})
	}
	return dbName
}

// RunSQLInDatabase runs SQL in a specific database
func RunSQLInDatabase(t *testing.T, dbName, stmt string) {
	t.Helper()
	dsn := DSNForDatabase(dbName)
	db, err := sql.Open("mysql", dsn)
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}

func RunSQL(t *testing.T, stmt string) {
	t.Helper()
	db, err := sql.Open("mysql", DSN())
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}
