package lint

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSchemaFromDir_Basic(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		name varchar(100) DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, dir, "orders.sql", `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		user_id bigint unsigned NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	assert.Len(t, tables, 2)

	// Verify table names are parsed
	names := make(map[string]bool)
	for _, ct := range tables {
		names[ct.TableName] = true
	}
	assert.True(t, names["users"])
	assert.True(t, names["orders"])
}

func TestLoadSchemaFromDir_SkipsNonSQL(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, dir, "README.md", "This is not SQL")
	writeFile(t, dir, "notes.txt", "This is not SQL either")

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
	assert.Equal(t, "users", tables[0].TableName)
}

func TestLoadSchemaFromDir_SkipsDirectories(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	// Create a subdirectory
	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir"), 0o755))

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
}

func TestLoadSchemaFromDir_Empty(t *testing.T) {
	dir := t.TempDir()

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	assert.Empty(t, tables)
}

func TestLoadSchemaFromDir_InvalidSQL(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "bad.sql", "THIS IS NOT VALID SQL")

	_, err := LoadSchemaFromDir(dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bad.sql")
}

func TestLoadSchemaFromDir_NonexistentDir(t *testing.T) {
	_, err := LoadSchemaFromDir("/nonexistent/path")
	assert.Error(t, err)
}

func TestLoadSchemaFromDir_CaseInsensitiveExtension(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.SQL", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, dir, "orders.Sql", `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	assert.Len(t, tables, 2)
}

// writeFile is a test helper that creates a file with the given content.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644)
	require.NoError(t, err)
}
