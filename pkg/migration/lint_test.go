package migration

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestLintingBasicInfrastructure tests that linting can be enabled and runs
// without errors on a clean table with no violations.
func TestLintingBasicInfrastructure(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1lint", `CREATE TABLE t1lint (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_name (name)
	)`)
	m := NewTestMigration(t, WithTable("t1lint"), WithAlter("ENGINE=InnoDB"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintInvisibleIndexBeforeDrop_Warning tests the invisible_index_before_drop linter
// with default config (warning, not error).
func TestLintInvisibleIndexBeforeDrop_Warning(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1idx", `CREATE TABLE t1idx (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_name (name)
	)`)
	// Dropping a visible index should trigger a warning (not error by default).
	m := NewTestMigration(t, WithTable("t1idx"), WithAlter("DROP INDEX idx_name"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintInvisibleIndexBeforeDrop_AlreadyInvisible tests that dropping an already-invisible
// index does not trigger the linter.
func TestLintInvisibleIndexBeforeDrop_AlreadyInvisible(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1idxinv", `CREATE TABLE t1idxinv (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_name (name) INVISIBLE
	)`)
	// Dropping an already-invisible index should not trigger the linter.
	m := NewTestMigration(t, WithTable("t1idxinv"), WithAlter("DROP INDEX idx_name"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintPrimaryKeyType_IntWarning tests that INT primary key triggers a warning.
func TestLintPrimaryKeyType_IntWarning(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1pkint", `CREATE TABLE t1pkint (
		id INT NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("t1pkint"), WithAlter("ENGINE=InnoDB"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintPrimaryKeyType_SignedBigintWarning tests that signed BIGINT primary key triggers a warning.
func TestLintPrimaryKeyType_SignedBigintWarning(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1pkbigint", `CREATE TABLE t1pkbigint (
		id BIGINT NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("t1pkbigint"), WithAlter("ENGINE=InnoDB"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintPrimaryKeyType_UnsignedBigintOK tests that BIGINT UNSIGNED primary key is acceptable.
func TestLintPrimaryKeyType_UnsignedBigintOK(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1pkubigint", `CREATE TABLE t1pkubigint (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("t1pkubigint"), WithAlter("ENGINE=InnoDB"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintPrimaryKeyType_BinaryOK tests that binary primary key is acceptable.
func TestLintPrimaryKeyType_VarbinaryOK(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1pkbin", `CREATE TABLE t1pkbin (
		id VARBINARY(16) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO t1pkbin (id, name) VALUES (RANDOM_BYTES(16), 'test')`)
	m := NewTestMigration(t, WithTable("t1pkbin"), WithAlter("ENGINE=InnoDB"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintMultipleAlterTable tests linting with multiple ALTER TABLE statements.
func TestLintMultipleAlterTable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1multi", `CREATE TABLE t1multi (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.NewTestTable(t, "t2multi", `CREATE TABLE t2multi (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	// Multiple ALTER statements on different tables - should not trigger the linter.
	m := NewTestMigration(t, WithLint(),
		WithStatement(`ALTER TABLE t1multi ADD COLUMN c1 INT; ALTER TABLE t2multi ADD COLUMN c2 INT`))
	require.NoError(t, m.Run())
}

// TestLintGetCreateTable tests that getCreateTable retrieves table definition correctly.
func TestLintGetCreateTable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1getcreate", `CREATE TABLE t1getcreate (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_email (email)
	)`)
	m := NewTestMigration(t, WithTable("t1getcreate"), WithAlter("ADD COLUMN age INT"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintCreateTableWithLinting tests linting on CREATE TABLE statements.
func TestLintCreateTableWithLinting(t *testing.T) {
	t.Parallel()
	// Create a table with proper primary key via Statement.
	m := NewTestMigration(t, WithLint(),
		WithStatement(`CREATE TABLE t1createalter (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, name VARCHAR(255))`))
	require.NoError(t, m.Run())
	// Now alter the table with linting enabled.
	m = NewTestMigration(t, WithTable("t1createalter"), WithAlter("ADD COLUMN email VARCHAR(255)"), WithLint())
	require.NoError(t, m.Run())
	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1createalter`)
}

// TestLintNoViolations tests that a clean migration with no violations succeeds.
func TestLintNoViolations(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1clean", `CREATE TABLE t1clean (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("t1clean"), WithAlter("ADD COLUMN email VARCHAR(255)"), WithLint())
	require.NoError(t, m.Run())
}

// TestLintWithoutEnablingLinting tests that linting doesn't run when not enabled.
func TestLintWithoutEnablingLinting(t *testing.T) {
	t.Parallel()
	// Create table with INT primary key but don't enable linting.
	m := NewTestMigration(t, WithStatement(`CREATE TABLE t1nolint (id INT NOT NULL PRIMARY KEY, name VARCHAR(255))`))
	require.NoError(t, m.Run())
	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1nolint`)
}

// TestLintOnly tests that LintOnly runs linters and exits without performing migration.
func TestLintOnly(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1lintonly", `CREATE TABLE t1lintonly (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		c1 varchar(255) NOT NULL,
		c2 varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY c2 (c2)
	)`)

	// LintOnly=true - should run linters and exit without migrating.
	m := NewTestMigration(t, WithTable("t1lintonly"), WithAlter("DROP INDEX c2"), WithLintOnly())
	require.NoError(t, m.Run())

	// Verify that the new table was NOT created (migration didn't run).
	var tableCount int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_t1lintonly_new'`).Scan(&tableCount))
	require.Equal(t, 0, tableCount)

	// Test with LintOnly=true and no violations.
	testutils.NewTestTable(t, "t1lintonly2", `CREATE TABLE t1lintonly2 (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	m = NewTestMigration(t, WithTable("t1lintonly2"), WithAlter("ADD COLUMN email VARCHAR(255)"), WithLintOnly())
	require.NoError(t, m.Run())

	// Verify that the new table was NOT created (migration didn't run).
	var tableCount2 int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_t1lintonly2_new'`).Scan(&tableCount2))
	require.Equal(t, 0, tableCount2)
}

// TestLintOnlyImpliesLint tests that --lint-only implies --lint behavior.
func TestLintOnlyImpliesLint(t *testing.T) {
	t.Parallel()
	// Use LintOnly without Lint - should still run linters.
	m := NewTestMigration(t, WithLintOnly(),
		WithStatement(`CREATE TABLE t1lintimply (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, name VARCHAR(255))`))
	require.NoError(t, m.Run())

	// Verify that the table was NOT created (lint-only should not execute the statement).
	tt := testutils.NewTestTable(t, "lint_helper", `CREATE TABLE lint_helper (id INT PRIMARY KEY)`)
	var tableCount int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 't1lintimply'`).Scan(&tableCount))
	require.Equal(t, 0, tableCount)
}
