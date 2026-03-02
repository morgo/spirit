package lint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLintCmd_BuildConfig_NoIgnoreTables(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	config, err := buildIgnoreTablesConfig("", source)
	require.NoError(t, err)
	assert.False(t, config.LintOnlyChanges)
	assert.Nil(t, config.IgnoreTables)
}

func TestLintCmd_BuildConfig_IgnoreTablesRegex(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE orders (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE order_items (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	config, err := buildIgnoreTablesConfig("^order", source)
	require.NoError(t, err)
	assert.True(t, config.IgnoreTables["orders"])
	assert.True(t, config.IgnoreTables["order_items"])
	assert.False(t, config.IgnoreTables["users"])
}

func TestLintCmd_BuildConfig_IgnoreTablesInvalidRegex(t *testing.T) {
	_, err := buildIgnoreTablesConfig("[invalid", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid --ignore-tables regex")
}

func TestLintCmd_LintEntireSchema(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			balance float DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE orders (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			total float DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	// Lint entire schema — no changes, no LintOnlyChanges
	violations, err := RunLinters(source, nil, Config{})
	require.NoError(t, err)

	// Should have has_float violations for both tables
	userViolations := filterByTable(violations, "users")
	assert.NotEmpty(t, userViolations, "expected violations for users table")

	orderViolations := filterByTable(violations, "orders")
	assert.NotEmpty(t, orderViolations, "expected violations for orders table")
}

func TestLintCmd_LintEntireSchemaFromDir(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		balance float DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	source, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)

	violations, err := RunLinters(source, nil, Config{})
	require.NoError(t, err)

	// Should have has_float violation
	floatViolations := FilterByLinter(violations, "has_float")
	assert.NotEmpty(t, floatViolations, "expected has_float violation")
}

func TestLintCmd_IgnoreTablesFiltersViolations(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			balance float DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE orders (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			total float DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	config, err := buildIgnoreTablesConfig("^users$", source)
	require.NoError(t, err)

	violations, err := RunLinters(source, nil, config)
	require.NoError(t, err)

	// users should be ignored
	userViolations := filterByTable(violations, "users")
	assert.Empty(t, userViolations, "expected no violations for users (ignored)")

	// orders should still have violations
	orderViolations := filterByTable(violations, "orders")
	assert.NotEmpty(t, orderViolations, "expected violations for orders table")
}
