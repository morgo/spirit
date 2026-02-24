package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffSchemas_AddColumn(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(100) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	target := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(100) DEFAULT NULL,
			email varchar(255) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	changes, err := diffSchemas(source, target)
	require.NoError(t, err)
	assert.NotEmpty(t, changes)

	// The diff should produce an ALTER TABLE for users
	foundUsersAlter := false
	for _, ch := range changes {
		if ch.Table == "users" {
			foundUsersAlter = true
		}
	}
	assert.True(t, foundUsersAlter, "expected ALTER TABLE for users")
}

func TestDiffSchemas_NewTable(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	target := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE orders (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	changes, err := diffSchemas(source, target)
	require.NoError(t, err)
	assert.NotEmpty(t, changes)

	// Should have a CREATE TABLE for orders
	foundOrdersCreate := false
	for _, ch := range changes {
		if ch.Table == "orders" {
			foundOrdersCreate = true
		}
	}
	assert.True(t, foundOrdersCreate, "expected CREATE TABLE for orders")
}

func TestDiffSchemas_DroppedTable(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE orders (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	target := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	changes, err := diffSchemas(source, target)
	require.NoError(t, err)
	assert.NotEmpty(t, changes)

	// Should have a DROP TABLE for orders
	foundOrdersDrop := false
	for _, ch := range changes {
		if ch.Table == "orders" {
			foundOrdersDrop = true
		}
	}
	assert.True(t, foundOrdersDrop, "expected DROP TABLE for orders")
}

func TestDiffSchemas_NoChanges(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(100) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	target := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(100) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	changes, err := diffSchemas(source, target)
	require.NoError(t, err)
	assert.Empty(t, changes)
}

func TestDiffSchemas_MultipleTables(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(100) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE orders (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			total decimal(10,2) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	target := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(100) DEFAULT NULL,
			email varchar(255) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		`CREATE TABLE orders (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			total decimal(10,2) DEFAULT NULL,
			status varchar(20) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	changes, err := diffSchemas(source, target)
	require.NoError(t, err)
	assert.NotEmpty(t, changes)

	// Both tables should have changes
	changedTables := make(map[string]bool)
	for _, ch := range changes {
		changedTables[ch.Table] = true
	}
	assert.True(t, changedTables["users"])
	assert.True(t, changedTables["orders"])
}

func TestLoadAlterChanges(t *testing.T) {
	changes, err := loadAlterChanges([]string{
		"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		"ALTER TABLE orders ADD INDEX idx_date (created_at)",
	})
	require.NoError(t, err)
	assert.Len(t, changes, 2)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "orders", changes[1].Table)
}

func TestLoadAlterChanges_InvalidSQL(t *testing.T) {
	_, err := loadAlterChanges([]string{"NOT VALID SQL AT ALL"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse ALTER statement")
}

func TestDiffCmd_DirToDirIntegration(t *testing.T) {
	// Create source directory
	sourceDir := t.TempDir()
	writeFile(t, sourceDir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		name varchar(100) DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, sourceDir, "orders.sql", `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		total float DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	// Create target directory with a FK added to orders
	targetDir := t.TempDir()
	writeFile(t, targetDir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		name varchar(100) DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, targetDir, "orders.sql", `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		user_id bigint unsigned NOT NULL,
		total float DEFAULT NULL,
		PRIMARY KEY (id),
		CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	// Load source and target, diff, and run linters
	source, err := LoadSchemaFromDir(sourceDir)
	require.NoError(t, err)

	target, err := LoadSchemaFromDir(targetDir)
	require.NoError(t, err)

	changes, err := diffSchemas(source, target)
	require.NoError(t, err)
	assert.NotEmpty(t, changes)

	// Run linters with LintOnlyChanges=true (only orders is changed)
	violations, err := RunLinters(source, changes, Config{
		LintOnlyChanges: true,
	})
	require.NoError(t, err)

	// Should have violations for orders (has_float, has_fk)
	orderViolations := filterByTable(violations, "orders")
	assert.NotEmpty(t, orderViolations, "expected violations for orders table")

	// Should NOT have violations for users (unchanged)
	userViolations := filterByTable(violations, "users")
	assert.Empty(t, userViolations, "expected no violations for users table")
}

func TestDiffCmd_AlterIntegration(t *testing.T) {
	source := parseCreateTables(t,
		`CREATE TABLE users (
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(100) DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	)

	changes, err := loadAlterChanges([]string{
		"ALTER TABLE users ADD COLUMN balance float DEFAULT NULL",
	})
	require.NoError(t, err)

	violations, err := RunLinters(source, changes, Config{
		LintOnlyChanges: true,
	})
	require.NoError(t, err)

	// Should have a has_float violation for users
	floatViolations := FilterByLinter(violations, "has_float")
	assert.NotEmpty(t, floatViolations, "expected has_float violation")
}

func TestRestoreCreateTable(t *testing.T) {
	ct, err := statement.ParseCreateTable(`CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		name varchar(100) DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	require.NoError(t, err)

	sql, err := restoreCreateTable(ct)
	require.NoError(t, err)
	assert.Contains(t, sql, "CREATE TABLE")
	assert.Contains(t, sql, "users")
}

// filterByTable filters violations to only those for a specific table.
func filterByTable(violations []Violation, table string) []Violation {
	var result []Violation
	for _, v := range violations {
		if v.Location != nil && v.Location.Table == table {
			result = append(result, v)
		}
	}
	return result
}

// parseCreateTables is a test helper that parses multiple CREATE TABLE statements.
func parseCreateTables(t *testing.T, sqls ...string) []*statement.CreateTable {
	t.Helper()
	var tables []*statement.CreateTable
	for _, sql := range sqls {
		ct, err := statement.ParseCreateTable(sql)
		require.NoError(t, err)
		tables = append(tables, ct)
	}
	return tables
}
