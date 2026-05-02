package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

// mustTableSchemas is a test helper that converts CreateTable objects to TableSchemas.
func mustTableSchemas(t *testing.T, tables []*statement.CreateTable) []table.TableSchema {
	t.Helper()
	schemas, err := createTablesToTableSchemas(tables)
	require.NoError(t, err)
	return schemas
}

func TestDiff_AddColumn(t *testing.T) {
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

	plan, err := PlanChanges(mustTableSchemas(t, source), mustTableSchemas(t, target), nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Equal(t, "users", plan.Changes[0].TableName)
	require.Contains(t, plan.Changes[0].Statement, "ALTER TABLE")
}

func TestDiff_NewTable(t *testing.T) {
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

	plan, err := PlanChanges(mustTableSchemas(t, source), mustTableSchemas(t, target), nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Equal(t, "orders", plan.Changes[0].TableName)
	require.Contains(t, plan.Changes[0].Statement, "CREATE TABLE")
}

func TestDiff_DroppedTable(t *testing.T) {
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

	plan, err := PlanChanges(mustTableSchemas(t, source), mustTableSchemas(t, target), nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Equal(t, "orders", plan.Changes[0].TableName)
	require.Contains(t, plan.Changes[0].Statement, "DROP TABLE")
}

func TestDiff_NoChanges(t *testing.T) {
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

	plan, err := PlanChanges(mustTableSchemas(t, source), mustTableSchemas(t, target), nil, nil)
	require.NoError(t, err)
	require.False(t, plan.HasChanges())
}

func TestDiff_MultipleTables(t *testing.T) {
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

	plan, err := PlanChanges(mustTableSchemas(t, source), mustTableSchemas(t, target), nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())

	// Both tables should have changes
	changedTables := make(map[string]bool)
	for _, ch := range plan.Changes {
		changedTables[ch.TableName] = true
	}
	require.True(t, changedTables["users"])
	require.True(t, changedTables["orders"])
}

func TestLoadAlterChanges(t *testing.T) {
	changes, err := loadAlterChanges([]string{
		"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		"ALTER TABLE orders ADD INDEX idx_date (created_at)",
	})
	require.NoError(t, err)
	require.Len(t, changes, 2)
	require.Equal(t, "users", changes[0].Table)
	require.Contains(t, changes[0].Statement, "ALTER TABLE", "expected ALTER TABLE statement for users")
	require.Equal(t, "orders", changes[1].Table)
	require.Contains(t, changes[1].Statement, "ALTER TABLE", "expected ALTER TABLE statement for orders")
}

func TestLoadAlterChanges_InvalidSQL(t *testing.T) {
	_, err := loadAlterChanges([]string{"NOT VALID SQL AT ALL"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse ALTER statement")
}

func TestDiff_DirToDirIntegration(t *testing.T) {
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

	// Load source and target, then PlanChanges
	source, err := LoadSchemaFromDir(sourceDir)
	require.NoError(t, err)

	target, err := LoadSchemaFromDir(targetDir)
	require.NoError(t, err)

	sourceSchemas := mustTableSchemas(t, source)
	targetSchemas := mustTableSchemas(t, target)

	plan, err := PlanChanges(sourceSchemas, targetSchemas, nil, &Config{LintOnlyChanges: true})
	require.NoError(t, err)
	require.True(t, plan.HasChanges())

	// Should have warnings for orders (has_float, has_fk)
	var ordersChange *PlannedChange
	for i := range plan.Changes {
		if plan.Changes[i].TableName == "orders" {
			ordersChange = &plan.Changes[i]
			break
		}
	}
	require.NotNil(t, ordersChange, "expected a change for orders table")
	require.NotEmpty(t, ordersChange.Warnings(), "expected warnings for orders table (has_float, has_fk)")

	// Should NOT have changes for users (unchanged)
	for _, ch := range plan.Changes {
		require.NotEqual(t, "users", ch.TableName, "users table is unchanged, should not appear in plan")
	}
}

func TestDiff_AlterIntegration(t *testing.T) {
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

	// The alter path uses RunLinters directly (no declarative target).
	violations, err := RunLinters(source, changes, Config{
		LintOnlyChanges: true,
	})
	require.NoError(t, err)

	// Should have a has_float violation for users
	floatViolations := FilterByLinter(violations, "has_float")
	require.NotEmpty(t, floatViolations, "expected has_float violation")
}

// filterByTable filters violations to only those for a specific table.
func filterByTable(violations []Violation, tableName string) []Violation {
	var result []Violation
	for _, v := range violations {
		if v.Location != nil && v.Location.Table == tableName {
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
