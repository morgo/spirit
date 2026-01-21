package repl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractStmt(t *testing.T) {
	tests := []struct {
		name     string
		input    []statement
		expected []string
	}{
		{
			name:     "empty input",
			input:    []statement{},
			expected: nil,
		},
		{
			name: "single statement",
			input: []statement{
				{numKeys: 1, stmt: "DELETE FROM table WHERE id = 1"},
			},
			expected: []string{"DELETE FROM table WHERE id = 1"},
		},
		{
			name: "multiple statements",
			input: []statement{
				{numKeys: 2, stmt: "DELETE FROM table WHERE id IN (1,2)"},
				{numKeys: 1, stmt: "INSERT INTO table VALUES (3)"},
			},
			expected: []string{
				"DELETE FROM table WHERE id IN (1,2)",
				"INSERT INTO table VALUES (3)",
			},
		},
		{
			name: "skip empty statements",
			input: []statement{
				{numKeys: 1, stmt: "DELETE FROM table WHERE id = 1"},
				{numKeys: 0, stmt: ""},
				{numKeys: 1, stmt: "INSERT INTO table VALUES (2)"},
			},
			expected: []string{
				"DELETE FROM table WHERE id = 1",
				"INSERT INTO table VALUES (2)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractStmt(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEncodeSchemaTable(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "basic case",
			schema:   "test",
			table:    "users",
			expected: "test.users",
		},
		{
			name:     "schema with underscore",
			schema:   "test_db",
			table:    "users",
			expected: "test_db.users",
		},
		{
			name:     "table with underscore",
			schema:   "test",
			table:    "user_data",
			expected: "test.user_data",
		},
		{
			name:     "empty schema",
			schema:   "",
			table:    "users",
			expected: ".users",
		},
		{
			name:     "empty table",
			schema:   "test",
			table:    "",
			expected: "test.",
		},
		{
			name:     "both empty",
			schema:   "",
			table:    "",
			expected: ".",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EncodeSchemaTable(tt.schema, tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeSchemaTable(t *testing.T) {
	tests := []struct {
		name           string
		encoded        string
		expectedSchema string
		expectedTable  string
	}{
		{
			name:           "basic case",
			encoded:        "test.users",
			expectedSchema: "test",
			expectedTable:  "users",
		},
		{
			name:           "schema with underscore",
			encoded:        "test_db.users",
			expectedSchema: "test_db",
			expectedTable:  "users",
		},
		{
			name:           "table with underscore",
			encoded:        "test.user_data",
			expectedSchema: "test",
			expectedTable:  "user_data",
		},
		{
			name:           "both with underscores",
			encoded:        "my_schema.my_table",
			expectedSchema: "my_schema",
			expectedTable:  "my_table",
		},
		{
			name:           "empty schema",
			encoded:        ".users",
			expectedSchema: "",
			expectedTable:  "users",
		},
		{
			name:           "empty table",
			encoded:        "test.",
			expectedSchema: "test",
			expectedTable:  "",
		},
		{
			name:           "both empty",
			encoded:        ".",
			expectedSchema: "",
			expectedTable:  "",
		},
		{
			name:           "no separator - returns empty strings",
			encoded:        "users",
			expectedSchema: "",
			expectedTable:  "",
		},
		{
			name:           "empty string - returns empty strings",
			encoded:        "",
			expectedSchema: "",
			expectedTable:  "",
		},
		{
			name:           "multiple dots - only splits on first",
			encoded:        "schema.table.with.dots",
			expectedSchema: "schema",
			expectedTable:  "table.with.dots",
		},
		{
			name:           "schema with hyphen",
			encoded:        "my-schema.users",
			expectedSchema: "my-schema",
			expectedTable:  "users",
		},
		{
			name:           "table with hyphen",
			encoded:        "test.my-table",
			expectedSchema: "test",
			expectedTable:  "my-table",
		},
		{
			name:           "numeric schema and table",
			encoded:        "db123.table456",
			expectedSchema: "db123",
			expectedTable:  "table456",
		},
		{
			name:           "special characters",
			encoded:        "schema$1.table#2",
			expectedSchema: "schema$1",
			expectedTable:  "table#2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, table := DecodeSchemaTable(tt.encoded)
			assert.Equal(t, tt.expectedSchema, schema, "schema mismatch")
			assert.Equal(t, tt.expectedTable, table, "table mismatch")
		})
	}
}

func TestEncodeDecodeSchemaTableRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
	}{
		{
			name:   "basic case",
			schema: "test",
			table:  "users",
		},
		{
			name:   "with underscores",
			schema: "test_db",
			table:  "user_data",
		},
		{
			name:   "empty schema",
			schema: "",
			table:  "users",
		},
		{
			name:   "empty table",
			schema: "test",
			table:  "",
		},
		{
			name:   "both empty",
			schema: "",
			table:  "",
		},
		{
			name:   "with hyphens",
			schema: "my-schema",
			table:  "my-table",
		},
		{
			name:   "numeric names",
			schema: "db123",
			table:  "table456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeSchemaTable(tt.schema, tt.table)
			schema, table := DecodeSchemaTable(encoded)
			assert.Equal(t, tt.schema, schema, "schema should match after round trip")
			assert.Equal(t, tt.table, table, "table should match after round trip")
		})
	}
}

func TestExtractTablesFromDDLStmts(t *testing.T) {
	tests := []struct {
		name          string
		defaultSchema string
		statement     string
		want          []string
		wantErr       bool
	}{
		{
			name:          "create table",
			defaultSchema: "test",
			statement:     "CREATE TABLE users (id INT PRIMARY KEY)",
			want:          []string{"test.users"},
		},
		{
			name:          "create table with schema",
			defaultSchema: "test",
			statement:     "CREATE TABLE mydb.users (id INT PRIMARY KEY)",
			want:          []string{"mydb.users"},
		},
		{
			name:          "drop table",
			defaultSchema: "test",
			statement:     "DROP TABLE users",
			want:          []string{"test.users"},
		},
		{
			name:          "drop multiple tables",
			defaultSchema: "test",
			statement:     "DROP TABLE users, orders",
			want:          []string{"test.users", "test.orders"},
		},
		{
			name:          "alter table",
			defaultSchema: "test",
			statement:     "ALTER TABLE users ADD COLUMN age INT",
			want:          []string{"test.users"},
		},
		{
			name:          "rename table",
			defaultSchema: "test",
			statement:     "RENAME TABLE users TO new_users",
			want:          []string{"test.users"},
		},
		{
			name:          "truncate table",
			defaultSchema: "test",
			statement:     "TRUNCATE TABLE users",
			want:          []string{"test.users"},
		},
		{
			name:          "create index",
			defaultSchema: "test",
			statement:     "CREATE INDEX idx_name ON users (name)",
			want:          []string{"test.users"},
		},
		{
			name:          "drop index",
			defaultSchema: "test",
			statement:     "DROP INDEX idx_name ON users",
			want:          []string{"test.users"},
		},
		{
			name:          "multiple statements",
			defaultSchema: "test",
			statement:     "CREATE TABLE users (id INT); CREATE INDEX idx_name ON users (name);",
			want:          []string{"test.users", "test.users"},
		},
		{
			name:          "invalid statement",
			defaultSchema: "test",
			statement:     "INVALID SQL",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractTablesFromDDLStmts(tt.defaultSchema, tt.statement)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestExtractTablesFromDDLStmtsComplex tests more complex DDL statements
func TestExtractTablesFromDDLStmtsComplex(t *testing.T) {
	tests := []struct {
		name          string
		defaultSchema string
		statement     string
		want          []string
		wantErr       bool
	}{
		{
			name:          "create table with complex columns",
			defaultSchema: "test",
			statement: `CREATE TABLE users (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				email VARCHAR(255) UNIQUE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
			)`,
			want: []string{"test.users"},
		},
		{
			name:          "alter table with multiple changes",
			defaultSchema: "test",
			statement: `ALTER TABLE users 
				ADD COLUMN age INT,
				DROP COLUMN old_field,
				MODIFY email VARCHAR(320),
				ADD INDEX idx_age (age),
				DROP INDEX idx_old`,
			want: []string{"test.users"},
		},
		{
			name:          "create table with foreign keys",
			defaultSchema: "test",
			statement: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				FOREIGN KEY (user_id) REFERENCES users(id)
			)`,
			want: []string{"test.orders"},
		},
		{
			name:          "multiple schema references",
			defaultSchema: "test",
			statement: `CREATE TABLE shop.orders (
				id INT PRIMARY KEY,
				user_id INT,
				FOREIGN KEY (user_id) REFERENCES auth.users(id)
			)`,
			want: []string{"shop.orders"},
		},
		{
			name:          "partition definition",
			defaultSchema: "test",
			statement: `CREATE TABLE sales (
				id INT,
				amount DECIMAL(10,2),
				sale_date DATE
			)
			PARTITION BY RANGE (YEAR(sale_date)) (
				PARTITION p0 VALUES LESS THAN (2020),
				PARTITION p1 VALUES LESS THAN (2021),
				PARTITION p2 VALUES LESS THAN (2022)
			)`,
			want: []string{"test.sales"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractTablesFromDDLStmts(tt.defaultSchema, tt.statement)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
