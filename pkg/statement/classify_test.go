package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantType  StatementType
		wantTable string
		wantDDL   bool
		wantDML   bool
	}{
		{
			name:      "ALTER TABLE",
			sql:       "ALTER TABLE t1 ADD COLUMN foo INT",
			wantType:  StatementAlterTable,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "CREATE TABLE",
			sql:       "CREATE TABLE t1 (id INT PRIMARY KEY)",
			wantType:  StatementCreateTable,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "DROP TABLE",
			sql:       "DROP TABLE t1",
			wantType:  StatementDropTable,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "DROP TABLE IF EXISTS",
			sql:       "DROP TABLE IF EXISTS t1",
			wantType:  StatementDropTable,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "RENAME TABLE",
			sql:       "RENAME TABLE t1 TO t2",
			wantType:  StatementRenameTable,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "TRUNCATE TABLE",
			sql:       "TRUNCATE TABLE t1",
			wantType:  StatementTruncateTable,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "CREATE INDEX",
			sql:       "CREATE INDEX idx ON t1 (col1)",
			wantType:  StatementCreateIndex,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "DROP INDEX",
			sql:       "DROP INDEX idx ON t1",
			wantType:  StatementDropIndex,
			wantTable: "t1",
			wantDDL:   true,
		},
		{
			name:      "CREATE VIEW",
			sql:       "CREATE VIEW v1 AS SELECT 1",
			wantType:  StatementCreateView,
			wantTable: "v1",
			wantDDL:   true,
		},
		{
			name:      "CREATE OR REPLACE VIEW",
			sql:       "CREATE OR REPLACE VIEW v1 AS SELECT 1",
			wantType:  StatementCreateView,
			wantTable: "v1",
			wantDDL:   true,
		},
		{
			name:      "CREATE VIEW with definer",
			sql:       "CREATE DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW v1 AS SELECT 1",
			wantType:  StatementCreateView,
			wantTable: "v1",
			wantDDL:   true,
		},
		{
			name:      "INSERT",
			sql:       "INSERT INTO t1 (a) VALUES (1)",
			wantType:  StatementInsert,
			wantTable: "t1",
			wantDML:   true,
		},
		{
			name:      "UPDATE",
			sql:       "UPDATE t1 SET a = 1 WHERE id = 1",
			wantType:  StatementUpdate,
			wantTable: "t1",
			wantDML:   true,
		},
		{
			name:      "DELETE",
			sql:       "DELETE FROM t1 WHERE id = 1",
			wantType:  StatementDelete,
			wantTable: "t1",
			wantDML:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := Classify(tt.sql)
			require.NoError(t, err)
			require.Len(t, results, 1)

			c := results[0]
			assert.Equal(t, tt.wantType, c.Type)
			assert.Equal(t, tt.wantTable, c.Table)
			assert.Equal(t, tt.wantDDL, c.Type.IsDDL())
			assert.Equal(t, tt.wantDML, c.Type.IsDML())
		})
	}
}

func TestClassifySchema(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantType   StatementType
		wantSchema string
		wantTable  string
	}{
		{
			name:       "ALTER TABLE",
			sql:        "ALTER TABLE test.t1 ADD COLUMN foo INT",
			wantType:   StatementAlterTable,
			wantSchema: "test",
			wantTable:  "t1",
		},
		{
			name:       "DROP TABLE",
			sql:        "DROP TABLE mydb.users",
			wantType:   StatementDropTable,
			wantSchema: "mydb",
			wantTable:  "users",
		},
		{
			name:       "CREATE TABLE",
			sql:        "CREATE TABLE mydb.orders (id INT PRIMARY KEY)",
			wantType:   StatementCreateTable,
			wantSchema: "mydb",
			wantTable:  "orders",
		},
		{
			name:       "RENAME TABLE",
			sql:        "RENAME TABLE mydb.t1 TO mydb.t2",
			wantType:   StatementRenameTable,
			wantSchema: "mydb",
			wantTable:  "t1",
		},
		{
			name:       "TRUNCATE TABLE",
			sql:        "TRUNCATE TABLE mydb.t1",
			wantType:   StatementTruncateTable,
			wantSchema: "mydb",
			wantTable:  "t1",
		},
		{
			name:       "CREATE INDEX",
			sql:        "CREATE INDEX idx ON mydb.t1 (col1)",
			wantType:   StatementCreateIndex,
			wantSchema: "mydb",
			wantTable:  "t1",
		},
		{
			name:       "DROP INDEX",
			sql:        "DROP INDEX idx ON mydb.t1",
			wantType:   StatementDropIndex,
			wantSchema: "mydb",
			wantTable:  "t1",
		},
		{
			name:       "CREATE VIEW",
			sql:        "CREATE VIEW mydb.v1 AS SELECT 1",
			wantType:   StatementCreateView,
			wantSchema: "mydb",
			wantTable:  "v1",
		},
		{
			name:       "INSERT",
			sql:        "INSERT INTO mydb.t1 (a) VALUES (1)",
			wantType:   StatementInsert,
			wantSchema: "mydb",
			wantTable:  "t1",
		},
		{
			name:       "UPDATE",
			sql:        "UPDATE mydb.t1 SET a = 1",
			wantType:   StatementUpdate,
			wantSchema: "mydb",
			wantTable:  "t1",
		},
		{
			name:       "DELETE",
			sql:        "DELETE FROM mydb.t1 WHERE id = 1",
			wantType:   StatementDelete,
			wantSchema: "mydb",
			wantTable:  "t1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := Classify(tt.sql)
			require.NoError(t, err)
			require.Len(t, results, 1)
			assert.Equal(t, tt.wantType, results[0].Type)
			assert.Equal(t, tt.wantSchema, results[0].Schema)
			assert.Equal(t, tt.wantTable, results[0].Table)
		})
	}
}

func TestClassifyMultiStatement(t *testing.T) {
	results, err := Classify("ALTER TABLE t1 ADD COLUMN a INT; ALTER TABLE t2 ADD COLUMN b INT")
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, StatementAlterTable, results[0].Type)
	assert.Equal(t, "t1", results[0].Table)
	assert.Equal(t, StatementAlterTable, results[1].Type)
	assert.Equal(t, "t2", results[1].Table)
}

func TestClassifyMixed(t *testing.T) {
	results, err := Classify("INSERT INTO t1 (a) VALUES (1); ALTER TABLE t2 ADD COLUMN b INT")
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, StatementInsert, results[0].Type)
	assert.Equal(t, "t1", results[0].Table)
	assert.True(t, results[0].Type.IsDML())
	assert.Equal(t, StatementAlterTable, results[1].Type)
	assert.Equal(t, "t2", results[1].Table)
	assert.True(t, results[1].Type.IsDDL())
}

func TestClassifyDropMultipleTables(t *testing.T) {
	// DROP TABLE with multiple tables returns the first table
	results, err := Classify("DROP TABLE t1, t2, t3")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, StatementDropTable, results[0].Type)
	assert.Equal(t, "t1", results[0].Table)
}

func TestClassifyTruncateShorthand(t *testing.T) {
	// TRUNCATE without TABLE keyword
	results, err := Classify("TRUNCATE t1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, StatementTruncateTable, results[0].Type)
	assert.Equal(t, "t1", results[0].Table)
}

func TestClassifyUnknown(t *testing.T) {
	// SELECT parses but isn't a DDL/DML we classify
	results, err := Classify("SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, StatementUnknown, results[0].Type)
	assert.False(t, results[0].Type.IsDDL())
	assert.False(t, results[0].Type.IsDML())
	assert.Equal(t, "UNKNOWN", results[0].Type.String())
}

func TestClassifyInvalid(t *testing.T) {
	_, err := Classify("NOT VALID SQL HERE !@#$")
	require.Error(t, err)

	_, err = Classify("-- just a comment")
	require.ErrorIs(t, err, ErrNoStatements)

	_, err = Classify("")
	require.ErrorIs(t, err, ErrNoStatements)
}

func TestStatementTypeString(t *testing.T) {
	assert.Equal(t, "ALTER TABLE", StatementAlterTable.String())
	assert.Equal(t, "CREATE TABLE", StatementCreateTable.String())
	assert.Equal(t, "DROP TABLE", StatementDropTable.String())
	assert.Equal(t, "RENAME TABLE", StatementRenameTable.String())
	assert.Equal(t, "TRUNCATE TABLE", StatementTruncateTable.String())
	assert.Equal(t, "CREATE INDEX", StatementCreateIndex.String())
	assert.Equal(t, "DROP INDEX", StatementDropIndex.String())
	assert.Equal(t, "CREATE VIEW", StatementCreateView.String())
	assert.Equal(t, "INSERT", StatementInsert.String())
	assert.Equal(t, "UPDATE", StatementUpdate.String())
	assert.Equal(t, "DELETE", StatementDelete.String())
	assert.Equal(t, "UNKNOWN", StatementUnknown.String())
}
