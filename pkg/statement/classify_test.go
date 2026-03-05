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
	results, err := Classify("ALTER TABLE test.t1 ADD COLUMN foo INT")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, StatementAlterTable, results[0].Type)
	assert.Equal(t, "test", results[0].Schema)
	assert.Equal(t, "t1", results[0].Table)

	results, err = Classify("DROP TABLE mydb.users")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, StatementDropTable, results[0].Type)
	assert.Equal(t, "mydb", results[0].Schema)
	assert.Equal(t, "users", results[0].Table)
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
	// DML mixed with DDL
	results, err := Classify("INSERT INTO t1 (a) VALUES (1); ALTER TABLE t2 ADD COLUMN b INT")
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Type.IsDML())
	assert.True(t, results[1].Type.IsDDL())
}

func TestClassifyInvalid(t *testing.T) {
	_, err := Classify("NOT VALID SQL HERE !@#$")
	assert.Error(t, err)

	_, err = Classify("-- just a comment")
	assert.ErrorIs(t, err, ErrNoStatements)
}

func TestStatementTypeString(t *testing.T) {
	assert.Equal(t, "ALTER TABLE", StatementAlterTable.String())
	assert.Equal(t, "CREATE TABLE", StatementCreateTable.String())
	assert.Equal(t, "DROP TABLE", StatementDropTable.String())
	assert.Equal(t, "RENAME TABLE", StatementRenameTable.String())
	assert.Equal(t, "TRUNCATE TABLE", StatementTruncateTable.String())
	assert.Equal(t, "CREATE INDEX", StatementCreateIndex.String())
	assert.Equal(t, "INSERT", StatementInsert.String())
	assert.Equal(t, "UPDATE", StatementUpdate.String())
	assert.Equal(t, "DELETE", StatementDelete.String())
	assert.Equal(t, "UNKNOWN", StatementUnknown.String())
}
