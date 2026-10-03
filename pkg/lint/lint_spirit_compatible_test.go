package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// lintSpiritCompatible runs the linter over stmts as the incoming changes, with
// the given existing table definitions.
func lintSpiritCompatible(t *testing.T, existing []string, stmts ...string) []Violation {
	t.Helper()
	var tables []*statement.CreateTable
	for _, def := range existing {
		ct, err := statement.ParseCreateTable(def)
		require.NoError(t, err)
		tables = append(tables, ct)
	}
	var changes []*statement.AbstractStatement
	for _, sql := range stmts {
		parsed, err := statement.New(sql)
		require.NoError(t, err)
		changes = append(changes, parsed...)
	}
	return (&SpiritCompatibleLinter{}).Lint(tables, changes)
}

func TestSpiritCompatible_Registered(t *testing.T) {
	l := registeredLinter(t, "spirit_compatible")
	require.IsType(t, &SpiritCompatibleLinter{}, l)
}

func TestSpiritCompatible_CompatibleTable(t *testing.T) {
	for _, sql := range []string{
		`CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))`,
		`CREATE TABLE t1 (a INT NOT NULL, b VARCHAR(64) NOT NULL, PRIMARY KEY (a, b))`,
		`CREATE TABLE t1 (id VARBINARY(16) NOT NULL PRIMARY KEY)`,
		// DOUBLE compares equal to its text form; only FLOAT does not.
		`CREATE TABLE t1 (id DOUBLE NOT NULL PRIMARY KEY)`,
		`CREATE TABLE mydb.t1 (id BIGINT NOT NULL PRIMARY KEY)`,
	} {
		t.Run(sql, func(t *testing.T) {
			require.Empty(t, lintSpiritCompatible(t, nil, sql))
		})
	}
}

func TestSpiritCompatible_NoPrimaryKey(t *testing.T) {
	violations := lintSpiritCompatible(t, nil, `CREATE TABLE t1 (id BIGINT, name VARCHAR(255))`)
	require.Len(t, violations, 1)
	v := violations[0]
	require.Equal(t, "spirit_compatible", v.Linter.Name())
	require.Equal(t, SeverityError, v.Severity)
	require.Equal(t, "t1", v.Location.Table)
	require.Contains(t, v.Message, "no primary key")
	require.NotNil(t, v.Suggestion)
}

// TestSpiritCompatible_FixedLaterInChanges verifies the post-state is checked:
// an ALTER in the same changes that fixes the new table clears the violation.
func TestSpiritCompatible_FixedLaterInChanges(t *testing.T) {
	violations := lintSpiritCompatible(t, nil, `CREATE TABLE t1 (id BIGINT NOT NULL, parent_id BIGINT,
		CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent (id))`,
		`ALTER TABLE t1 ADD PRIMARY KEY (id), DROP FOREIGN KEY fk_parent`)
	require.Empty(t, violations)
}

func TestSpiritCompatible_PrimaryKeyType(t *testing.T) {
	tests := []struct {
		sql    string
		column string
		typ    string
	}{
		{`CREATE TABLE t1 (id FLOAT NOT NULL PRIMARY KEY)`, "id", "FLOAT"},
		{`CREATE TABLE t1 (id BIT(8) NOT NULL PRIMARY KEY)`, "id", "BIT"},
		{`CREATE TABLE t1 (a BIGINT NOT NULL, b BIT(1) NOT NULL, PRIMARY KEY (a, b))`, "b", "BIT"},
		// The key may spell the column in a different case to its definition.
		{`CREATE TABLE t1 (Val FLOAT NOT NULL, PRIMARY KEY (val))`, "Val", "FLOAT"},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			violations := lintSpiritCompatible(t, nil, tt.sql)
			require.Len(t, violations, 1)
			v := violations[0]
			require.Equal(t, SeverityError, v.Severity)
			require.Contains(t, v.Message, tt.typ)
			require.NotNil(t, v.Location.Column)
			require.Equal(t, tt.column, *v.Location.Column)
		})
	}
}

// TestSpiritCompatible_PrimaryKeyTypeModifiedLater verifies a MODIFY in the
// same changes that turns a key column into a FLOAT is caught.
func TestSpiritCompatible_PrimaryKeyTypeModifiedLater(t *testing.T) {
	violations := lintSpiritCompatible(t, nil, `CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY)`,
		`ALTER TABLE t1 MODIFY id FLOAT NOT NULL`)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "FLOAT")
}

func TestSpiritCompatible_ForeignKey(t *testing.T) {
	violations := lintSpiritCompatible(t, nil, `CREATE TABLE child (
		id BIGINT NOT NULL PRIMARY KEY,
		parent_id BIGINT,
		CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent (id)
	)`)
	require.Len(t, violations, 1)
	v := violations[0]
	require.Equal(t, SeverityError, v.Severity)
	require.Contains(t, v.Message, "fk_child_parent")
	// The parent can no longer be altered by Spirit either.
	require.Contains(t, v.Message, `"parent"`)
	require.NotNil(t, v.Location.Constraint)
	require.Equal(t, "fk_child_parent", *v.Location.Constraint)
}

func TestSpiritCompatible_UnnamedForeignKey(t *testing.T) {
	violations := lintSpiritCompatible(t, nil, `CREATE TABLE child (
		id BIGINT NOT NULL PRIMARY KEY,
		parent_id BIGINT,
		FOREIGN KEY (parent_id) REFERENCES parent (id)
	)`)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "has a FOREIGN KEY constraint")
	require.NotContains(t, violations[0].Message, `""`)
	require.Equal(t, "child", violations[0].Location.Table)
}

// TestSpiritCompatible_InlineReferences verifies an inline REFERENCES, which
// MySQL 9.0 and later turn into a foreign key, is reported once.
func TestSpiritCompatible_InlineReferences(t *testing.T) {
	violations := lintSpiritCompatible(t, nil, `CREATE TABLE child (
		id BIGINT NOT NULL PRIMARY KEY,
		parent_id BIGINT REFERENCES parent (id)
	)`)
	require.Len(t, violations, 1)
	v := violations[0]
	require.Equal(t, SeverityError, v.Severity)
	require.Contains(t, v.Message, "inline REFERENCES")
	require.Contains(t, v.Message, `"parent"`)
	require.NotNil(t, v.Location.Column)
	require.Equal(t, "parent_id", *v.Location.Column)
}

func TestSpiritCompatible_UnsupportedIdentifier(t *testing.T) {
	tests := []struct {
		sql  string
		kind string
	}{
		{"CREATE TABLE `a.b` (id BIGINT NOT NULL PRIMARY KEY)", "table name"},
		{"CREATE TABLE `a``b` (id BIGINT NOT NULL PRIMARY KEY)", "table name"},
		{"CREATE TABLE `my.db`.t1 (id BIGINT NOT NULL PRIMARY KEY)", "schema name"},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			violations := lintSpiritCompatible(t, nil, tt.sql)
			require.Len(t, violations, 1)
			require.Equal(t, SeverityError, violations[0].Severity)
			require.Contains(t, violations[0].Message, tt.kind)
		})
	}
}

// TestSpiritCompatible_ReportsEveryProblem verifies one table with several
// problems gets one violation per problem.
func TestSpiritCompatible_ReportsEveryProblem(t *testing.T) {
	violations := lintSpiritCompatible(t, nil, "CREATE TABLE `a.b` (parent_id BIGINT, "+
		"CONSTRAINT fk1 FOREIGN KEY (parent_id) REFERENCES parent (id))")
	require.Len(t, violations, 3)
}

// TestSpiritCompatible_ExistingTablesIgnored verifies that a legacy table
// Spirit cannot alter does not block unrelated changes.
func TestSpiritCompatible_ExistingTablesIgnored(t *testing.T) {
	existing := []string{
		`CREATE TABLE legacy (id BIGINT, parent_id BIGINT, CONSTRAINT fk1 FOREIGN KEY (parent_id) REFERENCES parent (id))`,
	}
	require.Empty(t, lintSpiritCompatible(t, existing))
	require.Empty(t, lintSpiritCompatible(t, existing, `ALTER TABLE legacy ADD COLUMN c INT`))
}

func TestSpiritCompatible_TemporaryTableIgnored(t *testing.T) {
	require.Empty(t, lintSpiritCompatible(t, nil, `CREATE TEMPORARY TABLE tmp (id BIGINT)`))
}
