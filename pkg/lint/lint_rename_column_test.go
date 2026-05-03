package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestRenameColumnLinter_NoRename(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_RenameColumn(t *testing.T) {
	sql := `ALTER TABLE users RENAME COLUMN old_name TO new_name`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, "rename_column", violations[0].Linter.Name())
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Contains(t, violations[0].Message, "old_name")
	require.Contains(t, violations[0].Message, "new_name")
	require.Contains(t, violations[0].Message, "users")
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "old_name", *violations[0].Location.Column)
	require.NotNil(t, violations[0].Suggestion)
}

func TestRenameColumnLinter_ChangeColumnWithRename(t *testing.T) {
	sql := `ALTER TABLE users CHANGE COLUMN old_col new_col INT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, "rename_column", violations[0].Linter.Name())
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Contains(t, violations[0].Message, "old_col")
	require.Contains(t, violations[0].Message, "new_col")
	require.Contains(t, violations[0].Message, "CHANGE COLUMN")
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "old_col", *violations[0].Location.Column)
}

func TestRenameColumnLinter_ChangeColumnSameName(t *testing.T) {
	// CHANGE COLUMN with same name is just a type change, not a rename
	sql := `ALTER TABLE users CHANGE COLUMN name name VARCHAR(512)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_ModifyColumnNoRename(t *testing.T) {
	// MODIFY COLUMN only changes type, never renames
	sql := `ALTER TABLE users MODIFY COLUMN name VARCHAR(512)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_MultipleRenames(t *testing.T) {
	sql := `ALTER TABLE users 
		RENAME COLUMN a TO b,
		RENAME COLUMN c TO d`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 2)
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Equal(t, SeverityWarning, violations[1].Severity)
}

func TestRenameColumnLinter_MixedOperations(t *testing.T) {
	// Only the rename should be flagged, not the add or modify
	sql := `ALTER TABLE users 
		ADD COLUMN email VARCHAR(255),
		RENAME COLUMN old_name TO new_name,
		MODIFY COLUMN age BIGINT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "old_name")
	require.Contains(t, violations[0].Message, "new_name")
}

func TestRenameColumnLinter_NonAlterStatement(t *testing.T) {
	// CREATE TABLE should not trigger any violations
	sql := `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_DropColumn(t *testing.T) {
	sql := `ALTER TABLE users DROP COLUMN old_col`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_Name(t *testing.T) {
	linter := &RenameColumnLinter{}
	require.Equal(t, "rename_column", linter.Name())
}

func TestRenameColumnLinter_Description(t *testing.T) {
	linter := &RenameColumnLinter{}
	require.NotEmpty(t, linter.Description())
}

func TestRenameColumnLinter_String(t *testing.T) {
	linter := &RenameColumnLinter{}
	require.Contains(t, linter.String(), "rename_column")
}
