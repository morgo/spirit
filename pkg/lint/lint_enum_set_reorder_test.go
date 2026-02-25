package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create existing table with enum/set columns
func createTableWithEnum(t *testing.T, sql string) *statement.CreateTable {
	t.Helper()
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)
	return ct
}

func TestEnumSetReorderLinter_AppendNewValueSafe(t *testing.T) {
	// Adding new values at the end is safe
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive')
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN status ENUM('active', 'inactive', 'pending')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_ReorderExistingValues(t *testing.T) {
	// Swapping existing values changes ordinals — this is unsafe
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending')
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN status ENUM('inactive', 'active', 'pending')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "enum_set_reorder", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "status")
	assert.Contains(t, violations[0].Message, "t1")
	assert.Contains(t, violations[0].Message, "ENUM")
	assert.Contains(t, violations[0].Message, "moved from position")
	assert.NotNil(t, violations[0].Suggestion)
}

func TestEnumSetReorderLinter_InsertInMiddle(t *testing.T) {
	// Inserting a new value in the middle shifts ordinals of existing values
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive')
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN status ENUM('active', 'pending', 'inactive')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "inactive")
}

func TestEnumSetReorderLinter_RemoveValue(t *testing.T) {
	// Removing a value changes ordinals of subsequent values
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending')
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN status ENUM('active', 'pending')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "removed")
}

func TestEnumSetReorderLinter_SetReorder(t *testing.T) {
	// SET types have the same ordinal issue
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		permissions SET('read', 'write', 'execute')
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN permissions SET('write', 'read', 'execute')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "SET")
	assert.Contains(t, violations[0].Message, "permissions")
}

func TestEnumSetReorderLinter_SetAppendSafe(t *testing.T) {
	// Appending to SET is safe
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		permissions SET('read', 'write')
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN permissions SET('read', 'write', 'execute')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_ChangeColumnReorder(t *testing.T) {
	// CHANGE COLUMN with reordered enum values
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive')
	)`)

	sql := `ALTER TABLE t1 CHANGE COLUMN status status ENUM('inactive', 'active')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestEnumSetReorderLinter_ChangeColumnRenameAndReorder(t *testing.T) {
	// CHANGE COLUMN that renames and reorders — should detect the reorder
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		old_status ENUM('active', 'inactive')
	)`)

	sql := `ALTER TABLE t1 CHANGE COLUMN old_status new_status ENUM('inactive', 'active')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	// The violation should reference the new column name
	assert.Contains(t, violations[0].Message, "new_status")
}

func TestEnumSetReorderLinter_NoExistingTable(t *testing.T) {
	// Without existing table info, we can't compare — no violation
	sql := `ALTER TABLE t1 MODIFY COLUMN status ENUM('inactive', 'active')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint(nil, stmts)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_NonEnumColumn(t *testing.T) {
	// Modifying a non-enum column should not trigger
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN name VARCHAR(512)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_NewColumnNotExisting(t *testing.T) {
	// Column doesn't exist in the table — can't compare, no violation
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN status ENUM('active', 'inactive')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_IdenticalValues(t *testing.T) {
	// Same values in same order — no violation
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending')
	)`)

	sql := `ALTER TABLE t1 MODIFY COLUMN status ENUM('active', 'inactive', 'pending')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_AddColumn(t *testing.T) {
	// ADD COLUMN with enum is fine — it's a new column, no existing data
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY
	)`)

	sql := `ALTER TABLE t1 ADD COLUMN status ENUM('active', 'inactive')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_NonAlterStatement(t *testing.T) {
	// CREATE TABLE should not trigger any violations
	sql := `CREATE TABLE t1 (id INT PRIMARY KEY, status ENUM('active', 'inactive'))`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	assert.Empty(t, violations)
}

func TestEnumSetReorderLinter_MultipleColumns(t *testing.T) {
	// Multiple enum columns, one reordered and one safe
	existingTable := createTableWithEnum(t, `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive'),
		priority ENUM('low', 'medium', 'high')
	)`)

	sql := `ALTER TABLE t1 
		MODIFY COLUMN status ENUM('active', 'inactive', 'pending'),
		MODIFY COLUMN priority ENUM('high', 'medium', 'low')`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &EnumSetReorderLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	// Only priority should be flagged (reordered), status is safe (appended)
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "priority")
}

func TestEnumSetReorderLinter_Name(t *testing.T) {
	linter := &EnumSetReorderLinter{}
	assert.Equal(t, "enum_set_reorder", linter.Name())
}

func TestEnumSetReorderLinter_Description(t *testing.T) {
	linter := &EnumSetReorderLinter{}
	assert.NotEmpty(t, linter.Description())
}

func TestEnumSetReorderLinter_String(t *testing.T) {
	linter := &EnumSetReorderLinter{}
	assert.Contains(t, linter.String(), "enum_set_reorder")
}

func TestDetectReorder_AppendOnly(t *testing.T) {
	issues := detectReorder([]string{"a", "b"}, []string{"a", "b", "c"})
	assert.Empty(t, issues)
}

func TestDetectReorder_Swap(t *testing.T) {
	issues := detectReorder([]string{"a", "b"}, []string{"b", "a"})
	assert.NotEmpty(t, issues)
}

func TestDetectReorder_InsertMiddle(t *testing.T) {
	issues := detectReorder([]string{"a", "b"}, []string{"a", "c", "b"})
	assert.NotEmpty(t, issues)
}

func TestDetectReorder_Remove(t *testing.T) {
	issues := detectReorder([]string{"a", "b", "c"}, []string{"a", "c"})
	assert.NotEmpty(t, issues)
}

func TestDetectReorder_Identical(t *testing.T) {
	issues := detectReorder([]string{"a", "b", "c"}, []string{"a", "b", "c"})
	assert.Empty(t, issues)
}

func TestDetectReorder_Empty(t *testing.T) {
	issues := detectReorder([]string{}, []string{"a", "b"})
	assert.Empty(t, issues)
}
