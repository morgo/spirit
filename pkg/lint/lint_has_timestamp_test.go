package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- CREATE TABLE tests ---

func TestHasTimestampLinter_NoTimestamp(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255),
		created_at DATETIME,
		updated_at DATETIME
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No TIMESTAMP columns — no violations
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_SingleTimestamp(t *testing.T) {
	sql := `CREATE TABLE events (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, "has_timestamp", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
	assert.Contains(t, violations[0].Message, "TIMESTAMP")
	assert.Contains(t, violations[0].Message, "2038-01-19")
	assert.Contains(t, violations[0].Message, "DATETIME")
	assert.Equal(t, "events", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "created_at", *violations[0].Location.Column)
}

func TestHasTimestampLinter_MultipleTimestamps(t *testing.T) {
	sql := `CREATE TABLE audit_log (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 2)

	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "created_at")
	assert.Contains(t, columnNames, "updated_at")

	for _, v := range violations {
		assert.Equal(t, SeverityError, v.Severity)
		assert.Equal(t, "has_timestamp", v.Linter.Name())
	}
}

func TestHasTimestampLinter_MixedDatetimeAndTimestamp(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at DATETIME,
		updated_at TIMESTAMP,
		deleted_at DATE
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Only TIMESTAMP should be flagged, not DATETIME or DATE
	require.Len(t, violations, 1)
	assert.Equal(t, "updated_at", *violations[0].Location.Column)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_TimestampWithDefaultCurrentTimestamp(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should still flag TIMESTAMP regardless of default value
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
}

func TestHasTimestampLinter_TimestampWithOnUpdate(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "updated_at")
}

func TestHasTimestampLinter_TimestampNullable(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		deleted_at TIMESTAMP NULL
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Nullable TIMESTAMP is still TIMESTAMP
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_TimestampNotNull(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP NOT NULL
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_CaseInsensitive(t *testing.T) {
	// Test lowercase
	sql := `CREATE TABLE test1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		ts timestamp
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)

	// Test mixed case
	sql = `CREATE TABLE test2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		ts Timestamp
	)`
	ct, err = statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_MultipleTables(t *testing.T) {
	sql1 := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		ts TIMESTAMP
	)`
	sql2 := `CREATE TABLE t2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	sql3 := `CREATE TABLE t3 (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`

	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3}, nil)

	// t1 has 1, t2 has 0, t3 has 2 = 3 total
	require.Len(t, violations, 3)
	for _, v := range violations {
		assert.Equal(t, SeverityError, v.Severity)
	}
}

// --- CREATE TABLE via changes (AbstractStatement) ---

func TestHasTimestampLinter_CreateTableAsChange(t *testing.T) {
	sql := `CREATE TABLE events (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
}

// --- ALTER TABLE adding TIMESTAMP column (Error) ---

func TestHasTimestampLinter_AlterAddTimestampColumn(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN created_at TIMESTAMP`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "has_timestamp", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
	assert.Contains(t, violations[0].Message, "TIMESTAMP")
	assert.Contains(t, violations[0].Message, "2038-01-19")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "created_at", *violations[0].Location.Column)
}

func TestHasTimestampLinter_AlterAddMultipleTimestampColumns(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN created_at TIMESTAMP, ADD COLUMN updated_at TIMESTAMP`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 2)
	for _, v := range violations {
		assert.Equal(t, SeverityError, v.Severity)
	}
	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "created_at")
	assert.Contains(t, columnNames, "updated_at")
}

func TestHasTimestampLinter_AlterAddNonTimestampColumn(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN created_at DATETIME`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// DATETIME should not be flagged
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_AlterAddMixedColumns(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN created_at TIMESTAMP, ADD COLUMN name VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// Only the TIMESTAMP column should be flagged
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Equal(t, "created_at", *violations[0].Location.Column)
}

func TestHasTimestampLinter_AlterModifyToTimestamp(t *testing.T) {
	sql := `ALTER TABLE users MODIFY COLUMN created_at TIMESTAMP`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
}

func TestHasTimestampLinter_AlterChangeToTimestamp(t *testing.T) {
	sql := `ALTER TABLE users CHANGE COLUMN old_col new_col TIMESTAMP`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "new_col")
}

func TestHasTimestampLinter_AlterModifyToDatetime(t *testing.T) {
	sql := `ALTER TABLE users MODIFY COLUMN created_at DATETIME`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// Modifying to DATETIME should not be flagged
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_AlterAddTimestampWithDefault(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

// --- ALTER TABLE modifying table with existing TIMESTAMP (Warning) ---

func TestHasTimestampLinter_AlterExistingTableWithTimestamp(t *testing.T) {
	// Existing table has a TIMESTAMP column
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER adds a non-TIMESTAMP column
	alterSQL := `ALTER TABLE users ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Should produce a Warning for the existing TIMESTAMP column on the table
	// Plus an Error for the CREATE TABLE itself
	// Filter to just the warnings from the ALTER
	var warnings []Violation
	var errors []Violation
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			warnings = append(warnings, v)
		}
		if v.Severity == SeverityError {
			errors = append(errors, v)
		}
	}

	// The existing table CREATE produces an Error
	require.Len(t, errors, 1)
	assert.Equal(t, "created_at", *errors[0].Location.Column)

	// The ALTER on a table with existing TIMESTAMP produces a Warning
	require.Len(t, warnings, 1)
	assert.Equal(t, "has_timestamp", warnings[0].Linter.Name())
	assert.Equal(t, SeverityWarning, warnings[0].Severity)
	assert.Contains(t, warnings[0].Message, "created_at")
	assert.Contains(t, warnings[0].Message, "TIMESTAMP")
	assert.Contains(t, warnings[0].Message, "2038-01-19")
	assert.Equal(t, "users", warnings[0].Location.Table)
}

func TestHasTimestampLinter_AlterExistingTableWithMultipleTimestamps(t *testing.T) {
	existingSQL := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	alterSQL := `ALTER TABLE logs ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Filter warnings from the ALTER
	var warnings []Violation
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			warnings = append(warnings, v)
		}
	}

	require.Len(t, warnings, 2)
	columnNames := []string{*warnings[0].Location.Column, *warnings[1].Location.Column}
	assert.Contains(t, columnNames, "created_at")
	assert.Contains(t, columnNames, "updated_at")
}

func TestHasTimestampLinter_AlterExistingTableNoTimestamp(t *testing.T) {
	// Existing table has no TIMESTAMP columns
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at DATETIME,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER adds a non-TIMESTAMP column
	alterSQL := `ALTER TABLE users ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// No TIMESTAMP anywhere — no violations (the CREATE TABLE has DATETIME, not TIMESTAMP)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_AlterAddTimestampToTableWithExistingTimestamp(t *testing.T) {
	// Existing table already has a TIMESTAMP column
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER adds another TIMESTAMP column
	alterSQL := `ALTER TABLE users ADD COLUMN updated_at TIMESTAMP`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Should have Error for CREATE TABLE's existing TIMESTAMP + Error for the new TIMESTAMP being added
	// Should NOT have a Warning because the ALTER is adding TIMESTAMP (Error takes precedence)
	var errors []Violation
	var warnings []Violation
	for _, v := range violations {
		if v.Severity == SeverityError {
			errors = append(errors, v)
		}
		if v.Severity == SeverityWarning {
			warnings = append(warnings, v)
		}
	}

	// Error from CREATE TABLE (created_at) + Error from ALTER (updated_at)
	require.Len(t, errors, 2)
	// No warnings — the ALTER is adding TIMESTAMP so it gets Error, not Warning
	assert.Empty(t, warnings)
}

func TestHasTimestampLinter_AlterDropColumn(t *testing.T) {
	// Existing table has a TIMESTAMP column
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER drops a column (not adding TIMESTAMP)
	alterSQL := `ALTER TABLE users DROP COLUMN updated_at`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Errors from CREATE TABLE + Warnings from ALTER on table with existing TIMESTAMP
	var warnings []Violation
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			warnings = append(warnings, v)
		}
	}

	// The ALTER is modifying a table with TIMESTAMP columns — should produce warnings
	require.Len(t, warnings, 2)
}

func TestHasTimestampLinter_AlterAddIndex(t *testing.T) {
	// Existing table has a TIMESTAMP column
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER adds an index (not adding TIMESTAMP)
	alterSQL := `ALTER TABLE users ADD INDEX idx_name (name)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Filter warnings
	var warnings []Violation
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			warnings = append(warnings, v)
		}
	}

	// The ALTER is modifying a table with TIMESTAMP — should produce a warning
	require.Len(t, warnings, 1)
	assert.Equal(t, "created_at", *warnings[0].Location.Column)
}

func TestHasTimestampLinter_AlterTableNotInExisting(t *testing.T) {
	// ALTER on a table not in existingTables (no existing schema available)
	alterSQL := `ALTER TABLE unknown_table ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// No existing table info, no TIMESTAMP being added — no violations
	assert.Empty(t, violations)
}

// --- Linter metadata ---

func TestHasTimestampLinter_Name(t *testing.T) {
	linter := &HasTimestampLinter{}
	assert.Equal(t, "has_timestamp", linter.Name())
}

func TestHasTimestampLinter_Description(t *testing.T) {
	linter := &HasTimestampLinter{}
	assert.NotEmpty(t, linter.Description())
	assert.Contains(t, linter.Description(), "TIMESTAMP")
}

func TestHasTimestampLinter_String(t *testing.T) {
	linter := &HasTimestampLinter{}
	str := linter.String()
	assert.Contains(t, str, "has_timestamp")
	assert.Contains(t, str, linter.Description())
}

// --- Registration ---

func TestHasTimestampLinter_Registered(t *testing.T) {
	Reset()
	Register(&HasTimestampLinter{})

	names := List()
	found := false
	for _, name := range names {
		if name == "has_timestamp" {
			found = true
			break
		}
	}
	assert.True(t, found, "has_timestamp linter should be registered")
}

// --- Edge cases ---

func TestHasTimestampLinter_EmptyTable(t *testing.T) {
	// Table with no columns (unusual but valid parse)
	sql := `CREATE TABLE empty_table (id INT PRIMARY KEY)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	assert.Empty(t, violations)
}

func TestHasTimestampLinter_NilInputs(t *testing.T) {
	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, nil)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_TimestampWithPrecision(t *testing.T) {
	sql := `CREATE TABLE events (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP(6)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// TIMESTAMP(6) is still TIMESTAMP
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
}

func TestHasTimestampLinter_AlterAddTimestampWithPrecision(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN created_at TIMESTAMP(3)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

// --- Violation message format ---

func TestHasTimestampLinter_MessageFormat(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		my_ts TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	msg := violations[0].Message
	assert.True(t, strings.Contains(msg, "my_ts"), "message should contain column name")
	assert.True(t, strings.Contains(msg, "TIMESTAMP"), "message should contain TIMESTAMP")
	assert.True(t, strings.Contains(msg, "2038-01-19"), "message should contain overflow date")
	assert.True(t, strings.Contains(msg, "DATETIME"), "message should suggest DATETIME")
}

func TestHasTimestampLinter_ViolationString(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		my_ts TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	str := violations[0].String()
	assert.Contains(t, str, "[ERROR]")
	assert.Contains(t, str, "has_timestamp")
}

// --- Other date/time types should NOT be flagged ---

func TestHasTimestampLinter_DatetimeNotFlagged(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at DATETIME
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_DateNotFlagged(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		birth_date DATE
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_TimeNotFlagged(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		duration TIME
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_YearNotFlagged(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		birth_year YEAR
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations)
}

// --- existingTables parameter tests ---

func TestHasTimestampLinter_ExistingTablesOnly(t *testing.T) {
	// When existingTables has TIMESTAMP and no changes, should produce errors
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_ChangesOnly(t *testing.T) {
	// When only changes are provided (no existing tables)
	sql := `ALTER TABLE users ADD COLUMN ts TIMESTAMP`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

// --- ALTER TABLE with non-matching table name ---

func TestHasTimestampLinter_AlterDifferentTable(t *testing.T) {
	// Existing table "users" has TIMESTAMP
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER is on a different table "orders"
	alterSQL := `ALTER TABLE orders ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Error from the CREATE TABLE (users has TIMESTAMP)
	// No warning for the ALTER (orders is not in existingTables)
	var warnings []Violation
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			warnings = append(warnings, v)
		}
	}
	assert.Empty(t, warnings)
}
