package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- CREATE TABLE in changes (new tables — Error) ---

func TestHasTimestampLinter_CreateTableAsChange_NoTimestamp(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255),
		created_at DATETIME,
		updated_at DATETIME
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// No TIMESTAMP columns — no violations
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_CreateTableAsChange_SingleTimestamp(t *testing.T) {
	sql := `CREATE TABLE events (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP
	)`
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
	assert.Contains(t, violations[0].Message, "DATETIME")
	assert.Equal(t, "events", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "created_at", *violations[0].Location.Column)
}

func TestHasTimestampLinter_CreateTableAsChange_MultipleTimestamps(t *testing.T) {
	sql := `CREATE TABLE audit_log (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 2)
	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "created_at")
	assert.Contains(t, columnNames, "updated_at")

	for _, v := range violations {
		assert.Equal(t, SeverityError, v.Severity)
		assert.Equal(t, "has_timestamp", v.Linter.Name())
	}
}

func TestHasTimestampLinter_CreateTableAsChange_MixedDatetimeAndTimestamp(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at DATETIME,
		updated_at TIMESTAMP,
		deleted_at DATE
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// Only TIMESTAMP should be flagged, not DATETIME or DATE
	require.Len(t, violations, 1)
	assert.Equal(t, "updated_at", *violations[0].Location.Column)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_CreateTableAsChange_TimestampWithDefault(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// Should still flag TIMESTAMP regardless of default value
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_CreateTableAsChange_TimestampWithOnUpdate(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_CreateTableAsChange_TimestampNullable(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		deleted_at TIMESTAMP NULL
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_CreateTableAsChange_TimestampNotNull(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP NOT NULL
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_CreateTableAsChange_CaseInsensitive(t *testing.T) {
	linter := &HasTimestampLinter{}

	// Test lowercase
	stmts, err := statement.New(`CREATE TABLE test1 (id BIGINT UNSIGNED PRIMARY KEY, ts timestamp)`)
	require.NoError(t, err)
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)

	// Test mixed case
	stmts, err = statement.New(`CREATE TABLE test2 (id BIGINT UNSIGNED PRIMARY KEY, ts Timestamp)`)
	require.NoError(t, err)
	violations = linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestHasTimestampLinter_CreateTableAsChange_TimestampWithPrecision(t *testing.T) {
	sql := `CREATE TABLE events (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP(6)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// TIMESTAMP(6) is still TIMESTAMP
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
}

// --- Existing tables (legacy schemas — Warning) ---

func TestHasTimestampLinter_ExistingTable_SingleTimestamp(t *testing.T) {
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Existing tables with TIMESTAMP get Warning — don't boil the ocean
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "created_at")
	assert.Contains(t, violations[0].Message, "TIMESTAMP")
	assert.Contains(t, violations[0].Message, "2038-01-19")
}

func TestHasTimestampLinter_ExistingTable_MultipleTimestamps(t *testing.T) {
	existingSQL := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 2)
	for _, v := range violations {
		assert.Equal(t, SeverityWarning, v.Severity)
	}
}

func TestHasTimestampLinter_ExistingTable_NoTimestamp(t *testing.T) {
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at DATETIME
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	assert.Empty(t, violations)
}

func TestHasTimestampLinter_ExistingTable_MultipleTables(t *testing.T) {
	ct1, err := statement.ParseCreateTable(`CREATE TABLE t1 (id BIGINT PRIMARY KEY, ts TIMESTAMP)`)
	require.NoError(t, err)
	ct2, err := statement.ParseCreateTable(`CREATE TABLE t2 (id BIGINT PRIMARY KEY, name VARCHAR(255))`)
	require.NoError(t, err)
	ct3, err := statement.ParseCreateTable(`CREATE TABLE t3 (id BIGINT PRIMARY KEY, a TIMESTAMP, b TIMESTAMP)`)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3}, nil)

	// t1=1, t2=0, t3=2 = 3 total, all Warning
	require.Len(t, violations, 3)
	for _, v := range violations {
		assert.Equal(t, SeverityWarning, v.Severity)
	}
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

func TestHasTimestampLinter_AlterAddTimestampWithPrecision(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN created_at TIMESTAMP(3)`
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

	// Existing table produces Warning + ALTER on table with TIMESTAMP produces Warning
	// All should be Warning — no Errors since nothing new is introducing TIMESTAMP
	for _, v := range violations {
		assert.Equal(t, SeverityWarning, v.Severity)
	}

	// Should have warnings from both the existing table and the ALTER path
	// (existing table warning + ALTER-on-existing-table warning)
	var warningsForCreatedAt int
	for _, v := range violations {
		if v.Location != nil && v.Location.Column != nil && *v.Location.Column == "created_at" {
			warningsForCreatedAt++
		}
	}
	assert.GreaterOrEqual(t, warningsForCreatedAt, 1)
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

	// All violations should be Warning
	for _, v := range violations {
		assert.Equal(t, SeverityWarning, v.Severity)
	}

	// Should have warnings mentioning both TIMESTAMP columns
	columnSet := make(map[string]bool)
	for _, v := range violations {
		if v.Location != nil && v.Location.Column != nil {
			columnSet[*v.Location.Column] = true
		}
	}
	assert.True(t, columnSet["created_at"])
	assert.True(t, columnSet["updated_at"])
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

	// No TIMESTAMP anywhere — no violations
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

	// Error from ALTER adding updated_at TIMESTAMP
	require.Len(t, errors, 1)
	assert.Equal(t, "updated_at", *errors[0].Location.Column)

	// Warning from existing table having created_at TIMESTAMP
	require.Len(t, warnings, 1)
	assert.Equal(t, "created_at", *warnings[0].Location.Column)
}

// --- ALTER TABLE that fixes TIMESTAMP (no false-positive warnings) ---

func TestHasTimestampLinter_AlterDropTimestampColumn(t *testing.T) {
	// Existing table has two TIMESTAMP columns
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER drops one of the TIMESTAMP columns — actively fixing the problem
	alterSQL := `ALTER TABLE users DROP COLUMN updated_at`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Filter to just ALTER-path warnings (exclude existing table warnings)
	var alterWarnings []Violation
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			alterWarnings = append(alterWarnings, v)
		}
	}

	// Should warn about created_at (still TIMESTAMP) but NOT about updated_at (being dropped)
	// Existing table produces 2 warnings, ALTER path should only warn about created_at
	// Total warnings: 2 from existing table + 1 from ALTER path = but updated_at is being fixed
	// Let's check that we don't have warnings for updated_at from the ALTER path
	// The existing table warnings are separate from ALTER warnings
	// With the fix: existing table warns about both, ALTER only warns about created_at (not updated_at)
	createdAtCount := 0
	updatedAtCount := 0
	for _, v := range alterWarnings {
		if v.Location != nil && v.Location.Column != nil {
			switch *v.Location.Column {
			case "created_at":
				createdAtCount++
			case "updated_at":
				updatedAtCount++
			}
		}
	}

	// created_at appears in both existing table warning and ALTER warning
	assert.GreaterOrEqual(t, createdAtCount, 1)
	// updated_at: existing table still warns (it doesn't know about the ALTER),
	// but the ALTER path should NOT warn about it since it's being dropped.
	// We expect exactly 1 warning for updated_at (from existing table only),
	// not 2 (which would mean the ALTER path also warned about it).
	assert.Equal(t, 1, updatedAtCount, "updated_at should only be warned about from existing table, not from ALTER path")
}

func TestHasTimestampLinter_AlterModifyTimestampToDatetime(t *testing.T) {
	// Existing table has a TIMESTAMP column
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER converts one TIMESTAMP to DATETIME — actively fixing the problem
	alterSQL := `ALTER TABLE users MODIFY COLUMN created_at DATETIME`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// The ALTER is fixing created_at, so the ALTER path should only warn about updated_at
	var alterPathWarnings []Violation
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			alterPathWarnings = append(alterPathWarnings, v)
		}
	}

	// Existing table warns about both (2 warnings)
	// ALTER path warns about updated_at only (1 warning), created_at is being fixed
	createdAtCount := 0
	updatedAtCount := 0
	for _, v := range alterPathWarnings {
		if v.Location != nil && v.Location.Column != nil {
			switch *v.Location.Column {
			case "created_at":
				createdAtCount++
			case "updated_at":
				updatedAtCount++
			}
		}
	}

	// created_at: 1 from existing table (the ALTER path excludes it since it's being fixed)
	assert.Equal(t, 1, createdAtCount)
	// updated_at: 1 from existing table + 1 from ALTER path = 2
	assert.Equal(t, 2, updatedAtCount)

	// No errors — nothing is introducing TIMESTAMP
	for _, v := range violations {
		assert.NotEqual(t, SeverityError, v.Severity)
	}
}

func TestHasTimestampLinter_AlterChangeTimestampToDatetime(t *testing.T) {
	// Existing table has a TIMESTAMP column
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		old_ts TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// CHANGE renames and converts the column — fixing the problem
	alterSQL := `ALTER TABLE users CHANGE COLUMN old_ts new_dt DATETIME`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Existing table warns about old_ts (1 warning)
	// ALTER path should NOT warn about old_ts since it's being converted away
	var errors []Violation
	for _, v := range violations {
		if v.Severity == SeverityError {
			errors = append(errors, v)
		}
	}
	assert.Empty(t, errors, "converting TIMESTAMP to DATETIME should not produce errors")

	// The ALTER path should not add a warning for old_ts since it's being fixed
	// Only the existing table warning for old_ts should remain
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "old_ts", *violations[0].Location.Column)
}

func TestHasTimestampLinter_AlterDropAllTimestampColumns(t *testing.T) {
	// Existing table has TIMESTAMP columns
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER drops both TIMESTAMP columns
	alterSQL := `ALTER TABLE users DROP COLUMN created_at, DROP COLUMN updated_at`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Existing table still warns (2 warnings) — it doesn't know about the ALTER
	// ALTER path should NOT warn since both columns are being dropped
	var alterPathViolations []Violation
	for _, v := range violations {
		// All violations should be Warning from existing table
		assert.Equal(t, SeverityWarning, v.Severity)
		alterPathViolations = append(alterPathViolations, v)
	}

	// Only 2 warnings from existing table, none from ALTER path
	require.Len(t, alterPathViolations, 2)
}

func TestHasTimestampLinter_AlterModifyTimestampToDatetimeNoExisting(t *testing.T) {
	// No existing table info — just the ALTER
	alterSQL := `ALTER TABLE users MODIFY COLUMN created_at DATETIME`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	// Converting to DATETIME — no violations
	assert.Empty(t, violations)
}

// --- ALTER TABLE with other operations ---

func TestHasTimestampLinter_AlterAddIndex(t *testing.T) {
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		created_at TIMESTAMP,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER adds an index (not touching TIMESTAMP columns)
	alterSQL := `ALTER TABLE users ADD INDEX idx_name (name)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// All should be Warning
	for _, v := range violations {
		assert.Equal(t, SeverityWarning, v.Severity)
	}

	// Should have warnings mentioning created_at
	found := false
	for _, v := range violations {
		if v.Location != nil && v.Location.Column != nil && *v.Location.Column == "created_at" {
			found = true
		}
	}
	assert.True(t, found)
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

	// Warning from existing table (users has TIMESTAMP)
	// No warning from ALTER (orders is not in existingTables)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "users", violations[0].Location.Table)
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
	resetForTest(t)
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
	sql := `CREATE TABLE empty_table (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	assert.Empty(t, violations)
}

func TestHasTimestampLinter_NilInputs(t *testing.T) {
	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, nil)
	assert.Empty(t, violations)
}

// --- Violation message format ---

func TestHasTimestampLinter_MessageFormat(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		my_ts TIMESTAMP
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

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
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	str := violations[0].String()
	assert.Contains(t, str, "[ERROR]")
	assert.Contains(t, str, "has_timestamp")
}

// --- Other date/time types should NOT be flagged ---

func TestHasTimestampLinter_DatetimeNotFlagged(t *testing.T) {
	stmts, err := statement.New(`CREATE TABLE t1 (id BIGINT PRIMARY KEY, created_at DATETIME)`)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_DateNotFlagged(t *testing.T) {
	stmts, err := statement.New(`CREATE TABLE t1 (id BIGINT PRIMARY KEY, birth_date DATE)`)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_TimeNotFlagged(t *testing.T) {
	stmts, err := statement.New(`CREATE TABLE t1 (id BIGINT PRIMARY KEY, duration TIME)`)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

func TestHasTimestampLinter_YearNotFlagged(t *testing.T) {
	stmts, err := statement.New(`CREATE TABLE t1 (id BIGINT PRIMARY KEY, birth_year YEAR)`)
	require.NoError(t, err)

	linter := &HasTimestampLinter{}
	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}
