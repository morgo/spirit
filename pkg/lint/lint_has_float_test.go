package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasFloatLinter_NoFloatOrDouble(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255),
		age INT,
		balance DECIMAL(10, 2)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No FLOAT or DOUBLE - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_FloatColumn(t *testing.T) {
	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT,
		humidity INT
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// FLOAT should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "temperature")
	assert.Contains(t, violations[0].Message, "measurements")
	// The parser returns lowercase type names
	assert.Contains(t, violations[0].Message, "float")
	assert.Equal(t, "measurements", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_DoubleColumn(t *testing.T) {
	sql := `CREATE TABLE coordinates (
		id BIGINT UNSIGNED PRIMARY KEY,
		latitude DOUBLE,
		longitude DOUBLE
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Both DOUBLE columns should be detected
	require.Len(t, violations, 2)

	// First violation
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "latitude")
	assert.Contains(t, violations[0].Message, "coordinates")
	// The parser returns lowercase type names
	assert.Contains(t, violations[0].Message, "double")
	assert.Equal(t, "coordinates", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "latitude", *violations[0].Location.Column)

	// Second violation
	assert.Equal(t, "has_float", violations[1].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[1].Severity)
	assert.Contains(t, violations[1].Message, "longitude")
	assert.Contains(t, violations[1].Message, "coordinates")
	// The parser returns lowercase type names
	assert.Contains(t, violations[1].Message, "double")
	assert.Equal(t, "coordinates", violations[1].Location.Table)
	assert.NotNil(t, violations[1].Location.Column)
	assert.Equal(t, "longitude", *violations[1].Location.Column)
}

func TestHasFloatLinter_MixedFloatAndDouble(t *testing.T) {
	sql := `CREATE TABLE metrics (
		id BIGINT UNSIGNED PRIMARY KEY,
		value1 FLOAT,
		value2 DOUBLE,
		value3 DECIMAL(10, 2)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Both FLOAT and DOUBLE should be detected, but not DECIMAL
	require.Len(t, violations, 2)

	// Check both violations are present
	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "value1")
	assert.Contains(t, columnNames, "value2")
}

func TestHasFloatLinter_CaseInsensitive(t *testing.T) {
	// Test lowercase
	sql := `CREATE TABLE test1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value float
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should detect lowercase 'float'
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
}

func TestHasFloatLinter_CaseInsensitiveDouble(t *testing.T) {
	// Test lowercase double
	sql := `CREATE TABLE test2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value double
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should detect lowercase 'double'
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
}

func TestHasFloatLinter_MultipleTables(t *testing.T) {
	sql1 := `CREATE TABLE table1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value FLOAT
	)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE table2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE table3 (
		id BIGINT UNSIGNED PRIMARY KEY,
		score DOUBLE
	)`
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3}, nil)

	// Should find violations in table1 and table3, but not table2
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "table1")
	assert.Contains(t, tables, "table3")
}

func TestHasFloatLinter_FloatWithPrecision(t *testing.T) {
	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		value FLOAT(7, 4)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// FLOAT with precision should still be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
	// The parser may normalize the type name
	assert.True(t, strings.Contains(strings.ToLower(violations[0].Message), "float"))
}

func TestHasFloatLinter_DoubleWithPrecision(t *testing.T) {
	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		value DOUBLE(15, 8)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// DOUBLE with precision should still be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
	// The parser may normalize the type name
	assert.True(t, strings.Contains(strings.ToLower(violations[0].Message), "double"))
}

func TestHasFloatLinter_EmptyInput(t *testing.T) {
	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, nil)

	// No tables - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_EmptyTableList(t *testing.T) {
	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{}, nil)

	// Empty list - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_TableWithNoColumns(t *testing.T) {
	// This is a theoretical test case - in practice, tables always have columns
	ct := &statement.CreateTable{
		TableName: "empty_table",
		Columns:   statement.Columns{},
	}

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No columns - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_SimilarTypeNames(t *testing.T) {
	// Test that similar type names don't trigger false positives
	sql := `CREATE TABLE test (
		id BIGINT UNSIGNED PRIMARY KEY,
		data VARCHAR(255),
		amount DECIMAL(10, 2),
		count INT,
		flag BOOLEAN
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// None of these types should trigger violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_Integration(t *testing.T) {
	Reset()
	Register(&HasFloatLinter{})

	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{})
	require.NoError(t, err)

	// Should find at least one violation (from HasFloatLinter)
	var hasFloatViolations []Violation
	for _, v := range violations {
		if v.Linter.Name() == "has_float" {
			hasFloatViolations = append(hasFloatViolations, v)
		}
	}
	require.Len(t, hasFloatViolations, 1)
	assert.Equal(t, "has_float", hasFloatViolations[0].Linter.Name())
}

func TestHasFloatLinter_IntegrationDisabled(t *testing.T) {
	Reset()
	Register(&HasFloatLinter{})

	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{
		Enabled: map[string]bool{
			"has_float": false,
		},
	})
	require.NoError(t, err)

	// HasFloatLinter is disabled, so no violations from it
	for _, v := range violations {
		assert.NotEqual(t, "has_float", v.Linter.Name())
	}
}

func TestHasFloatLinter_Metadata(t *testing.T) {
	linter := &HasFloatLinter{}

	assert.Equal(t, "has_float", linter.Name())
	assert.NotEmpty(t, linter.Description())
	assert.Contains(t, linter.Description(), "FLOAT")
	assert.Contains(t, linter.Description(), "DOUBLE")
}

func TestHasFloatLinter_AllFloatTypes(t *testing.T) {
	// Test all variations of FLOAT and DOUBLE
	sql := `CREATE TABLE all_floats (
		id BIGINT UNSIGNED PRIMARY KEY,
		f1 FLOAT,
		f2 FLOAT(7, 4),
		d1 DOUBLE,
		d2 DOUBLE(15, 8)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// All 4 float/double columns should be detected
	require.Len(t, violations, 4)

	// Verify all violations are warnings
	for _, v := range violations {
		assert.Equal(t, SeverityWarning, v.Severity)
		assert.Equal(t, "all_floats", v.Location.Table)
	}
}

func TestHasFloatLinter_RealType(t *testing.T) {
	// REAL is a synonym for DOUBLE in MySQL
	// However, the parser might normalize it, so this test checks actual behavior
	sql := `CREATE TABLE test (
		id BIGINT UNSIGNED PRIMARY KEY,
		value REAL
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// If the parser normalizes REAL to DOUBLE, it should be detected
	// If not, this test documents that behavior
	// The linter currently only checks for FLOAT and DOUBLE explicitly
	if len(violations) > 0 {
		assert.Contains(t, violations[0].Message, "value")
	}
}

func TestHasFloatLinter_DecimalNotDetected(t *testing.T) {
	// Ensure DECIMAL types are not flagged (they are exact numeric types)
	sql := `CREATE TABLE financial (
		id BIGINT UNSIGNED PRIMARY KEY,
		price DECIMAL(10, 2),
		tax DECIMAL(5, 2),
		total NUMERIC(12, 2)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// DECIMAL and NUMERIC should not be detected
	assert.Empty(t, violations)
}

func TestHasFloatLinter_MixedCase(t *testing.T) {
	// Test various case combinations
	sql := `CREATE TABLE test (
		id BIGINT UNSIGNED PRIMARY KEY,
		v1 Float,
		v2 FLOAT,
		v3 float,
		v4 Double,
		v5 DOUBLE,
		v6 double
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// All 6 columns should be detected regardless of case
	require.Len(t, violations, 6)
}

func TestHasFloatLinter_String(t *testing.T) {
	linter := &HasFloatLinter{}
	str := linter.String()

	// String() should return a non-empty string
	assert.NotEmpty(t, str)
}
