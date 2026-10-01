package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// Mock linter for testing
type mockLinter struct {
	name        string
	description string
	violations  []Violation
}

func (m *mockLinter) String() string {
	// TODO implement me
	panic("implement me")
}

func (m *mockLinter) Name() string        { return m.name }
func (m *mockLinter) Description() string { return m.description }
func (m *mockLinter) Lint(createTables []*statement.CreateTable, statements []*statement.AbstractStatement) []Violation {
	return m.violations
}

// Configurable mock linter for testing
type mockConfigurableLinter struct {
	mockLinter

	configCalled bool
	configValue  map[string]string
}

func (m *mockConfigurableLinter) String() string {
	// TODO implement me
	panic("implement me")
}

func (m *mockConfigurableLinter) Configure(config map[string]string) error {
	m.configCalled = true
	m.configValue = config

	return nil
}

func (m *mockConfigurableLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"default": "value",
	}
}

// filterByLinter returns only violations from the specified linter.
func filterByLinter(violations []Violation, linterName string) []Violation {
	var filtered []Violation
	for _, v := range violations {
		if v.Linter.Name() == linterName {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func TestRegister(t *testing.T) {
	// Reset registry before test
	resetForTest(t)

	linter := &mockLinter{
		name:        "test_linter",
		description: "A test linter",
	}

	Register(linter)

	// Verify linter was registered
	names := registeredLinterNames()
	require.Contains(t, names, "test_linter")

	// Verify we can get it back
	retrieved := registeredLinter(t, "test_linter")
	require.Equal(t, "test_linter", retrieved.Name())
}

func TestRegisterMultiple(t *testing.T) {
	resetForTest(t)

	linter1 := &mockLinter{name: "linter1"}
	linter2 := &mockLinter{name: "linter2"}
	linter3 := &mockLinter{name: "linter3"}

	Register(linter1)
	Register(linter2)
	Register(linter3)

	names := registeredLinterNames()
	require.Len(t, names, 3)
	require.Contains(t, names, "linter1")
	require.Contains(t, names, "linter2")
	require.Contains(t, names, "linter3")
}

func TestRunLinters_Empty(t *testing.T) {
	resetForTest(t)

	violations, err := RunLinters(nil, nil, Config{})
	require.NoError(t, err)
	require.Empty(t, violations)
}

func TestRunLinters_SingleLinter(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{
		name: "test_linter",
	}

	expectedViolations := []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Test error",
		},
	}
	linter.violations = expectedViolations

	Register(linter)

	violations, err := RunLinters(nil, nil, Config{})
	require.NoError(t, err)
	require.Len(t, violations, 1)
	require.Equal(t, "test_linter", violations[0].Linter.Name())
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Equal(t, "Test error", violations[0].Message)
}

func TestRunLinters_MultipleLinters(t *testing.T) {
	resetForTest(t)

	linter1 := &mockLinter{
		name: "linter1",
	}
	linter1.violations = []Violation{
		{Linter: linter1, Severity: SeverityWarning, Message: "Error 1"},
	}

	linter2 := &mockLinter{
		name: "linter2",
	}
	linter2.violations = []Violation{
		{Linter: linter2, Severity: SeverityWarning, Message: "Warning 1"},
		{Linter: linter2, Severity: SeverityWarning, Message: "Info 1"},
	}

	Register(linter1)
	Register(linter2)

	violations, err := RunLinters(nil, nil, Config{})
	require.NoError(t, err)
	require.Len(t, violations, 3)
}

func TestRunLinters_WithConfig_Disabled(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{
		name: "test_linter",
	}
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityWarning, Message: "Should not see this"},
	}
	Register(linter)

	// Disable the linter via config
	violations, err := RunLinters(nil, nil, Config{
		Enabled: map[string]bool{
			"test_linter": false,
		},
	})
	require.NoError(t, err)

	require.Empty(t, violations)
}

func TestRunLinters_WithConfig_Enabled(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{
		name: "test_linter",
	}
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityWarning, Message: "Should see this"},
	}

	// Disable by default
	Register(linter)
	lock.Lock()
	linters["test_linter"].enabled = false
	lock.Unlock()

	// But explicitly enable via config
	violations, err := RunLinters(nil, nil, Config{
		Enabled: map[string]bool{
			"test_linter": true,
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "Should see this", violations[0].Message)
}

func TestRunLinters_ConfigurableLinter(t *testing.T) {
	resetForTest(t)

	linter := &mockConfigurableLinter{}
	linter.name = "configurable_linter"
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityWarning, Message: "Test"},
	}
	Register(linter)

	config := map[string]string{"key": "value"}
	violations, err := RunLinters(nil, nil, Config{
		Settings: map[string]map[string]string{
			"configurable_linter": config,
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.True(t, linter.configCalled)
	// User config should be merged with defaults
	expected := map[string]string{
		"default": "value", // from DefaultConfig
		"key":     "value", // from user config
	}
	require.Equal(t, expected, linter.configValue)
}

func TestRunLinters_ConfigurableLinter_NoConfig(t *testing.T) {
	resetForTest(t)

	linter := &mockConfigurableLinter{}
	linter.name = "configurable_linter"
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityWarning, Message: "Test"},
	}
	Register(linter)

	violations, err := RunLinters(nil, nil, Config{})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	// Now Configure is always called (with defaults)
	require.True(t, linter.configCalled)
	// Should have received the default config
	require.Equal(t, map[string]string{"default": "value"}, linter.configValue)
}

func TestHasErrors(t *testing.T) {
	violations := []Violation{
		{Severity: SeverityWarning},
		{Severity: SeverityWarning},
	}
	// All linters now use SeverityWarning, so HasErrors should return false
	require.False(t, HasErrors(violations))

	// Even adding more warnings shouldn't make HasErrors return true
	violations = append(violations, Violation{Severity: SeverityWarning})
	require.False(t, HasErrors(violations))
}

func TestViolationWithLocation(t *testing.T) {
	column := "test_column"
	index := "test_index"
	constraint := "test_constraint"
	linter := &mockLinter{name: "test_linter"}

	violation := Violation{
		Linter:   linter,
		Severity: SeverityWarning,
		Message:  "Test message",
		Location: &Location{
			Table:      "test_table",
			Column:     &column,
			Index:      &index,
			Constraint: &constraint,
		},
	}

	require.Equal(t, "test_table", violation.Location.Table)
	require.Equal(t, "test_column", *violation.Location.Column)
	require.Equal(t, "test_index", *violation.Location.Index)
	require.Equal(t, "test_constraint", *violation.Location.Constraint)
}

func TestViolationWithSuggestion(t *testing.T) {
	suggestion := "Try this instead"
	linter := &mockLinter{name: "test_linter"}

	violation := Violation{
		Linter:     linter,
		Severity:   SeverityWarning,
		Message:    "Test message",
		Suggestion: &suggestion,
	}

	require.NotNil(t, violation.Suggestion)
	require.Equal(t, "Try this instead", *violation.Suggestion)
}

func TestViolationWithContext(t *testing.T) {
	linter := &mockLinter{name: "test_linter"}

	violation := Violation{
		Linter:   linter,
		Severity: SeverityWarning,
		Message:  "Test message",
		Context: map[string]any{
			"key1": "value1",
			"key2": 42,
		},
	}

	require.Len(t, violation.Context, 2)
	require.Equal(t, "value1", violation.Context["key1"])
	require.Equal(t, 42, violation.Context["key2"])
}

// ConfigBool tests

func TestConfigBool_ValidTrue(t *testing.T) {
	tests := []string{"true", "TRUE", "True", "TrUe"}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			result, err := ConfigBool(value, "testKey")
			require.NoError(t, err)
			require.True(t, result)
		})
	}
}

func TestConfigBool_ValidFalse(t *testing.T) {
	tests := []string{"false", "FALSE", "False", "FaLsE"}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			result, err := ConfigBool(value, "testKey")
			require.NoError(t, err)
			require.False(t, result)
		})
	}
}

func TestConfigBool_Invalid(t *testing.T) {
	tests := []struct {
		value string
		key   string
	}{
		{"yes", "testKey"},
		{"no", "testKey"},
		{"1", "testKey"},
		{"0", "testKey"},
		{"True ", "testKey"}, // trailing space
		{" true", "testKey"}, // leading space
		{"", "testKey"},
		{"invalid", "myOption"},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			result, err := ConfigBool(tt.value, tt.key)
			require.Error(t, err)
			require.False(t, result)
			require.Contains(t, err.Error(), "invalid value for "+tt.key)
			require.Contains(t, err.Error(), tt.value)
			require.Contains(t, err.Error(), "expected 'true' or 'false'")
		})
	}
}

// DefaultConfig tests

func TestRunLinters_AppliesDefaultConfig(t *testing.T) {
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Run without any config - should apply default (raiseError=true)
	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	// Default raiseError is "true", so severity should be Error
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestRunLinters_UserConfigOverridesDefault(t *testing.T) {
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Override default raiseError=true with false
	violations, err := RunLinters(nil, stmts, Config{
		Settings: map[string]map[string]string{
			"invisible_index_before_drop": {
				"raiseError": "false",
			},
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	// User set raiseError=false, so severity should be Warning
	require.Equal(t, SeverityWarning, violations[0].Severity)
}

// LintOnlyChanges tests

func TestRunLinters_LintOnlyChanges_False(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on products table",
			Location: &Location{Table: "products"},
		},
	}
	Register(linter)

	// Parse changes that only affect users table
	sql := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	changes, err := statement.New(sql)
	require.NoError(t, err)

	// With LintOnlyChanges=false, all violations should be returned
	violations, err := RunLinters(nil, changes, Config{
		LintOnlyChanges: false,
	})
	require.NoError(t, err)

	require.Len(t, violations, 3)
	require.Equal(t, "Violation on users table", violations[0].Message)
	require.Equal(t, "Violation on orders table", violations[1].Message)
	require.Equal(t, "Violation on products table", violations[2].Message)
}

func TestRunLinters_LintOnlyChanges_True(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on products table",
			Location: &Location{Table: "products"},
		},
	}
	Register(linter)

	// Parse changes that only affect users table
	sql := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	changes, err := statement.New(sql)
	require.NoError(t, err)

	// With LintOnlyChanges=true, only violations for changed tables should be returned
	violations, err := RunLinters(nil, changes, Config{
		LintOnlyChanges: true,
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "Violation on users table", violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
}

func TestRunLinters_LintOnlyChanges_MultipleChangedTables(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on products table",
			Location: &Location{Table: "products"},
		},
	}
	Register(linter)

	// Parse changes that affect both users and orders tables
	sql := `
		ALTER TABLE users ADD COLUMN email VARCHAR(255);
		ALTER TABLE orders ADD COLUMN total DECIMAL(10,2);
	`
	changes, err := statement.New(sql)
	require.NoError(t, err)

	// With LintOnlyChanges=true, violations for users and orders should be returned
	violations, err := RunLinters(nil, changes, Config{
		LintOnlyChanges: true,
	})
	require.NoError(t, err)

	require.Len(t, violations, 2)

	// Check that we have violations for the right tables
	tableNames := make(map[string]bool)
	for _, v := range violations {
		tableNames[v.Location.Table] = true
	}
	require.True(t, tableNames["users"])
	require.True(t, tableNames["orders"])
	require.False(t, tableNames["products"])
}

func TestRunLinters_LintOnlyChanges_NoChanges(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
	}
	Register(linter)

	// No changes provided
	violations, err := RunLinters(nil, nil, Config{
		LintOnlyChanges: true,
	})
	require.NoError(t, err)

	// With no changes, no violations should be returned
	require.Empty(t, violations)
}

func TestRunLinters_LintOnlyChanges_ViolationWithoutLocation(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation without location",
			Location: nil, // No location
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
	}
	Register(linter)

	sql := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	changes, err := statement.New(sql)
	require.NoError(t, err)

	// With LintOnlyChanges=true, violations without location should be filtered out
	violations, err := RunLinters(nil, changes, Config{
		LintOnlyChanges: true,
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "Violation on users table", violations[0].Message)
}

// IgnoreTables tests

func TestRunLinters_IgnoreTables_Empty(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
	}
	Register(linter)

	// With empty IgnoreTables, all violations should be returned
	violations, err := RunLinters(nil, nil, Config{
		IgnoreTables: map[string]bool{},
	})
	require.NoError(t, err)

	require.Len(t, violations, 2)
}

func TestRunLinters_IgnoreTables_SingleTable(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
	}
	Register(linter)

	// Ignore violations on users table
	violations, err := RunLinters(nil, nil, Config{
		IgnoreTables: map[string]bool{
			"users": true,
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "Violation on orders table", violations[0].Message)
	require.Equal(t, "orders", violations[0].Location.Table)
}

func TestRunLinters_IgnoreTables_MultipleTables(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on products table",
			Location: &Location{Table: "products"},
		},
	}
	Register(linter)

	// Ignore violations on users and products tables
	violations, err := RunLinters(nil, nil, Config{
		IgnoreTables: map[string]bool{
			"users":    true,
			"products": true,
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "Violation on orders table", violations[0].Message)
	require.Equal(t, "orders", violations[0].Location.Table)
}

func TestRunLinters_IgnoreTables_AllTables(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
	}
	Register(linter)

	// Ignore all tables
	violations, err := RunLinters(nil, nil, Config{
		IgnoreTables: map[string]bool{
			"users":  true,
			"orders": true,
		},
	})
	require.NoError(t, err)

	require.Empty(t, violations)
}

func TestRunLinters_IgnoreTables_ViolationWithoutLocation(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation without location",
			Location: nil, // No location
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
	}
	Register(linter)

	// Ignore users table
	violations, err := RunLinters(nil, nil, Config{
		IgnoreTables: map[string]bool{
			"users": true,
		},
	})
	require.NoError(t, err)

	// Violations without location should be kept
	require.Len(t, violations, 1)
	require.Equal(t, "Violation without location", violations[0].Message)
	require.Nil(t, violations[0].Location)
}

func TestRunLinters_IgnoreTables_FalseValue(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
	}
	Register(linter)

	// Set users to false (should not be ignored)
	violations, err := RunLinters(nil, nil, Config{
		IgnoreTables: map[string]bool{
			"users": false,
		},
	})
	require.NoError(t, err)

	// Both violations should be returned since users is not truly ignored
	require.Len(t, violations, 2)
}

// Combined tests for LintOnlyChanges and IgnoreTables

func TestRunLinters_LintOnlyChanges_And_IgnoreTables(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on products table",
			Location: &Location{Table: "products"},
		},
	}
	Register(linter)

	// Changes affect users and orders
	sql := `
		ALTER TABLE users ADD COLUMN email VARCHAR(255);
		ALTER TABLE orders ADD COLUMN total DECIMAL(10,2);
	`
	changes, err := statement.New(sql)
	require.NoError(t, err)

	// LintOnlyChanges=true (filter to users and orders)
	// IgnoreTables ignores orders
	// Result: only users violation should remain
	violations, err := RunLinters(nil, changes, Config{
		LintOnlyChanges: true,
		IgnoreTables: map[string]bool{
			"orders": true,
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "Violation on users table", violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
}

func TestRunLinters_LintOnlyChanges_And_IgnoreTables_NoResults(t *testing.T) {
	resetForTest(t)

	linter := &mockLinter{name: "test_linter"}
	linter.violations = []Violation{
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on users table",
			Location: &Location{Table: "users"},
		},
		{
			Linter:   linter,
			Severity: SeverityWarning,
			Message:  "Violation on orders table",
			Location: &Location{Table: "orders"},
		},
	}
	Register(linter)

	// Changes affect only users
	sql := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	changes, err := statement.New(sql)
	require.NoError(t, err)

	// LintOnlyChanges=true (filter to users only)
	// IgnoreTables ignores users
	// Result: no violations should remain
	violations, err := RunLinters(nil, changes, Config{
		LintOnlyChanges: true,
		IgnoreTables: map[string]bool{
			"users": true,
		},
	})
	require.NoError(t, err)

	require.Empty(t, violations)
}
