package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock linter for testing
type mockLinter struct {
	name        string
	description string
	violations  []Violation
}

func (m *mockLinter) String() string {
	//TODO implement me
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
	//TODO implement me
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

func TestRegister(t *testing.T) {
	// Reset registry before test
	Reset()

	linter := &mockLinter{
		name:        "test_linter",
		description: "A test linter",
	}

	Register(linter)

	// Verify linter was registered
	names := List()
	assert.Contains(t, names, "test_linter")

	// Verify we can get it back
	retrieved, err := Get("test_linter")
	require.NoError(t, err)
	assert.Equal(t, "test_linter", retrieved.Name())
}

func TestRegisterMultiple(t *testing.T) {
	Reset()

	linter1 := &mockLinter{name: "linter1"}
	linter2 := &mockLinter{name: "linter2"}
	linter3 := &mockLinter{name: "linter3"}

	Register(linter1)
	Register(linter2)
	Register(linter3)

	names := List()
	assert.Len(t, names, 3)
	assert.Contains(t, names, "linter1")
	assert.Contains(t, names, "linter2")
	assert.Contains(t, names, "linter3")
}

func TestEnableDisable(t *testing.T) {
	Reset()

	linter := &mockLinter{name: "test_linter"}
	Register(linter)

	// Linters are enabled by default
	assert.True(t, linters["test_linter"].enabled)

	// Disable it
	err := Disable("test_linter")
	require.NoError(t, err)
	assert.False(t, linters["test_linter"].enabled)

	// Enable it again
	err = Enable("test_linter")
	require.NoError(t, err)
	assert.True(t, linters["test_linter"].enabled)
}

func TestEnableDisableNonexistent(t *testing.T) {
	Reset()

	err := Enable("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	err = Disable("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGet(t *testing.T) {
	Reset()

	linter := &mockLinter{
		name:        "test_linter",
		description: "A test linter",
	}
	Register(linter)

	retrieved, err := Get("test_linter")
	require.NoError(t, err)
	assert.Equal(t, "test_linter", retrieved.Name())
	assert.Equal(t, "A test linter", retrieved.Description())
}

func TestGetNonexistent(t *testing.T) {
	Reset()

	_, err := Get("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestRunLinters_Empty(t *testing.T) {
	Reset()

	violations, err := RunLinters(nil, nil, Config{})
	require.NoError(t, err)
	assert.Empty(t, violations)
}

func TestRunLinters_SingleLinter(t *testing.T) {
	Reset()

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
	assert.Len(t, violations, 1)
	assert.Equal(t, "test_linter", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "Test error", violations[0].Message)
}

func TestRunLinters_MultipleLinters(t *testing.T) {
	Reset()

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
	assert.Len(t, violations, 3)
}

func TestRunLinters_WithConfig_Disabled(t *testing.T) {
	Reset()

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

	assert.Empty(t, violations)
}

func TestRunLinters_WithConfig_Enabled(t *testing.T) {
	Reset()

	linter := &mockLinter{
		name: "test_linter",
	}
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityWarning, Message: "Should see this"},
	}

	// Disable by default
	Register(linter)
	require.NoError(t, Disable("test_linter"))

	// But explicitly enable via config
	violations, err := RunLinters(nil, nil, Config{
		Enabled: map[string]bool{
			"test_linter": true,
		},
	})
	require.NoError(t, err)

	assert.Len(t, violations, 1)
	assert.Equal(t, "Should see this", violations[0].Message)
}

func TestRunLinters_ConfigurableLinter(t *testing.T) {
	Reset()

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

	assert.Len(t, violations, 1)
	assert.True(t, linter.configCalled)
	// User config should be merged with defaults
	expected := map[string]string{
		"default": "value", // from DefaultConfig
		"key":     "value", // from user config
	}
	assert.Equal(t, expected, linter.configValue)
}

func TestRunLinters_ConfigurableLinter_NoConfig(t *testing.T) {
	Reset()

	linter := &mockConfigurableLinter{}
	linter.name = "configurable_linter"
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityWarning, Message: "Test"},
	}
	Register(linter)

	violations, err := RunLinters(nil, nil, Config{})
	require.NoError(t, err)

	assert.Len(t, violations, 1)
	// Now Configure is always called (with defaults)
	assert.True(t, linter.configCalled)
	// Should have received the default config
	assert.Equal(t, map[string]string{"default": "value"}, linter.configValue)
}

func TestHasErrors(t *testing.T) {
	violations := []Violation{
		{Severity: SeverityWarning},
		{Severity: SeverityWarning},
	}
	// All linters now use SeverityWarning, so HasErrors should return false
	assert.False(t, HasErrors(violations))

	// Even adding more warnings shouldn't make HasErrors return true
	violations = append(violations, Violation{Severity: SeverityWarning})
	assert.False(t, HasErrors(violations))
}

func TestHasWarnings(t *testing.T) {
	violations := []Violation{
		{Severity: SeverityWarning},
		{Severity: SeverityWarning},
	}
	// All violations are warnings, so HasWarnings should return true
	assert.True(t, HasWarnings(violations))

	violations = append(violations, Violation{Severity: SeverityWarning})
	assert.True(t, HasWarnings(violations))
}

func TestFilterBySeverity(t *testing.T) {
	violations := []Violation{
		{Severity: SeverityWarning, Message: "Warning 1"},
		{Severity: SeverityWarning, Message: "Warning 2"},
		{Severity: SeverityWarning, Message: "Warning 3"},
		{Severity: SeverityWarning, Message: "Warning 4"},
	}

	// All violations are warnings now
	warnings := FilterBySeverity(violations, SeverityWarning)
	assert.Len(t, warnings, 4)
	assert.Equal(t, "Warning 1", warnings[0].Message)
	assert.Equal(t, "Warning 2", warnings[1].Message)

	// No errors exist
	errors := FilterBySeverity(violations, SeverityError)
	assert.Empty(t, errors)

	// No info exist
	infos := FilterBySeverity(violations, SeverityInfo)
	assert.Empty(t, infos)
}

func TestFilterByLinter(t *testing.T) {
	linter1 := &mockLinter{name: "linter1"}
	linter2 := &mockLinter{name: "linter2"}

	violations := []Violation{
		{Linter: linter1, Message: "Message 1"},
		{Linter: linter2, Message: "Message 2"},
		{Linter: linter1, Message: "Message 3"},
	}

	linter1Violations := FilterByLinter(violations, "linter1")
	assert.Len(t, linter1Violations, 2)
	assert.Equal(t, "Message 1", linter1Violations[0].Message)
	assert.Equal(t, "Message 3", linter1Violations[1].Message)

	linter2Violations := FilterByLinter(violations, "linter2")
	assert.Len(t, linter2Violations, 1)
	assert.Equal(t, "Message 2", linter2Violations[0].Message)

	nonexistentViolations := FilterByLinter(violations, "nonexistent")
	assert.Empty(t, nonexistentViolations)
}

func TestListSorted(t *testing.T) {
	Reset()

	// Register in non-alphabetical order
	Register(&mockLinter{name: "zebra"})
	Register(&mockLinter{name: "alpha"})
	Register(&mockLinter{name: "beta"})

	names := List()
	assert.Equal(t, []string{"alpha", "beta", "zebra"}, names)
}

func TestReset(t *testing.T) {
	Reset()

	Register(&mockLinter{name: "linter1"})
	Register(&mockLinter{name: "linter2"})

	assert.Len(t, List(), 2)

	Reset()

	assert.Empty(t, List())
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

	assert.Equal(t, "test_table", violation.Location.Table)
	assert.Equal(t, "test_column", *violation.Location.Column)
	assert.Equal(t, "test_index", *violation.Location.Index)
	assert.Equal(t, "test_constraint", *violation.Location.Constraint)
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

	assert.NotNil(t, violation.Suggestion)
	assert.Equal(t, "Try this instead", *violation.Suggestion)
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

	assert.Len(t, violation.Context, 2)
	assert.Equal(t, "value1", violation.Context["key1"])
	assert.Equal(t, 42, violation.Context["key2"])
}

// ConfigBool tests

func TestConfigBool_ValidTrue(t *testing.T) {
	tests := []string{"true", "TRUE", "True", "TrUe"}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			result, err := ConfigBool(value, "testKey")
			require.NoError(t, err)
			assert.True(t, result)
		})
	}
}

func TestConfigBool_ValidFalse(t *testing.T) {
	tests := []string{"false", "FALSE", "False", "FaLsE"}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			result, err := ConfigBool(value, "testKey")
			require.NoError(t, err)
			assert.False(t, result)
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
			assert.False(t, result)
			assert.Contains(t, err.Error(), "invalid value for "+tt.key)
			assert.Contains(t, err.Error(), tt.value)
			assert.Contains(t, err.Error(), "expected 'true' or 'false'")
		})
	}
}

// DefaultConfig tests

func TestRunLinters_AppliesDefaultConfig(t *testing.T) {
	Reset()
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Run without any config - should apply default (raiseError=false)
	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	// Default raiseError is "false", so severity should be Warning
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestRunLinters_UserConfigOverridesDefault(t *testing.T) {
	Reset()
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Override default raiseError=false with true
	violations, err := RunLinters(nil, stmts, Config{
		Settings: map[string]map[string]string{
			"invisible_index_before_drop": {
				"raiseError": "true",
			},
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	// User set raiseError=true, so severity should be Error
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}
