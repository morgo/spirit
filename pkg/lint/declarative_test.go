package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// infoLinter is a test linter that produces SeverityInfo violations for any ALTER TABLE change.
type infoLinter struct{}

func (l infoLinter) Name() string        { return "test_info" }
func (l infoLinter) Description() string { return "test linter that produces info violations" }
func (l infoLinter) String() string      { return Stringer(l) }
func (l infoLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation
	for _, ch := range changes {
		if ch.IsAlterTable() {
			violations = append(violations, Violation{
				Linter:   l,
				Severity: SeverityInfo,
				Message:  "this is an informational suggestion",
				Location: &Location{Table: ch.Table},
			})
		}
	}
	return violations
}

func TestPlanChanges_NoChanges(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	plan, err := PlanChanges(current, current, nil, nil)
	require.NoError(t, err)
	assert.False(t, plan.HasChanges())
	assert.False(t, plan.HasErrors())
	assert.False(t, plan.HasWarnings())
}

func TestPlanChanges_AddColumn(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	assert.Contains(t, plan.Changes[0].Statement, "ALTER TABLE")
	assert.Equal(t, "t1", plan.Changes[0].TableName)
	// Statement should be semicolon-terminated
	assert.True(t, plan.Changes[0].Statement[len(plan.Changes[0].Statement)-1] == ';')
}

func TestPlanChanges_NewTable(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
		{Name: "t2", Schema: "CREATE TABLE t2 (id BIGINT PRIMARY KEY, val INT)"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	assert.Contains(t, plan.Changes[0].Statement, "CREATE TABLE")
	assert.Equal(t, "t2", plan.Changes[0].TableName)
}

func TestPlanChanges_DropTable(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
		{Name: "t2", Schema: "CREATE TABLE t2 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	assert.Contains(t, plan.Changes[0].Statement, "DROP TABLE")
}

func TestPlanChanges_Statements(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	stmts := plan.Statements()
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "ALTER TABLE")
}

func TestPlanChanges_WithLintViolations(t *testing.T) {
	// Create a table with a foreign key — the has_fk linter should flag it
	current := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id BIGINT PRIMARY KEY)"},
		{Name: "child", Schema: `CREATE TABLE child (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	desired := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id BIGINT PRIMARY KEY)"},
		{Name: "child", Schema: `CREATE TABLE child (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			name VARCHAR(100),
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	assert.Equal(t, "child", plan.Changes[0].TableName)
	// The has_fk linter should produce a warning for the child table
	assert.True(t, plan.HasWarnings(), "expected lint warnings for table with FK")
	assert.NotEmpty(t, plan.Changes[0].Warnings)
}

func TestPlanChanges_WithLintConfig(t *testing.T) {
	// Use a config that disables all linters — should get no violations
	current := []table.TableSchema{
		{Name: "t1", Schema: `CREATE TABLE t1 (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: `CREATE TABLE t1 (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			name VARCHAR(100),
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	cfg := &Config{
		Enabled: map[string]bool{
			"has_fk": false,
		},
	}
	plan, err := PlanChanges(current, desired, nil, cfg)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	// has_fk is disabled, so no FK warnings on the change
	for _, ch := range plan.Changes {
		for _, w := range ch.Warnings {
			assert.NotContains(t, w, "has_fk")
		}
	}
}

func TestPlanChanges_EmptySchemas(t *testing.T) {
	plan, err := PlanChanges(nil, nil, nil, nil)
	require.NoError(t, err)
	assert.False(t, plan.HasChanges())
	assert.Empty(t, plan.Statements())
}

func TestPlanChanges_WithInfos(t *testing.T) {
	// Register a test linter that produces SeverityInfo violations.
	Register(infoLinter{})
	t.Cleanup(func() {
		lock.Lock()
		delete(linters, "test_info")
		lock.Unlock()
	})

	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	assert.Equal(t, "t1", plan.Changes[0].TableName)

	// The test_info linter should produce an info for the ALTER TABLE change.
	assert.True(t, plan.HasInfos(), "expected lint infos from test_info linter")
	require.NotEmpty(t, plan.Changes[0].Infos)
	assert.Contains(t, plan.Changes[0].Infos[0], "test_info")
	assert.Contains(t, plan.Changes[0].Infos[0], "informational suggestion")

	// Infos should not appear as warnings or errors.
	assert.Empty(t, plan.Changes[0].Errors)
}

func TestPlanChanges_HasInfosFalseWhenNone(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	// No info-producing linter is registered, so HasInfos should be false.
	assert.False(t, plan.HasInfos())
	assert.Empty(t, plan.Changes[0].Infos)
}

func TestPlanChanges_InfosNotOnNonAlter(t *testing.T) {
	// Register a test linter that only produces infos for ALTER TABLE.
	Register(infoLinter{})
	t.Cleanup(func() {
		lock.Lock()
		delete(linters, "test_info")
		lock.Unlock()
	})

	// A CREATE TABLE (new table) should not trigger the info linter.
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
		{Name: "t2", Schema: "CREATE TABLE t2 (id BIGINT PRIMARY KEY, val INT)"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	assert.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	assert.Contains(t, plan.Changes[0].Statement, "CREATE TABLE")
	// The info linter only fires on ALTER, so no infos here.
	assert.False(t, plan.HasInfos())
	assert.Empty(t, plan.Changes[0].Infos)
}

func TestTerminatedStmt(t *testing.T) {
	assert.Equal(t, "SELECT 1;", terminatedStmt("SELECT 1"))
	assert.Equal(t, "SELECT 1;", terminatedStmt("SELECT 1;"))
	assert.Equal(t, "SELECT 1;", terminatedStmt("  SELECT 1  "))
	assert.Equal(t, "", terminatedStmt(""))
	assert.Equal(t, "", terminatedStmt("   "))
}
