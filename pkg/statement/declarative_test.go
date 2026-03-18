package statement

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertChangesContain checks that each expected substring appears in at least
// one of the returned statements.
func assertChangesContain(t *testing.T, changes []*AbstractStatement, expected []string) {
	t.Helper()
	strs := statementsToStrings(changes)
	for _, exp := range expected {
		found := false
		for _, s := range strs {
			if strings.Contains(s, exp) {
				found = true
				break
			}
		}
		assert.True(t, found, "expected some statement to contain %q, got %v", exp, strs)
	}
}

func statementsToStrings(changes []*AbstractStatement) []string {
	strs := make([]string, len(changes))
	for i, ch := range changes {
		strs[i] = ch.Statement
	}
	return strs
}

func TestDeclarativeToImperative(t *testing.T) {
	tests := []struct {
		name          string
		current       []table.TableSchema
		desired       []table.TableSchema
		expectedCount int
		contains      []string // substrings that must appear somewhere in the results
	}{
		{
			name: "NoChanges",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			expectedCount: 0,
		},
		{
			name: "AddColumn",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)"},
			},
			expectedCount: 1,
			contains:      []string{"ALTER TABLE `t1` ADD COLUMN"},
		},
		{
			name: "NewTable",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
				{Name: "t2", Schema: "CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR(100))"},
			},
			expectedCount: 1,
			contains:      []string{"CREATE TABLE"},
		},
		{
			name: "DropTable",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
				{Name: "t2", Schema: "CREATE TABLE t2 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			expectedCount: 1,
			contains:      []string{"DROP TABLE `t2`"},
		},
		{
			name: "MixedChanges",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
				{Name: "t2", Schema: "CREATE TABLE t2 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)"},
				{Name: "t3", Schema: "CREATE TABLE t3 (id INT PRIMARY KEY)"},
			},
			expectedCount: 3,
			contains: []string{
				"ALTER TABLE `t1`",
				"CREATE TABLE",
				"DROP TABLE `t2`",
			},
		},
		{
			name:          "EmptyCurrentAndDesired",
			current:       nil,
			desired:       nil,
			expectedCount: 0,
		},
		{
			name:    "EmptyCurrentNewTables",
			current: nil,
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			expectedCount: 1,
			contains:      []string{"CREATE TABLE"},
		},
		{
			name: "EmptyDesiredDropAll",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired:       nil,
			expectedCount: 1,
			contains:      []string{"DROP TABLE `t1`"},
		},
		{
			name: "AddIndex",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))"},
			},
			expectedCount: 1,
			contains:      []string{"ALTER TABLE `t1` ADD INDEX"},
		},
		{
			name: "DropIndex",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"},
			},
			expectedCount: 1,
			contains:      []string{"ALTER TABLE `t1` DROP INDEX"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := DeclarativeToImperative(tt.current, tt.desired, nil)
			require.NoError(t, err)

			assert.Len(t, changes, tt.expectedCount)
			if tt.contains != nil {
				assertChangesContain(t, changes, tt.contains)
			}
		})
	}
}

func TestDeclarativeToImperativeWithOptions(t *testing.T) {
	// With default options, AUTO_INCREMENT differences are ignored
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=100"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=200"},
	}

	changes, err := DeclarativeToImperative(current, desired, NewDiffOptions())
	require.NoError(t, err)
	assert.Empty(t, changes, "AUTO_INCREMENT differences should be ignored with default options")
}

func TestToTableSchema(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)")
	require.NoError(t, err)

	ts, err := ct.ToTableSchema()
	require.NoError(t, err)
	assert.Equal(t, "t1", ts.Name)
	assert.Contains(t, ts.Schema, "CREATE TABLE")
	assert.Contains(t, ts.Schema, "t1")
	assert.Contains(t, ts.Schema, "id")
	assert.Contains(t, ts.Schema, "name")
}
