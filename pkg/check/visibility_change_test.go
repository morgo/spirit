package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVisibilityChange(t *testing.T) {
	testCheck := func(stmt string, shouldError bool, expectedError string) {
		r := Resources{Statement: statement.MustNew("ALTER TABLE t1 " + stmt)[0]}
		err := VisibilityCheck(t.Context(), r, nil)
		if shouldError {
			require.ErrorContains(t, err, expectedError)
		} else {
			require.NoError(t, err)
		}
	}

	testDirect := func(stmt string, shouldError bool, expectedError string) {
		err := AlterContainsIndexVisibility(statement.MustNew("ALTER TABLE t1 " + stmt)[0])
		if shouldError {
			require.ErrorContains(t, err, expectedError)
		} else {
			require.NoError(t, err)
		}
	}

	// Test both check function and direct function with same data
	tests := []struct {
		stmt        string
		shouldError bool
		errorMsg    string
		desc        string
	}{
		// Safe operations (original logic was more permissive)
		{"DROP COLUMN foo", false, "", "regular DDL"},
		{"ALTER INDEX idx1 VISIBLE", false, "", "pure visibility change"},
		{"ALTER INDEX idx1 INVISIBLE", false, "", "pure visibility change"},
		{"ALTER INDEX idx1 VISIBLE, drop index old", false, "", "visibility + metadata"},
		{"drop index a", false, "", "drop index"},
		{"modify a varchar(100)", false, "", "varchar modification"},

		// Unsafe operations (blocks visibility mixed with table-rebuilding)
		{"ALTER INDEX idx1 VISIBLE, ADD COLUMN col INT", true, "mixed with table-rebuilding operations", "visibility + add column"},
		{"ALTER INDEX b INVISIBLE, add unique(e)", true, "mixed with table-rebuilding operations", "visibility + unique"},
		{"ALTER INDEX b INVISIBLE, modify a int", true, "mixed with table-rebuilding operations", "visibility + non-varchar"},
	}

	for _, tt := range tests {
		t.Run("check_"+tt.desc, func(t *testing.T) {
			testCheck(tt.stmt, tt.shouldError, tt.errorMsg)
		})
		t.Run("direct_"+tt.desc, func(t *testing.T) {
			testDirect(tt.stmt, tt.shouldError, tt.errorMsg)
		})
	}

	// Test non-ALTER statements (only for direct function)
	assert.Equal(t, statement.ErrNotAlterTable,
		AlterContainsIndexVisibility(statement.MustNew("CREATE TABLE t1 (id INT)")[0]))
}
