package validation

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
)

// Helper function to parse ALTER TABLE statements for testing
func parseAlterStatement(sql string) (*ast.AlterTableStmt, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	if len(stmtNodes) != 1 {
		return nil, err
	}
	alterStmt, ok := stmtNodes[0].(*ast.AlterTableStmt)
	if !ok {
		return nil, err
	}
	return alterStmt, nil
}

func TestAlterContainsIndexVisibilityLogic(t *testing.T) {
	var test = func(stmt string) error {
		alterStmt, err := parseAlterStatement("ALTER TABLE `t1` " + stmt)
		if err != nil {
			return err
		}
		return AlterContainsIndexVisibilityLogic(alterStmt)
	}

	// Pure metadata operations should be safe (no index visibility)
	assert.NoError(t, test("drop index `a`"))
	assert.NoError(t, test("rename index `a` to `b`"))
	assert.NoError(t, test("drop index `a`, drop index `b`"))
	assert.NoError(t, test("drop index `a`, rename index `b` to c"))
	assert.NoError(t, test("drop partition `p1`"))
	assert.NoError(t, test("truncate partition `p1`"))
	assert.NoError(t, test("add partition (partition `p1` values less than (100))"))

	// Pure table-rebuilding operations should be safe (no index visibility)
	assert.NoError(t, test("ADD COLUMN `a` INT"))
	assert.NoError(t, test("ADD index (a)"))
	assert.NoError(t, test("drop index `a`, add index `b` (`b`)"))
	assert.NoError(t, test("engine=innodb"))
	assert.NoError(t, test("add unique(b)"))
	assert.NoError(t, test("modify `a` int"))
	assert.NoError(t, test("change column `a` `a` int"))

	// Pure index visibility operations should be safe
	assert.NoError(t, test("ALTER INDEX b INVISIBLE"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE"))

	// Index visibility mixed with metadata-only operations should be safe
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, drop index `c`"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE, rename index `a` to `new_a`"))
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, drop partition `p1`"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE, truncate partition `p2`"))
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, add partition (partition `p3` values less than (200))"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE, modify `a` varchar(100)"))
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, change column `a` `a` varchar(150)"))

	// Index visibility mixed with table-rebuilding operations should fail
	assert.Error(t, test("ALTER INDEX b INVISIBLE, ADD COLUMN `c` INT"))
	assert.Error(t, test("ALTER INDEX b VISIBLE, ADD index (d)"))
	assert.Error(t, test("ALTER INDEX b INVISIBLE, engine=innodb"))
	assert.Error(t, test("ALTER INDEX b VISIBLE, add unique(e)"))
	assert.Error(t, test("ALTER INDEX b INVISIBLE, modify `a` int"))
	assert.Error(t, test("ALTER INDEX b VISIBLE, change column `a` `a` int"))

	// Multiple index visibility changes with mixed operations
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, ALTER INDEX c VISIBLE, drop index `d`"))          // All metadata-only
	assert.Error(t, test("ALTER INDEX b INVISIBLE, ALTER INDEX c VISIBLE, ADD COLUMN `e` INT"))        // Mixed with table-rebuilding
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, ALTER INDEX c VISIBLE, modify `a` varchar(200)")) // VARCHAR is metadata-only
	assert.Error(t, test("ALTER INDEX b INVISIBLE, ALTER INDEX c VISIBLE, modify `a` text"))           // TEXT is table-rebuilding
}

func TestAlterContainsIndexVisibilityLogic_ErrorMessages(t *testing.T) {
	var test = func(stmt string) error {
		alterStmt, err := parseAlterStatement("ALTER TABLE `t1` " + stmt)
		if err != nil {
			return err
		}
		return AlterContainsIndexVisibilityLogic(alterStmt)
	}

	// Test specific error message for mixed index visibility and table-rebuilding operations
	err := test("ALTER INDEX b INVISIBLE, ADD COLUMN `c` INT")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the ALTER operation contains a change to index visibility mixed with table-rebuilding operations")
	assert.Contains(t, err.Error(), "Please split the ALTER statement into separate statements")
}
