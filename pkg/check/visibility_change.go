package check

import (
	"context"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/validation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("visibility", VisibilityCheck, ScopePostSetup)
}

// VisibilityCheck validates index visibility changes in ALTER statements.
// It allows index visibility changes when mixed with other metadata-only operations,
// but blocks them when mixed with table-rebuilding operations to avoid semantic issues.
// It likely means the user is combining this operation with other unsafe operations,
// which is not a good idea. We need to protect them by not allowing it.
// https://github.com/block/spirit/issues/283
func VisibilityCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	return AlterContainsIndexVisibility(r.Statement)
}

// AlterContainsIndexVisibility checks to see if there are any clauses of an ALTER to change index visibility.
// Allows index visibility changes when mixed with other metadata-only operations,
// but blocks them when mixed with table-rebuilding operations to avoid semantic issues.
func AlterContainsIndexVisibility(stmt *statement.AbstractStatement) error {
	alterStmt, ok := (*stmt.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return statement.ErrNotAlterTable
	}
	return validation.AlterContainsIndexVisibilityLogic(alterStmt)
}
