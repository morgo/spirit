// Package validation provides shared validation functions for ALTER statement analysis.
package validation

import (
	"errors"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// AlterContainsIndexVisibilityLogic checks to see if there are any clauses of an ALTER to change index visibility.
// It blocks index visibility changes when mixed with table-rebuilding operations.
func AlterContainsIndexVisibilityLogic(alterStmt *ast.AlterTableStmt) error {
	hasIndexVisibility := false
	hasNonMetadataOperation := false

	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableIndexInvisible:
			hasIndexVisibility = true
		case ast.AlterTableDropIndex,
			ast.AlterTableRenameIndex,
			ast.AlterTableDropPartition,
			ast.AlterTableTruncatePartition,
			ast.AlterTableAddPartitions,
			ast.AlterTableAlterColumn:
			// These are safe metadata-only operations, don't block
			continue
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			// Only VARCHAR modifications are metadata-only, others are table-rebuilding
			if spec.NewColumns[0].Tp != nil && spec.NewColumns[0].Tp.GetType() == mysql.TypeVarchar {
				continue // VARCHAR is metadata-only
			}
			hasNonMetadataOperation = true
		default:
			// All other operations (ADD COLUMN, ADD INDEX, ADD CONSTRAINT, ENGINE, etc) are table-rebuilding
			hasNonMetadataOperation = true
		}
	}

	// Block index visibility if mixed with any table-rebuilding operations
	if hasIndexVisibility && hasNonMetadataOperation {
		return errors.New("the ALTER operation contains a change to index visibility mixed with table-rebuilding operations. This creates semantic issues for experiments. Please split the ALTER statement into separate statements for changing the invisible index and other operations")
	}

	return nil
}
