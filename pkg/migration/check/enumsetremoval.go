package check

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("enumSetRemoval", enumSetRemovalCheck, ScopePreflight)
}

// enumSetRemovalCheck prevents converting ENUM/SET columns to non-ENUM/SET
// types in buffered mode.
//
// In buffered mode, the binlog replay path (bufferedMap) receives ENUM/SET
// values as integer ordinals from the go-mysql library. If the target column
// has been changed to a non-ENUM/SET type (e.g., VARCHAR), those ordinals are
// inserted as literal integers ("1", "2", "3") instead of the original string
// values, causing data corruption.
//
// The unbuffered path is safe because it uses REPLACE INTO ... SELECT
// (SQL-level string operations) which correctly handles the conversion.
func enumSetRemovalCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if !r.Buffered {
		return nil // only applies to buffered mode
	}

	cols := findModifiedEnumSetToNonEnumSetColumns(r)
	if len(cols) == 0 {
		return nil
	}
	col := cols[0]
	typeName := "ENUM"
	if strings.HasPrefix(strings.ToLower(col.existingType), "set(") {
		typeName = "SET"
	}
	return fmt.Errorf("unsafe %s to non-%s type conversion on column %q is not supported in buffered mode. "+
		"In buffered mode, the binlog replay path uses integer ordinals for %s values, "+
		"so converting to %s would insert ordinal numbers instead of the original string values. "+
		"Either use unbuffered mode (the default) or keep the column as %s type",
		typeName, typeName, col.lookupName, typeName, col.newType, typeName)
}

// modifiedEnumSetToNonEnumSetColumn describes a column that is being converted
// from an ENUM/SET type to a non-ENUM/SET type.
type modifiedEnumSetToNonEnumSetColumn struct {
	lookupName   string // column name in the existing table
	existingType string // the existing MySQL type (e.g., "enum('a','b')")
	newType      string // description of the new type for error messages
}

// findModifiedEnumSetToNonEnumSetColumns finds MODIFY/CHANGE COLUMN specs
// where the existing column is ENUM/SET but the new column type is not.
func findModifiedEnumSetToNonEnumSetColumns(r Resources) []modifiedEnumSetToNonEnumSetColumn {
	if r.Statement == nil || r.Statement.StmtNode == nil {
		return nil
	}
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return nil
	}

	var result []modifiedEnumSetToNonEnumSetColumn
	for _, spec := range alterStmt.Specs {
		var colDef *ast.ColumnDef
		var lookupName string

		switch spec.Tp { //nolint: exhaustive
		case ast.AlterTableModifyColumn:
			if len(spec.NewColumns) > 0 {
				colDef = spec.NewColumns[0]
				lookupName = colDef.Name.Name.O
			}
		case ast.AlterTableChangeColumn:
			if len(spec.NewColumns) > 0 {
				colDef = spec.NewColumns[0]
				if spec.OldColumnName != nil {
					lookupName = spec.OldColumnName.Name.O
				} else {
					lookupName = colDef.Name.Name.O
				}
			}
		}

		if colDef == nil || colDef.Tp == nil {
			continue
		}

		// Only interested in cases where the NEW type is NOT ENUM/SET.
		newTp := colDef.Tp.GetType()
		if newTp == mysql.TypeEnum || newTp == mysql.TypeSet {
			continue
		}

		// Check if the EXISTING column is ENUM/SET.
		if r.Table == nil {
			continue
		}
		existingType, ok := r.Table.GetColumnMySQLType(lookupName)
		if !ok {
			continue
		}
		if !isEnumOrSetType(existingType) {
			continue
		}

		result = append(result, modifiedEnumSetToNonEnumSetColumn{
			lookupName:   lookupName,
			existingType: existingType,
			newType:      colDef.Tp.String(),
		})
	}
	return result
}
