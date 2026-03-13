package check

import (
	"context"
	"fmt"
	"log/slog"

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

	// Look at all MODIFY/CHANGE columns where the new type is NOT ENUM/SET,
	// and check whether the existing column IS ENUM/SET.
	for _, col := range findModifiedColumns(*r.Statement.StmtNode) {
		newTp := col.ColDef.Tp.GetType()
		if newTp == mysql.TypeEnum || newTp == mysql.TypeSet {
			continue // new type is still ENUM/SET, handled by reorder checks
		}

		existingType, ok := r.Table.GetColumnMySQLType(col.LookupName)
		if !ok {
			continue
		}
		if !isEnumOrSetType(existingType) {
			continue
		}

		typeName := "ENUM"
		if isSetType(existingType) {
			typeName = "SET"
		}
		return fmt.Errorf("unsafe %s to non-%s type conversion on column %q is not supported in buffered mode. "+
			"In buffered mode, the binlog replay path uses integer ordinals for %s values, "+
			"so converting to %s would insert ordinal numbers instead of the original string values. "+
			"Either use unbuffered mode (the default) or keep the column as %s type",
			typeName, typeName, col.LookupName, typeName, col.ColDef.Tp.String(), typeName)
	}
	return nil
}
