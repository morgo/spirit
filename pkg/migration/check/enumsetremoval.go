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

// enumSetRemovalCheck prevents unsafe ENUM/SET type conversions in buffered mode.
//
// This covers two cases:
//
// 1. ENUM/SET → non-ENUM/SET (e.g., ENUM → VARCHAR): In buffered mode, the
// binlog replay path (bufferedMap) receives ENUM/SET values as integer
// ordinals/bitmasks from the go-mysql library. If the target column has been
// changed to a non-ENUM/SET type, those integers are inserted as literal
// values instead of the original strings, causing data corruption.
//
// 2. ENUM ↔ SET cross-conversions: ENUM uses ordinal integers while SET uses
// bitmask integers. Converting between them in buffered mode would
// misinterpret the integer encoding, causing data corruption.
//
// The unbuffered path is safe because it uses REPLACE INTO ... SELECT
// (SQL-level string operations) which correctly handles conversions.
func enumSetRemovalCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if !r.Buffered {
		return nil // only applies to buffered mode
	}

	for _, col := range findModifiedColumns(*r.Statement.StmtNode) {
		newTp := col.ColDef.Tp.GetType()

		existingType, ok := r.Table.GetColumnMySQLType(col.LookupName)
		if !ok {
			// For columns that don't exist yet (e.g., ADD COLUMN parsed as
			// MODIFY), there's no existing type to validate against.
			continue
		}

		if !isEnumOrSetType(existingType) {
			continue // existing column is not ENUM/SET; nothing to protect
		}

		// Case 1: ENUM/SET → non-ENUM/SET
		if newTp != mysql.TypeEnum && newTp != mysql.TypeSet {
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

		// Case 2: ENUM ↔ SET cross-conversion
		existingIsSet := isSetType(existingType)
		newIsSet := newTp == mysql.TypeSet
		if existingIsSet != newIsSet {
			oldName := "ENUM"
			newName := "SET"
			if existingIsSet {
				oldName = "SET"
				newName = "ENUM"
			}
			return fmt.Errorf("unsafe %s to %s type conversion on column %q is not supported in buffered mode. "+
				"In buffered mode, ENUM values are replayed as ordinal integers and SET values as bitmask integers. "+
				"Converting between them would misinterpret the encoding, causing data corruption. "+
				"Either use unbuffered mode (the default) or avoid converting between ENUM and SET",
				oldName, newName, col.LookupName)
		}
	}
	return nil
}
