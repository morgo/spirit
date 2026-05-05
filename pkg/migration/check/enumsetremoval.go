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

// enumSetRemovalCheck prevents unsafe ENUM/SET type conversions.
//
// This covers two cases:
//
// 1. ENUM/SET → non-ENUM/SET (e.g., ENUM → VARCHAR): the binlog replay path
// (bufferedMap) receives ENUM/SET values as integer ordinals/bitmasks from
// the go-mysql library. If the target column has been changed to a
// non-ENUM/SET type, those integers are inserted as literal values instead
// of the original strings, causing data corruption.
//
// 2. ENUM ↔ SET cross-conversions: ENUM uses ordinal integers while SET uses
// bitmask integers. Converting between them would misinterpret the integer
// encoding, causing data corruption.
func enumSetRemovalCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
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
			return fmt.Errorf("unsafe %s to non-%s type conversion on column %q is not supported. "+
				"The binlog replay path uses integer encodings for %s values (ENUM ordinals / SET bitmasks), "+
				"so converting to %s would insert those integer encodings instead of the original string values. "+
				"Keep the column as %s type",
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
			return fmt.Errorf("unsafe %s to %s type conversion on column %q is not supported. "+
				"ENUM values are replayed as ordinal integers and SET values as bitmask integers. "+
				"Converting between them would misinterpret the encoding, causing data corruption. "+
				"Avoid converting between ENUM and SET",
				oldName, newName, col.LookupName)
		}
	}
	return nil
}
