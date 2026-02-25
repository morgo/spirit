package check

import (
	"context"
	"fmt"
	"log/slog"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("enumReorder", enumReorderCheck, ScopePreflight)
}

// enumReorderCheck prevents ENUM value reordering in buffered mode.
//
// In buffered mode, the binlog replay path (bufferedMap) receives ENUM values
// as integer ordinals from the go-mysql library. When these ordinals are inserted
// into the target table via UpsertRows, MySQL interprets them as positions in the
// target table's ENUM definition. If the ENUM values have been reordered, ordinal N
// in the source maps to a different string value in the target, causing data corruption.
//
// The unbuffered path is safe because it uses REPLACE INTO ... SELECT (SQL-level
// string operations) which correctly maps ENUM string values.
//
// Adding new ENUM values at the end of the list is always safe. The new list must
// start with the existing values as a prefix.
func enumReorderCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if !r.Buffered {
		return nil // only applies to buffered mode
	}

	for _, col := range findModifiedEnumSetColumns(*r.Statement.StmtNode) {
		if col.IsSet {
			continue // handled by setReorderCheck
		}

		newElems := col.ColDef.Tp.GetElems()
		if len(newElems) == 0 {
			continue
		}

		existingType, ok := r.Table.GetColumnMySQLType(col.LookupName)
		if !ok {
			return fmt.Errorf("unable to validate ENUM reorder for column %q in buffered mode: existing column type not found in table metadata", col.LookupName)
		}

		existingElems := parseEnumSetValues(existingType)
		if len(existingElems) == 0 {
			continue
		}

		if !isPrefix(existingElems, newElems) {
			return fmt.Errorf("unsafe ENUM value reorder on column %q is not supported in buffered mode. "+
				"In buffered mode, the binlog replay path uses integer ordinals for ENUM values, "+
				"so reordering causes data corruption. Either use unbuffered mode (the default) or only "+
				"add new values at the end of the list",
				col.LookupName)
		}
	}
	return nil
}
