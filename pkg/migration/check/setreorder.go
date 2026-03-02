package check

import (
	"context"
	"fmt"
	"log/slog"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("setReorder", setReorderCheck, ScopePreflight)
}

// setReorderCheck prevents SET value reordering in all modes.
//
// MySQL outputs SET member strings in the order they are defined in the column
// definition. When SET members are reordered, the string representation of
// multi-member values changes (e.g. "read,execute" becomes "execute,read"),
// even though the actual data is correct. This causes Spirit's CRC32 checksum
// to fail because it compares CAST(col AS char) representations, which differ
// between source and target tables. The migration would always fail at the
// checksum phase.
//
// Since checksums are compulsory: it's better to fast-fail than get to the end
// and find out that the data is "corrupted".
//
// Adding new SET values at the end of the list is always safe. The new list must
// start with the existing values as a prefix.
func setReorderCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for _, col := range findModifiedEnumSetColumns(*r.Statement.StmtNode) {
		if !col.IsSet {
			continue // handled by enumReorderCheck
		}

		newElems := col.ColDef.Tp.GetElems()
		if len(newElems) == 0 {
			continue
		}

		existingType, ok := r.Table.GetColumnMySQLType(col.LookupName)
		if !ok {
			return fmt.Errorf("unable to validate SET reorder for column %q: existing column type not found in TableInfo", col.LookupName)
		}

		existingElems, err := parseEnumSetValues(existingType)
		if err != nil {
			return fmt.Errorf("unable to validate SET reorder for column %q: %w", col.LookupName, err)
		}
		if len(existingElems) == 0 {
			continue
		}

		if !isPrefix(existingElems, newElems) {
			return fmt.Errorf("unsafe SET value reorder on column %q. "+
				"Reordering SET members changes the string representation of multi-member values "+
				"(e.g. 'read,execute' becomes 'execute,read'), which causes the checksum to fail. "+
				"Only add new values at the end of the list",
				col.LookupName)
		}
	}
	return nil
}
