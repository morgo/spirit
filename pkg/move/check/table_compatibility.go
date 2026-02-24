package check

import (
	"context"
	"fmt"
	"log/slog"
)

func init() {
	registerCheck("table_compatibility", tableCompatibilityCheck, ScopePostSetup)
}

// tableCompatibilityCheck verifies that all source tables are compatible
// with move operations. This includes:
// - All tables must have a primary key (required for replication tracking)
// - All primary keys must be memory-comparable (required for buffered map subscription)
//
// The memory-comparable requirement exists because move operations always use
// the bufferedMap replication subscription, which requires binary-comparable
// primary keys for its internal map. Tables with non-memory-comparable PKs
// (e.g., VARCHAR, TEXT) would silently fall back to deltaQueue, which does
// not work in move flows because newTable is nil.
//
// TODO: support non-memory-comparable PKs
// See: https://github.com/block/spirit/issues/607
func tableCompatibilityCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for _, tbl := range r.SourceTables {
		if len(tbl.KeyColumns) == 0 {
			return fmt.Errorf("table '%s' does not have a primary key, which is required for move operations", tbl.TableName)
		}
		if err := tbl.PrimaryKeyIsMemoryComparable(); err != nil {
			return fmt.Errorf("table '%s' has a non-memory-comparable primary key (e.g. VARCHAR), which is not yet supported for move operations (see https://github.com/block/spirit/issues/607)", tbl.TableName)
		}
	}
	return nil
}
