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
// with move operations. The only remaining requirement is that every table
// has a primary key, which is required for replication tracking.
//
// Non-memory-comparable PKs (e.g. VARCHAR with a CI collation) are now
// supported: bufferedMap routes those subscriptions through its FIFO queue
// mode, which preserves binlog order so the target's collation-aware
// uniqueness produces the correct end state. See
// pkg/repl/subscription_buffered.go for the routing rules.
func tableCompatibilityCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for _, tbl := range r.SourceTables {
		if len(tbl.KeyColumns) == 0 {
			return fmt.Errorf("table '%s' does not have a primary key, which is required for move operations", tbl.TableName)
		}
	}
	return nil
}
