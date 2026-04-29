package check

import (
	"context"
	"fmt"
	"log/slog"
)

func init() {
	registerCheck("createtablename", createTableNameCheck, ScopePreflight)
}

// createTableNameCheck validates that CREATE TABLE statements do not exceed
// the maximum table name length that Spirit can manage. This is MySQL's
// 64-character limit minus the longest Spirit metadata suffix (e.g. _<table>_chkpnt),
// ensuring that a future ALTER TABLE via Spirit will be possible.
func createTableNameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Statement == nil || !r.Statement.IsCreateTable() {
		return nil
	}
	tableName := r.Statement.Table
	if len(tableName) > MaxMigratableTableNameLength {
		return fmt.Errorf("table name %q exceeds the maximum length of %d characters that Spirit can manage", tableName, MaxMigratableTableNameLength)
	}
	return nil
}
