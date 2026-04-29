package check

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("createtablename", createTableNameCheck, ScopePreflight)
}

// createTableNameCheck validates that CREATE TABLE statements do not exceed
// MySQL's maximum table name length of 64 characters.
func createTableNameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Statement == nil || !r.Statement.IsCreateTable() {
		return nil
	}
	tableName := r.Statement.Table
	if len(tableName) > utils.MaxTableNameLength {
		return fmt.Errorf("table name %q exceeds MySQL's maximum length of %d characters", tableName, utils.MaxTableNameLength)
	}
	return nil
}
