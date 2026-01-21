package check

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
)

func init() {
	registerCheck("source_modified", sourceModifiedCheck, ScopeCutover)
}

// sourceModifiedCheck validates that no new tables have been created on the source
// database during the move operation. This check runs at ScopeCutover (just before cutover)
// to ensure the source database state hasn't changed in ways that would make the move incomplete.
//
// The check compares the current list of tables on the source with the list of tables
// that were discovered at the start of the move operation (stored in r.SourceTables).
// If new tables are detected, the move operation should fail to prevent data loss.
func sourceModifiedCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.SourceTables) == 0 {
		// No source tables to validate against, skip check
		return nil
	}

	// Build a map of known source tables for quick lookup
	knownTables := make(map[string]bool)
	for _, tbl := range r.SourceTables {
		knownTables[tbl.TableName] = true
	}

	// Query current tables from source
	rows, err := r.SourceDB.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return fmt.Errorf("failed to query source tables: %w", err)
	}
	defer rows.Close()

	var tableName string
	var currentTables []string
	var newTables []string

	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name from source: %w", err)
		}

		// Skip checkpoint and sentinel tables as they are internal to spirit
		if tableName == "_spirit_checkpoint" || tableName == "_spirit_sentinel" {
			continue
		}

		currentTables = append(currentTables, tableName)

		// Check if this is a new table that wasn't present at the start
		if !knownTables[tableName] {
			newTables = append(newTables, tableName)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating source tables: %w", err)
	}

	// If new tables were detected, fail the move operation
	if len(newTables) > 0 {
		slices.Sort(newTables) // Sort for consistent error messages
		return fmt.Errorf("new table(s) detected on source database during move operation: %v. This indicates the source database schema has changed. The move operation cannot proceed as it would be incomplete. Please restart the move operation to include all tables", newTables)
	}

	logger.Debug("source modified check passed",
		"known_tables", len(r.SourceTables),
		"current_tables", len(currentTables))

	return nil
}
