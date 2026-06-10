package check

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	// The rename-safety conditions must hold for both a fresh copy and a
	// resume from checkpoint, and the runner only ever runs one of the two
	// scopes, so the same check is registered under both.
	registerCheck("rename_safety", renameSafetyCheck, ScopePostSetup)
	registerCheck("rename_safety_resume", renameSafetyCheck, ScopeResume)
}

// CutoverOldName returns the name the move cutover renames a source table
// to (`<table>_old`). Unlike the migration package's utils.OldTableName, no
// truncation is applied — renameSafetyCheck instead rejects tables whose
// _old name would exceed MySQL's identifier limit before any rows are copied.
func CutoverOldName(tableName string) string {
	return tableName + "_old"
}

// renameSafetyCheck verifies, before any rows are copied, that the final
// cutover's RENAME TABLE `<table>` TO `<table>_old` can succeed on every
// source:
//
//  1. `<table>_old` must fit within MySQL's 64-character identifier limit,
//     i.e. the source table name must be 60 characters or fewer.
//  2. `<table>_old` must not already exist on any source (e.g. left over
//     from a previous move against the same database).
//
// Without this check both conditions would only surface at cutover time,
// after the full copy and checksum have already run.
func renameSafetyCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	for _, tbl := range r.SourceTables {
		oldName := CutoverOldName(tbl.TableName)
		if len(oldName) > utils.MaxTableNameLength {
			return fmt.Errorf("table name '%s' is too long: the cutover renames it to '%s' (%d characters), which exceeds MySQL's %d-character identifier limit; rename the table before moving",
				tbl.TableName, oldName, len(oldName), utils.MaxTableNameLength)
		}
	}
	for i, src := range r.Sources {
		for _, tbl := range r.SourceTables {
			oldName := CutoverOldName(tbl.TableName)
			var exists int
			err := src.DB.QueryRowContext(ctx,
				"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
				src.Config.DBName, oldName).Scan(&exists)
			if err == sql.ErrNoRows {
				continue // no leftover _old table, good.
			}
			if err != nil {
				return fmt.Errorf("failed to check for table '%s' on source %d: %w", oldName, i, err)
			}
			return fmt.Errorf("table '%s' already exists on source %d (%s): the cutover renames '%s' to '%s'; drop or rename the leftover table before moving",
				oldName, i, src.Config.DBName, tbl.TableName, oldName)
		}
	}
	return nil
}
