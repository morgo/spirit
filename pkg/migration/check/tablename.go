package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

const (
	// Formats for table names used by Spirit during migrations.
	NameFormatCheckpoint   = "_%s_chkpnt"
	NameFormatNew          = "_%s_new"
	NameFormatOld          = "_%s_old"
	NameFormatOldTimeStamp = "_%s_old_%s"
	NameFormatTimestamp    = "20060102_150405"

	// MaxMigratableTableNameLength is the maximum table name length that Spirit
	// can manage. This is MySQL's 64-character limit minus the longest metadata
	// suffix (_<table>_chkpnt = 8 extra characters), giving 56 usable characters.
	MaxMigratableTableNameLength = utils.MaxTableNameLength - 8 // len("_") + len("_chkpnt") = 8

	// NameFormatTimestampExtraChars is the number of extra characters added by
	// the timestamped old table name format (_%s_old_%s with a 15-char timestamp).
	// This equals len("_") + len("_old_") + len("20060102_150405") = 21.
	NameFormatTimestampExtraChars = 21
)

func init() {
	registerCheck("tablename", tableNameCheck, ScopePreflight)
}

func tableNameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	tableName := r.Table.TableName
	if len(tableName) < 1 {
		return errors.New("table name must be at least 1 character")
	}
	if len(tableName) > MaxMigratableTableNameLength {
		return fmt.Errorf("table name must be less than %d characters", MaxMigratableTableNameLength)
	}
	return nil
}
