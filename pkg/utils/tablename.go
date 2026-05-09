package utils

const (
	// NameFormatTimestamp is the time.Format layout used in the timestamped
	// _<table>_old_<timestamp> name when SkipDropAfterCutover is set.
	NameFormatTimestamp = "20060102_150405"

	suffixCheckpoint = "_chkpnt"
	suffixNew        = "_new"
	suffixOld        = "_old"
)

// AuxTableName builds a deterministic auxiliary table name for the given
// original table name and suffix (e.g. "_chkpnt", "_new", "_old"). The
// returned name is `_<table><suffix>`. If the result would exceed MySQL's
// 64-character limit, the original table name portion is truncated.
//
// Truncation is deterministic: the same (tableName, suffix) input always
// produces the same output. Two distinct table names that share a long
// common prefix can collide; callers must record the original table name
// out-of-band (e.g. in the checkpoint) so collisions can be detected.
func AuxTableName(tableName, suffix string) string {
	truncated := TruncateTableName(tableName, 1+len(suffix))
	return "_" + truncated + suffix
}

// CheckpointTableName returns the auxiliary checkpoint table name for the
// given original table.
func CheckpointTableName(tableName string) string {
	return AuxTableName(tableName, suffixCheckpoint)
}

// NewTableName returns the auxiliary _new table name for the given original
// table.
func NewTableName(tableName string) string {
	return AuxTableName(tableName, suffixNew)
}

// OldTableName returns the auxiliary _old table name for the given original
// table.
func OldTableName(tableName string) string {
	return AuxTableName(tableName, suffixOld)
}

// OldTableNameWithTimestamp returns the auxiliary _old_<timestamp> table name
// for the given original table and timestamp string. Used when
// SkipDropAfterCutover is set so the renamed-away table is preserved with a
// unique name across multiple migrations.
func OldTableNameWithTimestamp(tableName, timestamp string) string {
	return AuxTableName(tableName, suffixOld+"_"+timestamp)
}
