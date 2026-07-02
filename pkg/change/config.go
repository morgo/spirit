package change

import (
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/dbconn"
)

// FatalReason tells the caller's CancelFunc why the change client hit a fatal
// condition, so the caller can decide which of its state (if any) must be
// invalidated before cancelling.
type FatalReason int

const (
	// FatalReasonSchemaChange means DDL was detected on a watched table.
	// Persisted resume state (checkpoints) describes the table's old
	// definition, so a caller that keeps such state must invalidate it:
	// resuming against the changed table could corrupt data.
	FatalReasonSchemaChange FatalReason = iota
	// FatalReasonStreamError means the change stream itself failed fatally
	// (streamer recreation attempts exhausted, or a row event could not be
	// processed). The watched tables are not known to have changed, so
	// persisted resume state remains valid and a retry can resume from it.
	FatalReasonStreamError
)

// String implements fmt.Stringer for logging.
func (f FatalReason) String() string {
	switch f {
	case FatalReasonSchemaChange:
		return "schema-change"
	case FatalReasonStreamError:
		return "stream-error"
	default:
		return fmt.Sprintf("unknown-fatal-reason(%d)", int(f))
	}
}

type ClientConfig struct {
	Logger   *slog.Logger
	ServerID uint32
	DBConfig *dbconn.DBConfig // Database configuration including TLS settings

	// CancelFunc is an optional callback from the caller (e.g. migration or move runner).
	// It is called when a DDL change is detected on a subscribed table
	// (FatalReasonSchemaChange), or when a fatal stream error occurs, such as
	// minimal RBR detection or exhausted streamer recreation attempts
	// (FatalReasonStreamError). The caller is expected to handle cancellation
	// and cleanup, using reason to decide whether persisted resume state
	// (e.g. a checkpoint) must be invalidated (schema change) or is still
	// safe to resume from (stream error).
	// It returns true if the error was acted upon (caller actually cancelled),
	// or false if it was ignored (e.g. because the caller is already past cutover).
	CancelFunc func(reason FatalReason) bool

	// DDLFilterSchema, when set, broadens DDL detection to cancel on any DDL change
	// in the specified schema, rather than only on exact table matches against subscriptions.
	// This is used by the move runner to detect DDL on any table in the source database.
	DDLFilterSchema string

	// DDLFilterTables, when set alongside DDLFilterSchema, narrows the schema-level
	// DDL detection to only the specified table names. This is used for partial moves
	// where only specific tables from a schema are being moved — DDL on unrelated
	// tables in the same schema should not trigger cancellation.
	// If empty (and DDLFilterSchema is set), all tables in the schema trigger cancellation.
	DDLFilterTables []string

	// SubscriptionSoftLimitBytes overrides DefaultSubscriptionSoftLimitBytes
	// for new subscriptions. Set to a negative value to disable the cap
	// entirely (HasChanged will never block on memory). Zero (the
	// zero-value default) means use DefaultSubscriptionSoftLimitBytes.
	SubscriptionSoftLimitBytes int64
}

// NewClientDefaultConfig returns a default config for the copier.
func NewClientDefaultConfig() *ClientConfig {
	return &ClientConfig{
		Logger:   slog.Default(),
		ServerID: NewServerID(),
	}
}
