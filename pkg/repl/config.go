package repl

import (
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/dbconn"
)

type ClientConfig struct {
	TargetBatchTime time.Duration
	Concurrency     int
	Logger          *slog.Logger
	ServerID        uint32
	DBConfig        *dbconn.DBConfig // Database configuration including TLS settings

	// CancelFunc is an optional callback from the caller (e.g. migration or move runner).
	// It is called when a DDL change is detected on a subscribed table, or when a fatal
	// stream error occurs (such as minimal RBR detection or exhausted streamer recreation
	// attempts). The caller is expected to handle cancellation and cleanup.
	// It returns true if the error was acted upon (caller actually cancelled),
	// or false if it was ignored (e.g. because the caller is already past cutover).
	CancelFunc func() bool

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
		Concurrency:     4,
		TargetBatchTime: DefaultTargetBatchTime,
		Logger:          slog.Default(),
		ServerID:        NewServerID(),
	}
}
