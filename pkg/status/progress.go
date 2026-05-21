package status

import "github.com/block/spirit/pkg/throttler"

// Progress is returned as a struct because we may add more to it later.
// It is designed for wrappers (like a GUI) to be able to summarize the
// current status without parsing log output.
type Progress struct {
	CurrentState State  // current state, i.e. CopyRows
	Summary      string // text based representation, i.e. "12.5% copyRows ETA 1h 30m"

	// Tables contains per-table progress for multi-table migrations.
	// For single-table migrations, this will have one entry.
	Tables []TableProgress

	// Throttler is the active throttler. Always non-nil — when no throttler
	// has been configured a Noop is supplied so callers can dispatch on it
	// without nil checks. Use Throttler.IsThrottled() for the boolean state
	// and Throttler.String() for a human-readable reason (empty when not
	// throttled). Surfacing the throttler itself (rather than fields cherry-
	// picked from it) lets callers reach future throttler-specific data
	// without needing additions to this struct.
	Throttler throttler.Throttler
}

// TableProgress tracks progress for a single table in the migration.
type TableProgress struct {
	TableName  string // name of the table being migrated
	RowsCopied uint64 // rows copied so far
	RowsTotal  uint64 // total rows expected
	IsComplete bool   // true if this table's copy is complete
}
