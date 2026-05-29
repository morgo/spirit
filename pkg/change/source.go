package change

import (
	"context"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// Source is the abstraction spirit uses to consume a stream of row
// changes from a source database. It exists so spirit's replication
// pipeline is not pinned to the MySQL binlog protocol — alternative
// implementations (e.g. Vitess VStream) can plug in without touching the
// applier, the bufferedMap, or any other spirit-side machinery.
//
// The built-in implementation that uses go-mysql's BinlogSyncer lives in
// this package and backs the existing Client. Out-of-tree implementations
// construct their own Source value and pass it to spirit via the
// Move/Migration config.
//
// Lifecycle: construct → AddSubscription(...)* → Run(ctx) OR
// OpenFromPosition(ctx, pos) → Flush / BlockWait /
// FlushUnderTableLock as needed → Close().
//
// Events flow PUSH-style: when a row event matching one of the
// subscribed tables arrives, the source implementation looks up the
// Subscription whose Tables() includes that (schema, table) and calls
// sub.HasChanged(key, row, deleted) directly. There is no Next() / Recv()
// loop on this interface — the caller registers subscriptions and lets
// the source drive them.
//
// The surface area is intentionally broad to match the existing Client
// API so all spirit consumers (pkg/migration, pkg/move, pkg/checksum)
// can switch from `*Client` to `Source` without churning every
// callsite. Two pieces of the surface are MySQL-binlog-specific and
// will need to be generalized in a follow-up:
//
//   - GetBinlogApplyPosition() returns mysql.Position (file+offset).
//     The opaque Position() string is the new general form; consumers
//     that today reach for mysql.Position for checkpoint serialization
//     should migrate to Position() + OpenFromPosition.
//   - SetFlushedPos(mysql.Position) likewise. OpenFromPosition replaces
//     it for the resume case; mid-stream position updates are an
//     internal binlog concern that should not be on this interface
//     long-term.
//
// Alternative implementations that don't speak file+offset implement
// these methods as best-effort no-ops or return zero values; spirit's
// binlog-specific call sites will eventually be retired.
type Source interface {
	// AddSubscription constructs a bufferedMap from (currentTable,
	// newTable, chunker) and registers it. ROW events matching the
	// registered (schema, table) pair are pushed to the subscription's
	// HasChanged. Must be called before Run / OpenFromPosition.
	AddSubscription(currentTable, newTable *table.TableInfo, chunker table.MappedChunker) error

	// Run starts streaming from the current source head and spawns the
	// reader goroutine. Implementations perform any required validation
	// (privileges, connectivity, server settings) as part of Run.
	Run(ctx context.Context) error

	// Flush requests that all registered subscriptions flush their
	// buffered changes to their targets. Blocks until the flush
	// completes or ctx cancels.
	Flush(ctx context.Context) error

	// FlushUnderTableLock is the cutover-time variant of Flush: the
	// caller holds a table lock on the source side and we drain the
	// in-flight backlog against that quiescent state.
	FlushUnderTableLock(ctx context.Context, lock *dbconn.TableLock) error

	// BlockWait blocks until all events received from the underlying
	// stream up to call-time have been delivered to their subscriptions.
	// Used by the runner around cutover to drain the in-flight backlog.
	// Returns when drained or ctx cancels.
	BlockWait(ctx context.Context) error

	// GetDeltaLen returns the total number of pending changes across
	// all registered subscriptions. Used by callers to decide whether
	// the backlog is small enough to consider cutover.
	GetDeltaLen() int

	// SetWatermarkOptimization toggles the high/low watermark
	// optimization across all subscriptions. Disabled before
	// checksum/cutover to ensure all changes are flushed regardless of
	// watermark position.
	SetWatermarkOptimization(ctx context.Context, enabled bool) error

	// StartPeriodicFlush spawns a background goroutine that flushes the
	// changeset at the given interval. Used by the migrator to advance
	// the safe-flushed position. Calling Start while a periodic flush
	// is already running is a no-op.
	StartPeriodicFlush(ctx context.Context, interval time.Duration)

	// StopPeriodicFlush stops the goroutine started by
	// StartPeriodicFlush. Safe to call when no periodic flush is
	// running (no-op).
	StopPeriodicFlush()

	// AllChangesFlushed reports whether the buffered position has been
	// caught up to the flushed position (i.e. no in-flight changes
	// remain). For non-binlog implementations, this is equivalent to
	// "have all received events been applied?".
	AllChangesFlushed() bool

	// Close releases all resources. Safe to call more than once.
	Close()

	// Deprecated methods:
	// To be replaced by generic ones soon

	// GetBinlogApplyPosition is the MySQL-binlog-typed form of Position.
	// Returns the zero value for non-binlog implementations.
	// To be retired once consumers migrate to opaque Position strings.
	GetBinlogApplyPosition() mysql.Position

	// SetFlushedPos sets the safe-flushed binlog position. The
	// MySQL-binlog implementation uses this during resume from a
	// checkpoint; OpenFromPosition is the cross-implementation
	// equivalent. No-op on non-binlog implementations.
	// To be retired once consumers migrate to OpenFromPosition.
	SetFlushedPos(pos mysql.Position)

	// Suggestions for generic methods:
	// OpenFromPosition is the resume-time entry point. It primes the
	// source's internal position to the opaque string previously
	// returned by Position(), then starts streaming as if Run had been
	// called. Implementations validate the position is still resumable
	// (e.g. MySQL: the binlog file has not been purged).
	// OpenFromPosition(ctx context.Context, pos string) error

	// Position returns the latest safe-to-resume position as an opaque
	// string. The implementation owns the encoding; spirit never parses
	// it. Advances only at transaction commit boundaries.
	// Position() string
}
