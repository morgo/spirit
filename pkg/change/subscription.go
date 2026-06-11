package change

import (
	"context"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// Subscription defines how the replication changes are tracked. The single
// implementation is bufferedMap; row images come straight from the binlog
// and are written via the applier (no SELECT-from-source round trip).
//
// For non-memory-comparable PKs, bufferedMap uses LWW map dedup during the
// copy phase and switches to an internal FIFO queue post-copy. The queue
// exists because collation-equivalent keys ("A" and "a") hash to different
// map slots but resolve to the same MySQL row — map iteration would apply
// events out of order. During the copy phase the chunker's later SELECT
// covers in-window case-collision races, and the post-cutover checksum
// (with FixDifferences=true) repairs any residual divergence. The
// transition happens inside SetWatermarkOptimization, which drains the
// outgoing store inline.
//
// Memory-comparable PKs always use the map. See
// pkg/change/subscription_buffered.go for the routing rules.

type Subscription interface {
	HasChanged(key, row []any, deleted bool)
	Length() int
	// Flush writes the pending changes to the target(s) via the applier.
	// When underLock is true, locks carries the table locks the caller is
	// holding — one per target server — and the applier executes each
	// target's statements under that target's own lock.
	Flush(ctx context.Context, underLock bool, locks []*dbconn.TableLock) (allChangesFlushed bool, err error)
	Tables() []*table.TableInfo // returns the tables related to the subscription in currentTable, newTable order

	// SetWatermarkOptimization toggles both high and low watermark
	// optimizations. For non-memory-comparable PKs toggling switches the
	// subscription between map mode and queue mode; on such a transition
	// it drains the outgoing store via the applier so only one store has
	// pending entries at a time. Returns the drain error if any.
	SetWatermarkOptimization(ctx context.Context, enabled bool) error

	// Close signals that no further events will be delivered. Any HasChanged
	// caller currently parked on backpressure (e.g. the bufferedMap soft
	// memory limit) is unblocked so the binlog reader goroutine can exit.
	// Close does NOT flush; pending changes are discarded along with the
	// subscription. It is safe to call more than once.
	Close()
}
