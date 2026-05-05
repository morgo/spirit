package repl

import (
	"context"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// Subscription defines how the replication changes are tracked. The single
// implementation is bufferedMap; row images come straight from the binlog
// and are written via the applier (no SELECT-from-source round trip).
//
// For non-memory-comparable PKs, bufferedMap routes events through an
// internal FIFO queue rather than the hash map, since collation-equivalent
// keys ("A" and "a") hash to different slots but resolve to the same MySQL
// row — map iteration would apply events out of order. Two routing
// policies are available, controlled by Client.forceEnableBufferedMap:
//
//   - Default (forceEnableBufferedMap=false): queue mode is active full-time
//     for non-memory-comparable PKs, in both copy and post-copy phases.
//   - Optimization (forceEnableBufferedMap=true): map mode during the copy
//     phase (LWW dedup), queue mode post-copy. The transition happens
//     inside SetWatermarkOptimization, which drains the outgoing store
//     inline.
//
// Memory-comparable PKs always use the map regardless of the flag. See
// pkg/repl/subscription_buffered.go for the routing rules.

type Subscription interface {
	HasChanged(key, row []any, deleted bool)
	Length() int
	Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) (allChangesFlushed bool, err error)
	Tables() []*table.TableInfo // returns the tables related to the subscription in currentTable, newTable order

	// SetWatermarkOptimization toggles both high and low watermark
	// optimizations. Under the bufferedMap optimization
	// (Client.forceEnableBufferedMap=true) toggling can switch a
	// non-memory-comparable subscription between map mode and queue mode;
	// on such a transition it drains the outgoing store via the applier
	// so only one store has pending entries at a time. Returns the drain
	// error if any.
	SetWatermarkOptimization(ctx context.Context, enabled bool) error
}
