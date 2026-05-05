package repl

import (
	"context"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// Subscription defines how the replication changes are tracked. The single
// implementation is bufferedMap, which holds row images in a hash map by
// default and falls back to an internal FIFO queue (still row-image-based,
// still applier-driven) for non-memory-comparable PKs once the watermark
// optimization is disabled. See pkg/repl/subscription_buffered.go.

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
