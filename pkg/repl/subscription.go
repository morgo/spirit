package repl

import (
	"context"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// Subscription is now an interface! It defines how the replication changes are tracked.
// We have two strategies:
// 1. Delta Map: The OG, works only with memory comparable primary keys. Tracks the PK and if the last operation was a delete or not.
// 2. Delta Queue: A queue of changes, works with any primary key type. Does not effectively de-deplucate intermediate changes and must be replayed in order.
//
// We will add a third strategy in future which buffers all changes and doesn't use REPLACE INTO .. SELECT.

type Subscription interface {
	KeyHasChanged(key []any, deleted bool)
	Length() int
	Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error
	Table() *table.TableInfo
	SetKeyAboveWatermarkOptimization(enabled bool)
}
