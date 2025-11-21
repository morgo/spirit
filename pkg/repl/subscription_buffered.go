package repl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// The bufferedMap is an experiment to see if we can avoid using REPLACE INTO .. SELECT.
// See: https://github.com/block/spirit/issues/451
// This has the advantage that we can use spirit for MoveTable operations
// across different MySQL servers. In combination with Atomic DDL,
// we have all the components needed for cloning sets of tables between servers.

type bufferedMap struct {
	sync.Mutex // protects the subscription from changes.

	c       *Client         // reference back to the client.
	applier applier.Applier // applier for writing changes to the target

	table    *table.TableInfo
	newTable *table.TableInfo

	changes map[string]applier.LogicalRow

	watermarkOptimization bool
	chunker               table.Chunker
}

// Assert that bufferedMap implements subscription
var _ Subscription = (*bufferedMap)(nil)

func (s *bufferedMap) Length() int {
	s.Lock()
	defer s.Unlock()

	return len(s.changes)
}

func (s *bufferedMap) Tables() []*table.TableInfo {
	return []*table.TableInfo{s.table, s.newTable}
}

func (s *bufferedMap) HasChanged(key, row []any, deleted bool) {
	s.Lock()
	defer s.Unlock()

	// The KeyAboveWatermark optimization has to be enabled
	// We enable it once all the setup has been done (since we create a repl client
	// earlier in setup to ensure binary logs are available).
	// We then disable the optimization after the copier phase has finished.
	if s.watermarkOptimizationEnabled() && s.chunker.KeyAboveHighWatermark(key[0]) {
		s.c.logger.Debug("key above watermark", "key", key[0])
		return
	}

	hashedKey := utils.HashKey(key)

	if deleted {
		s.changes[hashedKey] = applier.LogicalRow{IsDeleted: true}
		return
	}

	// Set the logical row to be the new row
	s.changes[hashedKey] = applier.LogicalRow{RowImage: row}
}

// Flush writes the pending changes to the new table.
// We do this under a mutex, which means that unfortunately pending changes
// are blocked from being collected while we do this. In future we may
// come up with a more sophisticated approach to allow concurrent
// collection of changes while we flush.
func (s *bufferedMap) Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) (allChangesFlushed bool, err error) {
	s.Lock()
	defer s.Unlock()

	// We must now apply the changeset to the new table.
	var deleteKeys []string
	var upsertRows []applier.LogicalRow
	var keysFlushed []string
	var i int64
	allChangesFlushed = true // assume all changes are flushed unless we find some that are not.
	target := atomic.LoadInt64(&s.c.targetBatchSize)

	// Determine which lock to use (nil if not underLock)
	var lockToUse *dbconn.TableLock
	if underLock {
		lockToUse = lock
	}

	for key, logicalRow := range s.changes {
		unhashedKey := utils.UnhashKey(key)
		// Check low watermark only if the optimization is enabled
		// Note: bufferedMap has inverted logic - KeyBelowLowWatermark returns true
		// for keys that are still being copied, so we skip flushing them
		if s.watermarkOptimizationEnabled() && s.chunker.KeyBelowLowWatermark(unhashedKey[0]) {
			s.c.logger.Debug("key below watermark", "key", unhashedKey[0])
			allChangesFlushed = false
			continue
		}
		i++
		keysFlushed = append(keysFlushed, key) // we are going to flush this key
		if logicalRow.IsDeleted {
			deleteKeys = append(deleteKeys, key)
		} else {
			upsertRows = append(upsertRows, logicalRow)
		}
		if (i % target) == 0 {
			// Flush this batch
			if err := s.flushBatch(ctx, deleteKeys, upsertRows, lockToUse); err != nil {
				return false, err
			}
			deleteKeys = nil
			upsertRows = nil
		}
	}

	// Flush any remaining changes
	if err := s.flushBatch(ctx, deleteKeys, upsertRows, lockToUse); err != nil {
		return false, err
	}

	// The statements have been executed successfully.
	// We can now remove the flushed keys from the map.
	for _, key := range keysFlushed {
		delete(s.changes, key)
	}
	return allChangesFlushed, nil
}

// flushBatch flushes a batch of deletes and upserts using the applier.
// If lock is non-nil, the operations are executed under the table lock.
func (s *bufferedMap) flushBatch(ctx context.Context, deleteKeys []string, upsertRows []applier.LogicalRow, lock *dbconn.TableLock) error {
	if len(deleteKeys) == 0 && len(upsertRows) == 0 {
		return nil
	}

	startTime := time.Now()

	// Execute deletes
	if len(deleteKeys) > 0 {
		affectedRows, err := s.applier.DeleteKeys(ctx, s.table, s.newTable, deleteKeys, lock)
		if err != nil {
			return fmt.Errorf("failed to delete keys: %w", err)
		}
		s.c.feedback(int(affectedRows), time.Since(startTime))
	}

	// Execute upserts
	if len(upsertRows) > 0 {
		affectedRows, err := s.applier.UpsertRows(ctx, s.table, s.newTable, upsertRows, lock)
		if err != nil {
			return fmt.Errorf("failed to upsert rows: %w", err)
		}
		s.c.feedback(int(affectedRows), time.Since(startTime))
	}

	return nil
}

// watermarkOptimizationEnabled returns true if the watermark optimization
// is enabled. This is already called under a mutex.
func (s *bufferedMap) watermarkOptimizationEnabled() bool {
	return s.watermarkOptimization && s.chunker != nil
}

func (s *bufferedMap) SetWatermarkOptimization(enabled bool) {
	s.Lock()
	defer s.Unlock()
	s.watermarkOptimization = enabled
}
