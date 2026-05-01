package applier

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// SingleTargetApplier applies rows to a single target database.
// It internally splits rows into chunklets for optimal batching and tracks
// completion to invoke callbacks when all chunklets for a set of rows are done.
type SingleTargetApplier struct {
	sync.Mutex

	target   Target
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger

	// Internal chunklet processing
	chunkletBuffer      chan chunklet
	chunkletCompletions chan chunkletCompletion

	// Pending work tracking
	pendingWork  map[int64]*pendingWork
	pendingMutex sync.Mutex
	nextWorkID   int64 // Atomic counter for work IDs

	// Worker management
	writeWorkersCount    int32
	writeWorkersFinished int32
	workerIDCounter      int32

	// Context management
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State management to make Start/Stop idempotent
	started bool
	stopped bool
}

// chunklet represents a small batch of rows for internal processing
// Limited by either chunkletMaxRows or chunkletMaxSize, whichever is reached first
type chunklet struct {
	workID int64        // ID of the parent work
	chunk  *table.Chunk // Original chunk for column info
	rows   []rowData    // Batch of rows limited by row count or size
}

// chunkletCompletion represents a completed chunklet
type chunkletCompletion struct {
	workID       int64 // ID of the parent work
	affectedRows int64 // Rows affected by this chunklet
	err          error // Error if any
}

// pendingWork tracks a set of rows that are being processed
type pendingWork struct {
	callback           ApplyCallback
	totalChunklets     int   // Total number of chunklets for this work
	completedChunklets int   // Number of completed chunklets
	totalAffectedRows  int64 // Sum of affected rows from all chunklets
}

// NewSingleTargetApplier creates a new SingleTargetApplier
func NewSingleTargetApplier(target Target, cfg *ApplierConfig) (*SingleTargetApplier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &SingleTargetApplier{
		target:              target,
		dbConfig:            cfg.DBConfig,
		logger:              cfg.Logger,
		chunkletBuffer:      make(chan chunklet, defaultBufferSize),
		chunkletCompletions: make(chan chunkletCompletion, defaultBufferSize),
		pendingWork:         make(map[int64]*pendingWork),
		writeWorkersCount:   int32(cfg.Threads),
	}, nil
}

// Start initializes the applier's async write workers and begins processing
// This does not control the synchronous methods like UpsertRows/DeleteKeys
// This method is idempotent - calling it multiple times is safe.
func (a *SingleTargetApplier) Start(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()

	// If already started, return without error
	if a.started {
		a.logger.Debug("SingleTargetApplier already started, skipping")
		return nil
	}

	// If previously stopped, we need to reinitialize channels
	if a.stopped {
		a.logger.Info("restarting SingleTargetApplier after previous stop")
		a.chunkletBuffer = make(chan chunklet, defaultBufferSize)
		a.chunkletCompletions = make(chan chunkletCompletion, defaultBufferSize)
		a.writeWorkersFinished = 0
		a.workerIDCounter = 0
		a.stopped = false
	}

	workerCtx, cancelFunc := context.WithCancel(ctx)
	a.cancelFunc = cancelFunc

	a.started = true
	a.logger.Info("starting SingleTargetApplier", "writeWorkers", a.writeWorkersCount)

	// Start write workers
	for range a.writeWorkersCount {
		a.wg.Add(1)
		go a.writeWorker(workerCtx)
	}

	// Start feedback coordinator
	a.wg.Add(1)
	go a.feedbackCoordinator()

	return nil
}

// Apply sends rows to be written to the target database
func (a *SingleTargetApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	if len(rows) == 0 {
		// No rows to apply, invoke callback immediately
		callback(0, nil)
		return nil
	}

	// Assign a work ID for tracking
	workID := atomic.AddInt64(&a.nextWorkID, 1)

	// Convert rows to rowData format
	rowDataList := make([]rowData, len(rows))
	for i, row := range rows {
		rowDataList[i] = rowData{values: row}
	}

	// Split into chunklets based on both row count and size thresholds
	// Then convert row batches into chunklets with metadata
	rowBatches := splitRowsIntoChunklets(rowDataList)
	chunklets := make([]chunklet, len(rowBatches))
	for i, batch := range rowBatches {
		chunklets[i] = chunklet{
			workID: workID,
			chunk:  chunk,
			rows:   batch,
		}
	}

	// Register the pending work
	a.pendingMutex.Lock()
	a.pendingWork[workID] = &pendingWork{
		callback:           callback,
		totalChunklets:     len(chunklets),
		completedChunklets: 0,
		totalAffectedRows:  0,
	}
	a.pendingMutex.Unlock()

	// Send chunklets to buffer
	for _, chunkletData := range chunklets {
		select {
		case a.chunkletBuffer <- chunkletData:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// Wait blocks until all pending work is complete and all callbacks have been invoked
func (a *SingleTargetApplier) Wait(ctx context.Context) error {
	// Wait until there's no pending work
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		a.pendingMutex.Lock()
		pendingCount := len(a.pendingWork)
		a.pendingMutex.Unlock()

		if pendingCount == 0 {
			a.logger.Debug("Wait: all pending work complete")
			return nil
		}

		a.logger.Debug("Wait: waiting for pending work", "pendingCount", pendingCount)

		// Wait for next tick or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue loop
		}
	}
}

// Stop signals the applier to shut down gracefully
// This does not control the synchronous methods like UpsertRows/DeleteKeys,
// which can continue after Stop() is called.
// This method is idempotent - calling it multiple times is safe.
func (a *SingleTargetApplier) Stop() error {
	a.Lock()

	// If already stopped or never started, return without error
	if a.stopped || !a.started {
		a.Unlock()
		a.logger.Debug("SingleTargetApplier already stopped or never started, skipping")
		return nil
	}

	a.logger.Info("stopping SingleTargetApplier")

	// Cancel the context to signal workers to stop
	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// Close the chunklet buffer to signal no more work
	close(a.chunkletBuffer)

	// Mark as stopped before releasing lock
	a.stopped = true
	a.started = false

	// Release the lock before waiting for workers to finish
	a.Unlock()

	// Wait for all workers to finish
	a.wg.Wait()

	a.logger.Info("SingleTargetApplier stopped")
	return nil
}

// writeWorker processes chunklets from the buffer
func (a *SingleTargetApplier) writeWorker(ctx context.Context) {
	defer a.wg.Done()
	workerID := atomic.AddInt32(&a.workerIDCounter, 1)

	defer func() {
		finishedCount := atomic.AddInt32(&a.writeWorkersFinished, 1)
		a.logger.Debug("writeWorker finished", "workerID", workerID, "finishedCount", finishedCount, "totalWorkers", a.writeWorkersCount)

		// If all write workers are finished, close the completions channel
		if finishedCount == a.writeWorkersCount {
			a.logger.Debug("writeWorker all write workers finished, closing completions channel", "workerID", workerID)
			close(a.chunkletCompletions)
		}
	}()

	for {
		select {
		case chunkletData, ok := <-a.chunkletBuffer:
			if !ok {
				a.logger.Debug("writeWorker channel closed, exiting", "workerID", workerID)
				return
			}

			a.logger.Debug("writeWorker processing chunklet", "workerID", workerID, "workID", chunkletData.workID, "rowCount", len(chunkletData.rows))

			// Write chunklet
			affectedRows, err := a.writeChunklet(ctx, chunkletData)

			// Send completion — always send after attempting the write so the
			// feedbackCoordinator can track progress. We must not race this
			// with ctx.Done(); a lost completion leaves pendingWork stuck and
			// causes Wait() to hang or report incorrect results.
			a.chunkletCompletions <- chunkletCompletion{
				workID:       chunkletData.workID,
				affectedRows: affectedRows,
				err:          err,
			}

		case <-ctx.Done():
			return
		}
	}
}

// writeChunklet writes a single chunklet (up to chunkletMaxRows or chunkletMaxSize)
func (a *SingleTargetApplier) writeChunklet(ctx context.Context, chunkletData chunklet) (int64, error) {
	if len(chunkletData.rows) == 0 {
		return 0, nil
	}

	// Get the intersected column names to match with the values
	columnList, _ := chunkletData.chunk.ColumnMapping.Columns()
	columnNames, _ := chunkletData.chunk.ColumnMapping.ColumnsSlice()

	// Build VALUES clauses for all rows in the chunklet
	var valuesClauses []string
	for _, row := range chunkletData.rows {
		if len(columnNames) != len(row.values) {
			return 0, fmt.Errorf("column count mismatch: chunk %s has %d columns, but chunklet has %d values",
				chunkletData.chunk.String(), len(columnNames), len(row.values))
		}
		var values []string
		for i, value := range row.values {
			columnType, ok := chunkletData.chunk.ColumnMapping.TargetTable().GetColumnMySQLType(columnNames[i])
			if !ok {
				return 0, fmt.Errorf("column %s not found in table info", columnNames[i])
			}
			datum, err := table.NewDatumFromValue(value, columnType)
			if err != nil {
				return 0, fmt.Errorf("failed to convert value to datum for column %s: %w", columnNames[i], err)
			}
			values = append(values, datum.String())
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// Build the INSERT statement
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		chunkletData.chunk.ColumnMapping.TargetTable().QuotedTableName,
		columnList,
		strings.Join(valuesClauses, ", "),
	)

	a.logger.Debug("writing chunklet", "rowCount", len(chunkletData.rows), "table", chunkletData.chunk.ColumnMapping.TargetTable().TableName)

	// Execute the batch insert
	result, err := dbconn.RetryableTransaction(ctx, a.target.DB, true, a.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute chunklet insert: %w", err)
	}

	return result, nil
}

// feedbackCoordinator tracks chunklet completions and invokes callbacks when work is done
func (a *SingleTargetApplier) feedbackCoordinator() {
	defer a.wg.Done()
	a.logger.Debug("feedbackCoordinator started")

	// processCompletion handles a single chunklet completion.
	processCompletion := func(completion chunkletCompletion) {
		a.logger.Debug("feedbackCoordinator received chunklet completion", "workID", completion.workID)

		// Update work completion status
		a.pendingMutex.Lock()
		pending, exists := a.pendingWork[completion.workID]
		if !exists {
			a.pendingMutex.Unlock()
			a.logger.Error("feedbackCoordinator received completion for unknown work", "workID", completion.workID)
			return
		}

		// If there was an error, invoke callback immediately
		if completion.err != nil {
			callback := pending.callback
			// Remove the work from pending map before invoking callback
			delete(a.pendingWork, completion.workID)
			a.pendingMutex.Unlock()
			callback(0, completion.err)
			return
		}

		// Update completion count and affected rows
		pending.completedChunklets++
		pending.totalAffectedRows += completion.affectedRows

		a.logger.Debug("feedbackCoordinator work progress", "workID", completion.workID,
			"completedChunklets", pending.completedChunklets, "totalChunklets", pending.totalChunklets)

		// Check if all chunklets for this work are complete
		if pending.completedChunklets == pending.totalChunklets {
			a.logger.Debug("feedbackCoordinator all chunklets complete, invoking callback", "workID", completion.workID)

			// Invoke the callback
			callback := pending.callback
			affectedRows := pending.totalAffectedRows

			// Remove completed work from pending map
			delete(a.pendingWork, completion.workID)
			a.pendingMutex.Unlock()

			// Invoke callback outside the lock
			callback(affectedRows, nil)
		} else {
			a.pendingMutex.Unlock()
		}
	}

	// Main loop: process completions until the channel is closed.
	// We do NOT exit on ctx.Done() here because write workers may still be
	// sending completions after writing data. Exiting early would leave
	// entries in pendingWork that are never cleared, causing Wait() to hang
	// or report incorrect results. The channel will be closed once all write
	// workers finish (including their deferred close logic), which is the
	// authoritative signal that no more completions will arrive.
	for completion := range a.chunkletCompletions {
		processCompletion(completion)
	}
	a.logger.Debug("feedbackCoordinator chunklet completions channel closed, exiting")
}

// DeleteKeys deletes rows by their key values synchronously.
// The keys are hashed key strings (from utils.HashKey).
// If lock is non-nil, the delete is executed under the table lock.
func (a *SingleTargetApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys []string, lock *dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	// For move operations, targetTable may be nil - use sourceTable for both
	if targetTable == nil {
		targetTable = sourceTable
	}
	// Convert hashed keys to row value constructor format
	var pkValues []string
	for _, key := range keys {
		pkValues = append(pkValues, utils.UnhashKeyToString(key))
	}

	// Build DELETE statement
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
		targetTable.QuotedTableName,
		table.QuoteColumns(sourceTable.KeyColumns),
		strings.Join(pkValues, ","),
	)

	a.logger.Debug("executing delete", "keyCount", len(keys), "table", targetTable.TableName)

	// Execute under lock if provided
	if lock != nil {
		if err := lock.ExecUnderLock(ctx, deleteStmt); err != nil {
			return 0, fmt.Errorf("failed to execute delete under lock: %w", err)
		}
		// We don't get affected rows from ExecUnderLock, so return the key count
		return int64(len(keys)), nil
	}

	// Execute as a retryable transaction
	affectedRows, err := dbconn.RetryableTransaction(ctx, a.target.DB, false, a.dbConfig, deleteStmt)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}

	return affectedRows, nil
}

// UpsertRows performs an upsert (INSERT ... ON DUPLICATE KEY UPDATE) synchronously.
// The rows are LogicalRow structs containing the row images.
// If lock is non-nil, the upsert is executed under the table lock.
func (a *SingleTargetApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []LogicalRow, lock *dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	_, targetColumnList := mapping.Columns()
	sourceColumnNames, targetColumnNames := mapping.ColumnsSlice()
	intersectedColumns := mapping.SourceColumnIndices()

	// Build the VALUES clause from the row images
	var valuesClauses []string
	for _, logicalRow := range rows {
		if logicalRow.IsDeleted {
			continue // Skip deleted rows
		}
		// Convert the row image to a VALUES clause
		var values []string
		for i, colIndex := range intersectedColumns {
			if colIndex >= len(logicalRow.RowImage) {
				return 0, fmt.Errorf("column index %d exceeds row image length %d", colIndex, len(logicalRow.RowImage))
			}
			// In order to create a datum we need to know the MySQL type,
			// which we can get from the source table.
			columnType, ok := mapping.SourceTable().GetColumnMySQLType(sourceColumnNames[i])
			if !ok {
				return 0, fmt.Errorf("column %s not found in table info", sourceColumnNames[i])
			}
			datum, err := table.NewDatumFromValue(logicalRow.RowImage[colIndex], columnType)
			if err != nil {
				return 0, fmt.Errorf("failed to convert value to datum for column %s: %w", sourceColumnNames[i], err)
			}
			values = append(values, datum.String())
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	if len(valuesClauses) == 0 {
		return 0, nil
	}

	// Build the ON DUPLICATE KEY UPDATE clause using target column names.
	var updateClauses []string
	for _, col := range targetColumnNames {
		if !slices.Contains(mapping.TargetTable().KeyColumns, col) {
			updateClauses = append(updateClauses, fmt.Sprintf("`%s` = new.`%s`", col, col))
		}
	}

	upsertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS new ON DUPLICATE KEY UPDATE %s",
		mapping.TargetTable().QuotedTableName,
		targetColumnList,
		strings.Join(valuesClauses, ", "),
		strings.Join(updateClauses, ", "),
	)

	a.logger.Debug("executing upsert", "rowCount", len(valuesClauses), "table", mapping.TargetTable().TableName)

	// Execute under lock if provided
	if lock != nil {
		if err := lock.ExecUnderLock(ctx, upsertStmt); err != nil {
			return 0, fmt.Errorf("failed to execute upsert under lock: %w", err)
		}
		// We don't get affected rows from ExecUnderLock, so return the row count
		return int64(len(valuesClauses)), nil
	}

	// Execute as a retryable transaction
	affectedRows, err := dbconn.RetryableTransaction(ctx, a.target.DB, false, a.dbConfig, upsertStmt)
	if err != nil {
		return 0, fmt.Errorf("failed to execute upsert: %w", err)
	}

	return affectedRows, nil
}

// GetTargets returns the target database configuration for direct access.
// This is used by operations like checksum that need to query targets directly.
func (a *SingleTargetApplier) GetTargets() []Target {
	return []Target{a.target}
}
