package applier

import (
	"context"
	"database/sql"
	"errors"
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

// Target represents a shard target with its database connection and key range.
// Key ranges are expressed as Vitess-style strings (e.g., "-80", "80-", "80-c0").
// An empty string means all key space (unsharded).
type Target struct {
	DB       *sql.DB
	KeyRange string // Vitess-style key range: "-80", "80-", "80-c0"
}

// VindexFunc is a hash function that takes a single column value and returns a uint64 hash.
// This matches Vitess vindex behavior where the hash is used to determine shard placement.
// The hash value is then matched against key ranges to find the target shard.
type VindexFunc func(value any) (uint64, error)

// ShardedApplier applies rows to multiple target databases based on a Vitess-style vindex.
// It extracts a specific column value from each row, applies a hash function to it,
// and routes the row to the appropriate shard based on the hash value and key ranges.
//
// The vindex column is specified by name (e.g., "user_id") and is extracted from each row
// using the table metadata. The hash function returns a uint64 which is then matched
// against the target key ranges to determine the destination shard.
type ShardedApplier struct {
	shards       []*shardTarget
	vindexFunc   VindexFunc // Hash function: value -> uint64
	vindexColumn string     // Column name to extract and hash (e.g., "user_id")
	dbConfig     *dbconn.DBConfig
	logger       *slog.Logger

	// Pending work tracking (shared across all shards)
	pendingWork  map[int64]*pendingWork
	pendingMutex sync.Mutex
	nextWorkID   int64 // Atomic counter for work IDs

	// Context management
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// shardTarget represents a single shard with its own connection, key range, and workers
type shardTarget struct {
	shardID              int
	writeDB              *sql.DB
	keyRange             keyRange // Parsed key range for this shard
	chunkletBuffer       chan shardedChunklet
	chunkletCompletions  chan shardedChunkletCompletion
	writeWorkersCount    int32
	writeWorkersFinished int32
	workerIDCounter      int32
	logger               *slog.Logger
	dbConfig             *dbconn.DBConfig
}

// shardedChunklet represents a chunklet destined for a specific shard
type shardedChunklet struct {
	workID  int64        // ID of the parent work
	shardID int          // Which shard this belongs to
	chunk   *table.Chunk // Original chunk for column info
	rows    []rowData    // Rows for this shard
}

// shardedChunkletCompletion represents a completed sharded chunklet
type shardedChunkletCompletion struct {
	workID       int64 // ID of the parent work
	shardID      int   // Which shard this came from
	affectedRows int64 // Rows affected by this chunklet
	err          error // Error if any
}

// NewShardedApplier creates a new ShardedApplier with multiple target databases.
//
// Parameters:
//   - targets: Slice of Target structs containing DB and key range for each shard
//   - vindexColumn: Name of the column to hash (e.g., "user_id") - REQUIRED
//   - vindexFunc: Hash function that takes a column value and returns uint64
//   - dbConfig: Database configuration
//   - logger: Logger instance
//
// The vindexColumn will be extracted from each row based on the table metadata,
// and the vindexFunc will be applied to determine which shard the row belongs to
// by matching the hash against the target key ranges.
func NewShardedApplier(targets []Target, vindexColumn string, vindexFunc VindexFunc, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*ShardedApplier, error) {
	if vindexColumn == "" {
		return nil, errors.New("vindexColumn is required for ShardedApplier")
	}

	shards := make([]*shardTarget, len(targets))
	for i, target := range targets {
		// Parse the key range
		kr, err := parseKeyRange(target.KeyRange)
		if err != nil {
			return nil, fmt.Errorf("failed to parse key range for shard %d: %w", i, err)
		}

		shards[i] = &shardTarget{
			shardID:             i,
			writeDB:             target.DB,
			keyRange:            kr,
			chunkletBuffer:      make(chan shardedChunklet, defaultBufferSize),
			chunkletCompletions: make(chan shardedChunkletCompletion, defaultBufferSize),
			writeWorkersCount:   defaultWriteWorkers / int32(len(targets)), // Divide workers among shards
			logger:              logger,
			dbConfig:            dbConfig,
		}
		// Ensure at least 1 worker per shard
		if shards[i].writeWorkersCount < 1 {
			shards[i].writeWorkersCount = 1
		}
	}

	// Log the parsed key ranges for debugging
	for i, shard := range shards {
		logger.Info("parsed key range for shard",
			"shardID", i,
			"keyRange", targets[i].KeyRange,
			"start", fmt.Sprintf("0x%016x", shard.keyRange.start),
			"end", fmt.Sprintf("0x%016x", shard.keyRange.end))
	}

	return &ShardedApplier{
		shards:       shards,
		vindexFunc:   vindexFunc,
		vindexColumn: vindexColumn,
		dbConfig:     dbConfig,
		logger:       logger,
		pendingWork:  make(map[int64]*pendingWork),
	}, nil
}

// Start initializes all shard workers and begins processing
func (a *ShardedApplier) Start(ctx context.Context) error {
	workerCtx, cancelFunc := context.WithCancel(ctx)
	a.cancelFunc = cancelFunc

	a.logger.Info("starting ShardedApplier", "shardCount", len(a.shards))

	// Start workers for each shard
	for _, shard := range a.shards {
		for range shard.writeWorkersCount {
			a.wg.Add(1)
			go a.writeWorker(workerCtx, shard)
		}
	}

	// Start a single feedback coordinator for all shards
	a.wg.Add(1)
	go a.feedbackCoordinator(workerCtx)

	return nil
}

// Apply sends rows to be written to the appropriate target shards.
// Rows are distributed across shards based on the vindex column and hash function.
func (a *ShardedApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	a.logger.Info("Apply called", "rowCount", len(rows), "vindexColumn", a.vindexColumn)

	if len(rows) == 0 {
		// No rows to apply, invoke callback immediately
		callback(0, nil)
		return nil
	}

	// Find the ordinal position of the vindex column
	vindexOrdinal := -1
	for i, col := range chunk.Table.Columns {
		if col == a.vindexColumn {
			vindexOrdinal = i
			break
		}
	}
	if vindexOrdinal == -1 {
		return fmt.Errorf("vindex column %s not found in table %s", a.vindexColumn, chunk.Table.TableName)
	}

	a.logger.Info("Found vindex column", "vindexColumn", a.vindexColumn, "ordinal", vindexOrdinal)

	// Assign a work ID for tracking
	workID := atomic.AddInt64(&a.nextWorkID, 1)

	// Group rows by shard
	shardRows := make([][]rowData, len(a.shards))
	for _, row := range rows {
		// Extract the vindex column value
		if vindexOrdinal >= len(row) {
			return fmt.Errorf("vindex column ordinal %d exceeds row length %d", vindexOrdinal, len(row))
		}
		vindexValue := row[vindexOrdinal]

		// Apply the hash function to get the hash value
		hashValue, err := a.vindexFunc(vindexValue)
		if err != nil {
			return fmt.Errorf("vindex function error: %w", err)
		}

		// Find which shard's key range contains this hash value
		shardID := -1
		for i, shard := range a.shards {
			contains := shard.keyRange.contains(hashValue)
			a.logger.Info("checking shard for hash",
				"vindexValue", vindexValue,
				"hashValue", fmt.Sprintf("0x%016x", hashValue),
				"shardID", i,
				"shardStart", fmt.Sprintf("0x%016x", shard.keyRange.start),
				"shardEnd", fmt.Sprintf("0x%016x", shard.keyRange.end),
				"contains", contains)
			if contains {
				shardID = i
				break
			}
		}
		if shardID == -1 {
			return fmt.Errorf("no shard found for hash value %x (vindex column: %s, value: %v)",
				hashValue, a.vindexColumn, vindexValue)
		}

		// Add to the appropriate shard's row list
		shardRows[shardID] = append(shardRows[shardID], rowData{values: row})
	}

	// Count total chunklets across all shards
	totalChunklets := 0
	for _, rows := range shardRows {
		if len(rows) > 0 {
			totalChunklets += (len(rows) + chunkletSize - 1) / chunkletSize
		}
	}

	// Register the pending work
	a.pendingMutex.Lock()
	a.pendingWork[workID] = &pendingWork{
		callback:           callback,
		totalChunklets:     totalChunklets,
		completedChunklets: 0,
		totalAffectedRows:  0,
	}
	a.pendingMutex.Unlock()

	// Send chunklets to each shard
	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			continue
		}

		// Split into chunklets and send to the shard's buffer
		for i := 0; i < len(rows); i += chunkletSize {
			end := min(i+chunkletSize, len(rows))

			chunkletData := shardedChunklet{
				workID:  workID,
				shardID: shardID,
				chunk:   chunk,
				rows:    rows[i:end],
			}

			select {
			case a.shards[shardID].chunkletBuffer <- chunkletData:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// Wait blocks until all pending work is complete and all callbacks have been invoked
func (a *ShardedApplier) Wait(ctx context.Context) error {
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

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue loop
		}
	}
}

// Close signals the applier to shut down gracefully
func (a *ShardedApplier) Close() error {
	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// Close all shard buffers
	for _, shard := range a.shards {
		close(shard.chunkletBuffer)
	}

	a.wg.Wait()
	return nil
}

// writeWorker processes chunklets for a specific shard
func (a *ShardedApplier) writeWorker(ctx context.Context, shard *shardTarget) {
	defer a.wg.Done()
	workerID := atomic.AddInt32(&shard.workerIDCounter, 1)

	defer func() {
		finishedCount := atomic.AddInt32(&shard.writeWorkersFinished, 1)
		a.logger.Debug("writeWorker finished", "shardID", shard.shardID, "workerID", workerID,
			"finishedCount", finishedCount, "totalWorkers", shard.writeWorkersCount)

		// If all write workers for this shard are finished, close its completions channel
		if finishedCount == shard.writeWorkersCount {
			a.logger.Debug("writeWorker all workers finished for shard, closing completions channel",
				"shardID", shard.shardID)
			close(shard.chunkletCompletions)
		}
	}()

	for {
		select {
		case chunkletData, ok := <-shard.chunkletBuffer:
			if !ok {
				a.logger.Debug("writeWorker channel closed, exiting", "shardID", shard.shardID, "workerID", workerID)
				return
			}

			a.logger.Debug("writeWorker processing chunklet", "shardID", shard.shardID,
				"workerID", workerID, "workID", chunkletData.workID, "rowCount", len(chunkletData.rows))

			// Write chunklet to this shard
			affectedRows, err := a.writeChunklet(ctx, shard, chunkletData)

			// Send completion
			completion := shardedChunkletCompletion{
				workID:       chunkletData.workID,
				shardID:      shard.shardID,
				affectedRows: affectedRows,
				err:          err,
			}

			select {
			case shard.chunkletCompletions <- completion:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// writeChunklet writes a single chunklet to a specific shard
func (a *ShardedApplier) writeChunklet(ctx context.Context, shard *shardTarget, chunkletData shardedChunklet) (int64, error) {
	if len(chunkletData.rows) == 0 {
		return 0, nil
	}

	// Get the intersected column names
	columnNames := utils.IntersectNonGeneratedColumnsAsSlice(chunkletData.chunk.Table, chunkletData.chunk.NewTable)
	columnList := utils.IntersectNonGeneratedColumns(chunkletData.chunk.Table, chunkletData.chunk.NewTable)

	// Build VALUES clauses for all rows in the chunklet
	var valuesClauses []string
	for _, row := range chunkletData.rows {
		if len(columnNames) != len(row.values) {
			return 0, fmt.Errorf("column count mismatch: chunk %s has %d columns, but chunklet has %d values",
				chunkletData.chunk.String(), len(columnNames), len(row.values))
		}
		var values []string
		for i, value := range row.values {
			columnType, ok := chunkletData.chunk.NewTable.GetColumnMySQLType(columnNames[i])
			if !ok {
				return 0, fmt.Errorf("column %s not found in table info", columnNames[i])
			}
			values = append(values, utils.EscapeMySQLType(columnType, value))
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// Build the INSERT statement
	// Note: We use just the table name, not the fully qualified name, because
	// the database connection (shard.writeDB) already determines which database to write to
	tableName := fmt.Sprintf("`%s`", chunkletData.chunk.NewTable.TableName)
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		tableName,
		columnList,
		strings.Join(valuesClauses, ", "),
	)

	a.logger.Debug("writing chunklet to shard", "shardID", shard.shardID,
		"rowCount", len(chunkletData.rows), "table", chunkletData.chunk.NewTable.QuotedName)

	// Execute the batch insert on this shard's database
	result, err := dbconn.RetryableTransaction(ctx, shard.writeDB, true, shard.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute chunklet insert on shard %d: %w", shard.shardID, err)
	}

	return result, nil
}

// feedbackCoordinator tracks chunklet completions from all shards and invokes callbacks when work is done
func (a *ShardedApplier) feedbackCoordinator(ctx context.Context) {
	defer a.wg.Done()
	a.logger.Debug("feedbackCoordinator started")

	// Create a merged channel to receive completions from all shards
	mergedCompletions := make(chan shardedChunkletCompletion, defaultBufferSize)

	// Start goroutines to forward completions from each shard to the merged channel
	var forwardWg sync.WaitGroup
	for _, shard := range a.shards {
		forwardWg.Add(1)
		go func(s *shardTarget) {
			defer forwardWg.Done()
			for completion := range s.chunkletCompletions {
				select {
				case mergedCompletions <- completion:
				case <-ctx.Done():
					return
				}
			}
		}(shard)
	}

	// Close merged channel when all shard channels are closed
	go func() {
		forwardWg.Wait()
		close(mergedCompletions)
	}()

	// Process completions
	for {
		select {
		case completion, ok := <-mergedCompletions:
			if !ok {
				a.logger.Debug("feedbackCoordinator merged completions channel closed, exiting")
				return
			}

			a.logger.Debug("feedbackCoordinator received chunklet completion",
				"workID", completion.workID, "shardID", completion.shardID)

			// Update work completion status
			a.pendingMutex.Lock()
			pending, exists := a.pendingWork[completion.workID]
			if !exists {
				a.pendingMutex.Unlock()
				a.logger.Error("feedbackCoordinator received completion for unknown work", "workID", completion.workID)
				continue
			}

			// If there was an error, invoke callback immediately
			if completion.err != nil {
				callback := pending.callback
				a.pendingMutex.Unlock()
				callback(0, completion.err)
				continue
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

		case <-ctx.Done():
			return
		}
	}
}

// DeleteKeys deletes rows by their key values synchronously, distributing across shards.
// The keys are hashed key strings (from utils.HashKey).
// If lock is non-nil, operations are executed under table locks (one per shard).
//
// Note: This method requires that the vindex column is part of the primary key,
// or that we can extract it from the key somehow. For now, we assume the vindex
// column is the first column in the key.
func (a *ShardedApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys []string, lock *dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Find the ordinal position of the vindex column in the key columns
	vindexOrdinalInKey := -1
	for i, col := range sourceTable.KeyColumns {
		if col == a.vindexColumn {
			vindexOrdinalInKey = i
			break
		}
	}
	if vindexOrdinalInKey == -1 {
		return 0, fmt.Errorf("vindex column %s not found in key columns for DeleteKeys", a.vindexColumn)
	}

	// Group keys by shard
	shardKeys := make([][]string, len(a.shards))
	for _, key := range keys {
		// Unhash the key to get the actual key values
		keyStrings := utils.UnhashKey(key)

		if vindexOrdinalInKey >= len(keyStrings) {
			return 0, fmt.Errorf("vindex column ordinal %d exceeds key length %d", vindexOrdinalInKey, len(keyStrings))
		}

		// Extract the vindex column value from the key
		vindexValueStr := keyStrings[vindexOrdinalInKey]

		// Convert to the appropriate type (for now, assume int64)
		// TODO: This should be more robust and handle different types
		var vindexValue int64
		_, err := fmt.Sscanf(vindexValueStr, "%d", &vindexValue)
		if err != nil {
			return 0, fmt.Errorf("failed to parse vindex value %s as int64: %w", vindexValueStr, err)
		}

		// Apply the hash function
		hashValue, err := a.vindexFunc(vindexValue)
		if err != nil {
			return 0, fmt.Errorf("vindex function error: %w", err)
		}

		// Find which shard's key range contains this hash value
		shardID := -1
		for i, shard := range a.shards {
			if shard.keyRange.contains(hashValue) {
				shardID = i
				break
			}
		}
		if shardID == -1 {
			return 0, fmt.Errorf("no shard found for hash value %x (vindex column: %s, value: %v)",
				hashValue, a.vindexColumn, vindexValue)
		}

		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

	// Execute deletes on each shard in parallel
	var totalAffected int64
	var mu sync.Mutex
	var errGroup error

	var wg sync.WaitGroup
	for shardID, keys := range shardKeys {
		if len(keys) == 0 {
			continue
		}

		wg.Add(1)
		go func(sid int, k []string) {
			defer wg.Done()

			// Convert hashed keys to row value constructor format
			var pkValues []string
			for _, key := range k {
				pkValues = append(pkValues, utils.UnhashKeyToString(key))
			}

			// Build DELETE statement
			deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
				targetTable.QuotedName,
				table.QuoteColumns(sourceTable.KeyColumns),
				strings.Join(pkValues, ","),
			)

			a.logger.Debug("executing delete on shard", "shardID", sid, "keyCount", len(k), "table", targetTable.QuotedName)

			var affected int64
			var err error

			// Execute under lock if provided
			if lock != nil {
				if err = lock.ExecUnderLock(ctx, deleteStmt); err != nil {
					err = fmt.Errorf("failed to execute delete under lock on shard %d: %w", sid, err)
				} else {
					affected = int64(len(k))
				}
			} else {
				// Execute as a retryable transaction
				affected, err = dbconn.RetryableTransaction(ctx, a.shards[sid].writeDB, false, a.shards[sid].dbConfig, deleteStmt)
				if err != nil {
					err = fmt.Errorf("failed to execute delete on shard %d: %w", sid, err)
				}
			}

			mu.Lock()
			if err != nil && errGroup == nil {
				errGroup = err
			}
			totalAffected += affected
			mu.Unlock()
		}(shardID, keys)
	}

	wg.Wait()

	if errGroup != nil {
		return 0, errGroup
	}

	return totalAffected, nil
}

// UpsertRows performs upserts synchronously, distributing across shards.
// The rows are LogicalRow structs containing the row images.
// If lock is non-nil, operations are executed under table locks (one per shard).
func (a *ShardedApplier) UpsertRows(ctx context.Context, sourceTable, targetTable *table.TableInfo, rows []LogicalRow, lock *dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Find the ordinal position of the vindex column
	vindexOrdinal := -1
	for i, col := range sourceTable.Columns {
		if col == a.vindexColumn {
			vindexOrdinal = i
			break
		}
	}
	if vindexOrdinal == -1 {
		return 0, fmt.Errorf("vindex column %s not found in table %s", a.vindexColumn, sourceTable.TableName)
	}

	// Get the intersected column indices
	var intersectedColumns []int
	for i, sourceCol := range sourceTable.NonGeneratedColumns {
		if slices.Contains(targetTable.NonGeneratedColumns, sourceCol) {
			intersectedColumns = append(intersectedColumns, i)
		}
	}

	// Group rows by shard
	shardRows := make([][]LogicalRow, len(a.shards))
	for _, row := range rows {
		if row.IsDeleted {
			continue // Skip deleted rows
		}

		// Extract the vindex column value from the row image
		if vindexOrdinal >= len(row.RowImage) {
			return 0, fmt.Errorf("vindex column ordinal %d exceeds row image length %d", vindexOrdinal, len(row.RowImage))
		}
		vindexValue := row.RowImage[vindexOrdinal]

		// Apply the hash function to get the hash value
		hashValue, err := a.vindexFunc(vindexValue)
		if err != nil {
			return 0, fmt.Errorf("vindex function error: %w", err)
		}

		// Find which shard's key range contains this hash value
		shardID := -1
		for i, shard := range a.shards {
			if shard.keyRange.contains(hashValue) {
				shardID = i
				break
			}
		}
		if shardID == -1 {
			return 0, fmt.Errorf("no shard found for hash value %x (vindex column: %s, value: %v)",
				hashValue, a.vindexColumn, vindexValue)
		}

		shardRows[shardID] = append(shardRows[shardID], row)
	}

	// Execute upserts on each shard in parallel
	var totalAffected int64
	var mu sync.Mutex
	var errGroup error

	var wg sync.WaitGroup
	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			continue
		}

		wg.Add(1)
		go func(sid int, r []LogicalRow) {
			defer wg.Done()

			// Build the upsert statement for this shard
			columnList := utils.IntersectNonGeneratedColumns(sourceTable, targetTable)
			columnNames := utils.IntersectNonGeneratedColumnsAsSlice(sourceTable, targetTable)

			// Build the VALUES clause
			var valuesClauses []string
			for _, logicalRow := range r {
				var values []string
				for i, colIndex := range intersectedColumns {
					if colIndex >= len(logicalRow.RowImage) {
						mu.Lock()
						if errGroup == nil {
							errGroup = fmt.Errorf("column index %d exceeds row image length %d", colIndex, len(logicalRow.RowImage))
						}
						mu.Unlock()
						return
					}
					value := logicalRow.RowImage[colIndex]
					if value == nil {
						values = append(values, "NULL")
					} else {
						if i >= len(columnNames) {
							mu.Lock()
							if errGroup == nil {
								errGroup = fmt.Errorf("column index %d exceeds columnNames length %d", i, len(columnNames))
							}
							mu.Unlock()
							return
						}
						columnType, ok := sourceTable.GetColumnMySQLType(columnNames[i])
						if !ok {
							mu.Lock()
							if errGroup == nil {
								errGroup = fmt.Errorf("column %s not found in table info", columnNames[i])
							}
							mu.Unlock()
							return
						}
						values = append(values, utils.EscapeMySQLType(columnType, value))
					}
				}
				valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
			}

			// Build the ON DUPLICATE KEY UPDATE clause
			var updateClauses []string
			for _, col := range targetTable.NonGeneratedColumns {
				isPrimaryKey := slices.Contains(targetTable.KeyColumns, col)
				if !isPrimaryKey && slices.Contains(sourceTable.NonGeneratedColumns, col) {
					updateClauses = append(updateClauses, fmt.Sprintf("`%s` = new.`%s`", col, col))
				}
			}

			upsertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS new ON DUPLICATE KEY UPDATE %s",
				targetTable.QuotedName,
				columnList,
				strings.Join(valuesClauses, ", "),
				strings.Join(updateClauses, ", "),
			)

			a.logger.Debug("executing upsert on shard", "shardID", sid, "rowCount", len(valuesClauses), "table", targetTable.QuotedName)

			var affected int64
			var err error

			// Execute under lock if provided
			if lock != nil {
				if err = lock.ExecUnderLock(ctx, upsertStmt); err != nil {
					err = fmt.Errorf("failed to execute upsert under lock on shard %d: %w", sid, err)
				} else {
					affected = int64(len(valuesClauses))
				}
			} else {
				// Execute as a retryable transaction
				affected, err = dbconn.RetryableTransaction(ctx, a.shards[sid].writeDB, false, a.shards[sid].dbConfig, upsertStmt)
				if err != nil {
					err = fmt.Errorf("failed to execute upsert on shard %d: %w", sid, err)
				}
			}

			mu.Lock()
			if err != nil && errGroup == nil {
				errGroup = err
			}
			totalAffected += affected
			mu.Unlock()
		}(shardID, rows)
	}

	wg.Wait()

	if errGroup != nil {
		return 0, errGroup
	}

	return totalAffected, nil
}
