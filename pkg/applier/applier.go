package applier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
)

const (
	chunkletMaxRows     = 1000             // Maximum number of rows per chunklet
	chunkletMaxSize     = 1024 * 1024      // Maximum size in bytes per chunklet (1 MiB)
	defaultBufferSize   = 128              // Size of the shared buffer channel for chunklets
	defaultWriteWorkers = 2                // Number of write workers, default low for tests, but in practice we can use 40+
	chunkTaskTimeout    = time.Second * 60 // Timeout for any task (copy chunk, delete keys, upsert rows)
)

// Target represents a shard target with its database connection, configuration, and key range.
// Key ranges are expressed as Vitess-style strings (e.g., "-80", "80-", "80-c0").
// An empty string or "0" means all key space (unsharded).
type Target struct {
	DB       *sql.DB
	Config   *mysql.Config
	KeyRange string // Vitess-style key range: "-80", "80-", "80-c0", or "0" for unsharded
}

// ApplyCallback is invoked when rows have been safely flushed to the target(s).
// affectedRows is the total number of rows affected across all targets.
// err is non-nil if there was an error applying the rows.
type ApplyCallback func(affectedRows int64, err error)

// Applier is an interface for applying rows to one or more target databases.
// Implementations can apply to a single target (SingleTargetApplier) or fan out to
// multiple targets based on a hash function (ShardedApplier).
//
// The Applier is responsible for:
// - Batching/splitting rows into optimal write sizes
// - Tracking pending writes
// - Invoking callbacks when writes are complete
type Applier interface {
	// Start initializes the applier and starts its workers
	Start(ctx context.Context) error

	// Apply sends rows to be written to the target(s).
	// The chunk parameter provides metadata about the source table and target table.
	// The rows parameter contains the actual row data to be written.
	// The callback is invoked when all rows are safely flushed.
	//
	// For the copier: callback will call chunker.Feedback()
	// For the subscription: callback will update binlog coordinates
	Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error

	// DeleteKeys deletes rows by their key values synchronously.
	// The keys are hashed key strings (from utils.HashKey).
	// If lock is non-nil, the delete is executed under the table lock.
	// Returns the number of rows affected and any error.
	DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys []string, lock *dbconn.TableLock) (int64, error)

	// UpsertRows performs an upsert (INSERT ... ON DUPLICATE KEY UPDATE) synchronously.
	// The rows are LogicalRow structs containing the row images.
	// If lock is non-nil, the upsert is executed under the table lock.
	// Returns the number of rows affected and any error.
	UpsertRows(ctx context.Context, sourceTable, targetTable *table.TableInfo, rows []LogicalRow, lock *dbconn.TableLock) (int64, error)

	// Wait blocks until all pending work is complete and all callbacks have been invoked
	Wait(ctx context.Context) error

	// Stops the applier workers
	Stop() error

	// GetTargets returns target information for direct database access.
	// This is used by operations like checksum that need to query targets directly.
	// For SingleTargetApplier, this returns a single target.
	// For ShardedApplier, this returns all shards.
	GetTargets() []Target
}

// LogicalRow represents the current state of a row in the subscription buffer.
// This could be that it is deleted, or that it has RowImage that describes it.
// If there is a RowImage, then it needs to be converted into the RowImage of the
// newTable.
type LogicalRow struct {
	IsDeleted bool
	RowImage  []any
}

type ApplierConfig struct {
	Threads         int // number of write threads
	ChunkletMaxRows int
	ChunkletMaxSize int
	Logger          *slog.Logger
	DBConfig        *dbconn.DBConfig
}

// NewApplierDefaultConfig returns a default config for the applier.
func NewApplierDefaultConfig() *ApplierConfig {
	return &ApplierConfig{
		Threads:         defaultWriteWorkers,
		ChunkletMaxRows: chunkletMaxRows, // will be renamed soon.
		ChunkletMaxSize: 1024 * 1024,     // will be supported soon.
		Logger:          slog.Default(),
		DBConfig:        dbconn.NewDBConfig(),
	}
}

// Validate checks the ApplierConfig for required fields.
func (cfg *ApplierConfig) Validate() error {
	if cfg.DBConfig == nil {
		return errors.New("dbConfig must be non-nil")
	}
	if cfg.Logger == nil {
		return errors.New("logger must be non-nil")
	}
	// We can set defaults for other fields.
	// If they are not set its not important.
	if cfg.Threads <= 0 {
		cfg.Threads = defaultWriteWorkers
	}
	if cfg.ChunkletMaxRows <= 0 {
		cfg.ChunkletMaxRows = chunkletMaxRows
	}
	if cfg.ChunkletMaxSize <= 0 {
		cfg.ChunkletMaxSize = 1024 * 1024
	}
	return nil
}

// estimateRowSize estimates the size in bytes of a row's values.
// This is a simple heuristic based on string representation length.
// Since we use a conservative 1 MiB threshold vs typical 64 MiB max_allowed_packet,
// we don't need precise measurements. This is much better than
// just hoping 1000 rows will fit under max_allowed_packet.
func estimateRowSize(values []any) int {
	size := 2 // minimal overhead for parentheses
	for _, value := range values {
		// Estimate size based on string representation
		// +2 bytes for quotes and 2 bytes for comma/separator
		size += len(fmt.Sprintf("%v", value)) + 4
	}
	return size
}

// rowData represents a single row with all its column values
type rowData struct {
	values []any
}

// splitRowsIntoChunklets splits rows into chunklets based on both row count and size thresholds.
// Returns a slice of row batches where each batch respects both chunkletMaxRows and chunkletMaxSize limits.
//
// Note: A single row can exceed chunkletMaxSize by itself. In this case, the row will be placed
// in its own chunklet regardless of size. This is an edge case where we rely on max_allowed_packet
// being large enough (typically 64 MiB default vs our 1 MiB threshold).
func splitRowsIntoChunklets(rows []rowData) [][]rowData {
	if len(rows) == 0 {
		return nil
	}
	var chunklets [][]rowData
	currentChunklet := make([]rowData, 0, chunkletMaxRows)
	currentSize := 0

	for _, row := range rows {
		rowSize := estimateRowSize(row.values)
		// Check if adding this row would exceed either threshold
		if len(currentChunklet) >= chunkletMaxRows ||
			(len(currentChunklet) > 0 && currentSize+rowSize > chunkletMaxSize) {
			// Save current chunklet and start a new one
			chunklets = append(chunklets, currentChunklet)
			currentChunklet = make([]rowData, 0, chunkletMaxRows)
			currentSize = 0
		}
		// Add row to current chunklet
		currentChunklet = append(currentChunklet, row)
		currentSize += rowSize
	}
	// Don't forget the last chunklet
	if len(currentChunklet) > 0 {
		chunklets = append(chunklets, currentChunklet)
	}
	return chunklets
}
