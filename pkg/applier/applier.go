package applier

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
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
	Threads         int
	ChunkletMaxRows int
	ChunkletMaxSize int
	Logger          *slog.Logger
	DBConfig        *dbconn.DBConfig
}

// NewApplierDefaultConfig returns a default config for the applier.
func NewApplierDefaultConfig() *ApplierConfig {
	return &ApplierConfig{
		Threads:         defaultWriteWorkers,
		ChunkletMaxRows: chunkletSize, // will be renamed soon.
		ChunkletMaxSize: 1024 * 1024,  // will be supported soon.
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
	if cfg.Threads <= 0 {
		return errors.New("threads must be greater than 0")
	}
	if cfg.ChunkletMaxRows <= 0 {
		return errors.New("chunkletMaxRows must be greater than 0")
	}
	if cfg.ChunkletMaxSize <= 0 {
		return errors.New("chunkletMaxSize must be greater than 0")
	}
	return nil
}
