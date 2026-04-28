package table

import (
	"log/slog"
	"time"
)

const (
	// StartingChunkSize is the initial chunkSize
	StartingChunkSize = 1000
	// MaxDynamicStepFactor is the maximum amount each recalculation of the dynamic chunkSize can
	// increase by. For example, if the newTarget is 5000 but the current target is 1000, the newTarget
	// will be capped back down to 1500. Over time the number 5000 will be reached, but not straight away.
	MaxDynamicStepFactor = 1.5
	// MinDynamicRowSize is the minimum chunkSize that can be used when dynamic chunkSize is enabled.
	// This helps prevent a scenario where the chunk size is too small (it can never be less than 1).
	MinDynamicRowSize = 10
	// MaxDynamicRowSize is the max allowed chunkSize that can be used when dynamic chunkSize is enabled.
	// This seems like a safe upper bound for now
	MaxDynamicRowSize = 100000
	// DynamicPanicFactor is the factor by which the feedback process takes immediate action when
	// the chunkSize appears to be too large. For example, if the PanicFactor is 5, and the target *time*
	// is 50ms, an actual time 250ms+ will cause the dynamic chunk size to immediately be reduced.
	DynamicPanicFactor = 5

	// ChunkerDefaultTarget is the default chunker target
	ChunkerDefaultTarget = 100 * time.Millisecond
)

type Chunker interface { //nolint: interfacebloat
	Open() error
	IsRead() bool
	Close() error
	Next() (*Chunk, error)
	Feedback(chunk *Chunk, duration time.Duration, actualRows uint64)
	KeyAboveHighWatermark(key0 any) bool
	KeyBelowLowWatermark(key0 any) bool
	Progress() (rowsRead uint64, chunksCopied uint64, totalRowsExpected uint64)
	OpenAtWatermark(watermark string) error
	GetLowWatermark() (watermark string, err error)
	// Reset resets the chunker to start from the beginning, as if Open() was just called.
	// This is used when retrying operations like checksums.
	Reset() error
	// Tables return a list of table names
	// By convention the first table is the "current" table,
	// and the second table (if any) is the "new" table.
	// There could be more than 2 tables in the case of multi-chunker.
	// In which case every second table is the "new" table, etc.
	Tables() []*TableInfo
}

// ChunkerConfig holds optional configuration for creating a Chunker.
// Only the source table (passed as the first argument to NewChunker) is required;
// all other fields have sensible defaults.
type ChunkerConfig struct {
	// NewTable is the destination table. If nil, defaults to the source table
	// (for move operations where there is no distinct new table).
	NewTable *TableInfo
	// TargetChunkTime is the target duration for each chunk. Defaults to ChunkerDefaultTarget.
	TargetChunkTime time.Duration
	// Logger is the structured logger. Defaults to slog.Default().
	Logger *slog.Logger
	// Key and Where are used for composite chunkers to specify a non-primary key index.
	// When Key is set, the composite chunker is always used regardless of whether the
	// table has an auto-increment primary key.
	Key   string
	Where string
}

// NewChunker creates a new Chunker for the given source table.
// It selects the optimistic chunker for single-column auto-increment primary keys
// (unless Key/Where overrides are specified), and the composite chunker otherwise.
func NewChunker(t *TableInfo, config ChunkerConfig) (Chunker, error) {
	if config.TargetChunkTime == 0 {
		config.TargetChunkTime = ChunkerDefaultTarget
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	newTable := config.NewTable
	if newTable == nil {
		// This supports moveTable cases where there is no new table per-se.
		// But the chunker needs a non-nil newTable for some operations,
		// like resuming from a checkpoint.
		newTable = t
	}
	// Use the optimistic chunker for auto_increment tables with a single
	// column key, unless a specific key/where is requested.
	if len(t.KeyColumns) == 1 && t.KeyIsAutoInc && config.Key == "" && config.Where == "" {
		return &chunkerOptimistic{
			Ti:                     t,
			NewTi:                  newTable,
			ChunkerTarget:          config.TargetChunkTime,
			lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
			logger:                 config.Logger,
		}, nil
	}
	return &chunkerComposite{
		Ti:                     t,
		NewTi:                  newTable,
		ChunkerTarget:          config.TargetChunkTime,
		keyName:                config.Key,
		where:                  config.Where,
		lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
		logger:                 config.Logger,
	}, nil
}
