package checksum

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
)

var (
	// Query template for row checksums
	queryTemplate = "SELECT CRC32(CONCAT(%s)) as row_checksum, CONCAT_WS(',', %s) as pk FROM %s WHERE %s"

	// ErrYieldTimeout is returned by runChecksum when the yield timeout expires.
	// This is distinct from the parent context being canceled, and signals that
	// the checksum should resume from the current watermark after releasing
	// long-running transactions to reduce HLL (history list length) growth.
	ErrYieldTimeout = errors.New("checksum yield timeout")

	// DefaultYieldTimeout is the default maximum duration for a single checksum
	// pass before yielding to release long-running REPEATABLE READ transactions.
	DefaultYieldTimeout = 24 * time.Hour

	// fixChunkTimeout bounds the DELETE + REPLACE (or DELETE + Apply) pair that
	// recopies a mismatched chunk. The pair runs under a context derived from
	// context.WithoutCancel so a sentinel-drop cancellation can't leave the
	// target in a partial state between the two transactions. The bound still
	// catches the case where one transaction is hung.
	fixChunkTimeout = 1 * time.Minute
)

type Checker interface {
	// Run performs the checksum operation.
	Run(ctx context.Context) error
	GetProgress() string
	StartTime() time.Time
	ExecTime() time.Duration
	// DifferencesFound returns the number of chunks where a source/target
	// mismatch was detected during the most recent (or in-flight) pass.
	// Useful for callers that need to distinguish "clean cancellation" from
	// "cancellation while a fix may have been mid-flight" — the continuous-
	// checksum loop uses it to decide whether a sentinel-drop swallow is
	// safe.
	DifferencesFound() uint64
}

type CheckerConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          *slog.Logger
	FixDifferences  bool
	Watermark       string // optional; defines a watermark to start from
	MaxRetries      int
	Applier         applier.Applier // optional; indicates it is a distributed checker
	YieldTimeout    time.Duration   // maximum duration for a single checksum pass before yielding to release long-running transactions
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		DBConfig:        dbconn.NewDBConfig(),
		Logger:          slog.Default(),
		FixDifferences:  false,
		MaxRetries:      3,
		YieldTimeout:    DefaultYieldTimeout,
	}
}

// NewChecker creates a new checksum object.
// sourceDBs contains the source database connections (one for single-source migrations,
// multiple for N:M moves). The distributed checker aggregates checksums across all sources.
// The single checker uses sourceDBs[0].
func NewChecker(sourceDBs []*sql.DB, chunker table.Chunker, feeds []*repl.Client, config *CheckerConfig) (Checker, error) {
	if len(sourceDBs) == 0 {
		return nil, errors.New("at least one source database must be provided")
	}
	if len(feeds) == 0 {
		return nil, errors.New("at least one feed must be provided")
	}
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		return nil, errors.New("dbconfig must be non-nil")
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.YieldTimeout == 0 {
		config.YieldTimeout = DefaultYieldTimeout
	}
	if config.Applier != nil {
		return &DistributedChecker{
			concurrency:    config.Concurrency,
			sourceDBs:      sourceDBs,
			feeds:          feeds,
			chunker:        chunker,
			dbConfig:       config.DBConfig,
			logger:         config.Logger,
			fixDifferences: config.FixDifferences,
			maxRetries:     config.MaxRetries,
			applier:        config.Applier,
		}, nil
	}
	return &SingleChecker{
		concurrency:    config.Concurrency,
		db:             sourceDBs[0],
		feed:           feeds[0],
		chunker:        chunker,
		dbConfig:       config.DBConfig,
		logger:         config.Logger,
		fixDifferences: config.FixDifferences,
		maxRetries:     config.MaxRetries,
		yieldTimeout:   config.YieldTimeout,
	}, nil
}
