// Package copier copies rows from one table to another.
// it makes use of tableinfo.Chunker, and does the parallelism
// and retries here. It fails on the first error.
package copier

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
)

const (
	copyEstimateInterval   = 10 * time.Second // how frequently to re-estimate copy speed
	copyETAInitialWaitTime = 1 * time.Minute  // how long to wait before first estimating copy speed (to allow for fast start)
)

// Copier is the interface which copiers use. Currently we only have
// one implementation, which we call unbuffered because it uses
// INSERT .. SELECT without any intermediate buffering in spirit.
// In future we may have another implementation, see:
// https://github.com/block/spirit/issues/451
type Copier interface {
	Run(ctx context.Context) error
	GetETA() string
	GetChunker() table.Chunker
	SetThrottler(throttler throttler.Throttler)
	GetThrottler() throttler.Throttler
	StartTime() time.Time
	GetProgress() string
}

type CopierConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	Throttler       throttler.Throttler
	Logger          *slog.Logger
	MetricsSink     metrics.Sink
	DBConfig        *dbconn.DBConfig
	// Applier is used by the buffered copier to write rows to the destination.
	// It is also used by callers (migration/move runner) for the replication
	// client. Construction is shared so that both paths use the same applier
	// for buffered copy (the default). When the unbuffered copier is selected
	// (Unbuffered=true) the copier ignores the applier.
	Applier applier.Applier
	// Unbuffered selects the legacy unbuffered copier, which issues
	// INSERT IGNORE INTO _new ... SELECT FROM original directly and ignores
	// Applier. When false (the default), the buffered copier is used, which
	// streams row images through Applier and therefore requires a non-nil
	// Applier. NewCopierDefaultConfig leaves this false (buffered), matching the
	// production default.
	Unbuffered bool
	// Autoscale configures experimental dynamic write-thread scaling. When
	// disabled (the default) the copier behaves exactly as before. See
	// AutoscaleConfig and issue #831.
	Autoscale AutoscaleConfig
}

// AutoscaleConfig controls the experimental write-thread autoscaler driven by
// throttler utilization. It only applies to the buffered copier whose Applier
// implements the dynamic-scaling capability (SingleTargetApplier).
type AutoscaleConfig struct {
	// Enabled gates the whole feature (the --enable-experimental-autoscaling
	// flag). Off by default.
	Enabled bool
	// StartThreads is the resolved write-thread count the applier was started
	// at; the controller scales from here.
	StartThreads int
	// MaxThreads is the cap the controller may scale up to.
	MaxThreads int
}

// NewCopierDefaultConfig returns a default config for the copier. It defaults
// to the buffered copier (Unbuffered=false), matching the production default,
// so callers must supply an Applier (see CopierConfig.Applier). Tests that want
// the legacy unbuffered copier set Unbuffered=true, which needs no Applier.
func NewCopierDefaultConfig() *CopierConfig {
	return &CopierConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		Throttler:       &throttler.Noop{},
		Logger:          slog.Default(),
		MetricsSink:     &metrics.NoopSink{},
		DBConfig:        dbconn.NewDBConfig(),
	}
}

// NewCopier creates a new copier object with the provided chunker.
// The chunker could have been opened at a watermark, we are agnostic to that.
// It could also return different tables on each Next() call in future,
// so we don't save any fields related to the table.
func NewCopier(db *sql.DB, chunker table.Chunker, config *CopierConfig) (Copier, error) {
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		return nil, errors.New("dbConfig must be non-nil")
	}
	if config.Unbuffered {
		return &Unbuffered{
			db:               db,
			concurrency:      config.Concurrency,
			throttler:        config.Throttler,
			chunker:          chunker,
			logger:           config.Logger,
			metricsSink:      config.MetricsSink,
			dbConfig:         config.DBConfig,
			copierEtaHistory: newcopierEtaHistory(),
		}, nil
	}
	if config.Applier == nil {
		return nil, errors.New("buffered copier requires a non-nil Applier")
	}
	return &buffered{
		db:               db,
		concurrency:      config.Concurrency,
		throttler:        config.Throttler,
		chunker:          chunker,
		logger:           config.Logger,
		metricsSink:      config.MetricsSink,
		dbConfig:         config.DBConfig,
		copierEtaHistory: newcopierEtaHistory(),
		applier:          config.Applier,
		autoscale:        config.Autoscale,
	}, nil
}
