package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
)

var (
	// Query template for row checksums. The first %s is the column expression
	// list from table.ColumnMapping.ChecksumExprs(), which already interleaves
	// a '#' separator between values so content cannot shift across column
	// boundaries undetected.
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
	// catches the case where one transaction is hung. This applies to every
	// repair path (initial and continuous checksum), so it has to be generous
	// enough for legitimate large/slow recopies on busy or distant replicas.
	fixChunkTimeout = 10 * time.Minute
)

// DefaultTargetChunkTime is the read time the checksum aims for per chunk.
//
// It is much larger than the copier's target because it is bounding a different
// thing. A copy chunk's time is a write transaction's lifetime, so it is a
// latency budget. A checksum chunk is a read-only aggregate inside a snapshot
// that is already held for the whole pass, and only one row per chunk crosses
// the wire no matter how many rows it covers — so a longer chunk buys longer
// sequential scans (which is what engages InnoDB linear read-ahead and Aurora's
// batched prefetch) at no cost in lock or memory terms.
//
// In practice this is not the binding constraint on a healthy server:
// table.MaxDynamicRowSize caps a chunk at 100k rows, and 100k rows of a typical
// table is well under 10s of read. That is deliberate — the row cap is a bound
// we can reason about (it is what bounds a repair, and what
// SingleChecker.inspectDifferences holds in memory on a mismatch), whereas a
// time target that binds would let chunk size follow load. The target's job is
// to be the safety valve for the case the cap cannot see: rows so wide, or
// storage so slow, that even 100k rows is too much work for one chunk. At the
// copier's 500ms that valve trips on perfectly healthy tables and shrinks chunks
// precisely when read-ahead matters most.
const DefaultTargetChunkTime = 10 * time.Second

// ChunkStartRows is the row count a checksum chunker starts at, as opposed to
// the copier's much smaller table.StartingChunkSize.
//
// The dynamic sizer converges upward slowly on purpose: growth is capped at
// table.MaxDynamicStepFactor per feedback window, and a window is 10 chunks. From
// 1000 rows that is ~130 chunks to reach table.MaxDynamicRowSize — more chunks
// than many whole tables have, so the checksum could spend an entire pass
// converging and never once use the chunk size it had measured as correct. (A
// 1M-row table measured 126 chunks, ending at 25k rows, with p90 chunk time at
// 6% of the 500ms target and not one chunk reaching the row cap.)
//
// Starting at the cap inverts that, and the asymmetry justifies it: the sizer
// shrinks without any per-step cap and panic-shrinks on a single chunk that
// exceeds DynamicPanicFactor × the target, so overshooting costs one slow chunk,
// while undershooting costs the whole pass.
const ChunkStartRows = table.MaxDynamicRowSize

// chunkMismatch describes why a chunk's source and target disagreed. It is
// returned by compareChunk so the caller can log a debuggable reason while
// treating any mismatch (checksum OR row count) identically — same retry,
// recopy, and differencesFound accounting.
type chunkMismatch struct {
	// checksumDiffers is true when the (aggregated) source and target CRC
	// differ.
	checksumDiffers bool
	// countDiffers is true when the (aggregated) source and target row
	// counts differ. This is the defense-in-depth signal that the CRC alone
	// can miss: BIT_XOR is pair-cancelling, so a row duplicated across two
	// sources (violating disjointness) or a row whose CRC32 happens to be 0
	// contributes nothing to the XOR, yet the count still moves.
	countDiffers bool
}

// mismatched reports whether the chunk is divergent for any reason.
func (m chunkMismatch) mismatched() bool {
	return m.checksumDiffers || m.countDiffers
}

// reason returns a human-readable description distinguishing a checksum
// mismatch from a row-count mismatch (and reporting both when both differ)
// for log/error debuggability. Only meaningful when mismatched() is true.
func (m chunkMismatch) reason(srcCount, tgtCount uint64) string {
	switch {
	case m.checksumDiffers && m.countDiffers:
		return fmt.Sprintf("checksum mismatch and row count mismatch (src=%d, target=%d)", srcCount, tgtCount)
	case m.countDiffers:
		return fmt.Sprintf("row count mismatch (src=%d, target=%d)", srcCount, tgtCount)
	default:
		return "checksum mismatch"
	}
}

// compareChunk is the central decision function used by every checker to
// decide whether a chunk's source and target agree. It compares BOTH the
// (aggregated) CRC and the (aggregated) row count. Comparing the count is
// free — the count is already returned alongside the CRC in the same query —
// and it closes a defense-in-depth gap where the CRC alone is insufficient
// (see chunkMismatch.countDiffers). A count mismatch is treated exactly like
// a checksum mismatch by callers.
func compareChunk(srcCRC, tgtCRC int64, srcCount, tgtCount uint64) chunkMismatch {
	return chunkMismatch{
		checksumDiffers: srcCRC != tgtCRC,
		countDiffers:    srcCount != tgtCount,
	}
}

type Checker interface {
	// Run performs the checksum operation.
	Run(ctx context.Context) error
	// GetProgress returns the structured checksum progress — rows verified so far
	// and the total to verify. Call String() on the result for the display form.
	GetProgress() status.ChecksumProgress
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

// AutoscaleConfig controls the checksum phase's worker-count control loop. It
// mirrors copier.AutoscaleConfig, minus a StartThreads field — the checksum
// starts at CheckerConfig.Concurrency.
//
// Enabled only turns on *scaling*. The throttler hard-stop applies either way:
// a checksum with autoscaling disabled still pauses when the throttler says to,
// which before this existed it did not do at all.
type AutoscaleConfig struct {
	Enabled bool
	// MaxThreads is the ceiling scaling may reach. The transaction pools are
	// provisioned at this size whether or not Enabled is set, so callers must
	// budget connections for it (see SingleChecker.initConnPool for why the
	// pools cannot grow on demand). Values below Concurrency are raised to it.
	MaxThreads int
}

// ThrottleAware is the optional capability a Checker exposes when it can pace
// itself against a throttler installed after construction. SingleChecker and
// DistributedChecker implement it; runners build their checker before the
// throttlers are open, so they type-assert for this and wire it later.
//
// It is an optional interface rather than part of Checker so that test doubles
// and the continuous checker (which manages its own pacing) do not have to
// carry a method they have no use for.
type ThrottleAware interface {
	SetThrottler(t throttler.Throttler)
}

// Paced is the optional capability a Checker exposes when it can report how it
// is currently being paced. Runner status lines use it so a slow checksum can be
// told apart from a throttled or scaled-down one — the same question
// copier-is-throttled answers for the copy phase.
type Paced interface {
	// Threads is the live worker count, which the autoscaler may have moved
	// away from the configured concurrency.
	Threads() int
	// IsThrottled reports whether the throttler is currently telling the
	// checksum to pause.
	IsThrottled() bool
}

// StatusSuffix renders the pacing fields for a runner status line, or "" if the
// checker does not report them. Mirrors applier.StatusSuffix, including the
// leading space so callers can append it unconditionally.
func StatusSuffix(c Checker) string {
	p, ok := c.(Paced)
	if !ok {
		return ""
	}
	return fmt.Sprintf(" checksum-threads=%d checksum-is-throttled=%v", p.Threads(), p.IsThrottled())
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
	// Throttler paces the checksum. Optional: nil installs a Noop, and callers
	// that build the checker before their throttlers are open should use
	// SetThrottler instead (the migration runner does).
	Throttler throttler.Throttler
	// Autoscale configures the worker-count control loop.
	Autoscale AutoscaleConfig
	// MetricsSink is where the control loop reports its gauges. Optional.
	MetricsSink metrics.Sink
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     4,
		TargetChunkTime: DefaultTargetChunkTime,
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
func NewChecker(sourceDBs []*sql.DB, chunker table.Chunker, feeds []change.Source, config *CheckerConfig) (Checker, error) {
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
	// A concurrency of at least 1 is required for the limiter and the
	// transaction pools to be usable; historically a zero here produced a pool
	// of zero transactions and a checksum that could not run.
	concurrency := max(config.Concurrency, 1)
	// The ceiling can never be below the start value: the pools are sized to
	// it, and a pool smaller than the starting worker count would starve.
	maxConcurrency := max(config.Autoscale.MaxThreads, concurrency)
	thr := config.Throttler
	if thr == nil {
		thr = &throttler.Noop{}
	}
	if config.Applier != nil {
		return &DistributedChecker{
			concurrency:     concurrency,
			maxConcurrency:  maxConcurrency,
			autoscale:       config.Autoscale.Enabled,
			throttler:       thr,
			metricsSink:     config.MetricsSink,
			targetChunkTime: config.TargetChunkTime,
			sourceDBs:       sourceDBs,
			feeds:           feeds,
			chunker:         chunker,
			dbConfig:        config.DBConfig,
			logger:          config.Logger,
			fixDifferences:  config.FixDifferences,
			maxRetries:      config.MaxRetries,
			applier:         config.Applier,
			yieldTimeout:    config.YieldTimeout,
		}, nil
	}
	return &SingleChecker{
		concurrency:     concurrency,
		maxConcurrency:  maxConcurrency,
		autoscale:       config.Autoscale.Enabled,
		throttler:       thr,
		metricsSink:     config.MetricsSink,
		targetChunkTime: config.TargetChunkTime,
		db:              sourceDBs[0],
		feed:            feeds[0],
		chunker:         chunker,
		dbConfig:        config.DBConfig,
		logger:          config.Logger,
		fixDifferences:  config.FixDifferences,
		maxRetries:      config.MaxRetries,
		yieldTimeout:    config.YieldTimeout,
	}, nil
}
