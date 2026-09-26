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

	// ErrDifferencesExhausted is returned by Run when every attempt completed
	// but kept finding row differences. The table is diverging in a way the
	// repairs cannot close, so a further attempt reproduces it: a lossy ALTER
	// (adding a UNIQUE index to non-unique data being the common one), or a
	// bug. Callers that decide whether to retry should not.
	ErrDifferencesExhausted = errors.New("checksum found differences on every attempt")

	// ErrAttemptsExhausted is returned by Run when every attempt errored before
	// it could compare the whole table — killed connections, a cancelled
	// context, a failure inside a pass. Nothing has been proven about the data,
	// and the condition may well be gone by the next attempt. It wraps the last
	// attempt's error, which is the one worth triaging.
	ErrAttemptsExhausted = errors.New("checksum errored on every attempt")

	// ErrRepairUnverified is returned by RunContinuous when a pass repaired a
	// mismatch and was cancelled before it could re-verify the rewritten rows.
	// A repair is not verification, so the target is unproven and cutover must
	// not proceed on the strength of that pass. It is distinct from an ordinary
	// cancellation, which a continuous pass filters to nil.
	ErrRepairUnverified = errors.New("checksum cancelled with a repair unverified")

	// DefaultYieldTimeout is the default maximum duration for a single checksum
	// pass before yielding to release long-running REPEATABLE READ transactions.
	DefaultYieldTimeout = 24 * time.Hour

	// fixChunkTimeout bounds the DELETE + Apply pair that recopies a mismatched
	// chunk. The pair runs under a context derived from context.WithoutCancel so
	// a sentinel-drop cancellation can't leave the target in a partial state
	// between the two steps. The bound still catches the case where one of them
	// is hung. This applies to every repair path (initial and continuous
	// checksum), so it has to be generous enough for legitimate large/slow
	// recopies on busy or distant replicas.
	fixChunkTimeout = 10 * time.Minute
)

const (
	// repairBatchRows and repairBatchBytes bound how much of a mismatched chunk
	// chunkRepairer.Recopy holds in memory at once: source rows are read in
	// batches and each batch is handed to the applier, which splits it further
	// into the statements it writes. Both bounds are deliberately of the same
	// order as the applier's own chunklet limits, so a batch is roughly one
	// statement's worth of rows and the read stays a little ahead of the writers
	// without buffering a whole (possibly enormous) chunk. The byte half is
	// measured with applier.EstimateRowSize, the same (approximate, cheap)
	// accounting the applier uses to cut its own statements.
	repairBatchRows  = 1000
	repairBatchBytes = applier.MaxStatementSizeBytes

	// DefaultConcurrency is the worker count every algorithm starts at when the
	// caller does not choose one. Four readers is enough to keep a chunk in
	// flight per available connection on a small instance without being a
	// meaningful share of a large one's capacity; the autoscaler moves it from
	// there when it is enabled.
	DefaultConcurrency = 4

	// defaultMaxRetries is how many whole-run attempts every algorithm makes
	// before giving up. Retrying is for transient infrastructure failures, so
	// the useful range is small: a third attempt that fails the way the first
	// two did is not going to be fixed by a fourth.
	defaultMaxRetries = 3
)

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
	// SetThrottler installs pacing before Run. Every finite checker supports it.
	SetThrottler(throttler.Throttler)
	// ResumeWatermark returns safe verification progress, or an empty string when
	// a resumed run must recheck everything. Read it instead of the walker watermark.
	ResumeWatermark() (string, error)
	// Run performs finite verification. A nil result authorizes completion;
	// deferred ranges and repairs alone are not verification.
	Run(ctx context.Context) error
	// RunContinuous reuses a successfully completed checker for background
	// verification. Calls to Run and RunContinuous must be sequential. A nil
	// return means cancellation was safe, not that the interrupted pass verified
	// every row. Other errors abort cutover. Once started, ResumeWatermark stays
	// empty: restarting requires full initial verification.
	RunContinuous(context.Context) error
	// ContinuousActive distinguishes a running pass from interval pacing.
	// It is safe to query concurrently with RunContinuous.
	ContinuousActive() bool
	// GetProgress returns the structured checksum progress — rows verified so far
	// and the total to verify. Call String() on the result for the display form.
	GetProgress() status.ChecksumProgress
	StartTime() time.Time
	ExecTime() time.Duration
	// DifferencesFound reports observed mismatches, including transient ones.
	// Snapshot checkers count the current pass; optimistic verification counts
	// across passes within Run. This is not resume evidence: use ResumeWatermark.
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

// loadOnlyThrottler narrows a throttler to the signals a checksum should react
// to, and is applied to every throttler a checker is given (at construction and
// via SetThrottler) so the rule holds however the checker was wired. nil yields
// a Noop.
//
// A checksum reacts to *load* and ignores binary budget signals — in practice,
// replica lag. It reads inside a REPEATABLE READ snapshot and writes nothing to
// the binlog, so it cannot be the cause of replica lag and pausing it cannot
// reduce that lag; what the pause does do is extend the pass, holding the
// snapshot open and pinning undo that the purge thread cannot advance past. The
// replica-lag throttler also fails closed on stale polling, so an unreachable
// replica would stall dispatch until the yield timeout with the snapshot still
// held. Load is different in kind: a checksum genuinely adds read load to the
// primary, so backing off on load both works and is warranted.
//
// The one part of a checksum that does replicate is a chunk repair, and it is
// deliberately left unpaced: repairs are rare and small, and blocking one incurs
// exactly the snapshot-hold cost this narrowing exists to avoid.
func loadOnlyThrottler(t throttler.Throttler) throttler.Throttler {
	if t == nil {
		return &throttler.Noop{}
	}
	return throttler.GradualOnly(t)
}

// Paced is the optional capability a Checker exposes when it can report how it
// is currently being paced. The runner status block uses it so a slow checksum can be
// told apart from a throttled or scaled-down one — the same question the copier
// row's throttled= answers for the copy phase.
type Paced interface {
	// Threads is the live worker count, which the autoscaler may have moved
	// away from the configured concurrency.
	Threads() int
	// IsThrottled reports whether the throttler is currently telling the
	// checksum to pause.
	IsThrottled() bool
	// ChunkSize is the row count of the most recently checksummed chunk. The
	// checksum sizes its chunks dynamically just as the copy does, so the same
	// field is worth watching in both phases.
	ChunkSize() uint64
}

// StatusSuffix renders the pacing fields for the checksum row of a runner
// status block, or "" if the checker does not report them. It keeps the leading
// two spaces used between fields within a row, so callers can append it
// unconditionally.
func StatusSuffix(c Checker) string {
	p, ok := c.(Paced)
	if !ok {
		return ""
	}
	return fmt.Sprintf("  chunk-size=%d  threads=%d  throttled=%v", p.ChunkSize(), p.Threads(), p.IsThrottled())
}

// Algorithm selects how a checker compares source and target. It is the only
// thing that selects one: before this existed, the distributed checker was
// chosen by CheckerConfig.Applier being non-nil, which meant the write path a
// repair goes through doubled as the algorithm switch. That is why there used
// to be a second applier field — a single-server checker needs a write path
// too, and setting the first one would have silently turned it into a
// distributed checker. Naming the algorithm separately lets the two collapse
// into one Applier.
type Algorithm int

const (
	// Single compares two tables on one server under a REPEATABLE READ
	// snapshot taken behind a brief table lock. This is the default, and what
	// a migration uses.
	Single Algorithm = iota

	// Sharded compares N sources against M targets, aggregating each chunk
	// across every source. Used by a move; see DistributedChecker. Requires
	// an Applier, which is also what routes a repaired row to the shard that
	// owns it.
	Sharded

	// Lockless compares two tables with optimistic READ COMMITTED reads and a
	// delayed-retry queue, taking no locks and holding no snapshot. Everything
	// in the common config section applies to it; the lockless section applies
	// only to it, and YieldTimeout does not apply at all (there is no snapshot
	// to yield).
	Lockless
)

func (a Algorithm) String() string {
	switch a {
	case Single:
		return "single"
	case Sharded:
		return "sharded"
	case Lockless:
		return "lockless"
	default:
		return fmt.Sprintf("Algorithm(%d)", int(a))
	}
}

type CheckerConfig struct {
	// Algorithm selects the checker. The zero value is Single.
	Algorithm   Algorithm
	Concurrency int
	// TargetChunkTime is reporting-only: it is the target the chunk-size
	// distribution summary is compared against at the end of each pass, so it
	// should match the TargetChunkTime the caller built the chunker with
	// (table.ChunkerDefaultTarget unless the caller overrode it). It does not
	// itself size anything — chunk sizing lives entirely in the chunker.
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          *slog.Logger
	// FixDifferences is the repair policy, and it means the same thing to every
	// algorithm: when set, a mismatched chunk is rewritten from the source and
	// re-verified; when unset, a mismatch is reported as an error. The factory
	// turns it into the Recopier the checker actually repairs through — see
	// newRecopier, which also says which applier field that write path needs.
	FixDifferences bool
	// Watermark is verification evidence from a previous run: every row below
	// it was read on both sides and observed equal. Supplying it makes the
	// factory open the chunker there, so verification resumes rather than
	// restarting; leave the chunker unopened when supplying it. Every algorithm
	// honours it — the claim it encodes does not depend on which checker
	// observed it. Take it from Checker.ResumeWatermark, never from the
	// chunker's traversal watermark.
	Watermark string
	// MaxRetries bounds whole-run attempts for every algorithm: a transient
	// infrastructure failure costs an attempt rather than the migration.
	MaxRetries int
	// Applier is the write path a mismatched chunk is rewritten through. It is
	// the caller's to supply because every runner already has one, and building
	// a second here would hide which one a repair actually goes through.
	//
	// Required when FixDifferences is set — a checker that cannot repair should
	// fail to build, not on the first mismatch hours in — and required by
	// Sharded either way, which needs it to reach its targets at all. The two
	// algorithms use it differently: Sharded owns its lifecycle for the whole
	// run (and reads GetTargets from it), while the others start and stop it
	// around each repair, since repairs are rare and serialized.
	Applier      applier.Applier
	YieldTimeout time.Duration // maximum duration for a single checksum pass before yielding to release long-running transactions
	// Throttler paces the checksum. Optional: nil installs a Noop, and callers
	// that build the checker before their throttlers are open should use
	// SetThrottler instead (the migration runner does).
	//
	// Whatever is passed is narrowed by loadOnlyThrottler — a checksum reacts to
	// load signals and ignores binary ones such as replica lag.
	Throttler throttler.Throttler
	// Autoscale configures the worker-count control loop.
	Autoscale AutoscaleConfig
	// MetricsSink is where the control loop reports its gauges. Optional.
	MetricsSink metrics.Sink

	// ---------------------------------------------------------------------
	// Lockless-only. Ignored unless Algorithm is Lockless.
	// ---------------------------------------------------------------------

	// RetryDelay is the minimum wait between attempts for any given chunk —
	// measured from the *last* attempt of that chunk, not from the original
	// failure. Default 1m, because changes are queued in the replication
	// applier for 30s by default. It is also what paces the re-walk between
	// finite passes, which is the same "give the target a moment to catch up"
	// wait.
	RetryDelay time.Duration

	// MaxQueueSize is the cap on entries in the delayed-retry queue. Reaching
	// it stalls the walker until retries drain rather than failing the run.
	// Default 1024.
	MaxQueueSize int

	// MaxHotAttempts is the number of observations allowed for a chunk whose
	// source signature keeps changing. Once reached, the chunk is deferred to
	// the next pass rather than holding the current pass open forever. Default
	// 10; a positive value below 2 is clamped to 2 because detecting a source
	// change requires an initial read and a retry. Deferral never counts as
	// verification and makes the pass not clean.
	MaxHotAttempts int

	// MinPassInterval is the minimum wall-clock time between the start of one
	// pass and the start of the next, measured from the previous pass's start
	// (a pass that already ran longer incurs no extra wait). Zero means passes
	// run back-to-back, which is convenient for tests but heavy in production:
	// RunContinuous substitutes LocklessMinPassInterval (1h) so a small table
	// whose pass finishes in seconds does not re-scan continuously, and the
	// finite gate substitutes RetryDelay. The wait honours cancellation.
	MinPassInterval time.Duration

	// MaxPasses bounds Run and RunUntilClean: once this many passes have
	// completed without one of them being clean, verification gives up with
	// ErrVerificationUnresolved rather than walking the table again. Zero means
	// unbounded, which is what RunContinuous always is; NewChecker defaults it
	// to DefaultLocklessMaxPasses so the finite gate terminates.
	MaxPasses int

	// TargetDB is the server holding the copy being verified, when that is not
	// the server being read from. Nil — the usual case — means the target lives
	// on sourceDBs[0], which is true of a migration (the shadow table sits
	// beside the original). A sync verifies two servers, so it supplies one.
	//
	// Setting it also changes the shape of a repair: a cross-server rewrite has
	// no ColumnMapping to apply, because both sides hold the same logical
	// table. See newRecopier.
	TargetDB *sql.DB

	// ExternalFlushLoop says the caller runs the feed's periodic flush itself,
	// for longer than any one run, so a run must not start and stop it. The
	// default (false) is that the checker owns flushing for the duration of a
	// run, which is what a migration and a move want: nothing else is driving
	// the feed while verification is the only thing happening.
	//
	// A sync is the opposite — its flush loop runs for the whole process at its
	// own configured interval, and a verification pass stopping it on the way
	// out would be stopping someone else's goroutine.
	ExternalFlushLoop bool
}

// defaultedConcurrency resolves a configured worker count. A concurrency of at
// least 1 is required for the limiter and the transaction pools to be usable —
// historically a zero here produced a pool of zero transactions and a checksum
// that could not run — and an unset one means the caller did not choose, so it
// gets the default rather than the minimum.
func defaultedConcurrency(n int) int {
	if n <= 0 {
		return DefaultConcurrency
	}
	return n
}

// applySharedDefaults fills in the settings that mean the same thing to every
// algorithm. The lockless-only ones are newLocklessChecker's; keeping the two
// sets apart is what stops a default that only one checker reads from looking
// like part of the common contract.
func applySharedDefaults(cfg *CheckerConfig) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.YieldTimeout == 0 {
		cfg.YieldTimeout = DefaultYieldTimeout
	}
	cfg.Concurrency = defaultedConcurrency(cfg.Concurrency)
	// The ceiling can never be below the start value: the pools are sized to
	// it, and a pool smaller than the starting worker count would starve.
	cfg.Autoscale.MaxThreads = max(cfg.Autoscale.MaxThreads, cfg.Concurrency)
	cfg.Throttler = loadOnlyThrottler(cfg.Throttler)
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     DefaultConcurrency,
		TargetChunkTime: table.ChunkerDefaultTarget,
		DBConfig:        dbconn.NewDBConfig(),
		Logger:          slog.Default(),
		FixDifferences:  false,
		MaxRetries:      defaultMaxRetries,
		YieldTimeout:    DefaultYieldTimeout,
	}
}

// newRecopier builds the repair path a mismatched chunk is rewritten through,
// or returns nil when the caller did not ask for repairs. Every algorithm goes
// through this one function, so the answer to "what happens on a divergence?"
// does not depend on which checker the config selected: a nil recopier means
// the mismatch is reported as an error, and a non-nil one means the chunk is
// rewritten and re-verified.
//
// Only the shape of the rewrite depends on the topology. Sharded must delete
// the range on every target and merge the rows from every source. A
// cross-server target reads from one server and writes to another, and has no
// ColumnMapping to apply because both sides hold the same logical table. The
// remaining case — one server holding both copies — is the migration's, where
// the mapping is exactly what makes a repair correct across a rename or a drop.
//
// config.DBConfig is the write side's in every case: it configures the DELETE
// that clears the range on the target before the rows are rewritten.
func newRecopier(sourceDBs []*sql.DB, targetDB *sql.DB, config *CheckerConfig) (Recopier, error) {
	if !config.FixDifferences {
		return nil, nil
	}
	if config.Applier == nil {
		return nil, errors.New("applier must be non-nil to repair differences")
	}
	switch {
	case config.Algorithm == Sharded:
		return newDistributedRepairer(sourceDBs, config.Applier, config.DBConfig, config.Logger), nil
	case targetDB != sourceDBs[0]:
		return newMySQLRecopier(sourceDBs[0], targetDB, config.Applier, config.DBConfig, config.Logger)
	default:
		return newChunkRepairer(sourceDBs[0], config.Applier, config.DBConfig, config.Logger), nil
	}
}

// NewChecker creates a new checksum object. CheckerConfig.Algorithm picks which
// one, and nothing else does; the zero value is Single.
//
// sourceDBs contains the source database connections (one for single-source
// migrations, multiple for N:M moves). Sharded aggregates checksums across all
// of them; the other two use sourceDBs[0]. The copy being verified lives on
// sourceDBs[0] too unless CheckerConfig.TargetDB names another server, which is
// how a sync verifies across two. Open the chunker before construction unless
// supplying Watermark, in which case the factory opens it according to policy.
func NewChecker(sourceDBs []*sql.DB, chunker table.Chunker, feeds []change.Source, config *CheckerConfig) (Checker, error) {
	if config == nil {
		return nil, errors.New("config must be non-nil")
	}
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
	switch config.Algorithm {
	case Lockless:
		if len(sourceDBs) != 1 || len(feeds) != 1 {
			return nil, errors.New("lockless verification requires one source and one feed")
		}
		if sourceDBs[0] == nil || feeds[0] == nil {
			return nil, errors.New("lockless verification requires non-nil source and feed")
		}
	case Sharded:
		// The applier is how a sharded checker reaches its targets at all —
		// it reads them from GetTargets and owns its lifecycle for the run —
		// so it is required whether or not repairs are.
		if config.Applier == nil {
			return nil, errors.New("sharded verification requires an applier")
		}
	case Single:
	default:
		return nil, fmt.Errorf("unknown checksum algorithm %d", config.Algorithm)
	}
	// Cross-server verification is lockless-only. The snapshot checkers reach
	// the target through a connection to the server they read from — a table
	// lock and a REPEATABLE READ snapshot cannot span two servers — so naming a
	// second one for them would be silently ignored rather than honoured.
	targetDB := sourceDBs[0]
	if config.TargetDB != nil {
		if config.Algorithm != Lockless {
			return nil, fmt.Errorf("%s verification cannot span two servers", config.Algorithm)
		}
		targetDB = config.TargetDB
	}

	// Everything from here is shared: the same defaults, the same repair
	// policy, the same resume and pacing wiring, whichever checker the config
	// selects. Work on a copy so none of it is visible to the caller's config.
	cfg := *config
	applySharedDefaults(&cfg)
	recopier, err := newRecopier(sourceDBs, targetDB, &cfg)
	if err != nil {
		return nil, err
	}
	// A watermark is verification evidence, whichever algorithm produced it:
	// every row below it was observed equal and the change feed has kept it
	// that way since. Optimistic verification only publishes one for a prefix
	// it has actually resolved (see LocklessChecker.ResumeWatermark), so
	// resuming at it is the same trade the snapshot checkers make.
	if cfg.Watermark != "" {
		if err := chunker.OpenAtWatermark(cfg.Watermark); err != nil {
			return nil, err
		}
	}

	switch cfg.Algorithm {
	case Single:
		return &SingleChecker{
			concurrency:     cfg.Concurrency,
			maxConcurrency:  cfg.Autoscale.MaxThreads,
			autoscale:       cfg.Autoscale.Enabled,
			throttler:       cfg.Throttler,
			metricsSink:     cfg.MetricsSink,
			targetChunkTime: cfg.TargetChunkTime,
			db:              sourceDBs[0],
			feed:            feeds[0],
			chunker:         chunker,
			dbConfig:        cfg.DBConfig,
			logger:          cfg.Logger,
			recopier:        recopier,
			maxRetries:      cfg.MaxRetries,
			yieldTimeout:    cfg.YieldTimeout,
		}, nil
	case Lockless:
		checker := newLocklessChecker(sourceDBs[0], targetDB, chunker, feeds[0], recopier, &cfg)
		checker.ownsFeedFlush = !cfg.ExternalFlushLoop
		return checker, nil
	case Sharded:
		return &DistributedChecker{
			concurrency:     cfg.Concurrency,
			maxConcurrency:  cfg.Autoscale.MaxThreads,
			autoscale:       cfg.Autoscale.Enabled,
			throttler:       cfg.Throttler,
			metricsSink:     cfg.MetricsSink,
			targetChunkTime: cfg.TargetChunkTime,
			sourceDBs:       sourceDBs,
			feeds:           feeds,
			chunker:         chunker,
			dbConfig:        cfg.DBConfig,
			logger:          cfg.Logger,
			recopier:        recopier,
			maxRetries:      cfg.MaxRetries,
			applier:         cfg.Applier,
			yieldTimeout:    cfg.YieldTimeout,
		}, nil
	default:
		// Unreachable: the validation above rejects an unknown algorithm before
		// any of the construction work happens.
		return nil, fmt.Errorf("unknown checksum algorithm %d", cfg.Algorithm)
	}
}

// Flush during pacing, but stop before Run acquires snapshot setup locks.
// Each finite Run owns flushing after those locks have been released.
func runContinuousSnapshot(ctx context.Context, checker Checker, feeds []change.Source, resume *snapshotResume, reset func() error) error {
	var duration time.Duration
	for {
		for _, feed := range feeds {
			feed.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
		}
		ready := waitForChecksum(ctx, LocklessMinPassInterval-duration)
		for _, feed := range feeds {
			feed.StopPeriodicFlush()
		}
		if !ready {
			return nil
		}
		if err := reset(); err != nil {
			return fmt.Errorf("reset continuous checksum: %w", err)
		}
		before := resume.observed.Load()
		started := time.Now()
		resume.active.Store(true)
		err := checker.Run(ctx)
		resume.active.Store(false)
		if err != nil {
			// A retry can reset DifferencesFound even after a repair was interrupted.
			// Use the monotonic observation count for this entire Run instead.
			if ctx.Err() != nil && checksumCanceled(err) {
				if resume.observed.Load() == before {
					return nil
				}
				// A repair is not verification: the rewritten rows were never
				// observed equal. Cancelling before the pass could re-verify
				// them leaves the target unproven, so the cancellation is
				// refused rather than filtered. Say which of the two it is —
				// a bare "context canceled" reads as the shutdown working.
				return fmt.Errorf("%w: cancelled after repairing a mismatch and before re-verifying it", ErrRepairUnverified)
			}
			return err
		}
		duration = time.Since(started)
	}
}

func waitForChecksum(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(max(0, delay))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// Accept wrapped cancellation, but not a joined cancellation plus a real error.
func checksumCanceled(err error) bool {
	var joined interface{ Unwrap() []error }
	return !errors.As(err, &joined) && errors.Is(err, context.Canceled)
}
