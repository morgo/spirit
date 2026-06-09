package datasync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
)

// syncCheckpointTableName is the table, created on the target, that
// records the source change-feed position so a restart can resume the
// continuous stream instead of re-copying. It always lives on the target
// because the source may be read-only (e.g. a Vitess/PlanetScale replica).
const syncCheckpointTableName = "_spirit_sync_checkpoint"

// errNoSuchTable is MySQL's ER_NO_SUCH_TABLE (1146).
const errNoSuchTable = 1146

// shutdownFlushTimeout bounds the best-effort final flush on a clean shutdown,
// and shutdownCheckpointTimeout bounds the final checkpoint write (kept
// independent so a slow flush can't starve the checkpoint). Both are short so
// Ctrl-C / SIGTERM exits promptly even against a busy source whose change feed
// never fully catches up; unflushed changes are re-applied on the next run from
// the checkpoint.
const (
	shutdownFlushTimeout      = 10 * time.Second
	shutdownCheckpointTimeout = 10 * time.Second
)

// sourceInfo holds the single source's connection state.
type sourceInfo struct {
	db     *sql.DB
	config *mysql.Config
	dsn    string
}

// Runner executes a Sync: an initial copy followed by continuous
// replication that runs until the context is cancelled.
type Runner struct {
	sync *Sync

	source sourceInfo
	target applier.Target
	// ownsTarget is true when the runner opened the target DB itself (from
	// TargetDSN) and is therefore responsible for closing it. When the
	// caller injected Sync.Target, it owns the DB's lifecycle.
	ownsTarget bool

	sourceTables []*table.TableInfo

	applier     applier.Applier
	replClient  change.Source
	copyChunker table.Chunker
	copier      copier.Copier

	// resuming is set when a checkpoint was found on the target: the
	// initial copy is skipped and the change feed is opened from the
	// checkpointed position.
	resuming bool

	status    status.State
	startTime time.Time

	logger     *slog.Logger
	cancelFunc context.CancelFunc
	// sourceDBConfig connects to the read-only source: ForceKill and
	// RejectReadOnly are disabled (see Run). targetDBConfig connects to the
	// writable target and keeps the standard safe defaults — most importantly
	// RejectReadOnly=true for Aurora-failover safety.
	sourceDBConfig *dbconn.DBConfig
	targetDBConfig *dbconn.DBConfig

	// watchDone is closed when the status-logging goroutine exits.
	watchDone chan struct{}

	// checkpointDone is closed when the periodic checkpoint goroutine exits.
	checkpointDone chan struct{}

	// fatalErr records a fatal source-side event (e.g. DDL on a synced
	// table) that should surface as the Run error rather than a clean
	// cancellation. Guarded by fatalMu.
	fatalMu  sync.Mutex
	fatalErr error

	// progMu guards the progress-related fields (copier, copyChunker,
	// replClient, startTime, cancelFunc) that Run assigns during setup and
	// that the status.Task accessors (Progress/Status/DumpCheckpoint/Cancel)
	// read concurrently from a separate monitoring goroutine.
	progMu sync.RWMutex

	// continuousChecker is constructed in runContinuous (after the initial
	// copy and post-copy flush) and runs in a sibling goroutine until ctx
	// cancels. Programmatic callers can read FirstCleanPass / ChecksumStats
	// through accessors on Runner. nil before runContinuous.
	//
	// continuousReadyCh closes once the checker has been constructed (the
	// initial copy + post-copy flush have completed and runContinuousChecksum
	// has started). Callers can wait on ChecksumReady() to gate on
	// checker availability.
	//
	// firstCleanPassCh is the Runner-owned signal forwarded from the
	// checker's own FirstCleanPass channel. Owning a separate channel
	// keeps the FirstCleanPass accessor non-blocking — callers can grab it
	// before Run starts and select on it without deadlocking. It stays
	// open if the run exits without observing a clean pass.
	continuousChecker         *checksum.ContinuousChecker
	continuousChunker         table.Chunker
	continuousReadyCh         chan struct{}
	firstCleanPassCh          chan struct{}
	continuousCheckerInitOnce sync.Once
	firstCleanPassInitOnce    sync.Once
}

var _ status.Task = (*Runner)(nil)

// NewRunner validates the Sync config and returns a Runner. The CLI
// supplies defaults via kong; programmatic callers get the same defaults
// applied here as a safety net.
func NewRunner(s *Sync) (*Runner, error) {
	if s.Source != nil && s.Applier == nil {
		return nil, errors.New("Sync.Source requires Sync.Applier to also be set; the injected change.Source needs the same applier the copier uses")
	}
	if s.Threads <= 0 {
		s.Threads = 4
	}
	if s.WriteThreads <= 0 {
		s.WriteThreads = 4
	}
	if s.TargetChunkTime <= 0 {
		s.TargetChunkTime = 5 * time.Second
	}
	if s.FlushInterval <= 0 {
		s.FlushInterval = change.DefaultFlushInterval
	}
	r := &Runner{
		sync:              s,
		logger:            slog.Default(),
		continuousReadyCh: make(chan struct{}),
		firstCleanPassCh:  make(chan struct{}),
	}
	return r, nil
}

// SetLogger overrides the logger (used by programmatic callers to capture
// progress output).
func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// Run performs the initial copy and then streams changes continuously
// until ctx is cancelled. A clean cancellation returns nil; a fatal
// source event (e.g. DDL) returns an error.
func (r *Runner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.progMu.Lock()
	r.cancelFunc = cancel
	r.startTime = time.Now()
	r.progMu.Unlock()
	r.logger.Info("Starting sync", "source_dsn", redactDSN(r.sync.SourceDSN))

	r.sourceDBConfig = dbconn.NewDBConfig()
	// Sync only ever reads the source data (copy SELECTs + the change feed).
	// It never writes the source's data, acquires no source locks, and
	// performs no cutover. With an injected change.Source it needs only SELECT
	// on the source schema; the built-in MySQL binlog client additionally
	// needs REPLICATION SLAVE/CLIENT (validated on Start) and RELOAD, because
	// it issues FLUSH BINARY LOGS to establish its start position. Disable the
	// two dbConfig behaviours that would otherwise demand more:
	//   - ForceKill needs CONNECTION_ADMIN/PROCESS + performance_schema, and
	//     is only used to break metadata locks during cutover — which sync
	//     never does.
	//   - RejectReadOnly is an Aurora-failover guard that turns a read-only
	//     server error into driver.ErrBadConn; sync's source is read-only by
	//     design (e.g. a Vitess/PlanetScale replica), so it must not fire.
	r.sourceDBConfig.ForceKill = false
	r.sourceDBConfig.RejectReadOnly = false
	r.sourceDBConfig.MaxOpenConnections = 100

	// The target is written to (table creation, the copy/apply, the
	// checkpoint, and CREATE DATABASE on the admin connection), so it keeps the
	// standard safe defaults — crucially RejectReadOnly=true, so that if the
	// target Aurora fails over and we land on a demoted, now-read-only primary,
	// writes turn into driver.ErrBadConn and the pool reconnects instead of
	// silently erroring. Only the relaxations the target genuinely shares with
	// the source are applied (no cutover here either, so ForceKill is left at
	// its default but never fires).
	r.targetDBConfig = dbconn.NewDBConfig()
	r.targetDBConfig.MaxOpenConnections = 100

	// Open the source SQL connection. Even when the change feed is an
	// injected non-MySQL source, spirit still needs SQL access to the
	// source for SHOW TABLES / SHOW CREATE TABLE and the initial-copy
	// SELECTs.
	db, err := dbconn.New(r.sync.SourceDSN, r.sourceDBConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	cfg, err := mysql.ParseDSN(r.sync.SourceDSN)
	if err != nil {
		return fmt.Errorf("failed to parse source DSN: %w", err)
	}
	r.source = sourceInfo{db: db, config: cfg, dsn: r.sync.SourceDSN}

	// Resolve the single target. The target database is auto-created (if
	// missing) on both paths — the import's per-shard target databases are
	// not guaranteed to pre-exist on the destination cluster.
	if r.sync.Target != nil {
		r.target = *r.sync.Target
		// The injected target DB connects lazily, so ensure its database
		// exists before the first query (createTargetTables / checkpoint).
		if err := r.ensureTargetDatabase(ctx, r.target.Config); err != nil {
			return err
		}
	} else {
		tcfg, terr := mysql.ParseDSN(r.sync.TargetDSN)
		if terr != nil {
			return fmt.Errorf("failed to parse target DSN: %w", terr)
		}
		// Create the database before opening — a DSN-scoped open pings it.
		if err := r.ensureTargetDatabase(ctx, tcfg); err != nil {
			return err
		}
		tdb, terr := dbconn.New(r.sync.TargetDSN, r.targetDBConfig)
		if terr != nil {
			return fmt.Errorf("failed to connect to target: %w", terr)
		}
		r.target = applier.Target{KeyRange: "0", DB: tdb, Config: tcfg}
		r.ownsTarget = true
	}

	if err := r.setup(ctx); err != nil {
		return err
	}
	if len(r.sourceTables) == 0 {
		r.logger.Info("No tables to sync; nothing to do")
		return nil
	}

	// Background routines: periodic flush keeps the target caught up; the
	// status goroutine logs progress.
	r.startBackgroundRoutines(ctx)

	// Copy phase — runs for both a fresh sync and a resume. On a fresh sync
	// the chunker starts at the beginning; on a resume it was opened at the
	// checkpointed watermark (startResume), so the copier continues from there
	// — and finishes immediately if the copy had already completed. The applier
	// copies with INSERT IGNORE, so re-copying the chunks straddling the
	// watermark is idempotent.
	if !r.sync.CopyOnly && !r.resuming {
		// Watermark optimization ON during a fresh continuous copy: change
		// events for keys the copier has not reached yet (above the watermark)
		// are discarded, because the copier will copy those rows directly.
		//
		// CORRECTNESS CAVEAT — read replicas:
		// keyAboveWatermark is only safe when the copier reads from a source
		// that reflects every change the change feed has already delivered.
		// That holds on a PRIMARY, but NOT on a lagging REPLICA: an update to
		// an above-watermark key can be observed on the change stream — and
		// discarded — while the copier's later read of that key on the replica
		// still returns the pre-update (stale) value, silently losing it. The
		// intended safety net is the post-copy checksum, which isn't usable on
		// the read-only import source yet (it needs privileges that credential
		// lacks). So a replica source (e.g. the strata import) gets only
		// best-effort consistency. On resume we leave the optimization OFF
		// (startResume) so every change applies.
		if err := r.replClient.SetWatermarkOptimization(ctx, true); err != nil {
			return err
		}
	}
	r.status.Set(status.CopyRows)
	r.logger.Info("Starting copy", "resuming", r.resuming)
	if err := r.copier.Run(ctx); err != nil {
		return fmt.Errorf("copy failed: %w", err)
	}
	if !r.sync.CopyOnly {
		if !r.resuming {
			if err := r.replClient.SetWatermarkOptimization(ctx, false); err != nil {
				return err
			}
		}
		// Drain the copy-phase backlog so every change observed so far is
		// applied before steady-state streaming.
		if err := r.replClient.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush after copy: %w", err)
		}
	}

	// Copy-only past this point: no change capture is configured, but the
	// post-copy continuous checksum is independent of replication and still
	// has a job — verify the copy and, if a Recopier is configured, lazily
	// re-copy diverged rows. Write an intermediate checkpoint marking the
	// copy as complete (so a crash during the checksum resumes to a copy
	// no-op instead of re-copying), then block on the checker until ctx is
	// cancelled.
	if r.sync.CopyOnly {
		if err := r.dumpCheckpoint(ctx); err != nil {
			r.logger.Warn("post-copy checkpoint write failed", "error", err)
		}
		r.logger.Info("Copy complete; entering continuous checksum (CopyOnly mode)")
		return r.runCopyOnlyChecksum(ctx)
	}

	r.logger.Info("Copy complete; entering continuous sync")
	return r.runContinuous(ctx)
}

// runCopyOnlyChecksum runs the post-copy continuous checksum without a
// change feed. With CopyOnly there's no replication to drive, but the
// checker still verifies source vs. target convergence (and, with a
// Recopier configured, lazily re-copies diverged rows). Blocks until ctx
// is cancelled or the checker hits a permanent failure, then writes a
// final checkpoint.
//
// This is structurally a stripped-down runContinuous: same checker
// lifecycle and shutdown contract, no replClient calls.
func (r *Runner) runCopyOnlyChecksum(ctx context.Context) error {
	r.status.Set(status.ApplyChangeset)

	checksumCtx, cancelChecksum := context.WithCancel(ctx)
	defer cancelChecksum()
	checksumDone := make(chan struct{})
	var checksumErr error
	go func() {
		defer close(checksumDone)
		checksumErr = r.runContinuousChecksum(checksumCtx)
	}()

	r.logger.Info("Continuous checksum running; will run until cancelled")

	select {
	case <-ctx.Done():
		// Normal cancellation.
	case <-checksumDone:
		// Checker exited on its own — only happens on a real failure
		// (clean runs return only on ctx-cancel). Trigger the parent ctx
		// cancellation so the shutdown path proceeds.
		if checksumErr != nil && ctx.Err() == nil {
			r.logger.Error("continuous checksum failed; stopping sync", "error", checksumErr)
			r.progMu.RLock()
			cancelParent := r.cancelFunc
			r.progMu.RUnlock()
			if cancelParent != nil {
				cancelParent()
			}
		}
	}

	// Ensure the checksum goroutine is fully shut down before we write the
	// final checkpoint so its in-flight queries don't race the checkpoint.
	cancelChecksum()
	<-checksumDone

	r.logger.Info("Copy-only sync stopping; writing final checkpoint")
	cpCtx, cancelCp := context.WithTimeout(context.WithoutCancel(ctx), shutdownCheckpointTimeout)
	defer cancelCp()
	if err := r.dumpCheckpoint(cpCtx); err != nil {
		r.logger.Warn("Final checkpoint write failed", "error", err)
	}
	r.logger.Info("Copy-only sync stopped", "total_time", time.Since(r.startTime).Round(time.Second))

	// A real checksum failure outranks a clean nil — surface it.
	if checksumErr != nil && !errors.Is(checksumErr, context.Canceled) {
		return fmt.Errorf("continuous checksum failed: %w", checksumErr)
	}
	return nil
}

// runContinuous creates/maintains the target checkpoint and blocks until
// the context is cancelled, while the periodic flush (started in
// startBackgroundRoutines) keeps applying buffered changes. In parallel,
// the continuous (eventually-consistent) checksum walks the data and
// surfaces a FirstCleanPass signal for callers that gate on
// "data is known consistent". On a clean cancellation it drains the final
// backlog and returns nil; on a fatal source event or a checksum failure
// it returns that error.
func (r *Runner) runContinuous(ctx context.Context) error {
	// status moves off CopyRows so the status goroutine logs the continuous
	// phase. The checkpoint table and the periodic checkpoint loop were already
	// set up before the copy (createCheckpointTable in startFresh/startResume,
	// the loop in startBackgroundRoutines), so a restart at any point — copy or
	// continuous — resumes from the last checkpoint.
	r.status.Set(status.ApplyChangeset)

	r.logger.Info("Continuous sync running; will run until cancelled")

	// Spawn the continuous checksum. It uses a separate chunker so
	// checkpoint state is unaffected. The change feed keeps applying via
	// the periodic-flush loop already running from startBackgroundRoutines;
	// the checker is purely an observer of the resulting source/target
	// convergence.
	checksumCtx, cancelChecksum := context.WithCancel(ctx)
	defer cancelChecksum()
	checksumDone := make(chan struct{})
	var checksumErr error
	go func() {
		defer close(checksumDone)
		checksumErr = r.runContinuousChecksum(checksumCtx)
	}()

	// Wait for either ctx cancellation (the normal shutdown path) or the
	// checksum exiting on its own (which means it failed — a clean run
	// never returns until ctx is cancelled).
	select {
	case <-ctx.Done():
		// Normal cancellation. Fall through; we still want the drain logic.
	case <-checksumDone:
		// Checksum exited before our parent ctx cancelled. If its return
		// value is non-nil, that's a real verifier failure that should
		// fail the sync. The defer above cancels our own derived ctx; we
		// also need to cancel the parent ctx so the drain doesn't sit
		// waiting on a healthy change feed.
		if checksumErr != nil && ctx.Err() == nil {
			r.logger.Error("continuous checksum failed; stopping sync", "error", checksumErr)
			// Trigger the drain + shutdown path with a context cancellation.
			r.progMu.RLock()
			cancelParent := r.cancelFunc
			r.progMu.RUnlock()
			if cancelParent != nil {
				cancelParent()
			}
		}
	}

	// Make sure the checksum goroutine is fully shut down before we drain
	// & checkpoint, so its in-flight queries don't fight the final flush.
	cancelChecksum()
	<-checksumDone

	if ferr := r.fatal(); ferr != nil {
		r.logger.Error("Sync stopping due to fatal source event", "error", ferr)
		return ferr
	}

	r.logger.Info("Sync cancelled; draining final changes")
	r.replClient.StopPeriodicFlush()
	// Best-effort final flush, detached from the cancelled ctx with a short
	// bound so shutdown is prompt. Flush retries until the change feed catches
	// up to the source's *latest* position; against a busy source that never
	// converges, so without a tight cap shutdown would spin for the whole
	// budget (and a Ctrl-C / SIGTERM would feel hung). Whatever isn't flushed
	// here is re-applied on the next run from the checkpoint below.
	flushCtx, cancelFlush := context.WithTimeout(context.WithoutCancel(ctx), shutdownFlushTimeout)
	defer cancelFlush()
	if err := r.replClient.Flush(flushCtx); err != nil {
		r.logger.Warn("Final flush failed; some recent changes may not have been applied", "error", err)
	}
	// Write the final checkpoint on its own independent budget. It records the
	// copier watermark + change-feed position that let a restart resume instead
	// of re-copying, so a slow or timed-out final flush above must not starve
	// it of a shared deadline.
	cpCtx, cancelCp := context.WithTimeout(context.WithoutCancel(ctx), shutdownCheckpointTimeout)
	defer cancelCp()
	if err := r.dumpCheckpoint(cpCtx); err != nil {
		r.logger.Warn("Final checkpoint write failed", "error", err)
	}
	r.logger.Info("Sync stopped", "total_time", time.Since(r.startTime).Round(time.Second))
	// A real checksum failure outranks a clean nil — surface it so the
	// caller (and exit code) reflect the underlying problem rather than
	// just "ctx cancelled."
	if checksumErr != nil && !errors.Is(checksumErr, context.Canceled) {
		return fmt.Errorf("continuous checksum failed: %w", checksumErr)
	}
	return nil
}

// runContinuousChecksum builds a separate continuous-checksum chunker over
// the source tables and drives a ContinuousChecker until ctx is cancelled
// or a permanent failure surfaces. The checker uses READ COMMITTED reads
// (no table lock, no TrxPool), so it can run against a live system; see
// pkg/checksum/continuous.go for the convergence model.
func (r *Runner) runContinuousChecksum(ctx context.Context) error {
	chunker, err := r.buildContinuousChunker()
	if err != nil {
		return fmt.Errorf("build continuous-checksum chunker: %w", err)
	}
	if err := chunker.Open(); err != nil {
		return fmt.Errorf("open continuous-checksum chunker: %w", err)
	}
	defer utils.CloseAndLog(chunker)

	// Restart the applier before the recopier touches it. The copier
	// stops the applier when the initial copy finishes (see buffered.go:
	// Stop closes chunkletBuffer), which leaves any later Apply call
	// panicking with "send on closed channel". Start is idempotent and
	// reinitializes the channels on a previously-stopped applier, so
	// calling it here is the cheapest way to keep the recopier path
	// working in both fresh and CopyOnly continuous-sync modes.
	//
	// We pass context.WithoutCancel(ctx) to Start so the applier's
	// worker context is *not* tied to the parent: the recopier uses
	// context.WithoutCancel(ctx) between its DELETE and Apply to keep
	// that pair atomic, and if the applier workers exited on parent
	// cancel they would abandon the recopier's in-flight chunklet —
	// leaving the target with rows deleted but not yet rewritten. The
	// deferred Stop is the only thing that tears them down, and it
	// runs only after checker.Run has waited for all its workers
	// (recopier included) to finish, so the chunklet always lands.
	if err := r.applier.Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("restart applier for continuous checksum: %w", err)
	}
	defer func() {
		if cerr := r.applier.Stop(); cerr != nil {
			r.logger.Warn("continuous checksum: applier stop failed", "error", cerr)
		}
	}()

	// Construct the recopier — invoked by the checker when retry detects
	// stable target divergence. Without one configured, the checker would
	// instead return ErrPermanentDivergence and abort the sync.
	recopier, err := checksum.NewMySQLRecopier(r.source.db, r.target.DB, r.applier, r.targetDBConfig, r.logger)
	if err != nil {
		return fmt.Errorf("construct continuous-checksum recopier: %w", err)
	}

	checker, err := checksum.NewContinuousChecker(
		r.source.db, r.target.DB, chunker, r.replClient,
		checksum.ContinuousCheckerConfig{
			Concurrency:     r.sync.Threads,
			TargetChunkTime: r.sync.TargetChunkTime,
			Recopier:        recopier,
			Logger:          r.logger,
		},
	)
	if err != nil {
		return fmt.Errorf("construct continuous checker: %w", err)
	}

	// Publish the checker + chunker so accessors (FirstCleanPass,
	// ChecksumStats) can observe state from other goroutines. Close
	// continuousReadyCh exactly once, so callers can block on it.
	r.progMu.Lock()
	r.continuousChecker = checker
	r.continuousChunker = chunker
	r.progMu.Unlock()
	r.continuousCheckerInitOnce.Do(func() { close(r.continuousReadyCh) })

	// Forward the checker's first-clean-pass signal to the Runner-owned
	// channel that the FirstCleanPass accessor returns. This decouples
	// the accessor (which must be non-blocking and safe to call before
	// Run) from the checker's lifecycle.
	go func() {
		select {
		case <-checker.FirstCleanPass():
			r.firstCleanPassInitOnce.Do(func() { close(r.firstCleanPassCh) })
		case <-ctx.Done():
			// Run exited before a clean pass was observed. Leave the
			// channel open — callers should see Run's return error.
		}
	}()

	runErr := checker.Run(ctx)
	// A clean ctx-cancel run returns ctx.Err(); upstream filters that.
	return runErr
}

// buildContinuousChunker constructs a multi-chunker covering every source
// table. Unlike the copy chunker, this one isn't wired into the change
// feed (the checker doesn't need watermark filtering — every event has
// already been applied by the live replication path before the checker
// reads each chunk).
func (r *Runner) buildContinuousChunker() (table.Chunker, error) {
	chunkers := make([]table.Chunker, 0, len(r.sourceTables))
	for _, tbl := range r.sourceTables {
		cc, err := table.NewChunker(tbl, table.ChunkerConfig{
			TargetChunkTime: r.sync.TargetChunkTime,
			Logger:          r.logger,
		})
		if err != nil {
			return nil, fmt.Errorf("new continuous-checksum chunker for %s: %w", tbl.TableName, err)
		}
		chunkers = append(chunkers, cc)
	}
	return table.NewMultiChunker(chunkers...), nil
}

// FirstCleanPass returns a channel that is closed the first time the
// continuous checksum completes a clean pass — i.e. every chunk has gone
// clean at least once, including via retry. Programmatic callers that
// gate on "data is known consistent" (e.g. the import feature) should
// block on this channel.
//
// The accessor is non-blocking: it returns immediately with a channel
// the caller can wait on. The Runner-owned channel is closed by an
// internal goroutine once the checker fires its own FirstCleanPass —
// so it's safe to call before Run, after Run, or from a watchdog.
func (r *Runner) FirstCleanPass() <-chan struct{} {
	return r.firstCleanPassCh
}

// ChecksumReady returns a channel that is closed once the continuous
// checker has been constructed — that is, when the initial copy and the
// post-copy flush have completed and runContinuousChecksum has started.
func (r *Runner) ChecksumReady() <-chan struct{} {
	return r.continuousReadyCh
}

// ChecksumStats returns a point-in-time snapshot of continuous-checksum
// counters. Returns the zero value when the checker has not yet been
// constructed (initial copy still running).
func (r *Runner) ChecksumStats() checksum.ContinuousCheckerStats {
	r.progMu.RLock()
	defer r.progMu.RUnlock()
	if r.continuousChecker == nil {
		return checksum.ContinuousCheckerStats{}
	}
	return r.continuousChecker.Stats()
}

// setup discovers the source tables, builds the applier and change
// source, and prepares either a fresh copy or a checkpoint resume.
//
// Sync deliberately runs no source privilege/configuration preflight: it
// needs only SELECT on the source, and the change source validates any
// feed-specific requirements itself (the MySQL binlog client checks
// REPLICATION privileges + ROW binlog format on Start; a VStream
// authenticates over gRPC). A table without a primary key surfaces a clear
// error from getTables (SetInfo). The only target-side gate is that, for a
// fresh sync, the target tables must be empty.
func (r *Runner) setup(ctx context.Context) error {
	r.logger.Info("Fetching source table list")
	tables, err := r.getTables(ctx)
	if err != nil {
		return err
	}
	r.sourceTables = tables
	if len(r.sourceTables) == 0 {
		return nil
	}

	r.logger.Info("Creating applier")
	r.applier, err = r.createApplier()
	if err != nil {
		return err
	}

	// Wire the change source (continuous mode only): injected (e.g. VStream),
	// or a built-in MySQL binlog client constructed from the source DSN. Sync
	// replicates a whole schema, so the DDL filter is by schema only. Copy-only
	// sync constructs no change source.
	if !r.sync.CopyOnly {
		if r.sync.Source != nil {
			r.setReplClient(r.sync.Source)
		} else {
			replConfig := change.NewClientDefaultConfig()
			replConfig.Logger = r.logger
			replConfig.CancelFunc = r.fatalError
			replConfig.DDLFilterSchema = r.source.config.DBName
			replConfig.DBConfig = r.sourceDBConfig
			if r.sync.GTID {
				r.logger.Info("EXPERIMENTAL: using GTID-based change source")
				r.setReplClient(change.NewGTIDClient(r.source.db, r.source.config.Addr, r.source.config.User, r.source.config.Passwd, r.applier, replConfig))
			} else {
				r.setReplClient(change.NewBinlogClient(r.source.db, r.source.config.Addr, r.source.config.User, r.source.config.Passwd, r.applier, replConfig))
			}
		}
	}

	// If a checkpoint exists on the target, resume: open the copier chunker at
	// the saved watermark (continuing a partial copy) and open the change feed
	// at the saved position — skipping the target-empty check. So a restarted
	// sync resumes its partial copy instead of starting over.
	watermark, pos, hasCheckpoint, err := r.readCheckpoint(ctx)
	if err != nil {
		return err
	}
	if hasCheckpoint {
		r.resuming = true
		r.logger.Info("Found checkpoint on target; resuming", "position", pos)
		return r.startResume(ctx, watermark, pos)
	}

	// Fresh sync: the target tables must be empty so the copy can't clobber
	// or duplicate existing data.
	if err := r.checkTargetEmpty(ctx); err != nil {
		return err
	}
	return r.startFresh(ctx)
}

// getTables discovers all tables in the source schema. Sync operates on a
// whole schema at a time. Each table's metadata is populated via SetInfo.
func (r *Runner) getTables(ctx context.Context) ([]*table.TableInfo, error) {
	rows, err := r.source.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	var tableName string
	tables := make([]*table.TableInfo, 0)
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		// Skip the checkpoint table in case the source and target schemas
		// coincide (e.g. local testing).
		if tableName == syncCheckpointTableName {
			continue
		}
		ti := table.NewTableInfo(r.source.db, r.source.config.DBName, tableName)
		ti.Host = r.source.config.Addr
		// Sync only needs SELECT on the source, so skip the ANALYZE TABLE
		// (it needs INSERT + a writable server); the row estimate comes from
		// information_schema instead.
		ti.DisableAnalyze = true
		if err := ti.SetInfo(ctx); err != nil {
			return nil, err
		}
		tables = append(tables, ti)
	}
	return tables, rows.Err()
}

// checkTargetEmpty verifies that, for a fresh sync, none of the source
// tables already exist with data on the target. A table that does not yet
// exist is fine (startFresh will create it). Uses only SELECT on the
// target.
func (r *Runner) checkTargetEmpty(ctx context.Context) error {
	for _, t := range r.sourceTables {
		var dummy int
		err := r.target.DB.QueryRowContext(ctx,
			fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1", r.target.Config.DBName, t.TableName)).Scan(&dummy)
		if errors.Is(err, sql.ErrNoRows) {
			continue // table exists but is empty
		}
		if err != nil {
			// A missing target table is expected on a fresh sync.
			var myErr *mysql.MySQLError
			if errors.As(err, &myErr) && myErr.Number == errNoSuchTable {
				continue
			}
			return fmt.Errorf("failed to check whether target table %q is empty: %w", t.TableName, err)
		}
		return fmt.Errorf("target table %q already exists and is not empty; sync requires an empty target (drop it, or start from a checkpoint)", t.TableName)
	}
	return nil
}

// createApplier returns the caller-injected applier, or constructs a
// MySQL SingleTargetApplier for the target. The applier is not started
// here — the copier starts its async workers for the initial copy and
// stops them when it finishes; the subscription flush path uses the
// applier's synchronous UpsertRows/DeleteKeys, which do not require the
// workers, for steady-state streaming.
func (r *Runner) createApplier() (applier.Applier, error) {
	if r.sync.Applier != nil {
		r.logger.Info("Using caller-provided applier")
		return r.sync.Applier, nil
	}
	appl, err := applier.NewSingleTargetApplier(r.target, &applier.ApplierConfig{
		DBConfig: r.targetDBConfig,
		Logger:   r.logger,
		Threads:  r.sync.WriteThreads,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create SingleTargetApplier: %w", err)
	}
	return appl, nil
}

// ensureTargetDatabase creates the target database if it does not already
// exist, by connecting to the target server (from the target's config)
// without selecting a database. Used on both the injected-Target and
// TargetDSN paths — the import's per-shard target databases are not
// guaranteed to pre-exist on the destination cluster.
func (r *Runner) ensureTargetDatabase(ctx context.Context, cfg *mysql.Config) error {
	if cfg == nil {
		return errors.New("target config is nil; cannot ensure target database")
	}
	if cfg.DBName == "" {
		return errors.New("target must include a database name")
	}
	adminCfg := cfg.Clone()
	adminCfg.DBName = ""
	adminDB, err := dbconn.New(adminCfg.FormatDSN(), r.targetDBConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to target server to ensure database: %w", err)
	}
	defer utils.CloseAndLog(adminDB)
	// Force: drop and recreate the target database unless a resumable
	// checkpoint exists. We do this here, on the admin connection (no database
	// selected) and before r.target.DB is ever queried, so no live connection
	// has the database selected when it's dropped. A resumable run is left
	// intact and resumes as normal.
	if r.sync.Force {
		resumable, rerr := r.hasResumableCheckpoint(ctx, adminDB, cfg.DBName)
		if rerr != nil {
			return rerr
		}
		if resumable {
			r.logger.Info("force set, but a resumable checkpoint exists; keeping target and resuming", "database", cfg.DBName)
		} else {
			r.logger.Warn("force set and no resumable checkpoint; dropping and recreating target database", "database", cfg.DBName)
			if err := dbconn.Exec(ctx, adminDB, "DROP DATABASE IF EXISTS %n", cfg.DBName); err != nil {
				return fmt.Errorf("failed to drop target database %q: %w", cfg.DBName, err)
			}
		}
	}
	if err := dbconn.Exec(ctx, adminDB, "CREATE DATABASE IF NOT EXISTS %n", cfg.DBName); err != nil {
		return fmt.Errorf("failed to create target database %q: %w", cfg.DBName, err)
	}
	r.logger.Info("ensured target database exists", "database", cfg.DBName)
	return nil
}

// hasResumableCheckpoint reports whether the target database holds a sync
// checkpoint that can be resumed from (a row carrying a copier watermark). It
// runs on the passed connection (typically the no-database admin connection),
// using fully-qualified names so it works without a selected database — and so
// it can be called before r.target.DB is opened.
func (r *Runner) hasResumableCheckpoint(ctx context.Context, db *sql.DB, dbName string) (bool, error) {
	var exists int
	e := db.QueryRowContext(ctx,
		"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
		dbName, syncCheckpointTableName).Scan(&exists)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, fmt.Errorf("failed to check for checkpoint table: %w", e)
	}
	var watermark string
	e = db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT IFNULL(copier_watermark, '') FROM `%s`.`%s` WHERE id = 1", dbName, syncCheckpointTableName)).Scan(&watermark)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, fmt.Errorf("failed to read checkpoint: %w", e)
	}
	return watermark != "", nil
}

// createTargetTables creates each source table on the target using the
// source's SHOW CREATE TABLE. Tables that already exist are skipped (they
// were validated as empty + schema-compatible by the target_state check).
//
// This is MySQL→MySQL schema replication; a future heterogeneous target
// (e.g. Postgres) would translate or pre-create the schema instead.
//
// The DDL runs with a relaxed sql_mode. The source's SHOW CREATE TABLE can
// carry legacy column definitions — most commonly a TIMESTAMP/DATETIME with a
// zero-date default ('0000-00-00 00:00:00') — that the target's strict
// sql_mode (TRADITIONAL: NO_ZERO_DATE/NO_ZERO_IN_DATE/STRICT_*) rejects with
// "Invalid default value" (error 1067). We recreate the tables exactly as the
// source defines them, so we clear sql_mode for the CREATE statements. The DDL
// runs on a single dedicated connection so the SET SESSION applies to it, and
// we restore the mode before returning that connection to the pool (data
// writes keep the strict mode).
func (r *Runner) createTargetTables(ctx context.Context) error {
	conn, err := r.target.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire target connection for DDL: %w", err)
	}
	defer utils.CloseAndLog(conn)

	var prevSQLMode string
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&prevSQLMode); err != nil {
		return fmt.Errorf("failed to read target sql_mode: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
		return fmt.Errorf("failed to relax sql_mode for table creation: %w", err)
	}
	defer func() {
		// Restore before the connection returns to the pool so data writes
		// aren't silently relaxed.
		if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = ?", prevSQLMode); err != nil {
			r.logger.Warn("failed to restore sql_mode after table creation", "error", err)
		}
	}()

	for _, t := range r.sourceTables {
		var name, createStmt string
		row := r.source.db.QueryRowContext(ctx, "SHOW CREATE TABLE "+t.QuotedTableName)
		if err := row.Scan(&name, &createStmt); err != nil {
			return fmt.Errorf("failed to read CREATE TABLE for source %s: %w", t.TableName, err)
		}
		var exists int
		err := conn.QueryRowContext(ctx,
			"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
			r.target.Config.DBName, t.TableName).Scan(&exists)
		if err == nil {
			r.logger.Info("target table already exists, skipping creation",
				"table", t.TableName, "database", r.target.Config.DBName)
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to check if table %s exists on target: %w", t.TableName, err)
		}
		if _, err := conn.ExecContext(ctx, createStmt); err != nil {
			return fmt.Errorf("failed to create table %s on target: %w", t.TableName, err)
		}
		r.logger.Info("created table on target", "table", t.TableName, "database", r.target.Config.DBName)
	}
	return nil
}

// buildChunkers creates a chunker per source table and registers a
// subscription on the change feed so changes during the copy are captured +
// deduped. Returns the chunkers for assembly into the copy multi-chunker.
func (r *Runner) buildChunkers() ([]table.Chunker, error) {
	chunkers := make([]table.Chunker, 0, len(r.sourceTables))
	for _, tbl := range r.sourceTables {
		cc, err := table.NewChunker(tbl, table.ChunkerConfig{
			TargetChunkTime: r.sync.TargetChunkTime,
			Logger:          r.logger,
		})
		if err != nil {
			return nil, err
		}
		if !r.sync.CopyOnly {
			if err := r.replClient.AddSubscription(tbl, nil, cc); err != nil {
				return nil, err
			}
		}
		chunkers = append(chunkers, cc)
	}
	return chunkers, nil
}

// buildCopyPipeline builds the per-table chunkers (and, for continuous sync,
// their change-feed subscriptions), assembles the multi-chunker, and
// constructs the buffered copier. The caller opens the chunker afterwards —
// Open() for a fresh sync, OpenAtWatermark() for a resume.
func (r *Runner) buildCopyPipeline() error {
	chunkers, err := r.buildChunkers()
	if err != nil {
		return err
	}
	r.setCopyChunker(table.NewMultiChunker(chunkers...))
	cp, err := copier.NewCopier(r.source.db, r.copyChunker, &copier.CopierConfig{
		Concurrency:     r.sync.Threads,
		TargetChunkTime: r.sync.TargetChunkTime,
		Logger:          r.logger,
		Throttler:       &throttler.Noop{},
		MetricsSink:     &metrics.NoopSink{},
		DBConfig:        r.sourceDBConfig,
		Applier:         r.applier,
		Buffered:        true, // sync always uses the buffered copier
	})
	if err != nil {
		return err
	}
	r.setCopier(cp)
	return nil
}

// startFresh creates the target tables, builds the copy pipeline, opens the
// chunker from the beginning, starts the change feed (continuous only), and
// creates the checkpoint table so copy progress can be recorded from the
// start of the copy.
func (r *Runner) startFresh(ctx context.Context) error {
	if err := r.createTargetTables(ctx); err != nil {
		return err
	}
	if err := r.buildCopyPipeline(); err != nil {
		return err
	}
	if err := r.copyChunker.Open(); err != nil {
		return err
	}
	// Copy-only sync has no change feed to start.
	if !r.sync.CopyOnly {
		// The change-feed reader must outlive ctx. On a clean shutdown ctx is
		// cancelled first, and only then does the drain path (runContinuous)
		// issue its final Flush. That Flush relies on the reader goroutine to
		// keep advancing the buffered binlog position so BlockWait can converge;
		// if the reader were tied to ctx it would already be dead, the buffered
		// position would be frozen, and the final flush would spin (re-flushing
		// binary logs) until it burned its entire shutdownFlushTimeout budget.
		// Tie the reader's lifetime to Close() instead — Close() cancels its own
		// derived context — by handing it a cancellation-detached ctx here.
		if err := r.replClient.Start(context.WithoutCancel(ctx)); err != nil {
			return fmt.Errorf("failed to start change source: %w", err)
		}
	}
	return r.createCheckpointTable(ctx)
}

// startResume rebuilds the copy pipeline and opens the chunker at the
// checkpointed watermark, so the copier continues a partial copy (or finishes
// immediately if the copy had completed). For continuous sync it also disables
// the watermark optimization (every change applies) and opens the feed at the
// saved position. The target tables already exist (createTargetTables is
// idempotent, skipping them) and the target-empty check is skipped.
func (r *Runner) startResume(ctx context.Context, watermark, pos string) error {
	if err := r.createTargetTables(ctx); err != nil {
		return err
	}
	if err := r.buildCopyPipeline(); err != nil {
		return err
	}
	// Open at the saved watermark when we have one; otherwise (the prior
	// attempt failed before writing its first checkpoint) open from the start
	// and re-copy. The copy is idempotent (INSERT IGNORE), and we still skip
	// the fresh-sync target-empty check because this import owns the
	// (partially-populated) target.
	if watermark != "" {
		if err := r.copyChunker.OpenAtWatermark(watermark); err != nil {
			return fmt.Errorf("failed to open copier at checkpoint watermark: %w", err)
		}
	} else {
		if err := r.copyChunker.Open(); err != nil {
			return err
		}
	}
	if !r.sync.CopyOnly {
		if err := r.replClient.SetWatermarkOptimization(ctx, false); err != nil {
			return err
		}
		// The reader must outlive ctx so the drain-time final Flush can
		// converge; see the matching comment in startFresh. Close() stops it.
		streamCtx := context.WithoutCancel(ctx)
		if pos != "" {
			if err := r.replClient.StartFromPosition(streamCtx, pos); err != nil {
				return fmt.Errorf("failed to resume change source from position %q: %w", pos, err)
			}
		} else if err := r.replClient.Start(streamCtx); err != nil {
			// No saved position (prior attempt failed before checkpointing):
			// start the feed fresh; changes apply with the optimization off.
			return fmt.Errorf("failed to start change source: %w", err)
		}
	}
	return r.createCheckpointTable(ctx)
}

// createCheckpointTable creates the checkpoint table on the target (always
// the target, since the source may be read-only). It records the copier's low
// watermark (resume point for a partial copy) and the change-feed position
// (resume point for continuous sync).
func (r *Runner) createCheckpointTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.target.DB, `CREATE TABLE IF NOT EXISTS %n.%n (
	id INT NOT NULL PRIMARY KEY,
	copier_watermark TEXT,
	source_position TEXT,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`, r.target.Config.DBName, syncCheckpointTableName); err != nil {
		return fmt.Errorf("failed to create checkpoint table on target: %w", err)
	}
	return nil
}

// dumpCheckpoint records the copier's low watermark (so a partial copy can
// resume) and the change-feed position (so continuous sync can resume) in the
// target checkpoint table (single row, id=1). It is a no-op until the copy
// pipeline is built, and skips writing whenever the watermark isn't ready yet,
// so it never persists an unparseable watermark.
func (r *Runner) dumpCheckpoint(ctx context.Context) error {
	r.progMu.RLock()
	chunker := r.copyChunker
	repl := r.replClient
	r.progMu.RUnlock()
	if chunker == nil {
		return nil // pipeline not built yet; nothing to checkpoint
	}
	watermark, err := chunker.GetLowWatermark()
	if err != nil {
		// The chunker's watermark isn't ready yet (e.g. a single-table copy
		// whose only chunk hasn't produced a resumable boundary). Skip this
		// write so we never persist an unparseable watermark; we'll try again
		// on the next tick once it advances.
		return nil
	}
	var pos string
	if repl != nil {
		pos = repl.Position()
	}
	return dbconn.Exec(ctx, r.target.DB,
		"REPLACE INTO %n.%n (id, copier_watermark, source_position) VALUES (1, %?, %?)",
		r.target.Config.DBName, syncCheckpointTableName, watermark, pos)
}

// readCheckpoint reports whether the target carries a sync checkpoint and, if
// so, the saved copier watermark + change-feed position.
//
// The "resume" signal is the existence of the checkpoint TABLE, not merely a
// saved watermark. The table is created (in startFresh) before any rows are
// copied, so its presence means a prior attempt of this import already owns
// the target — even if that attempt died before writing its first watermark
// row, leaving partial data behind. Treating that as resumable lets the retry
// re-copy idempotently instead of tripping the fresh-sync target-empty guard.
// (watermark/pos may be empty in that case; startResume handles it.)
func (r *Runner) readCheckpoint(ctx context.Context) (watermark, pos string, ok bool, err error) {
	var exists int
	e := r.target.DB.QueryRowContext(ctx,
		"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
		r.target.Config.DBName, syncCheckpointTableName).Scan(&exists)
	if errors.Is(e, sql.ErrNoRows) {
		return "", "", false, nil // no checkpoint table → not a prior import; fresh sync
	}
	if e != nil {
		return "", "", false, fmt.Errorf("failed to check for checkpoint table: %w", e)
	}
	// The table exists: a prior attempt owns this target. Read its (optional)
	// saved watermark/position.
	e = r.target.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT IFNULL(copier_watermark, ''), IFNULL(source_position, '') FROM `%s`.`%s` WHERE id = 1",
			r.target.Config.DBName, syncCheckpointTableName)).Scan(&watermark, &pos)
	if errors.Is(e, sql.ErrNoRows) {
		return "", "", true, nil // table exists but no row written yet → resume, re-copy from scratch
	}
	if e != nil {
		return "", "", false, fmt.Errorf("failed to read checkpoint: %w", e)
	}
	return watermark, pos, true, nil
}

// dumpCheckpointLoop periodically records the source position on the
// target until ctx is cancelled.
func (r *Runner) dumpCheckpointLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(status.CheckpointDumpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.dumpCheckpoint(ctx); err != nil {
				r.logger.Warn("failed to write checkpoint", "error", err)
			}
		}
	}
}

// startBackgroundRoutines starts the periodic flush (which advances the
// applied position and keeps the target caught up), the status logger, and the
// periodic checkpoint loop. The checkpoint loop runs for the whole run (copy
// and continuous), so a restart at any point resumes from the last checkpoint.
func (r *Runner) startBackgroundRoutines(ctx context.Context) {
	// Copy-only sync has no change feed, so no periodic flush.
	if !r.sync.CopyOnly {
		r.replClient.StartPeriodicFlush(ctx, r.sync.FlushInterval)
	}
	r.watchDone = make(chan struct{})
	go r.watchStatus(ctx)
	r.checkpointDone = make(chan struct{})
	go r.dumpCheckpointLoop(ctx, r.checkpointDone)
}

// watchStatus logs progress periodically until ctx is cancelled.
func (r *Runner) watchStatus(ctx context.Context) {
	defer close(r.watchDone)
	ticker := time.NewTicker(status.StatusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Guard the change feed in case status logging starts before the
			// repl client is wired.
			pending := 0
			if r.replClient != nil {
				pending = r.replClient.GetDeltaLen()
			}
			switch {
			case r.status.Get() == status.CopyRows && r.copier != nil:
				r.logger.Info("sync status",
					"phase", "initial-copy",
					"progress", r.copier.GetProgress(),
					"eta", r.copier.GetETA(),
					"pending-changes", pending,
					"elapsed", time.Since(r.startTime).Round(time.Second),
				)
			case r.replClient != nil:
				r.logger.Info("sync status",
					"phase", "continuous",
					"pending-changes", pending,
					"position", r.replClient.Position(),
					"elapsed", time.Since(r.startTime).Round(time.Second),
				)
			}
		}
	}
}

// fatalError is the change client's CancelFunc: it records the fatal
// condition (e.g. DDL on a synced table) and cancels the run so
// runContinuous can surface it.
func (r *Runner) fatalError() bool {
	r.fatalMu.Lock()
	if r.fatalErr == nil {
		r.fatalErr = errors.New("a DDL change was detected on a synced table; sync cannot continue safely")
	}
	r.fatalMu.Unlock()
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
	return true
}

func (r *Runner) fatal() error {
	r.fatalMu.Lock()
	defer r.fatalMu.Unlock()
	return r.fatalErr
}

// Close releases resources. Safe to call once after Run returns. The
// applier's worker lifecycle is owned by the copier (which stops it after
// the initial copy), so Close does not stop it. An injected change.Source
// and injected applier/target are owned by the caller, but Close still
// calls change.Source.Close (documented idempotent) to release the
// runner's reference.
func (r *Runner) Close() error {
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
	if r.watchDone != nil {
		<-r.watchDone
	}
	if r.checkpointDone != nil {
		<-r.checkpointDone
	}
	if r.replClient != nil {
		r.replClient.StopPeriodicFlush()
		r.replClient.Close()
	}
	if r.copyChunker != nil {
		if err := r.copyChunker.Close(); err != nil {
			return err
		}
	}
	if r.ownsTarget && r.target.DB != nil {
		if err := r.target.DB.Close(); err != nil {
			return err
		}
	}
	if r.source.db != nil {
		if err := r.source.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// --- progress-field setters (guard the fields the status.Task accessors read) ---

func (r *Runner) setReplClient(c change.Source) {
	r.progMu.Lock()
	r.replClient = c
	r.progMu.Unlock()
}

func (r *Runner) setCopier(c copier.Copier) {
	r.progMu.Lock()
	r.copier = c
	r.progMu.Unlock()
}

func (r *Runner) setCopyChunker(c table.Chunker) {
	r.progMu.Lock()
	r.copyChunker = c
	r.progMu.Unlock()
}

// --- status.Task ---
//
// Runner implements status.Task so a caller (e.g. tern's import engine) can
// surface live progress and force a checkpoint, the same way it monitors a
// spirit migration.Runner. These accessors are safe to call concurrently with
// Run from another goroutine.

// Progress returns a structured snapshot of the sync's current state: the
// phase, a short human-readable summary, and per-table copy progress during
// the initial copy.
func (r *Runner) Progress() status.Progress {
	state := r.status.Get()

	r.progMu.RLock()
	cp := r.copier
	chunker := r.copyChunker
	repl := r.replClient
	r.progMu.RUnlock()

	var summary string
	switch state { //nolint:exhaustive // sync only uses Initial/CopyRows/ApplyChangeset
	case status.CopyRows:
		if cp != nil {
			summary = fmt.Sprintf("%s copyRows ETA %s", cp.GetProgress(), cp.GetETA())
		} else {
			summary = "copyRows"
		}
	case status.ApplyChangeset:
		if repl != nil {
			summary = fmt.Sprintf("continuous sync position=%s pending-changes=%d", repl.Position(), repl.GetDeltaLen())
		} else {
			summary = "continuous sync"
		}
	default:
		summary = state.String()
	}

	// Per-table progress: the multi-chunker reports each table; fall back to
	// the single-table chunker view if it's not a multi-chunker.
	var tables []status.TableProgress
	if mc, ok := chunker.(interface {
		PerTableProgress() []table.TableProgress
	}); ok {
		for _, tp := range mc.PerTableProgress() {
			tables = append(tables, status.TableProgress{
				TableName:  tp.TableName,
				RowsCopied: tp.RowsCopied,
				RowsTotal:  tp.RowsTotal,
				IsComplete: tp.IsComplete,
			})
		}
	} else if chunker != nil {
		rowsCopied, _, rowsTotal := chunker.Progress()
		name := ""
		if ts := chunker.Tables(); len(ts) > 0 {
			name = ts[0].TableName
		}
		tables = append(tables, status.TableProgress{
			TableName:  name,
			RowsCopied: rowsCopied,
			RowsTotal:  rowsTotal,
			IsComplete: chunker.IsRead(),
		})
	}

	return status.Progress{CurrentState: state, Summary: summary, Tables: tables}
}

// Status returns a one-line, human-readable status for logging. It does not
// log itself; status.WatchTask (when used) logs the returned value.
func (r *Runner) Status() string {
	state := r.status.Get()

	r.progMu.RLock()
	cp := r.copier
	repl := r.replClient
	start := r.startTime
	r.progMu.RUnlock()

	elapsed := time.Since(start).Round(time.Second)
	switch state { //nolint:exhaustive // sync only uses Initial/CopyRows/ApplyChangeset
	case status.CopyRows:
		progress, eta := "", ""
		if cp != nil {
			progress, eta = cp.GetProgress(), cp.GetETA()
		}
		pending := 0
		if repl != nil {
			pending = repl.GetDeltaLen()
		}
		return fmt.Sprintf("sync status: state=%s copy-progress=%s copy-eta=%s pending-changes=%d total-time=%s",
			state.String(), progress, eta, pending, elapsed)
	case status.ApplyChangeset:
		pos := ""
		pending := 0
		if repl != nil {
			pos, pending = repl.Position(), repl.GetDeltaLen()
		}
		return fmt.Sprintf("sync status: state=%s position=%s pending-changes=%d total-time=%s",
			state.String(), pos, pending, elapsed)
	default:
		return fmt.Sprintf("sync status: state=%s total-time=%s", state.String(), elapsed)
	}
}

// DumpCheckpoint records the copier watermark (and, for continuous sync, the
// change-feed position) on the target so a restart can resume a partial copy
// and the stream. It is a no-op until the copy pipeline is built.
func (r *Runner) DumpCheckpoint(ctx context.Context) error {
	r.progMu.RLock()
	chunker := r.copyChunker
	r.progMu.RUnlock()
	if chunker == nil {
		return nil // copy pipeline not built yet; nothing to checkpoint
	}
	// Idempotent: createCheckpointTable uses CREATE TABLE IF NOT EXISTS, so
	// this is safe even in the brief window before startFresh/startResume has
	// created the table.
	if err := r.createCheckpointTable(ctx); err != nil {
		return err
	}
	return r.dumpCheckpoint(ctx)
}

// Cancel stops the sync by cancelling the Run context. Safe to call before
// Run has started (no-op until cancelFunc is set).
func (r *Runner) Cancel() {
	r.progMu.RLock()
	cancel := r.cancelFunc
	r.progMu.RUnlock()
	if cancel != nil {
		cancel()
	}
}

// redactDSN strips credentials from a DSN for safe logging.
func redactDSN(dsn string) string {
	if i := strings.LastIndex(dsn, "@"); i >= 0 {
		return "<redacted>" + dsn[i:]
	}
	return dsn
}
