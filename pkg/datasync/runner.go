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
	dbConfig   *dbconn.DBConfig

	// watchDone is closed when the status-logging goroutine exits.
	watchDone chan struct{}

	// fatalErr records a fatal source-side event (e.g. DDL on a synced
	// table) that should surface as the Run error rather than a clean
	// cancellation. Guarded by fatalMu.
	fatalMu  sync.Mutex
	fatalErr error
}

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
	return &Runner{sync: s, logger: slog.Default()}, nil
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
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()
	r.startTime = time.Now()
	r.logger.Info("Starting sync", "source_dsn", redactDSN(r.sync.SourceDSN))

	r.dbConfig = dbconn.NewDBConfig()
	// Sync only ever reads from the source (copy SELECTs + the change feed).
	// It never writes to the source, acquires no source locks, and performs
	// no cutover, so it needs only SELECT on the source schema (plus
	// REPLICATION SLAVE/CLIENT when the change feed is the built-in MySQL
	// binlog client, which that client validates on Start). Disable the two
	// dbConfig behaviours that would otherwise demand more:
	//   - ForceKill needs CONNECTION_ADMIN/PROCESS + performance_schema, and
	//     is only used to break metadata locks during cutover — which sync
	//     never does.
	//   - RejectReadOnly is an Aurora-failover guard that turns a read-only
	//     server error into driver.ErrBadConn; sync's source is read-only by
	//     design (e.g. a Vitess/PlanetScale replica), so it must not fire.
	r.dbConfig.ForceKill = false
	r.dbConfig.RejectReadOnly = false
	r.dbConfig.MaxOpenConnections = 100

	// Open the source SQL connection. Even when the change feed is an
	// injected non-MySQL source, spirit still needs SQL access to the
	// source for SHOW TABLES / SHOW CREATE TABLE and the initial-copy
	// SELECTs.
	db, err := dbconn.New(r.sync.SourceDSN, r.dbConfig)
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
		tdb, terr := dbconn.New(r.sync.TargetDSN, r.dbConfig)
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

	if !r.resuming {
		// Watermark optimization ON during the copy: change events for keys
		// the copier has not reached yet are discarded (those rows are
		// copied directly), avoiding redundant work.
		if err := r.replClient.SetWatermarkOptimization(ctx, true); err != nil {
			return err
		}
		r.status.Set(status.CopyRows)
		r.logger.Info("Starting initial copy")
		if err := r.copier.Run(ctx); err != nil {
			return fmt.Errorf("initial copy failed: %w", err)
		}
		// Disable the watermark optimization and drain the copy-phase
		// backlog so every change observed so far is applied before
		// steady-state streaming.
		if err := r.replClient.SetWatermarkOptimization(ctx, false); err != nil {
			return err
		}
		if err := r.replClient.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush after initial copy: %w", err)
		}
		r.logger.Info("Initial copy complete; entering continuous sync")
	}

	return r.runContinuous(ctx)
}

// runContinuous creates/maintains the target checkpoint and blocks until
// the context is cancelled, while the periodic flush (started in
// startBackgroundRoutines) keeps applying buffered changes. On a clean
// cancellation it drains the final backlog and returns nil; on a fatal
// source event it returns that error.
func (r *Runner) runContinuous(ctx context.Context) error {
	// status is left at a non-CopyRows value so the status goroutine logs
	// the continuous phase.
	r.status.Set(status.ApplyChangeset)

	// The checkpoint table is created on the target only once the initial
	// copy is complete, so its existence means "copy done, safe to resume
	// from the recorded position". Dump once immediately, then periodically.
	if err := r.createCheckpointTable(ctx); err != nil {
		return err
	}
	if err := r.dumpCheckpoint(ctx); err != nil {
		r.logger.Warn("failed to write initial checkpoint", "error", err)
	}
	cpDone := make(chan struct{})
	go r.dumpCheckpointLoop(ctx, cpDone)

	r.logger.Info("Continuous sync running; will run until cancelled")
	<-ctx.Done()
	<-cpDone

	if ferr := r.fatal(); ferr != nil {
		r.logger.Error("Sync stopping due to fatal source event", "error", ferr)
		return ferr
	}

	r.logger.Info("Sync cancelled; draining final changes")
	r.replClient.StopPeriodicFlush()
	// Best-effort final flush + checkpoint, detached from the cancelled ctx
	// with a bound so a hung target can't wedge shutdown.
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := r.replClient.Flush(flushCtx); err != nil {
		r.logger.Warn("Final flush failed; some recent changes may not have been applied", "error", err)
	}
	if err := r.dumpCheckpoint(flushCtx); err != nil {
		r.logger.Warn("Final checkpoint write failed", "error", err)
	}
	r.logger.Info("Sync stopped", "total_time", time.Since(r.startTime).Round(time.Second))
	return nil
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

	// Wire the change source: injected (e.g. VStream), or a built-in MySQL
	// binlog client constructed from the source DSN. Sync replicates a whole
	// schema, so the DDL filter is by schema only (no table list).
	if r.sync.Source != nil {
		r.replClient = r.sync.Source
	} else {
		replConfig := change.NewClientDefaultConfig()
		replConfig.Logger = r.logger
		replConfig.CancelFunc = r.fatalError
		replConfig.DDLFilterSchema = r.source.config.DBName
		replConfig.DBConfig = r.dbConfig
		r.replClient = change.NewBinlogClient(r.source.db, r.source.config.Addr, r.source.config.User, r.source.config.Passwd, r.applier, replConfig)
	}

	// If a checkpoint already exists on the target, resume from it: skip the
	// initial copy and the target-empty check, and open the feed from the
	// recorded position.
	pos, hasCheckpoint, err := r.readCheckpoint(ctx)
	if err != nil {
		return err
	}
	if hasCheckpoint {
		r.resuming = true
		r.logger.Info("Found checkpoint on target; resuming continuous sync", "position", pos)
		return r.startResume(ctx, pos)
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
		DBConfig: r.dbConfig,
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
	adminDB, err := dbconn.New(adminCfg.FormatDSN(), r.dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to target server to ensure database: %w", err)
	}
	defer utils.CloseAndLog(adminDB)
	if err := dbconn.Exec(ctx, adminDB, "CREATE DATABASE IF NOT EXISTS %n", cfg.DBName); err != nil {
		return fmt.Errorf("failed to create target database %q: %w", cfg.DBName, err)
	}
	r.logger.Info("ensured target database exists", "database", cfg.DBName)
	return nil
}

// createTargetTables creates each source table on the target using the
// source's SHOW CREATE TABLE. Tables that already exist are skipped (they
// were validated as empty + schema-compatible by the target_state check).
//
// This is MySQL→MySQL schema replication; a future heterogeneous target
// (e.g. Postgres) would translate or pre-create the schema instead.
func (r *Runner) createTargetTables(ctx context.Context) error {
	for _, t := range r.sourceTables {
		var name, createStmt string
		row := r.source.db.QueryRowContext(ctx, "SHOW CREATE TABLE "+t.QuotedTableName)
		if err := row.Scan(&name, &createStmt); err != nil {
			return fmt.Errorf("failed to read CREATE TABLE for source %s: %w", t.TableName, err)
		}
		var exists int
		err := r.target.DB.QueryRowContext(ctx,
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
		if _, err := r.target.DB.ExecContext(ctx, createStmt); err != nil {
			return fmt.Errorf("failed to create table %s on target: %w", t.TableName, err)
		}
		r.logger.Info("created table on target", "table", t.TableName, "database", r.target.Config.DBName)
	}
	return nil
}

// buildSubscriptions creates a chunker per source table, registers a
// subscription on the change feed, and returns the chunkers for assembly
// into the copy multi-chunker.
func (r *Runner) buildSubscriptions() ([]table.Chunker, error) {
	chunkers := make([]table.Chunker, 0, len(r.sourceTables))
	for _, tbl := range r.sourceTables {
		cc, err := table.NewChunker(tbl, table.ChunkerConfig{
			TargetChunkTime: r.sync.TargetChunkTime,
			Logger:          r.logger,
		})
		if err != nil {
			return nil, err
		}
		if err := r.replClient.AddSubscription(tbl, nil, cc); err != nil {
			return nil, err
		}
		chunkers = append(chunkers, cc)
	}
	return chunkers, nil
}

// startFresh creates the target tables, registers subscriptions + chunkers,
// builds the buffered copier, and starts the change feed. The subscriptions
// buffer changes that arrive during the copy; the bufferedMap dedupes them
// against the copied rows.
func (r *Runner) startFresh(ctx context.Context) error {
	if err := r.createTargetTables(ctx); err != nil {
		return err
	}

	chunkers, err := r.buildSubscriptions()
	if err != nil {
		return err
	}
	r.copyChunker = table.NewMultiChunker(chunkers...)

	r.copier, err = copier.NewCopier(r.source.db, r.copyChunker, &copier.CopierConfig{
		Concurrency:     r.sync.Threads,
		TargetChunkTime: r.sync.TargetChunkTime,
		Logger:          r.logger,
		Throttler:       &throttler.Noop{},
		MetricsSink:     &metrics.NoopSink{},
		DBConfig:        r.dbConfig,
		Applier:         r.applier,
		Buffered:        true, // sync always uses the buffered copier
	})
	if err != nil {
		return err
	}
	if err := r.copyChunker.Open(); err != nil {
		return err
	}
	if err := r.replClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start change source: %w", err)
	}
	return nil
}

// startResume registers subscriptions + chunkers (needed for the change
// feed's column mapping) without copying, disables the watermark
// optimization so every change applies, and opens the feed at the
// checkpointed position.
func (r *Runner) startResume(ctx context.Context, pos string) error {
	chunkers, err := r.buildSubscriptions()
	if err != nil {
		return err
	}
	r.copyChunker = table.NewMultiChunker(chunkers...)
	if err := r.copyChunker.Open(); err != nil {
		return err
	}
	if err := r.replClient.SetWatermarkOptimization(ctx, false); err != nil {
		return err
	}
	if err := r.replClient.StartFromPosition(ctx, pos); err != nil {
		return fmt.Errorf("failed to resume change source from position %q: %w", pos, err)
	}
	return nil
}

// createCheckpointTable creates the checkpoint table on the target (always
// the target, since the source may be read-only).
func (r *Runner) createCheckpointTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.target.DB, `CREATE TABLE IF NOT EXISTS %n.%n (
	id INT NOT NULL PRIMARY KEY,
	source_position TEXT,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`, r.target.Config.DBName, syncCheckpointTableName); err != nil {
		return fmt.Errorf("failed to create checkpoint table on target: %w", err)
	}
	return nil
}

// dumpCheckpoint records the current source position in the target
// checkpoint table (single row, id=1).
func (r *Runner) dumpCheckpoint(ctx context.Context) error {
	pos := r.replClient.Position()
	return dbconn.Exec(ctx, r.target.DB,
		"REPLACE INTO %n.%n (id, source_position) VALUES (1, %?)",
		r.target.Config.DBName, syncCheckpointTableName, pos)
}

// readCheckpoint returns the saved source position from the target
// checkpoint table, and whether a resumable checkpoint exists.
func (r *Runner) readCheckpoint(ctx context.Context) (pos string, ok bool, err error) {
	var exists int
	e := r.target.DB.QueryRowContext(ctx,
		"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
		r.target.Config.DBName, syncCheckpointTableName).Scan(&exists)
	if errors.Is(e, sql.ErrNoRows) {
		return "", false, nil
	}
	if e != nil {
		return "", false, fmt.Errorf("failed to check for checkpoint table: %w", e)
	}
	e = r.target.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT source_position FROM `%s`.`%s` WHERE id = 1", r.target.Config.DBName, syncCheckpointTableName)).Scan(&pos)
	if errors.Is(e, sql.ErrNoRows) {
		return "", false, nil
	}
	if e != nil {
		return "", false, fmt.Errorf("failed to read checkpoint: %w", e)
	}
	return pos, pos != "", nil
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
// applied position and keeps the target caught up) and the status logger.
func (r *Runner) startBackgroundRoutines(ctx context.Context) {
	r.replClient.StartPeriodicFlush(ctx, r.sync.FlushInterval)
	r.watchDone = make(chan struct{})
	go r.watchStatus(ctx)
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
			if r.status.Get() == status.CopyRows {
				r.logger.Info("sync status",
					"phase", "initial-copy",
					"progress", r.copier.GetProgress(),
					"eta", r.copier.GetETA(),
					"pending-changes", r.replClient.GetDeltaLen(),
					"elapsed", time.Since(r.startTime).Round(time.Second),
				)
			} else {
				r.logger.Info("sync status",
					"phase", "continuous",
					"pending-changes", r.replClient.GetDeltaLen(),
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

// redactDSN strips credentials from a DSN for safe logging.
func redactDSN(dsn string) string {
	if i := strings.LastIndex(dsn, "@"); i >= 0 {
		return "<redacted>" + dsn[i:]
	}
	return dsn
}
