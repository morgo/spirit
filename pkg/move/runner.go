package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
)

var (
	sentinelCheckInterval   = 1 * time.Second
	tableStatUpdateInterval = 5 * time.Minute
	sentinelWaitLimit       = 48 * time.Hour
	sentinelTableName       = "_spirit_sentinel" // this is now a const.
	checkpointTableName     = "_spirit_checkpoint"
)

type Runner struct {
	move            *Move
	source          *sql.DB
	sourceConfig    *mysql.Config
	targets         []applier.Target // Combined DB, Config, and KeyRange
	status          status.State
	checkpointTable *table.TableInfo

	sourceTables []*table.TableInfo

	replClient        *repl.Client
	copyChunker       table.Chunker
	checksumChunker   table.Chunker
	copier            copier.Copier
	checker           *checksum.Checker
	checksumWatermark string

	// Track some key statistics.
	startTime                time.Time
	sentinelWaitStartTime    time.Time
	usedResumeFromCheckpoint bool

	cutoverFunc func(ctx context.Context) error

	logger     *slog.Logger
	cancelFunc context.CancelFunc
	dbConfig   *dbconn.DBConfig
}

var _ status.Task = (*Runner)(nil)

func NewRunner(m *Move) (*Runner, error) {
	r := &Runner{
		move:   m,
		logger: slog.Default(),
	}
	return r, nil
}

func (r *Runner) Close() error {
	if r.copyChunker != nil {
		r.copyChunker.Close()
	}
	if r.replClient != nil {
		r.replClient.Close()
	}
	return nil
}

// getTables connects to a DB and fetches the list of tables
// it can be run on either the source or the target.
func (r *Runner) getTables(ctx context.Context, db *sql.DB) ([]*table.TableInfo, error) {
	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableName string
	tables := make([]*table.TableInfo, 0)
	// TODO: how do these tables set their vindex info?
	// They need it for the reshard to work.
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		if tableName == checkpointTableName || tableName == sentinelTableName {
			continue // Skip if the table name is the checkpoint or sentinel table
		}
		tableInfo := table.NewTableInfo(r.source, r.sourceConfig.DBName, tableName)
		if err := tableInfo.SetInfo(ctx); err != nil {
			return nil, err
		}
		tables = append(tables, tableInfo)
	}
	return tables, rows.Err()
}

// checkTargetEmpty checks that all target databases are empty.
func (r *Runner) checkTargetEmpty(ctx context.Context) error {
	for i, target := range r.targets {
		rows, err := target.DB.QueryContext(ctx, "SHOW TABLES")
		if err != nil {
			return fmt.Errorf("failed to check target %d: %w", i, err)
		}
		defer rows.Close()
		if rows.Next() {
			return fmt.Errorf("target database %d (%s) is not empty", i, target.Config.DBName)
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}

// createTargetTables creates tables on all targets.
func (r *Runner) createTargetTables(ctx context.Context) error {
	for _, t := range r.sourceTables {
		var createStmt string
		row := r.source.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", t.TableName))
		var tbl string
		if err := row.Scan(&tbl, &createStmt); err != nil {
			return err
		}
		// Execute the create statement on all targets.
		for i, target := range r.targets {
			if _, err := target.DB.ExecContext(ctx, createStmt); err != nil {
				return fmt.Errorf("failed to create table on target %d: %w", i, err)
			}
		}
	}
	return nil
}

// createApplier creates the appropriate applier based on the number of targets.
func (r *Runner) createApplier(ctx context.Context) (applier.Applier, error) {
	if len(r.targets) == 1 {
		// Single target - use SingleTargetApplier
		appl := applier.NewSingleTargetApplier(r.targets[0].DB, r.dbConfig, r.logger)
		if err := appl.Start(ctx); err != nil {
			return nil, err
		}
		r.logger.Info("Created SingleTargetApplier")
		return appl, nil
	}

	// Multiple targets - use ShardedApplier
	r.logger.Info("Creating ShardedApplier", "targetCount", len(r.targets))

	// Create the ShardedApplier
	appl, err := applier.NewShardedApplier(
		r.targets,
		r.dbConfig,
		r.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ShardedApplier: %w", err)
	}

	// Start the applier
	if err := appl.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start ShardedApplier: %w", err)
	}

	r.logger.Info("ShardedApplier created and started successfully")
	return appl, nil
}

func (r *Runner) resumeFromCheckpoint(ctx context.Context) error {
	copyChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	var err error

	// For each table and each target, create a chunker and add a subscription
	// The src and dest are the same because this refers to the table structure
	// which is unchanged in move operations.
	for _, src := range r.sourceTables {
		copyChunker, err := table.NewChunker(src, src, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(src, src, copyChunker); err != nil {
			return err
		}

		checksumChunker, err := table.NewChunker(src, src, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}

		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}

	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Create applier
	appl, err := r.createApplier(ctx)
	if err != nil {
		return err
	}

	// Create copier with applier
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.move.Threads,
		TargetChunkTime:               r.move.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		Applier:                       appl, // Pass applier instead of WriteDB
	})
	if err != nil {
		return err
	}

	// Read checkpoint from SOURCE (not target)
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		r.sourceConfig.DBName, checkpointTableName)
	var copierWatermark, binlogName, statement string
	var id, binlogPos int
	err = r.source.QueryRow(query).Scan(&id, &copierWatermark, &r.checksumWatermark, &binlogName, &binlogPos, &statement)
	if err != nil {
		return fmt.Errorf("could not read from checkpoint table '%s' on source: %v", checkpointTableName, err)
	}

	r.replClient.SetFlushedPos(gomysql.Position{
		Name: binlogName,
		Pos:  uint32(binlogPos),
	})

	if err := r.copyChunker.OpenAtWatermark(copierWatermark); err != nil {
		return err
	}

	if err := r.replClient.Run(ctx); err != nil {
		return err
	}

	r.checkpointTable = table.NewTableInfo(r.source, r.sourceConfig.DBName, checkpointTableName)
	r.usedResumeFromCheckpoint = true
	return nil
}

func (r *Runner) setup(ctx context.Context) error {
	var err error
	r.logger.Info("Fetching source table list")
	if r.sourceTables, err = r.getTables(ctx, r.source); err != nil {
		return err
	}
	r.logger.Info("Setting up repl client")
	// Create applier for repl client
	applier, err := r.createApplier(ctx)
	if err != nil {
		return err
	}
	r.replClient = repl.NewClient(r.source, r.sourceConfig.Addr, r.sourceConfig.User, r.sourceConfig.Passwd, &repl.ClientConfig{
		Logger:                     r.logger,
		Concurrency:                r.move.Threads,
		TargetBatchTime:            r.move.TargetChunkTime,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		Applier:                    applier,
		DBConfig:                   r.dbConfig,
	})

	r.logger.Info("Checking target database state")

	if err := r.checkTargetEmpty(ctx); err != nil {
		if err := r.resumeFromCheckpoint(ctx); err != nil {
			return fmt.Errorf("target database is not empty and could not resume from checkpoint: %v", err)
		}
		r.logger.Info("Resumed move from existing checkpoint")
	} else {
		return r.newCopy(ctx)
	}
	return nil
}

func (r *Runner) newCopy(ctx context.Context) error {
	// Create tables on all targets
	if err := r.createTargetTables(ctx); err != nil {
		return err
	}

	// Create sentinel on SOURCE
	if r.move.CreateSentinel {
		if err := r.createSentinelTable(ctx); err != nil {
			return err
		}
	}

	// Create checkpoint on SOURCE
	if err := r.createCheckpointTable(ctx); err != nil {
		return err
	}

	copyChunkers := make([]table.Chunker, 0, len(r.sourceTables)*len(r.targets))
	checksumChunkers := make([]table.Chunker, 0, len(r.sourceTables)*len(r.targets))

	// For each table and each target, create a chunker and add a subscription
	// The src and dest are the same because this refers to the table structure
	// which is unchanged in move operations.
	for _, src := range r.sourceTables {
		copyChunker, err := table.NewChunker(src, src, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(src, src, copyChunker); err != nil {
			return err
		}

		checksumChunker, err := table.NewChunker(src, src, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}

		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)

	}

	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Create applier
	appl, err := r.createApplier(ctx)
	if err != nil {
		return err
	}

	// Create copier with applier
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.move.Threads,
		TargetChunkTime:               r.move.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		Applier:                       appl, // Pass applier instead of WriteDB
	})
	if err != nil {
		return err
	}

	if err := r.copyChunker.Open(); err != nil {
		return err
	}

	if err := r.replClient.Run(ctx); err != nil {
		return err
	}

	return nil
}

// createCheckpointTable creates checkpoint table on SOURCE (not target).
func (r *Runner) createCheckpointTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.source, "DROP TABLE IF EXISTS %n.%n", r.sourceConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.source, `CREATE TABLE %n.%n (
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_name VARCHAR(255),
	binlog_pos INT,
	statement TEXT
	)`,
		r.sourceConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	r.checkpointTable = table.NewTableInfo(r.source, r.sourceConfig.DBName, checkpointTableName)
	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()
	r.startTime = time.Now()
	r.logger.Info("Starting table move")

	var err error
	r.dbConfig = dbconn.NewDBConfig()
	r.dbConfig.ForceKill = true
	r.logger.Warn("the move command is experimental and not yet safe for production use.")

	r.source, err = dbconn.New(r.move.SourceDSN, r.dbConfig)
	if err != nil {
		return err
	}
	defer r.source.Close()

	// Parse source config
	r.sourceConfig, err = mysql.ParseDSN(r.move.SourceDSN)
	if err != nil {
		return err
	}

	if r.move.TargetDSN != "" && len(r.move.Targets) > 0 {
		return errors.New("cannot use both TargetDSN and TargetDSNs; please use only TargetDSNs for multiple targets")
	}

	if r.move.TargetDSN != "" {
		// Convert the target DSN to a target.
		targetCfg, err := mysql.ParseDSN(r.move.TargetDSN)
		if err != nil {
			return fmt.Errorf("failed to parse target DSN: %w", err)
		}
		targetDB, err := dbconn.New(r.move.TargetDSN, r.dbConfig)
		if err != nil {
			return fmt.Errorf("failed to connect to target DSN: %w", err)
		}
		r.targets = []applier.Target{
			{
				DB:       targetDB,
				Config:   targetCfg,
				KeyRange: "0",
			},
		}
	}

	if err := r.setup(ctx); err != nil {
		return err
	}

	// Take a single metadata lock for all tables to prevent concurrent DDL.
	// This uses a single DB connection instead of one per table.
	// We release the lock when this function finishes executing.
	lock, err := dbconn.NewMetadataLock(ctx, r.move.SourceDSN, r.sourceTables, r.dbConfig, r.logger)
	if err != nil {
		return err
	}

	// Release the lock
	defer func() {
		if err := lock.Close(); err != nil {
			r.logger.Error("failed to release metadata lock", "error", err)
		}
	}()

	// Start background monitoring routines
	r.startBackgroundRoutines(ctx)

	r.replClient.SetWatermarkOptimization(true)

	// Run the copier.
	r.status.Set(status.CopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}

	// Disable both watermark optimizations so that all changes can be flushed.
	// The watermark optimizations can prevent some keys from being flushed,
	// which would cause flushedPos to not advance, leading to a mismatch
	// with bufferedPos and causing AllChangesFlushed() to return false.
	r.replClient.SetWatermarkOptimization(false)

	// When the copier has finished, catch up the replication client
	// This is in a non-blocking way first.
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	r.logger.Info("All tables copied successfully.")

	r.sentinelWaitStartTime = time.Now()
	r.status.Set(status.WaitingOnSentinelTable)
	if err := r.waitOnSentinelTable(ctx); err != nil {
		return err
	}

	// Perform a checksum operation + ANALYZE TABLEs
	// To make sure they are all in a ready state.
	if err := r.prepareForCutover(ctx); err != nil {
		return err
	}

	r.logger.Info("Preparation for cutover completed successfully, starting cutover")

	r.status.Set(status.CutOver)
	cutover, err := NewCutOver(r.source, r.sourceTables, r.cutoverFunc, r.replClient, r.dbConfig, r.logger)
	if err != nil {
		return err
	}
	if err = cutover.Run(ctx); err != nil {
		return err
	}

	// Delete checkpoint table from SOURCE
	if err := dbconn.Exec(ctx, r.source, "DROP TABLE IF EXISTS %n.%n", r.sourceConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	r.logger.Info("Move operation complete.")
	return nil
}

// startBackgroundRoutines starts the background routines needed for monitoring.
// This includes table statistics updates, periodic binlog flushing, and DDL change notifications.
func (r *Runner) startBackgroundRoutines(ctx context.Context) {
	// Start routines in table and replication packages to
	// Continuously update the min/max and estimated rows
	// and to flush the binary log position periodically.
	// These will both be stopped when the copier finishes
	// and checksum starts, although the PeriodicFlush
	// will be restarted again after.
	for _, tbl := range r.sourceTables {
		go tbl.AutoUpdateStatistics(ctx, tableStatUpdateInterval, r.logger)
	}
	go r.replClient.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	status.WatchTask(ctx, r, r.logger)
}

func (r *Runner) Status() string {
	state := r.status.Get()
	if state > status.CutOver {
		return ""
	}
	switch state {
	case status.CopyRows:
		return fmt.Sprintf("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v",
			r.status.Get().String(),
			r.copier.GetProgress(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.copier.StartTime()).Round(time.Second),
			r.copier.GetETA(),
			r.copier.GetThrottler().IsThrottled(),
		)
	case status.WaitingOnSentinelTable:
		return fmt.Sprintf("migration status: state=%s total-time=%s sentinel-wait-time=%s sentinel-max-wait-time=%s",
			r.status.Get().String(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.sentinelWaitStartTime).Round(time.Second),
			sentinelWaitLimit,
		)
	case status.ApplyChangeset, status.PostChecksum:
		return fmt.Sprintf("migration status: state=%s binlog-deltas=%v total-time=%s",
			r.status.Get().String(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
		)
	case status.Checksum:
		return fmt.Sprintf("migration status: state=%s checksum-progress=%s binlog-deltas=%v total-time=%s checksum-time=%s",
			r.status.Get().String(),
			r.checker.GetProgress(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.checker.StartTime()).Round(time.Second),
		)
	default:
		return ""
	}
}

func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

func (r *Runner) prepareForCutover(ctx context.Context) error {
	r.replClient.StopPeriodicFlush()
	r.status.Set(status.ApplyChangeset)
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	// Run ANALYZE TABLE on all targets
	r.status.Set(status.AnalyzeTable)
	r.logger.Info("Running ANALYZE TABLE on all targets")
	for i, target := range r.targets {
		for _, tbl := range r.sourceTables {
			if err := dbconn.Exec(ctx, target.DB, "ANALYZE TABLE %n", tbl.TableName); err != nil {
				return fmt.Errorf("failed to analyze table on target %d: %w", i, err)
			}
		}
	}

	// TODO: Checksum needs to be refactored to work with multiple targets and appliers
	// For now, we skip the checksum phase
	r.logger.Warn("Checksum is currently disabled and needs refactoring for multiple targets")

	/*
		if err := r.checksumChunker.Open(); err != nil {
			return err
		}
		defer r.checksumChunker.Close()

		var err error
		r.checker, err = checksum.NewChecker(r.source, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
			Concurrency:     r.move.Threads,
			TargetChunkTime: r.move.TargetChunkTime,
			DBConfig:        r.dbConfig,
			Logger:          r.logger,
			WriteDB:         r.targets[0].DB, // This needs to be refactored to use applier
			FixDifferences:  true,
		})
		if err != nil {
			return err
		}
		r.status.Set(status.Checksum)
		return r.checker.Run(ctx)
	*/

	return nil
}

func (r *Runner) SetCutover(cutover func(ctx context.Context) error) {
	r.cutoverFunc = cutover
}

func (r *Runner) Progress() status.Progress {
	var summary string
	switch r.status.Get() {
	case status.CopyRows:
		summary = fmt.Sprintf("%v %s ETA %v",
			r.copier.GetProgress(),
			r.status.Get().String(),
			r.copier.GetETA(),
		)
	case status.WaitingOnSentinelTable:
		r.logger.Info("migration status",
			"state", r.status.Get().String(),
			"sentinel-table", fmt.Sprintf("%s.%s", r.sourceConfig.DBName, sentinelTableName),
			"total-time", time.Since(r.startTime).Round(time.Second),
			"sentinel-wait-time", time.Since(r.sentinelWaitStartTime).Round(time.Second),
			"sentinel-max-wait-time", sentinelWaitLimit,
		)
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.replClient.GetDeltaLen())
	case status.Checksum:
		summary = "Checksum Progress=" + r.checker.GetProgress()
	}
	return status.Progress{
		CurrentState: r.status.Get(),
		Summary:      summary,
	}
}

// createSentinelTable creates sentinel table on SOURCE (not target).
func (r *Runner) createSentinelTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.source, "DROP TABLE IF EXISTS %n.%n", r.sourceConfig.DBName, sentinelTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.source, "CREATE TABLE %n.%n (id int NOT NULL PRIMARY KEY)", r.sourceConfig.DBName, sentinelTableName); err != nil {
		return err
	}
	return nil
}

// sentinelTableExists checks if sentinel table exists on SOURCE (not target).
func (r *Runner) sentinelTableExists(ctx context.Context) (bool, error) {
	sql := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var sentinelTableExists int
	err := r.source.QueryRowContext(ctx, sql, r.sourceConfig.DBName, sentinelTableName).Scan(&sentinelTableExists)
	if err != nil {
		return false, err
	}
	return sentinelTableExists > 0, nil
}

func (r *Runner) waitOnSentinelTable(ctx context.Context) error {
	if sentinelExists, err := r.sentinelTableExists(ctx); err != nil {
		return err
	} else if !sentinelExists {
		return nil
	}

	r.logger.Warn("cutover deferred while sentinel table exists; will wait",
		"sentinel-table", sentinelTableName,
		"wait-limit", sentinelWaitLimit)

	timer := time.NewTimer(sentinelWaitLimit)
	defer timer.Stop()

	ticker := time.NewTicker(sentinelCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			sentinelExists, err := r.sentinelTableExists(ctx)
			if err != nil {
				return err
			}
			if !sentinelExists {
				r.logger.Info("sentinel table dropped", "time", t)
				return nil
			}
		case <-timer.C:
			return errors.New("timed out waiting for sentinel table to be dropped")
		}
	}
}

// DumpCheckpoint writes checkpoint to SOURCE (not target).
func (r *Runner) DumpCheckpoint(ctx context.Context) error {
	binlog := r.replClient.GetBinlogApplyPosition()
	copierWatermark, err := r.copyChunker.GetLowWatermark()
	if err != nil {
		return status.ErrWatermarkNotReady
	}

	var checksumWatermark string
	if r.status.Get() >= status.Checksum {
		if r.checker != nil {
			checksumWatermark, err = r.checksumChunker.GetLowWatermark()
			if err != nil {
				return status.ErrWatermarkNotReady
			}
		}
	}

	r.logger.Info("checkpoint",
		"low-watermark", copierWatermark,
		"log-file", binlog.Name,
		"log-pos", binlog.Pos)

	err = dbconn.Exec(ctx, r.source, "INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement) VALUES (%?, %?, %?, %?, %?)",
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		copierWatermark,
		checksumWatermark,
		binlog.Name,
		binlog.Pos,
		"",
	)
	if err != nil {
		return status.ErrCouldNotWriteCheckpoint
	}
	return nil
}

func (r *Runner) Cancel() {
	r.cancelFunc()
}
