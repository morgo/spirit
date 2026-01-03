package sync

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/projection"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/typeconv"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
)

// Runner orchestrates the sync operation
type Runner struct {
	config *Config

	// Source
	source       *sql.DB
	sourceConfig *mysql.Config
	dbConfig     *dbconn.DBConfig

	// Target
	applier    applier.Applier
	targetType TargetType

	// Projections
	projections []*projection.Projection

	// Replication
	replClient *repl.Client

	// Copy
	copier      copier.Copier
	copyChunker table.Chunker

	// Checksum
	checker         checksum.Checker
	checksumChunker table.Chunker
	checksumState   *ChecksumState

	// State
	status          SyncState
	checkpointTable *table.TableInfo

	// Lifecycle
	ctx        context.Context
	cancelFunc context.CancelFunc
	logger     *slog.Logger
}

// SyncState represents the current state of the sync operation
type SyncState int

const (
	SyncStateInitializing SyncState = iota
	SyncStateInitialCopy
	SyncStateContinualSync
	SyncStateChecksumming
	SyncStateError
)

// String returns the string representation of the sync state
func (s SyncState) String() string {
	switch s {
	case SyncStateInitializing:
		return "Initializing"
	case SyncStateInitialCopy:
		return "InitialCopy"
	case SyncStateContinualSync:
		return "ContinualSync"
	case SyncStateChecksumming:
		return "Checksumming"
	case SyncStateError:
		return "Error"
	default:
		return "Unknown"
	}
}

// NewRunner creates a new sync runner
func NewRunner(config *Config) (*Runner, error) {
	// Parse projections
	projections := make([]*projection.Projection, len(config.Sync.Projections))
	for i, projConfig := range config.Sync.Projections {
		// Convert ProjectionConfig to projection.Config
		proj, err := projection.NewProjection(projection.Config{
			Name:         projConfig.Name,
			SourceTable:  projConfig.SourceTable,
			TargetTable:  projConfig.TargetTable,
			Query:        projConfig.Query,
			PrimaryKey:   projConfig.PrimaryKey,
			TypeMappings: projConfig.TypeMappings,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create projection %s: %w", projConfig.Name, err)
		}
		projections[i] = proj
	}

	return &Runner{
		config:        config,
		projections:   projections,
		logger:        slog.Default(),
		checksumState: &ChecksumState{},
	}, nil
}

// SetLogger sets the logger for the runner
func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// Run starts the sync operation
func (r *Runner) Run(ctx context.Context) error {
	// Setup signal handling for graceful shutdown
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigChan
		r.logger.Info("received signal, initiating graceful shutdown", "signal", sig)
		r.cancelFunc()
	}()

	r.logger.Info("starting spirit sync",
		"name", r.config.Sync.Name,
		"source", maskDSN(r.config.Sync.Source.DSN),
		"target_type", r.config.Sync.Target.Type,
	)

	// Phase 1: Initialize (connects to source/target, validates config)
	r.status = SyncStateInitializing
	if err := r.initialize(ctx); err != nil {
		return fmt.Errorf("initialization failed: %w", err)
	}

	// Phase 2: Check for existing checkpoint and resume if possible
	resumed, err := r.tryResumeFromCheckpoint(ctx)
	if err != nil {
		r.logger.Warn("could not resume from checkpoint, starting fresh", "error", err)
		resumed = false
	}

	if !resumed {
		// Phase 3a: Initial Copy (first time only)
		r.status = SyncStateInitialCopy
		if err := r.initialCopy(ctx); err != nil {
			return fmt.Errorf("initial copy failed: %w", err)
		}
		r.logger.Info("initial copy complete")
	} else {
		r.logger.Info("resumed from checkpoint")
	}

	// Phase 3b: Continual Sync (runs forever until killed)
	r.status = SyncStateContinualSync
	r.logger.Info("starting continual sync loop")

	// Start background goroutines
	errChan := make(chan error, 4)
	go r.continualSyncLoop(ctx, errChan)
	go r.checksumLoop(ctx, errChan)
	// ddlMonitorLoop is started in createReplClient to avoid race conditions
	go r.checkpointLoop(ctx, errChan)

	// Wait for error or cancellation
	select {
	case err := <-errChan:
		r.logger.Error("sync failed", "error", err)
		r.status = SyncStateError
		return err
	case <-ctx.Done():
		r.logger.Info("shutting down gracefully")
		return r.shutdown(ctx)
	}
}

// initialize connects to source and target databases and validates configuration
func (r *Runner) initialize(ctx context.Context) error {
	var err error

	// Parse source DSN
	r.sourceConfig, err = mysql.ParseDSN(r.config.Sync.Source.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse source DSN: %w", err)
	}

	// Create DB config
	r.dbConfig = dbconn.NewDBConfig()
	r.dbConfig.MaxOpenConnections = r.config.Sync.Performance.Threads + 1

	// Connect to source
	r.source, err = dbconn.New(r.config.Sync.Source.DSN, r.dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

	r.logger.Info("connected to source database", "database", r.sourceConfig.DBName)

	// Parse target type
	r.targetType = ParseTargetType(r.config.Sync.Target.Type)
	if r.targetType == TargetTypeUnknown {
		return fmt.Errorf("unsupported target type: %s", r.config.Sync.Target.Type)
	}

	// Create applier based on target type
	if err := r.createApplier(ctx); err != nil {
		return fmt.Errorf("failed to create applier: %w", err)
	}

	// Create checkpoint table
	r.checkpointTable = table.NewTableInfo(r.source, r.sourceConfig.DBName, r.config.Sync.Checkpoint.Table)
	if err := CreateCheckpointTable(ctx, r.source, r.sourceConfig.DBName, r.config.Sync.Checkpoint.Table); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	// Create target tables if they don't exist
	r.logger.Info("ensuring target tables exist")
	if err := r.createTargetTables(ctx); err != nil {
		return fmt.Errorf("failed to create target tables: %w", err)
	}

	r.logger.Info("initialization complete")
	return nil
}

// createTargetTables creates tables on the target database for each projection
func (r *Runner) createTargetTables(ctx context.Context) error {
	// Get target DB connection from applier
	targets := r.applier.GetTargets()
	if len(targets) == 0 || len(targets) > 1 {
		return fmt.Errorf("unexpected number of targets")
	}
	targetDB := targets[0].DB

	// Create table creator
	typeMapper := typeconv.GetTypeMapper(toTypeconvTargetType(r.targetType))
	projTargetType := toProjectionTargetType(r.targetType)
	creator := projection.NewTableCreator(r.source, targetDB, projTargetType, typeMapper)

	// Create tables for each projection
	for _, proj := range r.projections {
		r.logger.Info("creating target table if not exists",
			"source_table", proj.SourceTable,
			"target_table", proj.TargetTable,
		)

		// Get source table info
		sourceTable := table.NewTableInfo(r.source, r.sourceConfig.DBName, proj.SourceTable)
		if err := sourceTable.SetInfo(ctx); err != nil {
			return fmt.Errorf("failed to get source table info for %s: %w", proj.SourceTable, err)
		}

		// Create target table
		if err := creator.CreateTableFromProjection(ctx, proj, sourceTable); err != nil {
			return fmt.Errorf("failed to create target table %s: %w", proj.TargetTable, err)
		}

		r.logger.Info("target table ready", "table", proj.TargetTable)
	}

	return nil
}

// createApplier creates the appropriate applier based on target type
func (r *Runner) createApplier(_ context.Context) error {
	switch r.targetType {
	case TargetTypePostgreSQL:
		// Connect to PostgreSQL
		pgDB, err := sql.Open("postgres", r.config.Sync.Target.DSN)
		if err != nil {
			return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
		}

		// Parse PostgreSQL DSN for config
		// For now, just use the DB connection
		target := applier.Target{
			DB:       pgDB,
			KeyRange: "0", // Single target (not sharded)
		}

		// Create type mapper
		typeMapper := typeconv.GetTypeMapper(toTypeconvTargetType(r.targetType))

		// Create PostgreSQL applier
		r.applier = applier.NewPostgresApplier(target, r.dbConfig, r.logger, typeMapper)
		r.logger.Info("created PostgreSQL applier")
		return nil
	default:
		return fmt.Errorf("unsupported target type: %s", r.targetType)
	}
}

// tryResumeFromCheckpoint attempts to resume from an existing checkpoint
func (r *Runner) tryResumeFromCheckpoint(ctx context.Context) (bool, error) {
	// Check if checkpoint exists
	exists, err := CheckpointExists(ctx, r.source, r.sourceConfig.DBName, r.config.Sync.Checkpoint.Table, r.config.Sync.Name)
	if err != nil {
		return false, fmt.Errorf("failed to check checkpoint: %w", err)
	}

	if !exists {
		r.logger.Info("no checkpoint found, starting fresh")
		return false, nil
	}

	// Load checkpoint
	checkpoint, err := LoadCheckpoint(ctx, r.source, r.sourceConfig.DBName, r.config.Sync.Checkpoint.Table, r.config.Sync.Name)
	if err != nil {
		return false, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	r.logger.Info("found checkpoint, resuming",
		"binlog_name", checkpoint.BinlogName,
		"binlog_pos", checkpoint.BinlogPos,
		"copier_watermark", checkpoint.CopierWatermark,
	)

	// Create chunkers for each projection
	if err := r.initChunkers(ctx); err != nil {
		return false, err
	}

	// Resume copier from watermark
	if checkpoint.CopierWatermark != "" {
		if err := r.copyChunker.OpenAtWatermark(checkpoint.CopierWatermark); err != nil {
			return false, fmt.Errorf("failed to resume copier: %w", err)
		}
	} else {
		// Initial copy was complete, just open normally
		if err := r.copyChunker.Open(); err != nil {
			return false, err
		}
	}

	// Resume checksum from watermark if it exists
	if checkpoint.ChecksumWatermark != "" {
		if err := r.checksumChunker.OpenAtWatermark(checkpoint.ChecksumWatermark); err != nil {
			r.logger.Warn("failed to resume checksum, will restart", "error", err)
			r.checksumChunker.Open()
		}
	} else {
		r.checksumChunker.Open()
	}

	// Create replication client
	if err := r.createReplClient(ctx); err != nil {
		return false, err
	}

	// Set replication position
	r.replClient.SetFlushedPos(gomysql.Position{
		Name: checkpoint.BinlogName,
		Pos:  uint32(checkpoint.BinlogPos),
	})

	// Restore checksum state
	r.checksumState = &ChecksumState{
		LastFullChecksumEnd:    checkpoint.LastChecksumEnd,
		LastFullChecksumStatus: checkpoint.LastChecksumStatus,
		TotalChunksChecked:     checkpoint.TotalChunksChecked,
		ChunksWithDifferences:  checkpoint.ChunksWithDifferences,
	}

	// Start replication client
	if err := r.replClient.Run(ctx); err != nil {
		return false, fmt.Errorf("failed to start replication client: %w", err)
	}

	return true, nil
}

// initialCopy performs the initial copy of data
func (r *Runner) initialCopy(ctx context.Context) error {
	r.logger.Info("starting initial copy")

	// Create chunkers
	if err := r.initChunkers(ctx); err != nil {
		return err
	}

	// Open chunkers
	if err := r.copyChunker.Open(); err != nil {
		return fmt.Errorf("failed to open copy chunker: %w", err)
	}

	if err := r.checksumChunker.Open(); err != nil {
		return fmt.Errorf("failed to open checksum chunker: %w", err)
	}

	// Create replication client
	if err := r.createReplClient(ctx); err != nil {
		return err
	}

	// Start replication client
	if err := r.replClient.Run(ctx); err != nil {
		return fmt.Errorf("failed to start replication client: %w", err)
	}

	// Create copier
	var err error
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.config.Sync.Performance.Threads,
		TargetChunkTime:               r.config.Sync.Performance.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		Applier:                       r.applier,
	})
	if err != nil {
		return fmt.Errorf("failed to create copier: %w", err)
	}

	// Run copier
	if err := r.copier.Run(ctx); err != nil {
		return fmt.Errorf("copier failed: %w", err)
	}

	r.logger.Info("initial copy complete")
	return nil
}

// initChunkers creates chunkers for all projections
func (r *Runner) initChunkers(ctx context.Context) error {
	var copyChunkers []table.Chunker
	var checksumChunkers []table.Chunker

	for _, proj := range r.projections {
		// Get source table info
		sourceTable := table.NewTableInfo(r.source, r.sourceConfig.DBName, proj.SourceTable)
		if err := sourceTable.SetInfo(ctx); err != nil {
			return fmt.Errorf("failed to get source table info for %s: %w", proj.SourceTable, err)
		}

		// Create chunkers (target table is nil for now - we'll handle this differently for cross-DB)
		copyChunker, err := table.NewChunker(sourceTable, nil, r.config.Sync.Performance.TargetChunkTime, r.logger)
		if err != nil {
			return fmt.Errorf("failed to create copy chunker for %s: %w", proj.SourceTable, err)
		}

		checksumChunker, err := table.NewChunker(sourceTable, nil, r.config.Sync.Performance.TargetChunkTime, r.logger)
		if err != nil {
			return fmt.Errorf("failed to create checksum chunker for %s: %w", proj.SourceTable, err)
		}

		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}

	// Wrap in multi-chunker
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	return nil
}

// createReplClient creates the replication client
func (r *Runner) createReplClient(ctx context.Context) error {
	// Create DDL notification channel before creating the replication client
	// This prevents a race condition where DDL events arrive before the channel is set
	ddlChan := make(chan string, 10)

	r.replClient = repl.NewClient(r.source, r.sourceConfig.Addr, r.sourceConfig.User, r.sourceConfig.Passwd, &repl.ClientConfig{
		Logger:                     r.logger,
		Concurrency:                r.config.Sync.Performance.Threads,
		TargetBatchTime:            r.config.Sync.Performance.TargetChunkTime,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		Applier:                    r.applier,
		DBConfig:                   r.dbConfig,
		OnDDL:                      ddlChan, // Set DDL channel during client creation
	})

	// Add subscriptions for each projection
	for _, proj := range r.projections {
		sourceTable := table.NewTableInfo(r.source, r.sourceConfig.DBName, proj.SourceTable)
		if err := sourceTable.SetInfo(ctx); err != nil {
			return fmt.Errorf("failed to get source table info for %s: %w", proj.SourceTable, err)
		}

		if err := r.replClient.AddSubscription(sourceTable, nil, r.copyChunker); err != nil {
			return fmt.Errorf("failed to add subscription for %s: %w", proj.SourceTable, err)
		}
	}

	// Start the DDL monitor loop now that the channel is set up
	go r.ddlMonitorLoop(ctx, make(chan error, 1), ddlChan)

	return nil
}

// shutdown performs graceful shutdown
func (r *Runner) shutdown(ctx context.Context) error {
	r.logger.Info("saving final checkpoint before shutdown")

	// Give ourselves 30 seconds to save checkpoint
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Save final checkpoint
	if err := r.saveCheckpoint(shutdownCtx); err != nil {
		r.logger.Error("failed to save final checkpoint", "error", err)
		return err
	}

	// Stop replication client
	if r.replClient != nil {
		r.replClient.Close()
	}

	// Close database connections
	if r.source != nil {
		r.source.Close()
	}

	r.logger.Info("graceful shutdown complete")
	return nil
}

// saveCheckpoint saves the current checkpoint state
func (r *Runner) saveCheckpoint(ctx context.Context) error {
	if r.replClient == nil {
		return nil
	}

	// Get current state
	binlog := r.replClient.GetBinlogApplyPosition()

	copierWatermark, err := r.copyChunker.GetLowWatermark()
	if err != nil {
		// Not ready yet, skip this checkpoint
		return nil
	}

	var checksumWatermark string
	if r.status >= SyncStateContinualSync {
		checksumWatermark, _ = r.checksumChunker.GetLowWatermark()
	}

	// Get row counts
	rowsSynced, _, _ := r.copyChunker.Progress()

	// Create checkpoint
	checkpoint := &Checkpoint{
		SyncName:              r.config.Sync.Name,
		ProjectionName:        r.projections[0].Name, // TODO: Handle multiple projections
		CopierWatermark:       copierWatermark,
		ChecksumWatermark:     checksumWatermark,
		BinlogName:            binlog.Name,
		BinlogPos:             int(binlog.Pos),
		LastChecksumEnd:       r.checksumState.LastFullChecksumEnd,
		LastChecksumStatus:    r.checksumState.LastFullChecksumStatus,
		TotalRowsSynced:       int64(rowsSynced),
		TotalChunksChecked:    r.checksumState.TotalChunksChecked,
		ChunksWithDifferences: r.checksumState.ChunksWithDifferences,
	}

	if err := SaveCheckpoint(ctx, r.source, r.sourceConfig.DBName, r.config.Sync.Checkpoint.Table, checkpoint); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	r.logger.Debug("checkpoint saved",
		"binlog_name", binlog.Name,
		"binlog_pos", binlog.Pos,
		"rows_synced", rowsSynced,
	)

	return nil
}

// checkpointLoop periodically saves checkpoints
func (r *Runner) checkpointLoop(ctx context.Context, errChan chan<- error) {
	ticker := time.NewTicker(r.config.Sync.Checkpoint.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.saveCheckpoint(ctx); err != nil {
				r.logger.Error("failed to save checkpoint", "error", err)
				// Don't send to errChan - checkpoint failures shouldn't stop sync
			}
		}
	}
}

// continualSyncLoop monitors replication health
func (r *Runner) continualSyncLoop(ctx context.Context, errChan chan<- error) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.replClient == nil {
				continue
			}

			// Log replication status
			r.logger.Debug("sync health check",
				"binlog_lag", r.replClient.GetDeltaLen(),
				"state", r.status,
			)
		}
	}
}

// checksumLoop runs checksums periodically
func (r *Runner) checksumLoop(ctx context.Context, errChan chan<- error) {
	// Wait for initial copy to complete
	for r.status < SyncStateContinualSync {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			continue
		}
	}

	// Wait a bit before starting first checksum
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	// Continually run checksums
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !r.config.Sync.Checksum.Enabled {
			r.logger.Info("checksum disabled, skipping")
			time.Sleep(r.config.Sync.Checksum.Interval)
			continue
		}

		r.logger.Info("starting checksum pass")
		r.status = SyncStateChecksumming

		// Reset chunker to start from beginning
		if err := r.checksumChunker.Reset(); err != nil {
			r.logger.Error("failed to reset checksum chunker", "error", err)
			errChan <- err
			return
		}

		// Create checker based on target type
		var err error
		if r.targetType == TargetTypePostgreSQL {
			r.checker = checksum.NewPostgresTargetChecker(r.source, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
				Concurrency:     r.config.Sync.Performance.Threads,
				TargetChunkTime: r.config.Sync.Performance.TargetChunkTime,
				DBConfig:        r.dbConfig,
				Logger:          r.logger,
				FixDifferences:  true,
				MaxRetries:      r.config.Sync.Checksum.MaxRetries,
				Applier:         r.applier,
			})
		} else {
			// Use standard checker for MySQL-to-MySQL
			r.checker, err = checksum.NewChecker(r.source, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
				Concurrency:     r.config.Sync.Performance.Threads,
				TargetChunkTime: r.config.Sync.Performance.TargetChunkTime,
				DBConfig:        r.dbConfig,
				Logger:          r.logger,
				FixDifferences:  true,
				MaxRetries:      r.config.Sync.Checksum.MaxRetries,
				Applier:         r.applier,
			})
			if err != nil {
				r.logger.Error("failed to create checker", "error", err)
				errChan <- err
				return
			}
		}

		// Run checksum
		if err := r.checker.Run(ctx); err != nil {
			if ctx.Err() != nil {
				// Context cancelled (shutdown), not an error
				return
			}
			r.logger.Error("checksum failed", "error", err)
			r.checksumState.LastFullChecksumStatus = "failed"
		} else {
			r.checksumState.LastFullChecksumStatus = "passed"
			now := time.Now()
			r.checksumState.LastFullChecksumEnd = &now
			r.logger.Info("checksum pass complete")
		}

		r.status = SyncStateContinualSync

		// Wait before next checksum pass
		interval := r.config.Sync.Checksum.Interval
		r.logger.Info("waiting before next checksum pass", "interval", interval)

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			continue
		}
	}
}

// ddlMonitorLoop monitors for DDL changes and validates projections
func (r *Runner) ddlMonitorLoop(ctx context.Context, errChan chan<- error, ddlChan <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case tableName := <-ddlChan:
			r.logger.Warn("DDL detected on source table", "table", tableName)

			// Find affected projection
			var affectedProj *projection.Projection
			for _, proj := range r.projections {
				if proj.SourceTable == tableName {
					affectedProj = proj
					break
				}
			}

			if affectedProj == nil {
				continue
			}

			// Validate projection still works
			valid, err := affectedProj.IsStillValid(ctx, r.source)
			if err != nil || !valid {
				r.logger.Error("projection no longer valid after DDL",
					"table", tableName,
					"projection", affectedProj.Name,
					"error", err,
				)
				errChan <- fmt.Errorf("projection %s broken by DDL on %s", affectedProj.Name, tableName)
				return
			}

			r.logger.Info("projection still valid after DDL",
				"table", tableName,
				"projection", affectedProj.Name,
			)
		}
	}
}

// Close cleans up resources
func (r *Runner) Close() error {
	if r.replClient != nil {
		r.replClient.Close()
	}
	if r.source != nil {
		r.source.Close()
	}
	return nil
}

// maskDSN masks the password in a DSN for logging
func maskDSN(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return dsn
	}
	cfg.Passwd = "***"
	return cfg.FormatDSN()
}

// toTypeconvTargetType converts sync.TargetType to typeconv.TargetType
func toTypeconvTargetType(t TargetType) typeconv.TargetType {
	switch t {
	case TargetTypeMySQL:
		return typeconv.TargetTypeMySQL
	case TargetTypePostgreSQL:
		return typeconv.TargetTypePostgreSQL
	default:
		return typeconv.TargetTypeMySQL // default to MySQL
	}
}

// toProjectionTargetType converts sync.TargetType to projection.TargetType
func toProjectionTargetType(t TargetType) projection.TargetType {
	switch t {
	case TargetTypeMySQL:
		return projection.TargetTypeMySQL
	case TargetTypePostgreSQL:
		return projection.TargetTypePostgreSQL
	default:
		return projection.TargetTypeMySQL // default to MySQL
	}
}
