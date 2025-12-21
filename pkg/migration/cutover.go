package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
)

type CutOver struct {
	db       *sql.DB
	feed     *repl.Client
	config   []*cutoverConfig
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger
}

type cutoverConfig struct {
	table          *table.TableInfo
	newTable       *table.TableInfo
	oldTableName   string
	useTestCutover bool
}

// NewCutOver contains the logic to perform the final cut over. It can cutover multiple tables
// at once based on config. A replication feed which is used to ensure consistency before the cut over.
func NewCutOver(db *sql.DB, config []*cutoverConfig, feed *repl.Client, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*CutOver, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	// validate the cutoverConfig
	for _, cfg := range config {
		if cfg.table == nil || cfg.newTable == nil {
			return nil, errors.New("table and newTable must be non-nil")
		}
		if cfg.oldTableName == "" {
			return nil, errors.New("oldTableName must be non-empty")
		}
	}
	return &CutOver{
		db:       db,
		config:   config,
		feed:     feed,
		dbConfig: dbConfig,
		logger:   logger,
	}, nil
}

func (c *CutOver) Run(ctx context.Context) error {
	var err error
	if c.dbConfig.MaxOpenConnections < 5 {
		// The gh-ost cutover algorithm requires a minimum of 3 connections:
		// - The LOCK TABLES connection
		// - The RENAME TABLE connection
		// - The Flush() threads
		// Because we want to safely flush quickly, we set the limit to 5.
		c.db.SetMaxOpenConns(5)
	}
	for i := range c.dbConfig.MaxRetries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Try and catch up before we attempt the cutover.
		// since we will need to catch up again with the lock held
		// and we want to minimize that.
		if err := c.feed.Flush(ctx); err != nil {
			return err
		}
		// We use maxCutoverRetries as our retrycount, but nested
		// within c.algorithmX() it may also have a retry for the specific statement
		c.logger.Warn("Attempting final cut over operation",
			"attempt", i+1,
			"max_retries", c.dbConfig.MaxRetries,
		)
		// if specified in c.config[0], we will use the test cutover for failure injection.
		// we don't need to exhaustively check all configs.
		if len(c.config) > 0 && c.config[0].useTestCutover {
			err = c.partialRenameForTest(ctx)
		} else {
			err = c.algorithmRenameUnderLock(ctx)
		}
		if err != nil {
			c.logger.Warn("cutover failed",
				"error", err.Error(),
			)
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return err
}

// algorithmRenameUnderLock is the preferred cutover algorithm.
// As of MySQL 8.0.13, you can rename tables locked with a LOCK TABLES statement
// https://dev.mysql.com/worklog/task/?id=9826
func (c *CutOver) algorithmRenameUnderLock(ctx context.Context) error {
	tablesToLock := []*table.TableInfo{}
	renameFragments := []string{}
	for _, cfg := range c.config {
		tablesToLock = append(tablesToLock, cfg.table, cfg.newTable)
		oldQuotedName := fmt.Sprintf("`%s`.`%s`", cfg.table.SchemaName, cfg.oldTableName)
		renameFragments = append(renameFragments,
			fmt.Sprintf("%s TO %s", cfg.table.QuotedName, oldQuotedName),
			fmt.Sprintf("%s TO %s", cfg.newTable.QuotedName, cfg.table.QuotedName),
		)
	}
	return c.executeRenameUnderLock(ctx, tablesToLock, renameFragments)
}

// executeRenameUnderLock is the shared implementation for performing renames under a table lock.
// It handles locking, binlog flushing, and executing the rename statement.
func (c *CutOver) executeRenameUnderLock(ctx context.Context, tablesToLock []*table.TableInfo, renameFragments []string) error {
	tableLock, err := dbconn.NewTableLock(ctx, c.db, tablesToLock, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer tableLock.Close()
	if err := c.feed.FlushUnderTableLock(ctx, tableLock); err != nil {
		return err
	}
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed, final flush might be broken")
	}

	renameStatement := "RENAME TABLE " + strings.Join(renameFragments, ", ")
	return tableLock.ExecUnderLock(ctx, renameStatement)
}

// partialRenameForTest performs a partial cutover (only renames original table to _old)
// This is intended for testing the atomicity/consistency of the cutover.
func (c *CutOver) partialRenameForTest(ctx context.Context) error {
	tablesToLock := []*table.TableInfo{}
	renameFragments := []string{}

	// Build the same locks and rename fragments as the normal algorithm,
	// but only include the first rename (original -> _old)
	for _, cfg := range c.config {
		tablesToLock = append(tablesToLock, cfg.table, cfg.newTable)
		oldQuotedName := fmt.Sprintf("`%s`.`%s`", cfg.table.SchemaName, cfg.oldTableName)
		// Only add the first rename: original table -> _old
		// Intentionally skip the second rename: _new -> original
		renameFragments = append(renameFragments,
			fmt.Sprintf("%s TO %s", cfg.table.QuotedName, oldQuotedName),
		)
	}
	// Execute the partial rename using the same code path
	if err := c.executeRenameUnderLock(ctx, tablesToLock, renameFragments); err != nil {
		return err
	}
	// Intentionally return an error to simulate a partial cutover failure
	return errors.New("intentional partial cutover failure for testing")
}
