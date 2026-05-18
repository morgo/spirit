package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

const (
	// cutoverInitialBackoff is the starting per-retry delay before
	// re-attempting the rename. It doubles on each subsequent failure up
	// to cutoverMaxBackoff. Without backoff a contended table lock
	// produces a tight retry loop that worsens contention; this gives
	// the source of contention time to clear.
	cutoverInitialBackoff = 100 * time.Millisecond
	cutoverMaxBackoff     = 10 * time.Second
	// cutoverUnlockTimeout caps the duration of the deferred UNLOCK TABLES
	// in executeRenameUnderLock. The unlock runs under a ctx derived via
	// context.WithoutCancel so it still fires when the parent ctx is
	// cancelled mid-cutover; without an explicit timeout the call could
	// block indefinitely on an unhealthy connection.
	cutoverUnlockTimeout = 30 * time.Second
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
	if c.dbConfig.MaxOpenConnections < 5 {
		// The gh-ost cutover algorithm requires a minimum of 3 connections:
		// - The LOCK TABLES connection
		// - The RENAME TABLE connection
		// - The Flush() threads
		// Because we want to safely flush quickly, we set the limit to 5.
		c.db.SetMaxOpenConns(5)
	}
	// Collect every attempt's error and join them on exit, so an operator
	// debugging a flapping cutover sees the full failure history rather
	// than just whatever happened on the last try.
	var attemptErrs []error
	backoff := cutoverInitialBackoff
	for i := range max(1, c.dbConfig.MaxRetries) {
		if ctx.Err() != nil {
			return errors.Join(append(attemptErrs, ctx.Err())...)
		}
		if i > 0 {
			// Exponential backoff between attempts. Without this a
			// contended lock produces a tight retry loop that worsens
			// contention. Sleeping ctx-aware so a cancel during backoff
			// returns promptly instead of waiting out the full delay.
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return errors.Join(append(attemptErrs, ctx.Err())...)
			case <-timer.C:
			}
			backoff *= 2
			if backoff > cutoverMaxBackoff {
				backoff = cutoverMaxBackoff
			}
		}
		// Try and catch up before we attempt the cutover.
		// since we will need to catch up again with the lock held
		// and we want to minimize that.
		if err := c.feed.Flush(ctx); err != nil {
			return errors.Join(append(attemptErrs, err)...)
		}
		// We use maxCutoverRetries as our retrycount, but nested
		// within c.algorithmX() it may also have a retry for the specific statement
		c.logger.Warn("Attempting final cut over operation",
			"attempt", i+1,
			"max_retries", c.dbConfig.MaxRetries,
		)
		// if specified in c.config[0], we will use the test cutover for failure injection.
		// we don't need to exhaustively check all configs.
		var err error
		if len(c.config) > 0 && c.config[0].useTestCutover {
			err = c.partialRenameForTest(ctx)
		} else {
			err = c.algorithmRenameUnderLock(ctx)
		}
		if err != nil {
			attemptErrs = append(attemptErrs, fmt.Errorf("attempt %d: %w", i+1, err))
			c.logger.Warn("cutover failed",
				"error", err.Error(),
				"next_backoff", backoff,
			)
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return errors.Join(attemptErrs...)
}

// algorithmRenameUnderLock is the preferred cutover algorithm.
// As of MySQL 8.0.13, you can rename tables locked with a LOCK TABLES statement
// https://dev.mysql.com/worklog/task/?id=9826
func (c *CutOver) algorithmRenameUnderLock(ctx context.Context) error {
	tablesToLock := []*table.TableInfo{}
	renameFragments := []string{}
	for _, cfg := range c.config {
		tablesToLock = append(tablesToLock, cfg.table, cfg.newTable)
		oldQuotedName := fmt.Sprintf("`%s`", cfg.oldTableName)
		renameFragments = append(renameFragments,
			fmt.Sprintf("%s TO %s", cfg.table.QuotedTableName, oldQuotedName),
			fmt.Sprintf("%s TO %s", cfg.newTable.QuotedTableName, cfg.table.QuotedTableName),
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
	// Run UNLOCK TABLES with a ctx that ignores the parent's cancellation
	// so a ctx cancel mid-cutover still releases the lock. ExecContext on
	// a cancelled ctx returns immediately without sending the statement;
	// the server would eventually release the lock when the connection
	// dies, but giving UNLOCK TABLES a fair chance is cleaner. Bound the
	// detached ctx with cutoverUnlockTimeout so an unhealthy connection
	// can't block the deferred close indefinitely.
	unlockCtx, cancelUnlock := context.WithTimeout(context.WithoutCancel(ctx), cutoverUnlockTimeout)
	defer cancelUnlock()
	defer utils.CloseAndLogWithContext(unlockCtx, tableLock)
	// FlushUnderTableLock itself does flush → BlockWait → flush, so the
	// flush's own binlog events (REPLACE INTO _new / DELETE FROM _new) are
	// already read back inside the BlockWait of the same call. The earlier
	// "second FlushUnderTableLock" workaround was added under the
	// assumption that an extra round-trip would close a position-bookkeeping
	// gap, but the real bug was upstream — events were being silently
	// dropped by KeyAboveHighWatermark in the chunkPtr.IsNil window
	// (issue #746, fixed in chunker_optimistic.go / chunker_composite.go).
	// Now that the source of the divergence is gone, a single
	// FlushUnderTableLock is sufficient.
	if err := c.feed.FlushUnderTableLock(ctx, tableLock); err != nil {
		return err
	}
	if !c.feed.AllChangesFlushed() {
		return fmt.Errorf("%w, final flush might be broken", repl.ErrChangesNotFlushed)
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
		oldQuotedName := fmt.Sprintf("`%s`", cfg.oldTableName)
		// Only add the first rename: original table -> _old
		// Intentionally skip the second rename: _new -> original
		renameFragments = append(renameFragments,
			fmt.Sprintf("%s TO %s", cfg.table.QuotedTableName, oldQuotedName),
		)
	}
	// Execute the partial rename using the same code path
	if err := c.executeRenameUnderLock(ctx, tablesToLock, renameFragments); err != nil {
		return err
	}
	// Intentionally return an error to simulate a partial cutover failure
	return errors.New("intentional partial cutover failure for testing")
}
