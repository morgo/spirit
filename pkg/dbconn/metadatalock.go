package dbconn

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

var (
	// getLockTimeout is the timeout for acquiring the GET_LOCK. We set it to 0
	// because we want to return immediately if the lock is not available
	getLockTimeout  = 0 * time.Second
	refreshInterval = 1 * time.Minute
)

type MetadataLock struct {
	cancel          context.CancelFunc
	closeCh         chan error
	refreshInterval time.Duration
	db              *sql.DB
	lockNames       []string // Multiple lock names for multiple tables
}

func NewMetadataLock(ctx context.Context, dsn string, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger, optionFns ...func(*MetadataLock)) (*MetadataLock, error) {
	if len(tables) == 0 {
		return nil, errors.New("no tables provided for metadata lock")
	}
	mdl := &MetadataLock{
		refreshInterval: refreshInterval,
		lockNames:       make([]string, 0, len(tables)),
	}

	// Apply option functions
	for _, optionFn := range optionFns {
		optionFn(mdl)
	}

	// Compute lock names for all tables
	for _, tbl := range tables {
		mdl.lockNames = append(mdl.lockNames, computeLockName(tbl))
	}

	// Setup the dedicated connection for this lock
	// Use the provided config but ensure MaxOpenConnections is 1 for metadata locks
	dbConfig := *config // Copy the config
	dbConfig.MaxOpenConnections = 1
	// newConnection opens the dedicated pool for this lock. GET_LOCK is
	// session scoped, so the pool must be exempt from the standard
	// connection max lifetime: database/sql's connection cleaner
	// proactively closes idle connections older than ConnMaxLifetime,
	// which would silently release the lock until the next refresh tick
	// re-acquired it on a fresh session — a window of up to refreshInterval
	// during which another spirit instance could acquire the lock and start
	// a concurrent migration on the same table. The refresh ticker already
	// detects and replaces dead connections, so an unbounded lifetime is
	// safe here.
	newConnection := func() (*sql.DB, error) {
		db, err := New(dsn, &dbConfig)
		if err != nil {
			return nil, err
		}
		db.SetConnMaxLifetime(0) // see comment above
		return db, nil
	}
	var err error
	mdl.db, err = newConnection()
	if err != nil {
		return nil, err
	}

	// Acquire all locks or return an error immediately
	logger.Info("attempting to acquire metadata lock")
	if err = mdl.getLocks(ctx, logger); err != nil {
		_ = mdl.db.Close() // close if we are not returning an MDL.
		return nil, err
	}
	for _, lockName := range mdl.lockNames {
		logger.Info("acquired metadata lock", "lock_name", lockName)
	}

	// Setup background refresh runner
	ctx, mdl.cancel = context.WithCancel(ctx)
	mdl.closeCh = make(chan error, 1) // Make it buffered to prevent blocking
	go func() {
		ticker := time.NewTicker(mdl.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Explicitly release the locks before closing the connection.
				// Relying on connection close alone leaves a small window where
				// MySQL has not yet finished tearing down the session, so a
				// rapid reacquire on a new connection can see the lock as still
				// held. RELEASE_LOCK on the same session avoids that race.
				mdl.releaseLocks(logger)
				// Use select with default to avoid blocking if Close() isn't called
				select {
				case mdl.closeCh <- mdl.db.Close():
				default:
					// If no one is listening, just close the connection anyway
					_ = mdl.db.Close()
				}
				return
			case <-ticker.C:
				if err = mdl.getLocks(ctx, logger); err != nil {
					// if we can't refresh the lock, it's okay.
					// We have other safety mechanisms in place to prevent corruption
					// for example, we watch the binary log to see metadata changes
					// that we did not make. This makes it a warning, not an error,
					// and we can try again on the next tick interval.
					logger.Warn("could not refresh metadata locks", "error", err)

					// try to close the existing connection
					if closeErr := mdl.db.Close(); closeErr != nil {
						logger.Warn("could not close database connection", "error", closeErr)
						continue
					}

					// try to re-establish the connection
					mdl.db, err = newConnection()
					if err != nil {
						logger.Warn("could not re-establish database connection", "error", err)
						continue
					}

					// try to acquire the locks again with the new connection
					if err = mdl.getLocks(ctx, logger); err != nil {
						logger.Warn("could not acquire metadata locks after re-establishing connection", "error", err)
						continue
					}

					logger.Info("re-acquired metadata locks after re-establishing connection")
				} else {
					logger.Debug("refreshed metadata locks")
				}
			}
		}
	}()

	return mdl, nil
}

// getLocks acquires (or re-confirms) all locks on the dedicated session.
// https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
//
// GET_LOCK is reference counted since MySQL 5.7: acquiring the same name N
// times on one session requires N RELEASE_LOCK calls before the lock is
// free. The refresh ticker calls this every refreshInterval on the same
// session (the pool has MaxOpenConnections=1), so we must not blindly
// re-acquire — the reference count would grow with every tick and Close()
// could never fully release the lock. Instead, skip names this session
// already holds; the IS_USED_LOCK check doubles as a keepalive and detects
// dead connections just as GET_LOCK did.
func (m *MetadataLock) getLocks(ctx context.Context, logger *slog.Logger) error {
	for _, lockName := range m.lockNames {
		var alreadyHeld sql.NullInt64
		stmt := sqlescape.MustEscapeSQL("SELECT IS_USED_LOCK(%?) = CONNECTION_ID()", lockName)
		if err := m.db.QueryRowContext(ctx, stmt).Scan(&alreadyHeld); err != nil {
			return fmt.Errorf("could not check metadata lock for %s: %w", lockName, err)
		}
		if alreadyHeld.Valid && alreadyHeld.Int64 == 1 {
			// Already held by this session: do not stack another reference.
			continue
		}
		var answer int
		stmt = sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
		if err := m.db.QueryRowContext(ctx, stmt).Scan(&answer); err != nil {
			return fmt.Errorf("could not acquire metadata lock for %s: %w", lockName, err)
		}
		if answer == 0 {
			// 0 means the lock is held by another connection
			logger.Warn("could not acquire metadata lock, lock is held by another connection", "lock_name", lockName)
			return fmt.Errorf("could not acquire metadata lock for %s, lock is held by another connection", lockName)
		} else if answer != 1 {
			// probably we never get here, but just in case
			return fmt.Errorf("could not acquire metadata lock %s, GET_LOCK returned: %d", lockName, answer)
		}
	}
	return nil
}

// releaseLocks explicitly releases all locks held by the dedicated session.
// Because GET_LOCK is reference counted (see getLocks), RELEASE_LOCK is
// called in a loop until it reports the session no longer holds the lock:
// it returns 1 while references remain, then NULL once fully released (or 0
// if another session holds it). A single release would leave the lock held
// whenever more than one reference was ever stacked.
func (m *MetadataLock) releaseLocks(logger *slog.Logger) {
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer releaseCancel()
	for _, lockName := range m.lockNames {
		logger.Info("releasing metadata lock", "lock_name", lockName)
		stmt := sqlescape.MustEscapeSQL("SELECT RELEASE_LOCK(%?)", lockName)
		for {
			var released sql.NullInt64
			if err := m.db.QueryRowContext(releaseCtx, stmt).Scan(&released); err != nil {
				logger.Warn("could not release metadata lock", "lock_name", lockName, "error", err)
				break
			}
			if !released.Valid || released.Int64 != 1 {
				// NULL: this session does not hold the lock (fully released).
				// 0: held by another session. Nothing more to release.
				break
			}
		}
	}
}

func (m *MetadataLock) Close() error {
	// Cancel the background refresh runner
	m.cancel()

	// Wait for the dedicated connection to be closed and return its error (if any)
	return <-m.closeCh
}

func (m *MetadataLock) CloseDBConnection(logger *slog.Logger) error {
	// Closes the database connection for the MetadataLock
	logger.Info("about to close MetadataLock database connection")
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func computeLockName(table *table.TableInfo) string {
	schemaNamePart := table.SchemaName
	if len(schemaNamePart) > 20 {
		schemaNamePart = schemaNamePart[:20]
	}

	// Key the lock on the truncated table-name prefix that Spirit uses when
	// generating auxiliary table names (_<table>_chkpnt, _<table>_new, etc).
	// Two migrations whose auxiliary tables would collide under truncation
	// produce the same lock name and serialize. For tables short enough that
	// no truncation occurs, this is identical to the original table name.
	auxPrefix := utils.TruncateTableName(table.TableName, 1+len("_chkpnt"))

	tableNamePart := auxPrefix
	if len(tableNamePart) > 32 {
		tableNamePart = tableNamePart[:32]
	}

	hash := sha1.New()
	hash.Write([]byte(table.SchemaName + auxPrefix))
	hashPart := hex.EncodeToString(hash.Sum(nil))[:8]

	return fmt.Sprintf("%s.%s-%s", schemaNamePart, tableNamePart, hashPart)
}
