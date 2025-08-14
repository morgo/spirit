package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/siddontang/loggers"
)

// We want to be able to kill long-running queries that prevent us from acquiring locks

var (
	LongRunningEventThreshold time.Duration = 30 * 1000 * time.Nanosecond // 30 seconds in picoseconds
)

const (
	LongRunningEventQuery = `SELECT
    t.processlist_id,
    t.processlist_user,
    t.processlist_host,
    t.processlist_info,
    ml.object_schema,
    ml.object_name,
    ml.lock_type,
    ml.lock_duration,
    ml.lock_status,
    trx.timer_wait,
    format_pico_time(trx.timer_wait) as wait_time
FROM
    performance_schema.metadata_locks ml
    JOIN performance_schema.threads t
        ON ml.owner_thread_id = t.thread_id
    JOIN performance_schema.events_transactions_current trx
        USING (thread_id)
WHERE
    /* trx.end_event_id IS NULL AND  */
    processlist_id <> CONNECTION_ID() AND 
    ml.object_type = 'table' AND 
	trx.TIMER_WAIT > ? `
	LongRunningEventQueryTableName = " AND CONCAT(ml.object_schema, '.', ml.object_name) IN (%s) "
	RDSKillStatement               = "CALL mysql.rds_kill(%d)"
	KillStatement                  = "KILL %d"
)

type LockDetail struct {
	PID          int
	User         sql.NullString
	Host         sql.NullString
	Info         sql.NullString
	ObjectSchema sql.NullString
	ObjectName   sql.NullString
	LockType     sql.NullString
	LockDuration sql.NullString
	LockStatus   sql.NullString
	WaitTime     sql.NullString
	TimerWait    int64 // in picoseconds
}

func KillLongRunningTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger loggers.Advanced) error {
	pids, err := GetLongRunningTransactions(ctx, db, tables, config, logger)
	if err != nil {
		return fmt.Errorf("failed to get long-running transactions: %w", err)
	}
	// Now we can kill these transactions
	var errs []error
	for _, pid := range pids {
		logger.Warnf("Killing long-running transaction %d", pid)
		err = KillTransaction(ctx, db, config, logger, pid)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to kill transaction %d: %w", pid, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while killing long-running transactions: %v", errs)
	}
	return nil
}

func GetLongRunningTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger loggers.Advanced) ([]int, error) {
	// This function should query the performance schema to find long-running transactions
	// that are holding locks on the specified tables.

	query := LongRunningEventQuery

	params := []any{LongRunningEventThreshold}
	if len(tables) > 0 {
		inList := ""
		for i, tableInfo := range tables {
			if tableInfo == nil {
				// This shouldn't happen, but if it does, we don't want to crash.
				//However, if there's only one table and it is nil the query will be malformed.
				logger.Warn("skipping nil table info in long-running transaction query")
				continue
			}
			inList += "?"
			if tableInfo.SchemaName != "" {
				params = append(params, tableInfo.SchemaName+"."+tableInfo.TableName)
			} else {
				params = append(params, tableInfo.TableName)
			}
			if i < len(tables)-1 {
				inList += ","
			}
		}
		query += fmt.Sprintf(LongRunningEventQueryTableName, inList)
	}

	logger.Infof("query: %s", query)
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Errorf("failed to query long-running transactions: %v", err)
		return nil, err
	}
	defer rows.Close()

	var locks []LockDetail
	for rows.Next() {
		var lock LockDetail
		if err := rows.Scan(
			&lock.PID,
			&lock.User,
			&lock.Host,
			&lock.Info,
			&lock.ObjectSchema,
			&lock.ObjectName,
			&lock.LockType,
			&lock.LockDuration,
			&lock.LockStatus,
			&lock.TimerWait,
			&lock.WaitTime,
		); err != nil {
			logger.Errorf("failed to scan row: %v", err)
			return nil, err
		}

		logger.Infof("Found long-running transaction: %#v", &lock)
		locks = append(locks, lock)
	}
	if err := rows.Err(); err != nil {
		logger.Errorf("error iterating over rows: %v", err)
		return nil, err
	}

	if len(locks) == 0 {
		logger.Infof("No long-running transactions found.")
		return nil, nil
	}

	var uniquePids []int
	for _, lock := range locks {
		// Check if this PID is already in the unique list
		found := false
		for _, pid := range uniquePids {
			if pid == lock.PID {
				found = true
				break
			}
		}
		if !found {
			uniquePids = append(uniquePids, lock.PID)
		}
	}

	logger.Infof("Found %d long-running transactions: %v", len(uniquePids), uniquePids)

	return uniquePids, nil
}

func KillTransaction(ctx context.Context, db *sql.DB, config *DBConfig, logger loggers.Advanced, pid int) error {
	var stmt string
	// If MYSQL_DSN is empty, we assume we are running on RDS and use the RDS-specific kill statement.
	if os.Getenv("MYSQL_DSN") == "" {
		stmt = RDSKillStatement
	} else {
		stmt = KillStatement
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(stmt, pid)); err != nil {
		logger.Errorf("failed to kill transaction %d: %v", pid, err)
	}
	return nil
}
