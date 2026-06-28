package dbconn

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestTrxPool(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Test database connectivity before proceeding
	err = db.PingContext(t.Context())
	require.NoError(t, err)

	config := NewDBConfig()
	config.LockWaitTimeout = 10
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.trxpool")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.trxpool (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.trxpool (id, colb) VALUES (1, 1)",
		"INSERT INTO test.trxpool (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(t.Context(), db, IgnoreDupKeyWarnings, config, stmts...)
	require.NoError(t, err)

	// Test that the transaction pool is working.
	pool, err := NewTrxPool(t.Context(), db, 2, config)
	require.NoError(t, err)

	// The pool is all repeatable-read transactions, so if I insert new rows
	// They can't be visible.
	_, err = RetryableTransaction(t.Context(), db, IgnoreDupKeyWarnings, config, "INSERT INTO test.trxpool (id, colb) VALUES (3, 3)")
	require.NoError(t, err)

	trx1, err := pool.Get()
	require.NoError(t, err)
	trx2, err := pool.Get()
	require.NoError(t, err)
	var count int
	err = trx1.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
	err = trx2.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	_, err = pool.Get()
	require.Error(t, err) // no trx in the pool

	pool.Put(trx1)
	trx3, err := pool.Get()
	require.NoError(t, err)
	pool.Put(trx3)

	require.NoError(t, pool.Close())
}

// TestTrxPoolBeginError verifies that NewTrxPool returns an error
// (rather than panicking on a nil *sql.Tx) when BeginTx fails while the
// context is still live, e.g. because the database is closed/unreachable.
// Note: a non-context error is required to reproduce the historical panic,
// because a nil (*sql.Tx).ExecContext checks ctx.Done() before it
// dereferences the receiver.
func TestTrxPoolBeginError(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	require.NoError(t, db.Close()) // BeginTx now fails with "sql: database is closed"

	config := NewDBConfig()
	pool, err := NewTrxPool(t.Context(), db, 2, config)
	require.Error(t, err)
	require.ErrorContains(t, err, "database is closed")
	require.Nil(t, pool)
}

// TestTrxPoolBeginErrorMidLoop verifies that when BeginTx fails partway
// through pool creation, NewTrxPool returns an error and the transactions
// created by earlier iterations are rolled back (no connection leak).
func TestTrxPoolBeginErrorMidLoop(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	config := NewDBConfig()
	// Allow only 2 connections but request 3 transactions. The first two
	// BeginTx calls succeed and hold both connections; the third blocks
	// waiting for a free connection until the context times out, so the
	// failure deterministically happens mid-loop.
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	pool, err := NewTrxPool(ctx, db, 3, config)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, pool)

	// The two transactions created before the failure must have been
	// rolled back and their connections returned to the pool. The rollback
	// of context-bound transactions can complete asynchronously, so poll.
	require.Eventually(t, func() bool {
		return db.Stats().InUse == 0
	}, 5*time.Second, 50*time.Millisecond, "transactions leaked: %d connections still in use", db.Stats().InUse)
}
