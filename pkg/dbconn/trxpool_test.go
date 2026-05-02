package dbconn

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
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
	_, err = RetryableTransaction(t.Context(), db, true, config, stmts...)
	require.NoError(t, err)

	// Test that the transaction pool is working.
	pool, err := NewTrxPool(t.Context(), db, 2, config)
	require.NoError(t, err)

	// The pool is all repeatable-read transactions, so if I insert new rows
	// They can't be visible.
	_, err = RetryableTransaction(t.Context(), db, true, config, "INSERT INTO test.trxpool (id, colb) VALUES (3, 3)")
	require.NoError(t, err)

	trx1, err := pool.Get()
	require.NoError(t, err)
	trx2, err := pool.Get()
	require.NoError(t, err)
	var count int
	err = trx1.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	err = trx2.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	_, err = pool.Get()
	assert.Error(t, err) // no trx in the pool

	pool.Put(trx1)
	trx3, err := pool.Get()
	require.NoError(t, err)
	pool.Put(trx3)

	assert.NoError(t, pool.Close())
}
