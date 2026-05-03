package dbconn

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func getVariable(trx *sql.Tx, name string, sessionScope bool) (string, error) {
	var value string
	scope := "GLOBAL"
	if sessionScope {
		scope = "SESSION"
	}
	err := trx.QueryRowContext(context.Background(), "SELECT @@"+scope+"."+name).Scan(&value)
	return value, err
}

func TestLockWaitTimeouts(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, err := db.BeginTx(context.Background(), nil) // not strictly required.
	require.NoError(t, err)

	lockWaitTimeout, err := getVariable(trx, "lock_wait_timeout", true)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(config.LockWaitTimeout), lockWaitTimeout)

	innodbLockWaitTimeout, err := getVariable(trx, "innodb_lock_wait_timeout", true)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(config.InnodbLockWaitTimeout), innodbLockWaitTimeout)
	require.NoError(t, trx.Rollback())
}

func TestRetryableTrx(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.dbexec")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.dbexec (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.dbexec (id, colb) VALUES (1, 1)",
		"", // test empty
		"INSERT INTO test.dbexec (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(t.Context(), db, true, NewDBConfig(), stmts...)
	require.NoError(t, err)

	_, err = RetryableTransaction(t.Context(), db, true, NewDBConfig(), "INSERT INTO test.dbexec (id, colb) VALUES (2, 2)") // duplicate
	require.Error(t, err)

	// duplicate, but creates a warning. Ignore duplicate warnings set to true.
	_, err = RetryableTransaction(t.Context(), db, true, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	require.NoError(t, err)

	// duplicate, but warning not ignored
	_, err = RetryableTransaction(t.Context(), db, false, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	require.Error(t, err)

	// start a transaction, acquire a lock for long enough that the first attempt times out
	// but a retry is successful.
	config.InnodbLockWaitTimeout = 1
	db, err = New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM test.dbexec WHERE id = 1 FOR UPDATE")
	require.NoError(t, err)
	go func() {
		time.Sleep(2 * time.Second)
		err2 := trx.Rollback()
		assert.NoError(t, err2)
	}()
	_, err = RetryableTransaction(t.Context(), db, false, config, "UPDATE test.dbexec SET colb=123 WHERE id = 1")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Same again, but make the retry unsuccessful
	config.InnodbLockWaitTimeout = 1
	config.MaxRetries = 2
	db, err = New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, err = db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM test.dbexec WHERE id = 2 FOR UPDATE")
	require.NoError(t, err)
	_, err = RetryableTransaction(t.Context(), db, false, config, "UPDATE test.dbexec SET colb=123 WHERE id = 2") // this will fail, since it times out and exhausts retries.
	require.Error(t, err)
	err = trx.Rollback() // now we can rollback.
	require.NoError(t, err)
}

func TestForceExec(t *testing.T) {
	config := NewDBConfig()
	config.LockWaitTimeout = 1 // as short as possible.
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS requires_mdl")
	require.NoError(t, err)

	err = Exec(t.Context(), db, "CREATE TABLE requires_mdl (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	ti := table.NewTableInfo(db, "test", "requires_mdl")
	err = ti.SetInfo(t.Context())
	require.NoError(t, err)

	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer trx.Rollback()                                                //nolint: errcheck
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM requires_mdl") // just a select, nothing else.
	require.NoError(t, err)

	// Under a normal exec applying an instant change will fail due to MDL timeout
	err = Exec(t.Context(), db, "ALTER TABLE requires_mdl ALGORITHM=INSTANT, ADD COLUMN colc INT")
	require.Error(t, err)

	// But change it to forceexec and it will work!
	err = ForceExec(t.Context(), db, []*table.TableInfo{ti}, config, slog.Default(), "ALTER TABLE requires_mdl ALGORITHM=INSTANT, ADD COLUMN colc INT")
	require.NoError(t, err)
}

func TestStandardTrx(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, connID, err := BeginStandardTrx(t.Context(), db, nil)
	require.NoError(t, err)
	var observedConnID int
	err = trx.QueryRowContext(t.Context(), "SELECT connection_id()").Scan(&observedConnID)
	require.NoError(t, err)
	require.Equal(t, connID, observedConnID)
}
