package dbconn

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

var (
	TestKillLongRunningTransactionsTableBaseName = "TestKillLongRunningTransactions"
)

func TestKillLongRunningTransactions(t *testing.T) {
	logger := slog.Default()

	dbConfig := NewDBConfig()
	dbConfig.InterpolateParams = true
	db, err := New(testutils.DSN(), dbConfig)
	if err != nil {
		t.Fatalf("Failed to create DB connection: %v", err)
	}
	defer utils.CloseAndLog(db)

	n := 2

	var schema string
	err = db.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema)
	require.NoError(t, err)
	require.NotEmpty(t, schema)

	// Create multiple tables for testing, each with a unique name
	// including an extra one for a non-transactional test
	tables := make([]*table.TableInfo, n+1)
	for i := range n + 1 {
		tbl := fmt.Sprintf("%s%d", TestKillLongRunningTransactionsTableBaseName, i)
		tables[i] = table.NewTableInfo(db, schema, tbl)
		err = Exec(t.Context(), db, "DROP TABLE IF EXISTS "+tables[i].QuotedTableName)
		require.NoError(t, err)
		err = Exec(t.Context(), db, "CREATE TABLE "+tables[i].QuotedTableName+" (id INT NOT NULL auto_increment PRIMARY KEY, i int)")
		require.NoError(t, err)
	}

	txIDs := make([]int, n)
	txs := make([]*sql.Tx, n)
	for i := range n {
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		err = tx.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&txIDs[i])
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "use "+schema)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) VALUES (%d)", tables[i].QuotedTableName, i))
		require.NoError(t, err)
		txs[i] = tx
	}

	nonTrx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)

	// Explicitly lock the table in a non-transactional way
	_, err = nonTrx.ExecContext(t.Context(), fmt.Sprintf("LOCK TABLES %s WRITE", tables[n].QuotedTableName))
	require.NoError(t, err)
	var nonTrxID int
	err = nonTrx.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&nonTrxID)
	require.NoError(t, err)

	// Insert a lot of rows in the 1st transaction to give it a higher "weight"
	for i := range 16 {
		_, err = txs[0].ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) SELECT %d FROM %s", tables[0].QuotedTableName, i, tables[0].QuotedTableName))
		require.NoError(t, err)
	}

	// Sleep to ensure the transactions are long-running
	time.Sleep(time.Second)

	tableLocks, err := GetTableLocks(t.Context(), db, tables, nil, logger, nil)
	require.NoError(t, err)
	require.Len(t, tableLocks, 1)
	require.True(t, tableLocks[0].ObjectName.Valid)
	require.Equal(t, strings.ToLower(tables[n].TableName), strings.ToLower(tableLocks[0].ObjectName.String))
	require.Equal(t, nonTrxID, tableLocks[0].PID)

	_, err = nonTrx.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err)
	err = nonTrx.Rollback()
	require.NoError(t, err)

	TransactionWeightThreshold = 1000 // Set a low threshold for testing purposes
	ids, err := GetLockingTransactions(t.Context(), db, tables, nil, logger, nil)
	require.NoError(t, err)

	// We expect only the second transaction to be considered
	// long-running for our purposes, because the first transaction has a high weight due to
	// the large number of rows inserted.
	require.Len(t, ids, 1)
	for _, id := range ids {
		require.Contains(t, txIDs, id)
	}

	TransactionWeightThreshold = 1e7 // Reset the threshold to a high value
	ids, err = GetLockingTransactions(t.Context(), db, tables, nil, logger, nil)
	require.NoError(t, err)
	// Now we expect both transactions to be considered long-running, because the weight threshold is higher.
	require.Len(t, ids, 2)
	for _, id := range ids {
		require.Contains(t, txIDs, id)
	}

	err = KillLockingTransactions(t.Context(), db, tables, nil, logger, nil)
	require.NoError(t, err)

	for _, tx := range txs {
		err = tx.Rollback()
		require.Error(t, err, "expected rollback to fail because transaction was killed")
	}
}

// TestSpiritSessionExcludedFromKill verifies the program_name based
// self-exclusion that protects Spirit's own pool members from being
// targeted by ForceKill at cutover.
//
// Setup: two DBs against the same MySQL server, each with its own
// DBConfig and therefore its own SessionID — they look like two
// independent Spirit invocations. dbA opens a long-running transaction
// holding an MDL on a target table.
//
// Properties asserted:
//
//  1. dbA querying GetLockingTransactions with its own config does NOT
//     see dbA's transaction (its program_name matches the filter).
//  2. dbB querying with its own config DOES see dbA's transaction
//     (different SessionID, different program_name).
//  3. Querying with a nil config (legacy callers / privileges check) DOES
//     see dbA's transaction (no filter applied → legacy behavior).
func TestSpiritSessionExcludedFromKill(t *testing.T) {
	logger := slog.Default()

	dbConfigA := NewDBConfig()
	dbConfigA.InterpolateParams = true
	dbA, err := New(testutils.DSN(), dbConfigA)
	require.NoError(t, err)
	defer utils.CloseAndLog(dbA)

	dbConfigB := NewDBConfig()
	dbConfigB.InterpolateParams = true
	require.NotEqual(t, dbConfigA.SessionID, dbConfigB.SessionID, "each NewDBConfig must generate a fresh SessionID")
	dbB, err := New(testutils.DSN(), dbConfigB)
	require.NoError(t, err)
	defer utils.CloseAndLog(dbB)

	var schema string
	require.NoError(t, dbA.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema))

	tbl := table.NewTableInfo(dbA, schema, "TestSpiritSessionExcluded")
	require.NoError(t, Exec(t.Context(), dbA, "DROP TABLE IF EXISTS "+tbl.QuotedTableName))
	require.NoError(t, Exec(t.Context(), dbA, "CREATE TABLE "+tbl.QuotedTableName+" (id INT NOT NULL auto_increment PRIMARY KEY, i int)"))
	defer func() { _ = Exec(t.Context(), dbA, "DROP TABLE IF EXISTS "+tbl.QuotedTableName) }()

	// Open a long-running transaction on dbA holding an MDL on tbl.
	tx, err := dbA.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) VALUES (1)", tbl.QuotedTableName))
	require.NoError(t, err)

	// Allow the MDL to register in performance_schema.
	time.Sleep(500 * time.Millisecond)

	prev := TransactionWeightThreshold
	TransactionWeightThreshold = 1_000_000_000 // high so the trx isn't filtered out by weight
	defer func() { TransactionWeightThreshold = prev }()

	tables := []*table.TableInfo{tbl}

	// 1. dbA's own config filters out dbA's transaction.
	idsFromA, err := GetLockingTransactions(t.Context(), dbA, tables, dbConfigA, logger, nil)
	require.NoError(t, err)
	require.Empty(t, idsFromA, "Spirit must not see its own pool members through its own SessionID filter")

	// 2. dbB's config (different SessionID) does see dbA's transaction.
	idsFromB, err := GetLockingTransactions(t.Context(), dbB, tables, dbConfigB, logger, nil)
	require.NoError(t, err)
	require.NotEmpty(t, idsFromB, "a sibling Spirit invocation must still observe foreign locking transactions")

	// 3. Legacy nil-config path (privileges check) preserves the
	//    pre-filter behaviour and sees the transaction too.
	idsFromNil, err := GetLockingTransactions(t.Context(), dbA, tables, nil, logger, nil)
	require.NoError(t, err)
	require.NotEmpty(t, idsFromNil, "nil config must fall back to legacy unfiltered behaviour")
}
