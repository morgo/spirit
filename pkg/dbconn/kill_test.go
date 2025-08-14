package dbconn

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	TestKillLongRunningTransactionsTableBaseName = "TestKillLongRunningTransactions"
)

func TestKillLongRunningTransactions(t *testing.T) {
	// This test is a placeholder. The actual implementation would require a
	// database connection and a setup to create long-running transactions.
	// Here we just check that the function can be called without error.
	db, err := New(testutils.DSN(), NewDBConfig())
	if err != nil {
		t.Fatalf("Failed to create DB connection: %v", err)
	}
	defer db.Close()

	LongRunningEventThreshold = 1 // Set a short threshold for testing purposes

	n := 2

	var schema string
	err = db.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema)
	require.NoError(t, err)
	require.NotEmpty(t, schema)

	tables := make([]*table.TableInfo, n)
	for i := range n {
		tbl := fmt.Sprintf("%s%d", TestKillLongRunningTransactionsTableBaseName, i)
		tables[i] = table.NewTableInfo(db, schema, tbl)
		err = Exec(t.Context(), db, "DROP TABLE IF EXISTS "+tables[i].QuotedName)
		require.NoError(t, err)
		err = Exec(t.Context(), db, "CREATE TABLE "+tables[i].QuotedName+" (id INT NOT NULL auto_increment PRIMARY KEY, i int)")
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
		_, err = tx.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) VALUES (%d)", tables[i].QuotedName, i))
		require.NoError(t, err)
		txs[i] = tx
	}

	// Sleep to ensure the transactions are long-running
	time.Sleep(time.Second)

	ids, err := GetLongRunningTransactions(t.Context(), db, tables, nil, logrus.New())
	require.NoError(t, err)
	require.Len(t, ids, 2)
	for _, id := range ids {
		require.Contains(t, txIDs, id)
	}

	err = KillLongRunningTransactions(t.Context(), db, tables, nil, logrus.New())
	require.NoError(t, err)

	for _, tx := range txs {
		err = tx.Rollback()
		require.Error(t, err, "expected rollback to fail because transaction was killed")
	}

}
