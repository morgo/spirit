package repl

import (
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func setupTestTables(t *testing.T) (*table.TableInfo, *table.TableInfo) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS subscription_test, _subscription_test_new`)
	testutils.RunSQL(t, `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	srcTable := table.NewTableInfo(db, "test", "subscription_test")
	dstTable := table.NewTableInfo(db, "test", "_subscription_test_new")

	err = srcTable.SetInfo(t.Context())
	assert.NoError(t, err)
	err = dstTable.SetInfo(t.Context())
	assert.NoError(t, err)

	return srcTable, dstTable
}
