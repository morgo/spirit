package check

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestTableCompatibilityCheckPass(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE compat_pass (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))")

	tblInfo := table.NewTableInfo(db, dbName, "compat_pass")
	assert.NoError(t, tblInfo.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{tblInfo},
	}
	err = tableCompatibilityCheck(context.Background(), r, slog.Default())
	assert.NoError(t, err)
}

func TestTableCompatibilityCheckNonMemoryComparablePK(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE compat_varchar_pk (id VARCHAR(255) NOT NULL PRIMARY KEY, val INT)")

	tblInfo := table.NewTableInfo(db, dbName, "compat_varchar_pk")
	assert.NoError(t, tblInfo.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{tblInfo},
	}
	err = tableCompatibilityCheck(context.Background(), r, slog.Default())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-memory-comparable primary key")
	assert.Contains(t, err.Error(), "compat_varchar_pk")
}

func TestTableCompatibilityCheckMultipleTables(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	dbName := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE good_table (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, val INT)")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE bad_table (id VARCHAR(255) NOT NULL PRIMARY KEY, val INT)")

	goodTable := table.NewTableInfo(db, dbName, "good_table")
	assert.NoError(t, goodTable.SetInfo(context.Background()))

	badTable := table.NewTableInfo(db, dbName, "bad_table")
	assert.NoError(t, badTable.SetInfo(context.Background()))

	// Should fail because bad_table has a non-memory-comparable PK
	r := Resources{
		SourceTables: []*table.TableInfo{goodTable, badTable},
	}
	err = tableCompatibilityCheck(context.Background(), r, slog.Default())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bad_table")
}

func TestTableCompatibilityCheckNoTables(t *testing.T) {
	// Empty table list should pass (nothing to check)
	r := Resources{
		SourceTables: []*table.TableInfo{},
	}
	err := tableCompatibilityCheck(context.Background(), r, slog.Default())
	assert.NoError(t, err)
}
