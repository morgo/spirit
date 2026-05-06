package check

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestTableCompatibilityCheckPass(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE compat_pass (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))")

	tblInfo := table.NewTableInfo(db, dbName, "compat_pass")
	require.NoError(t, tblInfo.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{tblInfo},
	}
	err := tableCompatibilityCheck(context.Background(), r, slog.Default())
	require.NoError(t, err)
}

// TestTableCompatibilityCheckNonMemoryComparablePK pins that a VARCHAR PK
// (non-memory-comparable) is now accepted. The subscription routes those
// tables through bufferedMap's FIFO queue mode — see issue #607.
func TestTableCompatibilityCheckNonMemoryComparablePK(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE compat_varchar_pk (id VARCHAR(255) NOT NULL PRIMARY KEY, val INT)")

	tblInfo := table.NewTableInfo(db, dbName, "compat_varchar_pk")
	require.NoError(t, tblInfo.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{tblInfo},
	}
	require.NoError(t, tableCompatibilityCheck(context.Background(), r, slog.Default()))
}

func TestTableCompatibilityCheckMultipleTables(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE int_pk_table (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, val INT)")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE varchar_pk_table (id VARCHAR(255) NOT NULL PRIMARY KEY, val INT)")

	intPKTable := table.NewTableInfo(db, dbName, "int_pk_table")
	require.NoError(t, intPKTable.SetInfo(context.Background()))

	varcharPKTable := table.NewTableInfo(db, dbName, "varchar_pk_table")
	require.NoError(t, varcharPKTable.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{intPKTable, varcharPKTable},
	}
	require.NoError(t, tableCompatibilityCheck(context.Background(), r, slog.Default()))
}

func TestTableCompatibilityCheckNoTables(t *testing.T) {
	// Empty table list should pass (nothing to check)
	r := Resources{
		SourceTables: []*table.TableInfo{},
	}
	err := tableCompatibilityCheck(context.Background(), r, slog.Default())
	require.NoError(t, err)
}
