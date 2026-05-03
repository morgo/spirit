package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)

	r := Resources{
		DB:    db,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}

	err = configurationCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all looks good of course.

	// Current binlog row image format.
	var binlogRowImage string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.binlog_row_image").Scan(&binlogRowImage))

	// Binlog row image is dynamic, so we can change it.
	// We could probably support NOBLOB with some testing, but it's not
	// used commonly so its useful for testing.
	_, err = db.ExecContext(t.Context(), "SET GLOBAL binlog_row_image = 'NOBLOB'")
	require.NoError(t, err)

	err = configurationCheck(t.Context(), r, slog.Default())
	require.Error(t, err)

	// restore the binlog row image format.
	_, err = db.ExecContext(t.Context(), "SET GLOBAL binlog_row_image = ?", binlogRowImage)
	require.NoError(t, err)

	// binlog_order_commits=OFF allows commit reordering that breaks Spirit's
	// binlog→applier visibility assumption — Spirit must reject this. See #818.
	_, err = db.ExecContext(t.Context(), "SET GLOBAL binlog_order_commits = OFF")
	require.NoError(t, err)
	defer func() {
		// Restore — binlog_order_commits is a boolean variable, so use the
		// literal ON form rather than a parameterised query.
		_, _ = db.ExecContext(t.Context(), "SET GLOBAL binlog_order_commits = ON")
	}()
	err = configurationCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "binlog_order_commits must be ON")
}
