package check

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestConfigurationCheck(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Test with valid configuration
	r := Resources{
		Sources: []SourceResource{{DB: db}},
	}
	err = configurationCheck(context.Background(), r, slog.Default())
	require.NoError(t, err)
}

func TestConfigurationCheckMultipleSources(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	db2, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db2)

	// Multiple valid sources should pass.
	r := Resources{
		Sources: []SourceResource{{DB: db}, {DB: db2}},
	}
	err = configurationCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// A nil DB on the second source should fail and reference source 1.
	r = Resources{
		Sources: []SourceResource{{DB: db}, {DB: nil}},
	}
	err = configurationCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.Contains(t, err.Error(), "source 1")
}

// TestConfigurationCheckBinlogOrderCommits ensures the move-check rejects
// servers running with binlog_order_commits=OFF, which allows commit
// reordering that breaks Spirit's binlog→applier visibility assumption.
// See issue #818.
func TestConfigurationCheckBinlogOrderCommits(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	r := Resources{
		Sources: []SourceResource{{DB: db}},
	}

	// With binlog_order_commits=OFF the check must fail.
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
