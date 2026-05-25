package throttler

import (
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAuroraSetup_NonAuroraReturnsEmpty(t *testing.T) {
	// Real local MySQL: IsAurora returns false (no AuroraDb_* vars). The
	// helper must skip and not open a monitor pool — that's the gate we
	// rely on to keep non-Aurora callers from paying any cost.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	openCalled := false
	res, err := AuroraSetup{
		Source: db,
		OpenMonitor: func() (*sql.DB, error) {
			openCalled = true
			return nil, errors.New("must not be called")
		},
		CommitLatencyThreshold: 100 * time.Millisecond,
		Logger:                 discardLogger(),
	}.Build(t.Context())
	require.NoError(t, err)
	require.Nil(t, res.Throttlers)
	require.Nil(t, res.MonitorDB)
	require.False(t, openCalled, "OpenMonitor must not be called on a non-Aurora source")
}

func TestAuroraSetup_NonAuroraSkipsMonitorEvenWithZeroThreshold(t *testing.T) {
	// Regression for PR #880: CommitLatencyThreshold=0 used to short-
	// circuit Build before probing IsAurora, which silently disabled the
	// active-threads throttler too. The two gates are now independent,
	// so Build must still probe IsAurora when threshold is 0 — but on a
	// non-Aurora source it still ends up with no throttlers and must not
	// open a monitor pool.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	openCalled := false
	res, err := AuroraSetup{
		Source: db,
		OpenMonitor: func() (*sql.DB, error) {
			openCalled = true
			return nil, errors.New("must not be called")
		},
		CommitLatencyThreshold: 0,
		Logger:                 discardLogger(),
	}.Build(t.Context())
	require.NoError(t, err)
	require.Nil(t, res.Throttlers)
	require.Nil(t, res.MonitorDB)
	require.False(t, openCalled, "OpenMonitor must not be called on a non-Aurora source even with threshold=0")
}

func TestAuroraSetup_IsAuroraProbeFailureIsNonFatal(t *testing.T) {
	// Closed DB → IsAurora returns an error (driver/sql.ErrConnDone-style).
	// The helper logs at Debug and returns a zero result rather than
	// failing the whole migration; we'd rather skip Aurora throttling
	// than refuse to migrate on a transient probe failure.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	openCalled := false
	res, err := AuroraSetup{
		Source: db,
		OpenMonitor: func() (*sql.DB, error) {
			openCalled = true
			return nil, errors.New("must not be called")
		},
		CommitLatencyThreshold: 100 * time.Millisecond,
		Logger:                 discardLogger(),
	}.Build(t.Context())
	require.NoError(t, err, "probe failure should be non-fatal")
	require.Nil(t, res.Throttlers)
	require.Nil(t, res.MonitorDB)
	require.False(t, openCalled, "OpenMonitor must not be called when the probe fails")
}
