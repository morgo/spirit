package throttler

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAuroraSetup_NonAuroraReturnsEmpty(t *testing.T) {
	// Real local MySQL: IsAurora returns false (no AuroraDb_* vars). The
	// helper must skip and not open a monitor pool — that's the gate we
	// rely on to keep non-Aurora callers from paying any cost.
	db, err := sql.Open("block-mysql", testutils.DSN())
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
	// A clean "not Aurora" answer is not a probe failure: autoscaling callers
	// warn on ProbeErr and stay quiet here.
	require.NoError(t, res.ProbeErr)
	require.False(t, res.RedoAware)
	require.False(t, openCalled, "OpenMonitor must not be called on a non-Aurora source")
}

func TestAuroraSetup_NonAuroraSkipsMonitorEvenWithZeroThreshold(t *testing.T) {
	// Regression for PR #880: CommitLatencyThreshold=0 used to short-
	// circuit Build before probing IsAurora, which silently disabled the
	// threads-running throttler too. The gates are now independent, so Build
	// must still probe IsAurora when threshold is 0 — but on a non-Aurora
	// source it still ends up with no throttlers and must not open a monitor
	// pool.
	db, err := sql.Open("block-mysql", testutils.DSN())
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

func TestAuroraSetup_RejectsNilRequiredFields(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	openMon := func() (*sql.DB, error) { return nil, nil }
	logger := discardLogger()

	// Each required field, exercised in isolation. Without these guards
	// Build would NPE deep in the probe / OpenMonitor / log calls.
	_, err = AuroraSetup{Source: nil, OpenMonitor: openMon, Logger: logger}.Build(t.Context())
	require.ErrorContains(t, err, "Source is required")

	_, err = AuroraSetup{Source: db, OpenMonitor: nil, Logger: logger}.Build(t.Context())
	require.ErrorContains(t, err, "OpenMonitor is required")

	_, err = AuroraSetup{Source: db, OpenMonitor: openMon, Logger: nil}.Build(t.Context())
	require.ErrorContains(t, err, "Logger is required")
}

func TestAuroraSetup_IsAuroraProbeFailureIsNonFatal(t *testing.T) {
	// Closed DB → IsAurora returns an error (driver/sql.ErrConnDone-style).
	// The helper logs at Debug and returns a zero result rather than
	// failing the whole migration; we'd rather skip Aurora throttling
	// than refuse to migrate on a transient probe failure.
	db, err := sql.Open("block-mysql", testutils.DSN())
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
	// Reported rather than swallowed, so an autoscaling caller can warn.
	require.ErrorContains(t, res.ProbeErr, "database is closed")
	require.False(t, openCalled, "OpenMonitor must not be called when the probe fails")
}

// closeCounter is a Throttler that counts Close calls and returns err from
// each. With a monitor set, it records whether the pool still answered a ping
// inside its Close: a real throttler's poller reads that pool until it closes.
type closeCounter struct {
	Noop
	closes      int
	err         error
	monitor     *sql.DB
	monitorLive bool
}

func (c *closeCounter) Close() error {
	c.closes++
	if c.monitor != nil {
		c.monitorLive = c.monitor.PingContext(context.Background()) == nil
	}
	return c.err
}

func TestAuroraResultCloseZero(t *testing.T) {
	// The non-Aurora result: nothing to close.
	require.NoError(t, AuroraResult{}.Close())
	require.NoError(t, AuroraResult{ProbeErr: errors.New("probe failed")}.Close())
}

func TestAuroraResultClose(t *testing.T) {
	monitor, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, monitor.PingContext(t.Context()))

	first := &closeCounter{monitor: monitor}
	second := &closeCounter{err: errors.New("second failed"), monitor: monitor}
	res := AuroraResult{Throttlers: []Throttler{first, second}, MonitorDB: monitor}
	err = res.Close()
	require.ErrorContains(t, err, "second failed", "a throttler's Close error is returned")
	require.Equal(t, 1, first.closes)
	require.Equal(t, 1, second.closes, "an earlier error does not skip later throttlers")
	require.True(t, first.monitorLive, "throttlers close before the monitor pool they poll")
	require.True(t, second.monitorLive, "throttlers close before the monitor pool they poll")
	require.ErrorContains(t, monitor.PingContext(t.Context()), "database is closed",
		"the monitor pool is closed even when a throttler's Close failed")
}

// TestAuroraResultDoubleClose covers the paths that close a result whose
// throttlers a multi-throttler already closed: the real Aurora throttlers and
// the pool must tolerate a second Close.
func TestAuroraResultDoubleClose(t *testing.T) {
	monitor, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	logger := discardLogger()
	cl, err := NewCommitLatencyThrottler(monitor, 100*time.Millisecond, logger)
	require.NoError(t, err)
	tr, err := newAuroraThreadsThrottler(monitor, globalStatusMode, logger)
	require.NoError(t, err)
	res := AuroraResult{Throttlers: []Throttler{cl, tr}, MonitorDB: monitor}
	// The threads throttler's Threads_running fallback runs on community
	// MySQL, so its poller is live when it is closed (commit latency needs
	// Aurora's status variables and stays unopened).
	require.NoError(t, tr.Open(t.Context()))

	require.NoError(t, NewMultiThrottler(res.Throttlers...).Close())
	require.NoError(t, res.Close())
	require.NoError(t, res.Close())
	require.ErrorContains(t, monitor.PingContext(t.Context()), "database is closed")
}
