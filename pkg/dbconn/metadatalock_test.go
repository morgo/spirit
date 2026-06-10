package dbconn

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// closeOnce wraps MetadataLock.Close in a sync.Once. MetadataLock.Close is not
// idempotent — it blocks on closeCh, so calling it twice deadlocks. Tests that
// close the lock mid-flow still want a t.Cleanup safety net so that an earlier
// assertion failure (which aborts the test before the explicit Close) cannot
// leak a live MDL session holding GET_LOCK into later integration tests. This
// helper returns a func that closes exactly once: register it via t.Cleanup
// immediately after NewMetadataLock and call it wherever the test would
// otherwise call mdl.Close() directly.
func closeOnce(mdl *MetadataLock) func() error {
	var (
		once sync.Once
		err  error
	)
	return func() error {
		once.Do(func() { err = mdl.Close() })
		return err
	}
}

func TestMetadataLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()
	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, mdl)

	// Confirm a second lock cannot be acquired
	_, err = NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.ErrorContains(t, err, "lock is held by another connection")

	// Close the original mdl
	require.NoError(t, mdl.Close())

	// Confirm a new lock can be acquired
	mdl3, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NoError(t, mdl3.Close())
}

func TestMetadataLockContextCancel(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-cancel"}
	lockTables := []*table.TableInfo{&lockTableInfo}

	logger := slog.Default()
	ctx, cancel := context.WithCancel(t.Context())
	mdl, err := NewMetadataLock(ctx, testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, mdl)

	// Cancel the context
	cancel()

	// Wait for the lock to be released
	<-mdl.closeCh

	// Confirm the lock is released by acquiring a new one
	mdl2, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, mdl2)
	require.NoError(t, mdl2.Close())
}

func TestMetadataLockRefresh(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-refresh"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(mdl *MetadataLock) {
		// override the refresh interval for faster testing
		mdl.refreshInterval = 1 * time.Second
	})
	require.NoError(t, err)
	require.NotNil(t, mdl)

	// wait for the refresh to happen
	time.Sleep(2 * time.Second)

	// Confirm the lock is still held
	_, err = NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.ErrorContains(t, err, "lock is held by another connection")

	// Close the lock
	require.NoError(t, mdl.Close())
}

func TestComputeLockName(t *testing.T) {
	tests := []struct {
		table    *table.TableInfo
		expected string
	}{
		{
			table:    &table.TableInfo{SchemaName: "shortschema", TableName: "shorttable"},
			expected: "shortschema.shorttable-",
		},
		{
			table:    &table.TableInfo{SchemaName: "averylongschemanamethatexceeds20chars", TableName: "averylongtablenamewhichexceeds32characters"},
			expected: "averylongschemanamet.averylongtablenamewhichexceeds32-",
		},
	}

	for _, test := range tests {
		lockName := computeLockName(test.table)
		require.Contains(t, lockName, test.expected, "Lock name should contain the expected prefix")
		require.Len(t, lockName, len(test.expected)+8, "Lock name should have the correct length")
	}
}

// TestComputeLockNameAuxPrefixCollision pins the safety contract that two
// distinct long table names whose auxiliary table names would collide under
// truncation also share the same MDL lock name. This is what causes a second
// concurrent migration on a colliding table to fail fast on GET_LOCK instead
// of silently clobbering the first migration's _new / _chkpnt.
func TestComputeLockNameAuxPrefixCollision(t *testing.T) {
	// Two tables that share the first 56 characters (the truncation budget
	// for `_<table>_chkpnt`) but differ after that.
	common := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 56 'a'
	a := &table.TableInfo{SchemaName: "test", TableName: common + "_one"}
	b := &table.TableInfo{SchemaName: "test", TableName: common + "_two"}
	require.Equal(t, computeLockName(a), computeLockName(b),
		"truncation-colliding tables must share an MDL lock name")

	// Tables that diverge before the 56-char boundary must not collide.
	c := &table.TableInfo{SchemaName: "test", TableName: "first_" + common}
	d := &table.TableInfo{SchemaName: "test", TableName: "second" + common}
	require.NotEqual(t, computeLockName(c), computeLockName(d),
		"non-colliding tables must keep distinct MDL lock names")
}

// TestMetadataLockAuxPrefixCollision verifies the contention behavior end-to-end:
// concurrent attempts on truncation-colliding tables fail with the standard
// "lock is held" error rather than racing through to aux-table creation.
func TestMetadataLockAuxPrefixCollision(t *testing.T) {
	common := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 56 'a'
	tablesA := []*table.TableInfo{{SchemaName: "test", TableName: common + "_one"}}
	tablesB := []*table.TableInfo{{SchemaName: "test", TableName: common + "_two"}}
	logger := slog.Default()

	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), tablesA, NewDBConfig(), logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(mdl)

	_, err = NewMetadataLock(t.Context(), testutils.DSN(), tablesB, NewDBConfig(), logger)
	require.ErrorContains(t, err, "lock is held by another connection")
}

func TestMetadataLockLength(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "thisisareallylongtablenamethisisareallylongtablenamethisisareallylongtablename"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	empty := []*table.TableInfo{}

	logger := slog.Default()

	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(mdl)

	_, err = NewMetadataLock(t.Context(), testutils.DSN(), empty, NewDBConfig(), logger)
	require.ErrorContains(t, err, "no tables provided for metadata lock")
}

// simulateConnectionClose simulates a temporary network issue by closing the connection
func simulateConnectionClose(t *testing.T, mdl *MetadataLock, logger *slog.Logger) {
	// close the existing connection to simulate a network issue
	err := mdl.CloseDBConnection(logger)
	require.NoError(t, err)

	// wait a bit to ensure the connection is closed
	time.Sleep(1 * time.Second)
}

// TestMetadataLockSurvivesConnMaxLifetime pins the fix for the silent lock
// drop bug: the MDL pool must be exempt from the pool-wide ConnMaxLifetime
// (it calls SetConnMaxLifetime(0) after New). GET_LOCK is session scoped and
// database/sql's connection cleaner proactively closes expired *idle*
// connections (no query required), so before the fix the lock was silently
// released every maxConnLifetime (3 minutes) and stayed free until the next
// refresh tick (up to 1 minute later) re-acquired it on a fresh session —
// an unprotected window where a second spirit instance could start a
// concurrent migration on the same table.
//
// We can't wait 3 minutes in a test, so maxConnLifetime (a package var) is
// drastically shortened and the lock is held idle — the refresh interval is
// set far in the future so a refresh tick cannot mask the drop by
// re-acquiring. The connection cleaner runs at least every second, so after
// a few seconds of idleness an expired connection is guaranteed to have been
// closed. A second connection then verifies via IS_USED_LOCK that the lock
// is still owned.
func TestMetadataLockSurvivesConnMaxLifetime(t *testing.T) {
	// maxConnLifetime is shortened to a few seconds for the whole package in
	// TestMain (not mutated here — that would race with other tests' New()
	// calls). This test waits past that lifetime, so run it in parallel.
	t.Parallel()

	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "w2a-conn-lifetime"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(mdl *MetadataLock) {
		// The refresh would re-acquire a dropped lock and hide the bug;
		// keep it out of the picture for the duration of the test.
		mdl.refreshInterval = time.Hour
	})
	require.NoError(t, err)
	require.NotNil(t, mdl)
	closeMDL := closeOnce(mdl)
	t.Cleanup(func() { _ = closeMDL() })

	// Observe lock ownership from a separate connection.
	observer, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(observer)

	lockName := computeLockName(&lockTableInfo)
	stmt := sqlescape.MustEscapeSQL("SELECT IS_USED_LOCK(%?)", lockName)

	// The MDL connection stays idle (refresh is parked an hour out), so once it
	// ages past maxConnLifetime the database/sql connection cleaner is free to
	// close it on its next cycle. Without the SetConnMaxLifetime(0) fix that
	// idle close silently drops the session-scoped GET_LOCK. Poll IS_USED_LOCK
	// across a window comfortably past the lifetime (covering an extra cleaner
	// cycle): fail the instant the lock is seen dropped, else confirm survival.
	deadline := time.Now().Add(maxConnLifetime + 5*time.Second)
	for time.Now().Before(deadline) {
		var owner sql.NullInt64
		require.NoError(t, observer.QueryRowContext(t.Context(), stmt).Scan(&owner))
		require.True(t, owner.Valid,
			"metadata lock was silently dropped: the MDL connection must be exempt from ConnMaxLifetime")
		time.Sleep(200 * time.Millisecond)
	}

	require.NoError(t, closeMDL())
}

// TestMetadataLockRefreshRenewsLock pins the refresh/release design. Each
// refresh tick re-runs GET_LOCK, which renews the lock and serves as the
// wait_timeout keepalive but, because GET_LOCK is reference counted since
// MySQL 5.7, intentionally stacks a reference on the single-connection
// session. That stacking is harmless precisely because the lock is released
// with one RELEASE_ALL_LOCKS() (what Close()'s releaseLocks runs), which
// drops every stacked reference across every name at once — so the lock is
// genuinely free afterward, which is what the back-to-back-migration case
// (immediate GET_LOCK(..., 0)) depends on.
//
// Refresh ticks are simulated deterministically by calling getLocks (the
// exact function the ticker runs) instead of sleeping through real ticks.
func TestMetadataLockRefreshRenewsLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "w2a-refresh-renew"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(mdl *MetadataLock) {
		// Keep the background ticker from racing the simulated refreshes below.
		mdl.refreshInterval = time.Hour
	})
	require.NoError(t, err)
	require.NotNil(t, mdl)
	closeMDL := closeOnce(mdl)
	t.Cleanup(func() { _ = closeMDL() })

	lockName := computeLockName(&lockTableInfo)

	// Simulate three refresh ticks on the same session. Each renews the lock
	// (and, by design, stacks a reference); the session must stay the holder.
	for range 3 {
		require.NoError(t, mdl.getLocks(t.Context(), logger))
		var heldByMe sql.NullInt64
		stmt := sqlescape.MustEscapeSQL("SELECT IS_USED_LOCK(%?) = CONNECTION_ID()", lockName)
		require.NoError(t, mdl.db.QueryRowContext(t.Context(), stmt).Scan(&heldByMe))
		require.True(t, heldByMe.Valid && heldByMe.Int64 == 1, "refresh must keep the lock held by this session")
	}

	// One RELEASE_ALL_LOCKS() must drop every stacked reference at once,
	// leaving the lock free — the property Close()'s single release relies on.
	// The pool has MaxOpenConnections=1, so this runs on the holding session.
	var releasedCount sql.NullInt64
	require.NoError(t, mdl.db.QueryRowContext(t.Context(), "SELECT RELEASE_ALL_LOCKS()").Scan(&releasedCount))
	require.True(t, releasedCount.Valid && releasedCount.Int64 >= 1,
		"RELEASE_ALL_LOCKS should report the stacked references it dropped")

	var free sql.NullInt64
	stmt := sqlescape.MustEscapeSQL("SELECT IS_FREE_LOCK(%?)", lockName)
	require.NoError(t, mdl.db.QueryRowContext(t.Context(), stmt).Scan(&free))
	require.True(t, free.Valid && free.Int64 == 1,
		"one RELEASE_ALL_LOCKS() must fully free the lock despite stacked references")

	require.NoError(t, closeMDL())
}

// TestMetadataLockCloseReleasesStackedLock pins Close()'s half of the
// reference counting fix: even if multiple GET_LOCK references were somehow
// stacked on the dedicated session, Close() must drain them all via
// RELEASE_LOCK *before* tearing down the connection, so that the lock is
// immediately acquirable by another session (the back-to-back migration
// case). Relying on session teardown alone is racy: a rapid GET_LOCK(..., 0)
// from a new connection can still see the lock as held.
func TestMetadataLockCloseReleasesStackedLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "w2a-stacked-close"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, mdl)
	// Close() is exercised mid-test below; the once-wrapper lets the cleanup
	// double as a safety net if an earlier assertion aborts the test before
	// that explicit Close runs, without risking a non-idempotent double Close.
	closeMDL := closeOnce(mdl)
	t.Cleanup(func() { _ = closeMDL() })

	// Manually stack two extra references on the dedicated session
	// (MaxOpenConnections=1 guarantees the same session), simulating what
	// the refresh ticker used to do once per minute.
	lockName := computeLockName(&lockTableInfo)
	for range 2 {
		var answer int
		stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
		require.NoError(t, mdl.db.QueryRowContext(t.Context(), stmt).Scan(&answer))
		require.Equal(t, 1, answer)
	}

	// Open (and warm up) the observer connection before Close() so the
	// GET_LOCK below races Close() as tightly as a real back-to-back
	// migration would.
	observer, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(observer)

	require.NoError(t, closeMDL())

	// The lock must be immediately acquirable from another connection with a
	// zero timeout — exactly what a back-to-back migration does on startup.
	var acquired int
	stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
	require.NoError(t, observer.QueryRowContext(t.Context(), stmt).Scan(&acquired))
	require.Equal(t, 1, acquired,
		"lock still held after Close(): stacked GET_LOCK references must all be released")

	var released sql.NullInt64
	stmt = sqlescape.MustEscapeSQL("SELECT RELEASE_LOCK(%?)", lockName)
	require.NoError(t, observer.QueryRowContext(t.Context(), stmt).Scan(&released))
}

func TestMetadataLockRefreshWithConnIssueSimulation(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-refresh"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	// create a new MetadataLock with a short refresh interval for testing
	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(mdl *MetadataLock) {
		mdl.refreshInterval = 2 * time.Second
	})
	require.NoError(t, err)
	require.NotNil(t, mdl)

	time.Sleep(4 * time.Second)

	// simulate a temporary network issue by closing the connection
	simulateConnectionClose(t, mdl, logger)

	// wait for the refresh interval to trigger the connection failure and recovery
	time.Sleep(4 * time.Second)

	// confirm the lock is still held by attempting to acquire it with a new connection
	_, err = NewMetadataLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.ErrorContains(t, err, "lock is held by another connection")

	// close the lock
	require.NoError(t, mdl.Close())
}
