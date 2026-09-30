package checkpoint

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// blockCheckpointRow creates a checkpoint table holding row id=1 and locks that
// row from a separate transaction, so a Write blocks on the row lock until the
// returned release function commits it.
func blockCheckpointRow(t *testing.T, name string) (*sql.DB, *Table, func()) {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tbl := NewTable(db, name, Transient)
	require.NoError(t, tbl.Create(t.Context()))
	t.Cleanup(func() { _ = tbl.Drop(context.Background()) })
	require.NoError(t, tbl.Write(t.Context(), Record{Position: "old"}))

	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "SELECT id FROM `"+name+"` WHERE id = 1 FOR UPDATE")
	require.NoError(t, err)
	release := func() { _ = trx.Commit() }
	t.Cleanup(release)
	return db, tbl, release
}

// waitForBlockedReplace waits until the server reports the REPLACE into name
// as running, so the test knows the statement reached the server.
func waitForBlockedReplace(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE ?",
			"REPLACE INTO `"+name+"`%").Scan(&n)
		return err == nil && n > 0
	}, 10*time.Second, 10*time.Millisecond)
}

// TestWriteWaitsForServerAfterCancel checks that canceling Write's context does
// not return while the server is still executing the REPLACE. Before the fix,
// Write returned context.Canceled at once and the REPLACE committed later,
// overwriting whatever the caller wrote to the row in between
// (github.com/block/spirit/issues/1313).
func TestWriteWaitsForServerAfterCancel(t *testing.T) {
	const name = "_ckpt_test_cancel_wait"
	db, tbl, release := blockCheckpointRow(t, name)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- tbl.Write(ctx, Record{Position: "new"}) }()
	waitForBlockedReplace(t, db, name)

	cancel()
	select {
	case err := <-done:
		t.Fatalf("Write returned while the REPLACE was still running on the server: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Write did not return after the row lock was released")
	}
	rec, err := tbl.ReadLatest(t.Context())
	require.NoError(t, err)
	require.Equal(t, "new", rec.Position)
}

// TestWriteCancelGraceBound checks that a canceled Write still returns once
// writeCancelGrace has elapsed, even if the server never answers.
func TestWriteCancelGraceBound(t *testing.T) {
	const name = "_ckpt_test_cancel_grace"
	old := writeCancelGrace
	writeCancelGrace = 200 * time.Millisecond
	t.Cleanup(func() { writeCancelGrace = old })
	db, tbl, _ := blockCheckpointRow(t, name)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- tbl.Write(ctx, Record{Position: "new"}) }()
	waitForBlockedReplace(t, db, name)

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Write did not return after writeCancelGrace")
	}
}
