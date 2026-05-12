package repl

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestClientGTIDAccessors exercises the GTID setter/getter pair in isolation
// from Run(). Verifies that:
//   - GetBinlogApplyGTID returns "" before SetFlushedGTID is called.
//   - An empty string passed to SetFlushedGTID is a no-op.
//   - A parseable GTID set round-trips through Get and flips gtidMode on.
//   - A malformed GTID set is rejected.
func TestClientGTIDAccessors(t *testing.T) {
	c := &Client{}

	require.Equal(t, "", c.GetBinlogApplyGTID(),
		"unset flushedGTID should serialize to empty string")
	require.False(t, c.GTIDModeEnabled(),
		"gtidMode should default to false until SetFlushedGTID or Run() flips it on")

	require.NoError(t, c.SetFlushedGTID(""), "empty string should be a no-op")
	require.False(t, c.GTIDModeEnabled(),
		"empty SetFlushedGTID should not flip gtidMode")

	good := "8e6d4cf2-2bd2-11ee-be56-0242ac120002:1-100"
	require.NoError(t, c.SetFlushedGTID(good))
	require.True(t, c.GTIDModeEnabled(),
		"a non-empty SetFlushedGTID should imply gtid_mode")

	got := c.GetBinlogApplyGTID()
	require.NotEmpty(t, got)
	// Don't require byte-for-byte equality; the parser normalizes whitespace
	// and a future library change could re-format the interval. Just
	// require the UUID and the high boundary are preserved.
	require.Contains(t, got, "8e6d4cf2-2bd2-11ee-be56-0242ac120002")
	require.Contains(t, got, "100")

	err := c.SetFlushedGTID("not-a-gtid")
	require.Error(t, err, "malformed GTID set should fail to parse")
	require.True(t, strings.Contains(err.Error(), "could not parse"))
}

// TestDetectGTIDMode runs against the integration MySQL and exercises
// detectGTIDMode. The expected result depends on which compose file the
// test suite is running against — we just verify that the call succeeds
// and that the boolean it returns is consistent with what `SELECT
// @@global.gtid_mode` says directly.
func TestDetectGTIDMode(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	c := &Client{db: db}
	got, err := c.detectGTIDMode(t.Context())
	require.NoError(t, err)

	var mode string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT @@global.gtid_mode").Scan(&mode))
	require.Equal(t, strings.EqualFold(mode, "ON"), got,
		"detectGTIDMode should agree with @@global.gtid_mode")
}
