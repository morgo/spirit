//go:build semisync

package migration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestCutoverAtomicitySemiSync is the semi-sync regression variant of
// TestCutoverAtomicityWithConcurrentWrites. It is gated behind the
// `semisync` build tag and intended to be run only by the dedicated
// `mysql-semisync-docker.yml` workflow.
//
// The workflow brings up a MySQL source configured with semi-sync enabled
// but no replica attached, and a very short `rpl_semi_sync_source_timeout`
// (~100 ms). With those settings every committing transaction blocks for
// the timeout window between binlog write and InnoDB commit-visibility —
// deterministically widening the binlog/visibility race documented in
// issue #746 from a rare CI flake to a near-certain event on every commit.
//
// Spirit's production code path now reads row images directly from the
// binlog via the buffered replication subscription (#821, #823), so it no
// longer issues `REPLACE INTO _new ... SELECT FROM original ...` after
// observing a binlog row event. The binlog/visibility window is therefore
// irrelevant: the row payload spirit needs is the binlog event itself.
//
// If a future change reintroduced any code path that SELECTed from
// `original` based on a key learned from the binlog stream, this test
// would fail with row-count or checksum divergence between `_old` and
// `_new` on every run, instead of needing CI parallel load to surface it
// once in N runs.
func TestCutoverAtomicitySemiSync(t *testing.T) {
	requireSemiSyncSourceActive(t)

	t.Parallel()

	cases := []struct {
		name      string
		tableName string
		schema    string
		buffered  bool
	}{
		{"optimistic_unbuffered", "t1semisync_oub", cutoverAtomicityOptimisticSchema, false},
		{"optimistic_buffered", "t1semisync_obu", cutoverAtomicityOptimisticSchema, true},
		{"composite_unbuffered", "t1semisync_cub", cutoverAtomicityCompositeSchema, false},
		{"composite_buffered", "t1semisync_cbu", cutoverAtomicityCompositeSchema, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCutoverAtomicityTest(t, tc.tableName, tc.schema, tc.buffered)
		})
	}
}

// requireSemiSyncSourceActive fails the test if the source MySQL isn't
// running with semi-sync enabled and a short enough timeout to actually
// widen the binlog/visibility window. A silent skip here would make the
// dedicated workflow look green without actually exercising the case it
// exists to cover.
func requireSemiSyncSourceActive(t *testing.T) {
	t.Helper()

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var enabled string
	err = db.QueryRowContext(ctx, "SELECT @@global.rpl_semi_sync_source_enabled").Scan(&enabled)
	require.NoError(t, err, "rpl_semi_sync_source_enabled lookup failed — is the semi-sync plugin loaded?")
	require.Equal(t, "1", enabled, "rpl_semi_sync_source_enabled must be ON for this test to be meaningful")

	var timeoutMs int
	err = db.QueryRowContext(ctx, "SELECT @@global.rpl_semi_sync_source_timeout").Scan(&timeoutMs)
	require.NoError(t, err)
	require.LessOrEqual(t, timeoutMs, 1000,
		"rpl_semi_sync_source_timeout=%dms is too high — the workflow should set it to ~100ms so every commit deterministically pays the visibility-lag tax",
		timeoutMs)

	// Confirm the source is actually waiting (status ON means semi-sync is
	// engaged for the current connection's commits).
	var statusVar, statusVal string
	err = db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_status'").Scan(&statusVar, &statusVal)
	require.NoError(t, err, "Rpl_semi_sync_source_status not exposed — semi-sync plugin not loaded?")
	require.Equal(t, "ON", statusVal, "Rpl_semi_sync_source_status must be ON; got %q", statusVal)

	t.Logf("semi-sync source confirmed: timeout=%dms, status=%s", timeoutMs, statusVal)
}
