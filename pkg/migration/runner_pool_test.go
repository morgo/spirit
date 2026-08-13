package migration

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestControlPlaneConns documents the connection headroom the main pool
// reserves above the copy hot path: a fixed +2 (checkpoint INSERT and the
// replication-flush poll) plus one per change table (AutoUpdateStatistics runs
// a goroutine each). A single fixed spare could not cover these once the
// copier + applier saturated the budget — see controlPlaneConns.
func TestControlPlaneConns(t *testing.T) {
	// Single-table: checkpoint + replication poll + one stats updater.
	single := &Runner{changes: make([]*tableChange, 1)}
	require.Equal(t, 3, single.controlPlaneConns())

	// Multi-table: the stats-updater term scales with the number of tables, so
	// a multi-table ALTER does not starve them behind the fixed headroom.
	multi := &Runner{changes: make([]*tableChange, 3)}
	require.Equal(t, 5, multi.controlPlaneConns())
}

// TestAutoscalingLeavesThreadFlagsAloneWhenItCannotEngage is the other half of
// "autoscaling owns the thread counts": it only owns them when it can actually
// steer. Asking for autoscaling against a target with no continuous load signal
// (any non-Aurora server, which is what the test suite runs against) must leave
// --threads and --write-threads exactly as configured, because the pools will
// run fixed at those values for the whole migration.
//
// The engaged path cannot be exercised here — it needs a real Aurora instance to
// read a vCPU count from. autoscale.ReadBounds covers the sizing it derives, and
// the assertion below covers the branch that decides whether to apply it.
func TestAutoscalingLeavesThreadFlagsAloneWhenItCannotEngage(t *testing.T) {
	testutils.NewTestTable(t, "autoscale_flags",
		`CREATE TABLE autoscale_flags (id INT NOT NULL PRIMARY KEY, pad VARCHAR(32))`)
	testutils.RunSQL(t, `INSERT INTO autoscale_flags VALUES (1, 'a'), (2, 'b')`)

	// Values distinct from both the defaults and each other, so an override
	// would be unmistakable.
	const threads, writeThreads = 3, 5
	m := NewTestRunner(t, "autoscale_flags", "ENGINE=InnoDB",
		WithAutoscaling(), WithThreads(threads), WithWriteThreads(writeThreads))
	require.NoError(t, m.Run(t.Context()))
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	require.Equal(t, threads, m.migration.Threads,
		"--threads must survive an autoscaling request that could not engage")
	require.Equal(t, writeThreads, m.migration.WriteThreads,
		"--write-threads must survive an autoscaling request that could not engage")
	// The connection budget is derived from the flags, not from an instance vCPU
	// count that was never read. The two pools budget differently on purpose:
	// the write ceiling is still 2x, because the applier's controller can grow
	// on any target that gains a gradual signal, while the read term stays at
	// the flag value — with no instance-derived ceiling the readers cannot grow,
	// and the checksum pre-creates its whole pool under the table lock, so
	// provisioning 2x would only lengthen that lock window for capacity nothing
	// can use.
	require.Equal(t, threads+2*writeThreads+m.controlPlaneConns()+checksumOffPoolConns,
		m.dbConfig.MaxOpenConnections)
}
