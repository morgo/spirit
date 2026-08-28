package migration

import (
	"testing"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/change"
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
	//
	// The flush term is present even here, where nothing was derived: the drain
	// runs change.DefaultFlushConcurrency concurrent REPLACEs on this same pool
	// regardless of whether autoscaling engaged, and a periodic flush overlaps
	// the copy. It was omitted from this sum until the flush shape became
	// derivable, which meant those connections were quietly borrowed from the
	// copier's and control plane's share.
	require.Equal(t, threads+2*writeThreads+change.DefaultFlushConcurrency+m.controlPlaneConns()+checksumOffPoolConns,
		m.dbConfig.MaxOpenConnections)
}

// TestFlushBoundsPreservesChangeDefaults pins the agreement between
// autoscale's rows-in-flight budget and the change package's defaults. It lives
// here because it is the assertion neither package can make: pkg/change imports
// pkg/autoscale, so autoscale cannot name change.DefaultBatchSize, and the
// budget would otherwise be a bare 8000 that drifts silently if either default
// moved.
//
// The consequence of drift is not a crash, it is a behaviour change nobody asked
// for: an instance small enough to hit the concurrency floor is supposed to
// receive exactly the pre-derivation values, so that this whole mechanism is a
// no-op below 16 vCPUs.
func TestFlushBoundsPreservesChangeDefaults(t *testing.T) {
	require.Equal(t, change.DefaultFlushConcurrency*change.DefaultBatchSize, autoscale.FlushRowsInFlight,
		"the rows-in-flight budget must remain the historical concurrency x batch size")
	require.Equal(t, change.DefaultFlushConcurrency, autoscale.MinFlushConcurrency,
		"the floor must be the historical default, so a small instance is unaffected")
	require.Greater(t, autoscale.MinFlushBatchSize, minAdaptiveBatchSizeForTest,
		"a derived batch size must stay well above the AIMD controller's distress floor")

	// Every instance size at or below the floor gets exactly today's pair.
	for vCPUs := 1; vCPUs <= change.DefaultFlushConcurrency+autoscale.VCPUReserve; vCPUs++ {
		concurrency, batchSize := autoscale.FlushBounds(vCPUs)
		require.Equal(t, change.DefaultFlushConcurrency, concurrency, "at %d vCPUs", vCPUs)
		require.Equal(t, change.DefaultBatchSize, batchSize, "at %d vCPUs", vCPUs)
	}
}

// minAdaptiveBatchSizeForTest mirrors the change package's unexported
// minAdaptiveBatchSize. Duplicated rather than exported: it is the AIMD
// controller's private distress floor, and the only thing outside that package
// with an interest in it is the assertion above.
const minAdaptiveBatchSizeForTest = 50
