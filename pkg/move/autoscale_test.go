package move

import (
	"errors"

	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/host"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

func TestMoveAutoscaleBounds(t *testing.T) {
	separate := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	shared := []host.Group{{Indices: []int{0, 1}}}
	_, heterogeneous := moveAutoscaleBounds([]int{64, 16}, separate, 128, true, true)
	_, reversed := moveAutoscaleBounds([]int{16, 64}, separate, 128, true, true)
	require.Equal(t, heterogeneous, reversed)
	require.Equal(t, autoscale.WriteStart(16), heterogeneous.StartThreads)
	_, colocated := moveAutoscaleBounds([]int{16}, shared, 128, true, true)
	require.Equal(t, heterogeneous.StartThreads/2, colocated.StartThreads)
	require.Equal(t, heterogeneous.MaxThreads/2, colocated.MaxThreads)
	require.Equal(t, heterogeneous.MaxReadThreads/2, colocated.MaxReadThreads)
	read, capped := moveAutoscaleBounds([]int{128, 128}, separate, 8, true, true)
	require.LessOrEqual(t, capped.MaxThreads*2, 8)
	require.LessOrEqual(t, capped.MaxReadThreads, 8)
	require.LessOrEqual(t, read, capped.MaxReadThreads)
	for _, sizes := range [][]int{nil, {64, 0}, {2, 64}} {
		_, config := moveAutoscaleBounds(sizes, separate, 128, true, true)
		require.False(t, config.Enabled)
	}
	_, minimum := moveAutoscaleBounds([]int{16}, shared, 1, true, true)
	require.Equal(t, 1, minimum.StartThreads)
	require.Equal(t, 1, minimum.MaxThreads)
}

func TestMoveAutoscaleNonAurora(t *testing.T) {
	tt := testutils.NewTestTable(t, "move_autoscale_probe", "CREATE TABLE move_autoscale_probe (id INT PRIMARY KEY)")
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Move{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true})
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	r.targets = []applier.Target{{DB: tt.DB, Config: config}}
	require.NoError(t, r.setupThrottling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Empty(t, r.monitorDBs)
	require.False(t, r.currentThrottler().IsThrottled())
}

func TestMoveAutoscaleDisabled(t *testing.T) {
	r, err := NewRunner(&Move{Threads: 3, WriteThreads: 5})
	require.NoError(t, err)
	// No targets or database config: the disabled path must not probe.
	require.NoError(t, r.setupThrottling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
}

func TestMoveThrottleStatus(t *testing.T) {
	r := &Runner{}
	r.setThrottler(&throttler.Mock{})
	require.True(t, r.throttleStatus(status.CopyRows).Throttled)
	require.False(t, r.throttleStatus(status.Checksum).Throttled)
	require.Empty(t, r.throttleStatus(status.CutOver))
	require.Empty(t, r.throttleStatus(status.Close))
}

func TestMoveAutoscaleFitsFixedPool(t *testing.T) {
	for _, start := range []int{4, 16} {
		r, err := NewRunner(&Move{Threads: 2, MaxConnections: 16, WriteThreads: 20})
		require.NoError(t, err)
		// Simulate counts resolved by the Aurora probe after flag validation.
		r.move.Threads = start
		r.autoscale.Enabled = true
		r.autoscale.MaxReadThreads = 32
		require.NoError(t, r.fitReadThreadsToPools())
		require.Equal(t, min(start, 10), r.move.Threads)
		require.Equal(t, 10, r.autoscale.MaxReadThreads)
		require.Equal(t, 16, r.move.MaxConnections)
		require.Equal(t, 20, r.move.WriteThreads)
	}
}

// Growth above the start needs the commit-latency backstop when a target runs
// the redo-aware signal, which cannot see the redo log oversubscribed. That is
// migration's rule (throttler.ResolveMaxWriteThreads), now driven by the
// target probe and --max-commit-latency rather than assumed.
func TestMoveAutoscaleWriteCeilingBackstop(t *testing.T) {
	separate := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	_, guarded := moveAutoscaleBounds([]int{16, 16}, separate, 128, true, true)
	require.Equal(t, 2*guarded.StartThreads, guarded.MaxThreads)
	_, unguarded := moveAutoscaleBounds([]int{16, 16}, separate, 128, true, false)
	require.Equal(t, unguarded.StartThreads, unguarded.MaxThreads)
	_, fallback := moveAutoscaleBounds([]int{16, 16}, separate, 128, false, false)
	require.Equal(t, 2*fallback.StartThreads, fallback.MaxThreads)
}

func TestMoveFlushBounds(t *testing.T) {
	one := []host.Group{{Indices: []int{0}}}
	separate := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	colocated := []host.Group{{Indices: []int{0, 1, 2, 3}}}

	// One source, one target: exactly what migration and sync derive.
	width, batch := moveFlushBounds([]int{64}, one, 1, 1024)
	wantWidth, wantBatch := autoscale.FlushBounds(64)
	require.Equal(t, wantWidth, width)
	require.Equal(t, wantBatch, batch)

	// Every source's feed fans out to the busiest host, as do co-located shards.
	width, _ = moveFlushBounds([]int{64, 64}, separate, 2, 1024)
	require.Equal(t, wantWidth/2, width)
	width, _ = moveFlushBounds([]int{64}, colocated, 1, 1024)
	require.Equal(t, max(autoscale.MinFlushConcurrency, wantWidth/4), width)

	// Sized from the smallest target, whichever order the targets are in.
	a, _ := moveFlushBounds([]int{64, 16}, separate, 1, 1024)
	b, _ := moveFlushBounds([]int{16, 64}, separate, 1, 1024)
	require.Equal(t, a, b)
	smallWidth, _ := autoscale.FlushBounds(16)
	require.Equal(t, smallWidth, a)

	// Never narrower than the default every move used before, so the
	// derivation cannot slow a drain down — not by dividing across sources
	// and shards...
	for _, sources := range []int{1, 2, 8, 64} {
		width, batch = moveFlushBounds([]int{16}, colocated, sources, 1024)
		require.Equal(t, autoscale.MinFlushConcurrency, width)
		require.Equal(t, autoscale.FlushBatchSize(width), batch)
	}
	// ...and not by the client ceiling either.
	width, batch = moveFlushBounds([]int{64}, one, 2, 4)
	require.Equal(t, autoscale.MinFlushConcurrency, width)
	require.Equal(t, autoscale.FlushBatchSize(width), batch)
	// Above the floor, the client ceiling still caps the width.
	width, _ = moveFlushBounds([]int{64}, one, 1, 10)
	require.Equal(t, 10, width)

	// Below MinVCPUs nothing is derived and the change package defaults apply.
	width, batch = moveFlushBounds([]int{64, 2}, separate, 1, 1024)
	require.Zero(t, width)
	require.Zero(t, batch)
}

// closeCountingThrottler records whether the runner closed a signal it owns.
type closeCountingThrottler struct {
	throttler.Mock
	closes int
}

func (c *closeCountingThrottler) Close() error { c.closes++; return nil }

// The Aurora load throttlers pace the copy whether or not autoscaling is
// enabled, matching migration. Before this, move built them only when
// autoscaling engaged, so a move on Aurora without the flag ran unthrottled.
func TestMoveThrottlesWithoutAutoscaling(t *testing.T) {
	r, err := NewRunner(&Move{Threads: 3, WriteThreads: 5})
	require.NoError(t, err)
	signal := &closeCountingThrottler{}
	groups := []host.Group{{Indices: []int{0}}}
	require.NoError(t, r.applyAuroraResults(t.Context(), groups, []throttler.AuroraResult{{Throttlers: []throttler.Throttler{signal}}}))
	require.True(t, r.currentThrottler().IsThrottled())
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Zero(t, r.flushConcurrency)
	require.NoError(t, r.Close())
	require.Equal(t, 1, signal.closes)
}

// A target whose probe failed or that is not Aurora keeps every other
// target's throttling; only autoscaling needs all of them.
func TestMoveAutoscaleNeedsEveryTarget(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	groups := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	for _, other := range []throttler.AuroraResult{{}, {ProbeErr: errors.New("probe failed")}} {
		r, err := NewRunner(&Move{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true})
		require.NoError(t, err)
		r.targets = []applier.Target{{Config: config}, {Config: config, KeyRange: "80-"}}
		signal := &closeCountingThrottler{}
		results := []throttler.AuroraResult{{Throttlers: []throttler.Throttler{signal}}, other}
		require.NoError(t, r.applyAuroraResults(t.Context(), groups, results))
		require.True(t, r.currentThrottler().IsThrottled())
		require.False(t, r.autoscale.Enabled)
		require.Equal(t, 3, r.move.Threads)
		require.Equal(t, 5, r.move.WriteThreads)
		require.NoError(t, r.Close())
		require.Equal(t, 1, signal.closes)
	}
}

// --max-commit-latency matches migrate: the Go-API zero value is not
// replaced with the CLI default, so zero disables the throttler here too.
func TestMoveMaxCommitLatencyZero(t *testing.T) {
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	require.Zero(t, r.move.MaxCommitLatency)
}
