package migration

import (
	"context"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
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

// warningRecorder keeps the WARN messages a Runner logs. The cap's warning is
// not decoration: a capped pool is a deliberate throughput trade, and the log
// line is the only place an operator learns it was made. Warning when nothing
// was capped is therefore its own small bug — see the exact-fit case below.
type warningRecorder struct {
	mu   sync.Mutex
	msgs []string
}

func (h *warningRecorder) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (h *warningRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *warningRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warningRecorder) WithGroup(string) slog.Handler      { return h }

func (h *warningRecorder) warnings() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.msgs...)
}

// newPoolSizingRunner builds the smallest Runner capPoolSize can be called on:
// it reads only the cap and the logger it warns through.
func newPoolSizingRunner(maxConnections int) (*Runner, *warningRecorder) {
	warnings := &warningRecorder{}
	return &Runner{
		migration: &Migration{MaxConnections: maxConnections},
		logger:    slog.New(warnings),
	}, warnings
}

// TestCapPoolSize covers the cap itself. Everything that sizes the pool sizes
// it for a guarantee — every worker ceiling added together, so no pool can
// starve another — which is the right sizing right up until the server cannot
// spare it, at which point the migration does not slow down, it dies on
// Error 1040. See capPoolSize.
//
// The cap is deliberately just min(): no floor, no phase-awareness, nothing
// derived. A safety net that reasons about what it is catching is a safety net
// that can be wrong about it.
func TestCapPoolSize(t *testing.T) {
	t.Run("under the cap the size passes through untouched", func(t *testing.T) {
		r, warnings := newPoolSizingRunner(128)
		require.Equal(t, 25, r.capPoolSize(25))
		require.Empty(t, warnings.warnings())
	})

	t.Run("a size equal to the cap is not capped", func(t *testing.T) {
		// The comparison is <=, not <: a size that exactly fits gets to keep
		// every guaranteed connection.
		//
		// At exactly the cap both branches return the same number, so the
		// warning is the only thing that can tell them apart — and a migration
		// told its pool was capped when it was not is a migration whose operator
		// goes looking for throughput that was never taken away.
		r, warnings := newPoolSizingRunner(128)
		require.Equal(t, 128, r.capPoolSize(128))
		require.Empty(t, warnings.warnings(), "an exact fit must not report contention")
	})

	t.Run("the derived ceilings that produced Error 1040 are capped", func(t *testing.T) {
		// A large Aurora instance: read ceiling half the box, write ceiling
		// twice the start, flush at autoscale.MaxFlushConcurrency, plus the
		// reserves. Comfortably past the default cap.
		want := 32 + 64 + autoscale.MaxFlushConcurrency + 3 + checksumOffPoolConns
		require.Greater(t, want, defaultMaxConnections, "or this case proves nothing")

		r, warnings := newPoolSizingRunner(defaultMaxConnections)
		require.Equal(t, defaultMaxConnections, r.capPoolSize(want),
			"a cap that binds must return the cap exactly, not the size asked for")
		require.Len(t, warnings.warnings(), 1, "capping is a throughput trade and must be said out loud")
	})

	t.Run("a cap below what the checksum pins is still obeyed", func(t *testing.T) {
		// The floor this used to apply is gone on purpose. Below the read
		// ceiling plus reserves the checksum phase will stall — every
		// transaction in its read pool holds a connection for the whole phase —
		// but that is the operator's number to get right. Silently raising a
		// limit spirit was handed is the worse failure, and the default sits far
		// above anything that could trip it.
		r, warnings := newPoolSizingRunner(10)
		require.Equal(t, 10, r.capPoolSize(32+64+32+5))
		require.Len(t, warnings.warnings(), 1)
	})

	t.Run("a negative cap restores the pre-cap behaviour", func(t *testing.T) {
		// Programmatic callers only: normalizeOptions turns a zero into the
		// default, so unbounded has to be asked for explicitly.
		r, warnings := newPoolSizingRunner(-1)
		require.Equal(t, 133, r.capPoolSize(133))
		require.Empty(t, warnings.warnings())
	})

	t.Run("an unfilled cap is unbounded rather than zero", func(t *testing.T) {
		// normalizeOptions is what turns a zero into the default, and not every
		// Runner is built through it (tests and embedders both construct
		// Migration literals). Reading a literal zero as "cap the pool at zero
		// connections" would deadlock the migration instantly, so it is read as
		// "no cap set".
		r, _ := newPoolSizingRunner(0)
		require.Equal(t, 133, r.capPoolSize(133))
	})
}

// TestMaxConnectionsDefaultMatchesItsFlag pins the constant to the kong tag it
// says it mirrors. The two are only connected by a comment, and the failure
// mode of drift is silent: the CLI would keep bounding at one number while
// every embedding orchestrator bounded at another.
func TestMaxConnectionsDefaultMatchesItsFlag(t *testing.T) {
	field, ok := reflect.TypeFor[Migration]().FieldByName("MaxConnections")
	require.True(t, ok)
	require.Equal(t, strconv.Itoa(defaultMaxConnections), field.Tag.Get("default"))

	// And an unset field lands on it, so a programmatic caller is bounded too.
	// That is not incidental: the derived ceilings only overflow on the large
	// instances, whose migrations are as likely to be driven by an embedder.
	m := &Migration{Statement: "ALTER TABLE t1 ENGINE=InnoDB", Database: "test"}
	_, err := m.normalizeOptions()
	require.NoError(t, err)
	require.Equal(t, defaultMaxConnections, m.MaxConnections)
}
