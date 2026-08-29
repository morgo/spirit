package change

import (
	"testing"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/stretchr/testify/require"
)

// newLoadTestMap builds a bare bufferedMap at the widest shape the runner can
// derive — autoscale.FlushBounds' 32x250 — with a settable load signal. That is
// the shape the load controller exists for: it is the one the instance-derived
// widening produces, and the one that was observed holding full width in
// production while the copier shed to almost nothing.
func newLoadTestMap(underLoad *bool) *bufferedMap {
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.flushConcurrency, sub.batchSize = autoscale.FlushBounds(64)
	sub.underLoad = func() bool { return *underLoad }
	return sub
}

// TestFlushNarrowsUnderLoadAndRecovers pins the load controller's shape: narrow
// while the signal says loaded, stop at the floor, and only widen again after a
// sustained run of drains with the signal clear.
func TestFlushNarrowsUnderLoadAndRecovers(t *testing.T) {
	loaded := false
	sub := newLoadTestMap(&loaded)

	require.Equal(t, 32, sub.effectiveFlushConcurrency())
	require.Equal(t, 250, sub.effectiveBatchSize())

	// A clear signal moves nothing, no matter how often it is sampled.
	for range 10 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, 32, sub.effectiveFlushConcurrency())

	loaded = true
	sub.adaptFlushLoad()
	require.Equal(t, 16, sub.effectiveFlushConcurrency())
	sub.adaptFlushLoad()
	require.Equal(t, 8, sub.effectiveFlushConcurrency())

	// The floor holds no matter how long the load persists. This is the safety
	// property the whole design rests on: the drain must keep advancing the
	// binlog position, so load may take back the widening and nothing more.
	for range 20 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, DefaultFlushConcurrency, sub.effectiveFlushConcurrency())

	// A single clear drain is not enough to widen again.
	loaded = false
	sub.adaptFlushLoad()
	require.Equal(t, 8, sub.effectiveFlushConcurrency())

	for range cleanDrainsToRecover - 1 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, 16, sub.effectiveFlushConcurrency())

	// Load part-way through a recovery run resets the streak, so the run has to
	// start over rather than resuming where it left off. Asserted by counting a
	// full run *minus one* and requiring no recovery: a streak that merely
	// carried over would already have widened by then, and stopping the check
	// one drain earlier cannot tell the two apart.
	sub.adaptFlushLoad()
	loaded = true
	sub.adaptFlushLoad()
	require.Equal(t, 8, sub.effectiveFlushConcurrency())
	loaded = false
	for range cleanDrainsToRecover - 1 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, 8, sub.effectiveFlushConcurrency(), "the clear streak must have reset")

	// Recovery stops at the configured width; it never overshoots.
	for range 20 * cleanDrainsToRecover {
		sub.adaptFlushLoad()
	}
	require.Equal(t, 32, sub.effectiveFlushConcurrency())
	require.Equal(t, 250, sub.effectiveBatchSize())
}

// TestFlushLoadShedHoldsRowsInFlight is the reason shedding on load is
// affordable at all. The copier pays for a shed thread in throughput; the drain
// does not have to, because concurrency x batch is a constant budget
// (autoscale.FlushRowsInFlight) and only the statement count is what the load
// signal is measuring. Narrowing 32x250 to 8x1000 moves the same rows through a
// quarter of the server threads.
//
// If this ever fails, load shedding has started costing the feed progress — and
// a feed that falls behind its binlog retention window fails the migration
// outright, which is a strictly worse outcome than the load it was shedding.
func TestFlushLoadShedHoldsRowsInFlight(t *testing.T) {
	loaded := true
	sub := newLoadTestMap(&loaded)

	require.Equal(t, autoscale.FlushRowsInFlight, sub.effectiveFlushConcurrency()*sub.effectiveBatchSize())

	sub.adaptFlushLoad()
	require.Equal(t, 16, sub.effectiveFlushConcurrency())
	require.Equal(t, 500, sub.effectiveBatchSize())
	require.Equal(t, autoscale.FlushRowsInFlight, sub.effectiveFlushConcurrency()*sub.effectiveBatchSize())

	sub.adaptFlushLoad()
	require.Equal(t, 8, sub.effectiveFlushConcurrency())
	require.Equal(t, DefaultBatchSize, sub.effectiveBatchSize(),
		"the re-paired batch must land exactly on the historical default")
	require.Equal(t, autoscale.FlushRowsInFlight, sub.effectiveFlushConcurrency()*sub.effectiveBatchSize())
}

// TestFlushLoadShedNeverWidensPastTheHistoricalBatch caps the other side of the
// trade. Re-pairing buys statement count with lock footprint, and a batch takes
// a next-key lock per row per UNIQUE secondary index — the exact surface the
// contention controller exists to shrink. The cap is the batch the drain would
// have used at DefaultFlushConcurrency, so the widest statement load shedding
// can produce is one the pre-derivation code produced routinely, and never a
// statement no version of spirit has issued.
func TestFlushLoadShedNeverWidensPastTheHistoricalBatch(t *testing.T) {
	loaded := true
	sub := newLoadTestMap(&loaded)
	// Force the concurrency below the load floor via the contention controller,
	// so the load shift is working from an already-narrow width.
	sub.concurrencyPenalty.Store(3) // 32 -> 4, batch 250 -> 50 (minAdaptiveBatchSize)
	require.Equal(t, 4, sub.effectiveFlushConcurrency())

	for range 10 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, 4, sub.effectiveFlushConcurrency(),
		"load must not narrow below a width contention already chose")
	require.Equal(t, minAdaptiveBatchSize, sub.effectiveBatchSize(),
		"with no narrowing to re-pair against, the contention batch stands unchanged")
	require.Zero(t, sub.loadPenalty.Load(), "a penalty that can never be spent must not accumulate")
}

// TestFlushLoadShedIsANoOpBelowTheWidening is the compatibility guarantee. An
// instance too small for autoscale.FlushBounds to widen anything runs at
// DefaultFlushConcurrency, which is already the floor — so it never acquired the
// imbalance this controller corrects, and the controller must not invent one for
// it by narrowing a drain that was never widened.
func TestFlushLoadShedIsANoOpBelowTheWidening(t *testing.T) {
	loaded := true
	sub := newLoadTestMap(&loaded)
	sub.flushConcurrency, sub.batchSize = autoscale.FlushBounds(8) // below the widening threshold
	require.Equal(t, DefaultFlushConcurrency, sub.flushConcurrency)

	for range 10 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, DefaultFlushConcurrency, sub.effectiveFlushConcurrency())
	require.Equal(t, DefaultBatchSize, sub.effectiveBatchSize())
	require.Zero(t, sub.loadPenalty.Load(), "a penalty that can never be spent must not accumulate")
}

// TestFlushWithoutLoadSignalIsUnchanged covers every caller that supplies no
// signal: out-of-tree change.Source implementations, spirit move, and every
// existing test. Nil must mean "as before", not "assume loaded" or "assume
// idle-and-recover" — either would change the width of drains for callers that
// never opted in.
func TestFlushWithoutLoadSignalIsUnchanged(t *testing.T) {
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.flushConcurrency, sub.batchSize = autoscale.FlushBounds(64)
	require.Nil(t, sub.underLoad)

	for range 10 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, 32, sub.effectiveFlushConcurrency())
	require.Equal(t, 250, sub.effectiveBatchSize())
	require.Zero(t, sub.loadPenalty.Load())
}

// TestFlushLoadAndContentionCompose pins the two controllers against each other.
// They run on different signals and want opposite things from batch size —
// contention shrinks it to reduce a statement's lock footprint, load widens it to
// hold rows in flight across fewer statements — so the risk is that one silently
// undoes the other. Each must keep its own accounting.
func TestFlushLoadAndContentionCompose(t *testing.T) {
	loaded := true
	sub := newLoadTestMap(&loaded)

	sub.adaptFlushLoad() // 32 -> 16, batch 250 -> 500
	require.Equal(t, 16, sub.effectiveFlushConcurrency())

	// Contention now halves on top of the load shed, and its batch decision
	// wins: a contended drain wants a smaller lock footprint, and re-pairing
	// must not hand that back.
	sub.adaptFlushConcurrency(true)
	require.Equal(t, 8, sub.effectiveFlushConcurrency())
	require.Equal(t, 250, sub.effectiveBatchSize())

	// The invariant that keeps the two honest: load shedding re-pairs against
	// whatever rows-in-flight budget the contention controller has left, not
	// against the configured one. One contention step cuts *both* terms, so it
	// is a 4x cut to the budget — deliberately removing rows from flight — and
	// re-pairing back to the configured 8000 would have handed every one of
	// them straight back, silently disarming the deadlock controller.
	require.Equal(t, autoscale.FlushRowsInFlight/4, sub.effectiveFlushConcurrency()*sub.effectiveBatchSize())

	// Clearing the load recovers only the load step; the contention penalty is
	// untouched, because clean-drain accounting is the only thing that repays it.
	loaded = false
	for range cleanDrainsToRecover {
		sub.adaptFlushLoad()
	}
	require.Equal(t, int64(1), sub.concurrencyPenalty.Load(), "load recovery must not repay a contention penalty")
	require.Equal(t, 16, sub.effectiveFlushConcurrency())
}

// TestFlushLoadShedNeverRaisesAContendedWidth is the direction that would be
// silently wrong rather than merely suboptimal: a load penalty *widening* the
// drain.
//
// The load floor is applied through shiftDown, whose floor argument is a max().
// Hand it a floor above the width it was given and it returns the floor — so a
// drain that contention had narrowed to 4, on evidence of real deadlocks, would
// be pushed back up to 8 by a signal asking for *less* concurrency. That is why
// the floor is min()'d against the contention width rather than being the flat
// DefaultFlushConcurrency the doc comment describes.
//
// It needs both penalties outstanding at once, which is why it is not covered by
// the narrower cases above: with no load penalty the shift is skipped entirely,
// and with no contention penalty the floor can never sit above the width.
func TestFlushLoadShedNeverRaisesAContendedWidth(t *testing.T) {
	loaded := true
	sub := newLoadTestMap(&loaded)

	// Shed on load first, so a load penalty is outstanding.
	sub.adaptFlushLoad()
	sub.adaptFlushLoad()
	require.Equal(t, 8, sub.effectiveFlushConcurrency())
	require.Equal(t, int64(2), sub.loadPenalty.Load())

	// Now contention narrows past the load floor.
	for range 3 {
		sub.adaptFlushConcurrency(true)
	}
	require.Equal(t, 4, sub.contendedFlushConcurrency())
	require.Equal(t, 4, sub.effectiveFlushConcurrency(),
		"an outstanding load penalty must never widen a drain contention narrowed")
}

// TestDrainAppliesTheLoadShedShape drives a real drain, and is the only test
// here that touches the wiring rather than the controller.
//
// It pins two things the direct-call tests above cannot. First that
// drainMapSnapshot samples the signal at all — with the call removed the whole
// feature is inert and every other test in this file still passes, because they
// drive the controller by hand. Second that it samples *before* the widths are
// read: the drain that observes the load has to be the drain that narrows, not
// the one after it. A flush can hold flushMu for minutes, so sampling at the
// far end would mean reacting to the load the server was under when the
// previous drain started.
func TestDrainAppliesTheLoadShedShape(t *testing.T) {
	loaded := true
	sub := newLoadTestMap(&loaded)
	fake := sub.applier.(*countingApplier)

	const totalRows = 2000
	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	require.Equal(t, totalRows, sub.Length())

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())

	require.Equal(t, 16, sub.effectiveFlushConcurrency(), "the drain must have sampled the load signal")

	// 250 rows per batch is the configured shape and 500 the shed one, so the
	// widest statement the drain actually issued says which shape it used.
	widest, applied := 0, 0
	for _, call := range fake.upserts() {
		widest = max(widest, len(call))
		applied += len(call)
	}
	require.Equal(t, totalRows, applied, "every row must still land")
	require.Equal(t, 500, widest, "the drain must render the shed shape, not the shape it started with")
}

// TestFlushLoadShedCapsTheBatchForConfiguredWidths exercises the re-pairing cap
// on the only shapes that can reach it. In-tree the width is capped at
// autoscale.MaxFlushConcurrency and the floor is DefaultFlushConcurrency, so the
// ratio never exceeds 4 and a 250-row batch re-pairs to exactly DefaultBatchSize
// — the cap is touched but never binds.
//
// ClientConfig.FlushConcurrency and BatchSize are public knobs, though, and an
// out-of-tree caller (a change.Source of its own, spirit move) can configure a
// width the in-tree derivation would never produce. Without the cap, shedding a
// 64-wide drain to the floor would multiply its batch eightfold and issue a
// statement holding eight times the next-key locks of anything spirit has ever
// sent — turning a load-shedding step into a deadlock-generating one.
func TestFlushLoadShedCapsTheBatchForConfiguredWidths(t *testing.T) {
	loaded := true
	sub := newLoadTestMap(&loaded)
	sub.flushConcurrency, sub.batchSize = 64, DefaultBatchSize

	for range 10 {
		sub.adaptFlushLoad()
	}
	require.Equal(t, DefaultFlushConcurrency, sub.effectiveFlushConcurrency(), "shed all the way to the floor")
	require.Equal(t, DefaultBatchSize, sub.effectiveBatchSize(),
		"re-pairing must not widen a statement past the historical batch")
}
