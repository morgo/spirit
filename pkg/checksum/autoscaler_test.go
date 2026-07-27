package checksum

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gradualStub is a throttler with a settable continuous signal, standing in
// for the Aurora throttlers (the only ones that implement GradualThrottler).
type gradualStub struct {
	throttler.Noop
	util float64
}

func (g *gradualStub) Utilization() float64 { return g.util }

var _ throttler.GradualThrottler = (*gradualStub)(nil)

// newTestScaler builds a scaler over a real limiter, plus a mutable backlog
// value the test can drive.
func newTestScaler(t throttler.Throttler, start, maxThreads int) (*checksumScaler, *autoscale.Limiter, *int) {
	backlog := new(int)
	l := autoscale.NewLimiter(start)
	s := newChecksumScaler(t, l, func() int { return *backlog }, start, maxThreads,
		slog.New(slog.DiscardHandler), &metrics.NoopSink{})
	return s, l, backlog
}

// tickN drives n ticks, which is how the tests wait out cooldowns without real
// time.
func tickN(s *checksumScaler, n int) {
	for range n {
		s.tick(context.Background())
	}
}

// flushCycles simulates n change-feed flush cycles, driving the scaler at the
// real tick:flush ratio.
//
// This is the shape the backlog signal has to survive: within a cycle the
// pending count climbs monotonically on every sample (the feed buffers), and
// when the flush lands it drops to residual. So "rose several ticks in a row"
// describes a perfectly healthy feed, and only the residual distinguishes one
// that is keeping up from one that is not.
//
// residual is called per cycle so a test can hold it at zero (healthy), or ramp
// it (the feed is not finishing what it starts).
func flushCycles(s *checksumScaler, backlog *int, n, perTickGrowth int, residual func(cycle int) int) {
	ticksPerFlush := int(change.DefaultFlushInterval / csTick)
	for c := range n {
		base := residual(c)
		for i := range ticksPerFlush {
			*backlog = base + (i+1)*perTickGrowth
			s.tick(context.Background())
		}
		// The flush lands, leaving the residual behind.
		*backlog = base
		s.tick(context.Background())
	}
}

func TestScalerGrowsAndShedsOnUtilization(t *testing.T) {
	g := &gradualStub{util: 0.1} // well below the low watermark
	s, l, _ := newTestScaler(g, 4, 8)

	// Growth is cooldown-gated: one step, then a wait.
	s.tick(context.Background())
	assert.Equal(t, 5, s.current)
	assert.Equal(t, 5, l.Limit(), "the limiter is the thing that actually gates workers")
	s.tick(context.Background())
	assert.Equal(t, 5, s.current, "second consecutive tick is inside the cooldown")

	tickN(s, autoscale.CooldownTicks)
	assert.Equal(t, 6, s.current)

	// Into the shed zone. Unlike growth, shedding is gated only by the down
	// cooldown, so it fires on the very next tick even though we just grew.
	g.util = 0.85
	s.tick(context.Background())
	assert.Equal(t, 5, s.current)
	assert.Equal(t, 5, l.Limit())
}

func TestScalerHoldsInDeadBand(t *testing.T) {
	g := &gradualStub{util: 0.55} // between the watermarks
	s, l, _ := newTestScaler(g, 4, 8)
	tickN(s, 10)
	assert.Equal(t, 4, s.current, "the dead band must not drift")
	assert.Equal(t, 4, l.Limit())
}

func TestScalerHalvesAtPanicThreshold(t *testing.T) {
	g := &gradualStub{util: 1.4}
	s, l, _ := newTestScaler(g, 8, 16)
	s.tick(context.Background())
	assert.Equal(t, 4, s.current)
	assert.Equal(t, 4, l.Limit())

	// Consecutive halvings wait out the cooldown, so a single stale signal
	// window cannot collapse the pool to the floor in a few ticks.
	s.tick(context.Background())
	assert.Equal(t, 4, s.current)
	tickN(s, autoscale.CooldownTicks)
	assert.Equal(t, 2, s.current)
}

func TestScalerRespectsBounds(t *testing.T) {
	g := &gradualStub{util: 0.0}
	s, _, _ := newTestScaler(g, 4, 5)
	tickN(s, 30)
	assert.Equal(t, 5, s.current, "must not exceed MaxThreads")

	g.util = 5.0
	tickN(s, 30)
	assert.Equal(t, 1, s.current, "must not shed below one worker, or the pass stalls")
}

func TestScalerMaxNeverBelowStart(t *testing.T) {
	// A caller passing a smaller ceiling than the start value would otherwise
	// have the pool immediately clamped below the transactions provisioned for
	// it.
	s, _, _ := newTestScaler(&gradualStub{util: 0.5}, 8, 2)
	assert.Equal(t, 8, s.max)
	assert.Equal(t, 8, s.current)
}

func TestScalerHealthyFeedNeverVetoes(t *testing.T) {
	// The regression this whole signal design exists for. A busy but healthy
	// feed buffers a large backlog between flushes and fully drains at each
	// one. Every sample within a cycle is higher than the last, and the peak is
	// far above csBacklogVetoDeltas — so any signal keyed on "the backlog is
	// large and rising" fires here, every cycle, forever.
	//
	// On Aurora that was unrecoverable: with a mid-dead-band utilization the
	// zone law holds, so there was no path back up and the pass ratcheted down
	// to a single worker on a completely healthy system.
	g := &gradualStub{util: 0.55} // dead band: no growth to mask a wrong shed
	s, _, backlog := newTestScaler(g, 4, 8)
	flushCycles(s, backlog, 10, 5000, func(int) int { return 0 })
	assert.Equal(t, 4, s.current, "a feed that drains at every flush is keeping up")
	assert.False(t, s.backlogLosing)
}

func TestScalerBacklogVetoShedsWithoutGradualSignal(t *testing.T) {
	// The stock-MySQL case: a plain throttler provides no continuous signal, so
	// the change-feed backlog is the only lever. This is the main thing the
	// non-Aurora path buys.
	//
	// Here the flushes no longer drain: each one leaves more behind than the
	// last, which is the feed reporting it cannot finish what it starts.
	s, l, backlog := newTestScaler(&throttler.Noop{}, 4, 8)
	require.Nil(t, s.gradual, "Noop must not be mistaken for a continuous signal")

	flushCycles(s, backlog, 6, 5000, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	assert.Less(t, s.current, 4, "a growing residual must shed")
	assert.Equal(t, s.current, l.Limit())
}

func TestScalerResidualBelowThresholdNeverVetoes(t *testing.T) {
	// Flushes are leaving a growing residual, but one small enough that
	// pkg/change would already consider the feed drained enough to cut over.
	// Not worth a worker.
	s, _, backlog := newTestScaler(&throttler.Noop{}, 4, 8)
	flushCycles(s, backlog, 10, 5000, func(cycle int) int { return cycle * 100 })
	assert.Equal(t, 4, s.current)
}

func TestScalerSteadyLargeResidualNeverVetoes(t *testing.T) {
	// A large but *flat* residual means the feed is holding its ground — it is
	// keeping up with the write rate, just at a steady-state depth. Only a
	// residual that keeps climbing means it is losing.
	s, _, backlog := newTestScaler(&throttler.Noop{}, 4, 8)
	flushCycles(s, backlog, 10, 5000, func(int) int { return csBacklogVetoDeltas * 3 })
	assert.Equal(t, 4, s.current)
}

func TestScalerRecoversToStartAfterBacklogClears(t *testing.T) {
	s, _, backlog := newTestScaler(&throttler.Noop{}, 4, 8)

	flushCycles(s, backlog, 6, 5000, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	require.Less(t, s.current, 4, "expected the veto to have shed at least once")

	// The feed catches up. Without recovery, one transient burst would
	// permanently cost throughput for the rest of the pass.
	*backlog = 0
	tickN(s, 30)
	assert.Equal(t, 4, s.current, "should recover to the configured start")

	// ...but no further: with no continuous signal there is nothing to justify
	// growing past what the operator asked for.
	tickN(s, 30)
	assert.Equal(t, 4, s.current)
}

func TestScalerBacklogVetoOutranksIdleUtilization(t *testing.T) {
	// The signals genuinely disagree here: the server looks idle, but the feed
	// is falling behind. The feed has to win, because its writes are a small
	// share of server load but a hard prerequisite for cut-over.
	//
	// The residual is necessarily blind for the first window — a trend needs two
	// troughs — so utilization is briefly in sole charge and grows on what is,
	// at that point, the only valid signal. What matters is that once the feed's
	// verdict is available it dominates: growth is suppressed for as long as the
	// feed is losing, and the count is driven all the way to the floor rather
	// than settling somewhere the two signals cancel out.
	g := &gradualStub{util: 0.0}
	s, _, backlog := newTestScaler(g, 4, 8)
	flushCycles(s, backlog, 20, 5000, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	assert.Equal(t, 1, s.current, "a persistently losing feed must beat an idle-looking server")
}

func TestScalerPanicZoneOutranksBacklogVeto(t *testing.T) {
	// Both signals want to shed, and the panic zone sheds multiplicatively.
	// Evaluating the veto first would let a tick inside its cooldown consume the
	// panic response, degrading backoff to -1 per cooldown exactly when the
	// server is most overloaded.
	g := &gradualStub{util: 1.4}
	s, _, backlog := newTestScaler(g, 8, 16)
	*backlog = csBacklogVetoDeltas * 10
	s.backlogLosing = true // as if a window had just closed on a rising residual

	s.tick(context.Background())
	assert.Equal(t, 4, s.current, "should halve, not decrement")
}

func TestScalerAuroraRecoversToStartInDeadBand(t *testing.T) {
	// Aurora counterpart of TestScalerRecoversToStartAfterBacklogClears: the
	// dead band must not strand a backlog-shed pool at the reduced count. It
	// recovers to start, but growth beyond start still needs a headroom signal.
	g := &gradualStub{util: 0.55}
	s, _, backlog := newTestScaler(g, 4, 8)
	flushCycles(s, backlog, 6, 5000, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	require.Less(t, s.current, 4)

	*backlog = 0
	tickN(s, 40)
	assert.Equal(t, 4, s.current, "dead band recovers to start and stops there")
}

func TestScalerAuroraGrowsPastStartAfterBacklogClears(t *testing.T) {
	// With headroom on the continuous signal, recovery keeps going past start
	// under the utilization law.
	g := &gradualStub{util: 0.1}
	s, _, backlog := newTestScaler(g, 4, 8)
	flushCycles(s, backlog, 20, 5000, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	require.Less(t, s.current, 4, "expected the losing feed to have shed below start")

	*backlog = 0
	tickN(s, 60)
	assert.Equal(t, 8, s.current, "should climb to MaxThreads while there is headroom")
}

func TestResidualWindowSpansAFlush(t *testing.T) {
	// The window must contain a flush trough or the signal measures nothing but
	// the rising edge. Two flush intervals give margin for phase misalignment.
	assert.Equal(t, 12, residualWindowTicks(30*time.Second, 5*time.Second))
	assert.GreaterOrEqual(t, csBacklogWindowTicks,
		2*int(change.DefaultFlushInterval/csTick), "window must span at least two flushes")
	assert.Equal(t, 2, residualWindowTicks(time.Second, time.Hour), "floored so a previous window exists")
	assert.Equal(t, 2, residualWindowTicks(time.Second, 0), "must not divide by zero")
}

func TestScalerNilBacklogIsSafe(t *testing.T) {
	// Guards the optional-signal path: a checker without a feed accessor must
	// still scale on utilization rather than panic.
	l := autoscale.NewLimiter(4)
	s := newChecksumScaler(&gradualStub{util: 0.1}, l, nil, 4, 8,
		slog.New(slog.DiscardHandler), nil)
	tickN(s, 20)
	assert.Equal(t, 8, s.current)
}
