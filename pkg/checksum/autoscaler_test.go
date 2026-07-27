package checksum

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/autoscale"
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

func TestScalerBacklogVetoShedsWithoutGradualSignal(t *testing.T) {
	// The stock-MySQL case: a plain throttler provides no continuous signal, so
	// the change-feed backlog is the only lever. This is the main thing the
	// non-Aurora path buys.
	s, l, backlog := newTestScaler(&throttler.Noop{}, 4, 8)
	require.Nil(t, s.gradual, "Noop must not be mistaken for a continuous signal")

	// Large but not yet rising for long enough: one tick must not act.
	*backlog = csBacklogVetoDeltas + 1
	s.tick(context.Background())
	assert.Equal(t, 4, s.current, "a single rising sample is the normal flush sawtooth")

	// Sustained growth is the real signal.
	*backlog = csBacklogVetoDeltas + 500
	s.tick(context.Background())
	assert.Equal(t, 3, s.current)
	assert.Equal(t, 3, l.Limit())
}

func TestScalerBacklogBelowThresholdNeverVetoes(t *testing.T) {
	s, _, backlog := newTestScaler(&throttler.Noop{}, 4, 8)
	// Rising steadily, but small enough that flushing is cheap. Should be
	// ignored entirely, however long it goes on.
	for i := range 20 {
		*backlog = i * 100 // stays under csBacklogVetoDeltas
		s.tick(context.Background())
	}
	assert.Equal(t, 4, s.current)
}

func TestScalerRecoversToStartAfterBacklogClears(t *testing.T) {
	s, _, backlog := newTestScaler(&throttler.Noop{}, 4, 8)

	// Shed twice on a sustained, growing backlog.
	for i := range 8 {
		*backlog = csBacklogVetoDeltas + i*500
		s.tick(context.Background())
	}
	require.Less(t, s.current, 4, "expected the veto to have shed at least once")

	// Backlog drains. Without recovery, one transient burst would permanently
	// cost throughput for the rest of the pass.
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
	// is falling behind. The feed wins, because its writes are a small share of
	// load and a hard prerequisite for cut-over.
	// Note the first tick still grows: the veto needs csBacklogPersistTicks to
	// confirm, and until it does, the utilization law is in charge. That is the
	// intended trade (anti-flap beats one tick of one extra worker), so the
	// assertion is about the settled direction, not the first step.
	g := &gradualStub{util: 0.0}
	s, _, backlog := newTestScaler(g, 4, 8)
	for i := range 10 {
		*backlog = csBacklogVetoDeltas + i*500
		s.tick(context.Background())
	}
	assert.Less(t, s.current, 4, "idle utilization must not override a rising backlog")
}

func TestScalerAuroraGrowsPastStartAfterBacklogClears(t *testing.T) {
	// With a continuous signal, recovery is governed by the utilization law
	// rather than clamped at start — the Aurora counterpart of
	// TestScalerRecoversToStartAfterBacklogClears.
	g := &gradualStub{util: 0.1}
	s, _, backlog := newTestScaler(g, 4, 8)
	for i := range 10 {
		*backlog = csBacklogVetoDeltas + i*500
		s.tick(context.Background())
	}
	require.Less(t, s.current, 4)

	*backlog = 0
	tickN(s, 40)
	assert.Equal(t, 8, s.current, "should climb to MaxThreads while there is headroom")
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
