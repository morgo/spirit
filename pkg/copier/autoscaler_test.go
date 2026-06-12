package copier

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// fakeScaler records SetWriteWorkers calls and reports them back.
type fakeScaler struct {
	n int
}

func (f *fakeScaler) SetWriteWorkers(n int) { f.n = n }

// utilThrottler is a GradualThrottler stub whose Utilization is scripted by
// the test. Only Utilization is exercised by the autoscaler; the rest satisfy
// the interface.
type utilThrottler struct{ util float64 }

var _ throttler.GradualThrottler = &utilThrottler{}

func (u *utilThrottler) Open(context.Context) error      { return nil }
func (u *utilThrottler) Close() error                    { return nil }
func (u *utilThrottler) IsThrottled() bool               { return u.util >= 1.0 }
func (u *utilThrottler) Utilization() float64            { return u.util }
func (u *utilThrottler) BlockWait(context.Context)       {}
func (u *utilThrottler) UpdateLag(context.Context) error { return nil }

// fakeScalingApplier satisfies applier.Applier (embedded, never called) plus
// the writeScaler capability, mimicking the SingleTargetApplier for gate tests.
type fakeScalingApplier struct {
	applier.Applier
	fakeScaler
}

func newTestScaler(start, max int) (*autoScaler, *fakeScaler, *utilThrottler) {
	fs := &fakeScaler{n: start}
	ut := &utilThrottler{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	as := newAutoScaler(ut, fs, start, max, logger, &metrics.NoopSink{})
	return as, fs, ut
}

func TestAutoScaler_IncreasesBelowLowWatermarkAfterCooldown(t *testing.T) {
	as, fs, ut := newTestScaler(2, 8)
	ut.util = 0.2 // well below low watermark

	// First sub-low tick increases immediately (cooldown starts at 0).
	as.tick(t.Context())
	require.Equal(t, 3, as.current)
	require.Equal(t, 3, fs.n)

	// Cooldown is now in effect: the next ticks hold despite continued headroom.
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "should hold during cooldown tick 1")
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "should hold during cooldown tick 2")

	// Cooldown elapsed → increase again.
	as.tick(t.Context())
	require.Equal(t, 4, as.current)
}

func TestAutoScaler_DecreasesImmediatelyAtHighWatermark(t *testing.T) {
	as, fs, ut := newTestScaler(8, 16)
	ut.util = 0.95 // at/above high watermark

	as.tick(t.Context())
	require.Equal(t, 4, as.current, "8 should halve to 4 immediately")
	require.Equal(t, 4, fs.n)

	// Consecutive halvings are cooldown-spaced: the signal updates on the same
	// cadence we tick on, so reacting every tick would halve repeatedly on one
	// stale window. Sustained overload halves again only after the cooldown.
	as.tick(t.Context())
	require.Equal(t, 4, as.current, "should hold during cooldown tick 1")
	as.tick(t.Context())
	require.Equal(t, 4, as.current, "should hold during cooldown tick 2")

	as.tick(t.Context())
	require.Equal(t, 2, as.current, "cooldown elapsed, halve again")
}

func TestAutoScaler_DecreaseNotBlockedByIncreaseCooldown(t *testing.T) {
	// An increase's cooldown must not delay a backoff: if the increase tipped
	// the server over the high watermark, the very next tick halves.
	as, _, ut := newTestScaler(4, 8)
	ut.util = 0.2
	as.tick(t.Context())
	require.Equal(t, 5, as.current, "increase under low watermark")

	ut.util = 0.95
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "halve immediately despite increase cooldown: ceil(5/2)=3")
}

func TestAutoScaler_HoldsInDeadBand(t *testing.T) {
	as, _, ut := newTestScaler(4, 16)
	ut.util = 0.7 // between low (0.5) and high (0.9)

	for range 5 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current, "dead-band should hold steady")
}

func TestAutoScaler_ClampsAtMax(t *testing.T) {
	as, _, ut := newTestScaler(3, 4)
	ut.util = 0.0 // maximum headroom, always wants to increase

	// Drive many ticks; should climb to the cap and stop.
	for range 30 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current)
}

func TestAutoScaler_ClampsAtMinOne(t *testing.T) {
	as, _, ut := newTestScaler(2, 8)
	ut.util = 1.5 // way over

	for range 10 {
		as.tick(t.Context())
	}
	require.Equal(t, 1, as.current, "must never drop below 1")
}

func TestAutoScaler_MaxFlooredAtStart(t *testing.T) {
	// A max below the start value is nonsensical; it must be floored at start so
	// we never scale below where we began except via the >high backoff path.
	as, _, _ := newTestScaler(6, 2)
	require.Equal(t, 6, as.max)
}

func TestCeilDiv(t *testing.T) {
	require.Equal(t, 1, ceilDiv(1, 2))
	require.Equal(t, 1, ceilDiv(2, 2))
	require.Equal(t, 2, ceilDiv(3, 2))
	require.Equal(t, 2, ceilDiv(4, 2))
	require.Equal(t, 3, ceilDiv(5, 2))
}

// TestAutoscalerIfEnabled_Gating covers the three conditions that must all
// hold for the autoscaler to engage: the flag is on, the applier supports
// dynamic write threads, and the throttler provides a continuous load signal
// (GradualThrottler). Missing any one of them means a fixed pool.
func TestAutoscalerIfEnabled_Gating(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gradual := &utilThrottler{}
	scalingApplier := &fakeScalingApplier{}

	// Disabled (the default): no autoscaler.
	c := &buffered{logger: logger, throttler: gradual, applier: scalingApplier}
	require.Nil(t, c.autoscalerIfEnabled())

	// Enabled + scaling applier + gradual throttler: engages with the
	// configured bounds.
	c.autoscale = AutoscaleConfig{Enabled: true, StartThreads: 2, MaxThreads: 4}
	as := c.autoscalerIfEnabled()
	require.NotNil(t, as)
	require.Equal(t, 2, as.current)
	require.Equal(t, 4, as.max)

	// Binary-only throttler (Noop here; replica lag and Mock behave the same):
	// no continuous signal to control on, so the pool stays fixed.
	c.throttler = &throttler.Noop{}
	require.Nil(t, c.autoscalerIfEnabled())

	// Applier without the dynamic-scaling capability: stays fixed.
	c.throttler = gradual
	c.applier = nil
	require.Nil(t, c.autoscalerIfEnabled())
}
