package autoscale

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyZones(t *testing.T) {
	tests := []struct {
		util float64
		want Action
	}{
		{0.0, Grow},
		{0.39, Grow},
		// Boundaries are inclusive at the lower edge of each higher zone, so
		// exactly-at-watermark reads as the calmer action.
		{LowWatermark, Hold},
		{0.69, Hold},
		{HighWatermark, Shed},
		{0.99, Shed},
		{PanicThreshold, Halve},
		{2.5, Halve},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, Classify(tc.util), "util=%v", tc.util)
	}
}

func TestCeilDiv(t *testing.T) {
	// The point of ceil is that halving never lands on zero, and 3 backs off
	// to 2 rather than 1.
	assert.Equal(t, 2, CeilDiv(3, 2))
	assert.Equal(t, 1, CeilDiv(1, 2))
	assert.Equal(t, 4, CeilDiv(8, 2))
}

func TestLimiterFloorsAtOne(t *testing.T) {
	// A limiter admitting nobody would deadlock its caller rather than
	// throttle it, so both entry points clamp to 1.
	assert.Equal(t, 1, NewLimiter(0).Limit())
	assert.Equal(t, 1, NewLimiter(-5).Limit())

	l := NewLimiter(4)
	l.SetLimit(0)
	assert.Equal(t, 1, l.Limit())
}

func TestLimiterAdmitsUpToLimit(t *testing.T) {
	ctx := context.Background()
	l := NewLimiter(2)

	require.NoError(t, l.Acquire(ctx))
	require.NoError(t, l.Acquire(ctx))
	assert.Equal(t, 2, l.InFlight())

	// The third must block. Prove it by racing it against a short timeout.
	blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, l.Acquire(blocked), context.DeadlineExceeded)

	// Releasing one lets exactly one more in.
	l.Release()
	require.NoError(t, l.Acquire(ctx))
	assert.Equal(t, 2, l.InFlight())
}

func TestLimiterGrowWakesWaiter(t *testing.T) {
	ctx := context.Background()
	l := NewLimiter(1)
	require.NoError(t, l.Acquire(ctx))

	acquired := make(chan struct{})
	go func() {
		if err := l.Acquire(ctx); err == nil {
			close(acquired)
		}
	}()

	// The waiter is parked behind the limit of 1. Raising the limit must wake
	// it without anyone releasing — this is the path the autoscaler drives
	// when it grows the pool mid-pass.
	select {
	case <-acquired:
		t.Fatal("acquired before the limit was raised")
	case <-time.After(50 * time.Millisecond):
	}

	l.SetLimit(2)
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("raising the limit did not wake the waiter")
	}
}

func TestLimiterShrinkDoesNotInterruptInFlight(t *testing.T) {
	ctx := context.Background()
	l := NewLimiter(4)
	for range 4 {
		require.NoError(t, l.Acquire(ctx))
	}

	// Shedding must never abort work already in flight: for the checksum a
	// cancelled chunk is wasted I/O that has to be redone. The reduction is
	// absorbed by subsequent Releases instead.
	l.SetLimit(2)
	assert.Equal(t, 4, l.InFlight(), "in-flight holders should be untouched by a reduction")

	// Two releases still leave us at the new limit, so no new work starts.
	l.Release()
	l.Release()
	assert.Equal(t, 2, l.InFlight())
	blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, l.Acquire(blocked), context.DeadlineExceeded)

	// A third release drops below the new limit and admits work again.
	l.Release()
	require.NoError(t, l.Acquire(ctx))
}

func TestLimiterCancelledContextDoesNotAdmit(t *testing.T) {
	l := NewLimiter(4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Even with permits free, a done context must not be admitted — otherwise
	// a cancelled pass keeps dispatching work.
	require.ErrorIs(t, l.Acquire(ctx), context.Canceled)
	assert.Equal(t, 0, l.InFlight())
}

// TestLimiterNeverOverAdmits hammers the limiter from many goroutines while
// the limit is being resized underneath them, asserting the invariant that
// matters: concurrent holders never exceed the limit in force when they were
// admitted. Run with -race to also cover the locking.
func TestLimiterNeverOverAdmits(t *testing.T) {
	const (
		workers  = 32
		maxLimit = 8
	)
	l := NewLimiter(maxLimit)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var live, peak atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range 50 {
				if err := l.Acquire(ctx); err != nil {
					return
				}
				n := live.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				live.Add(-1)
				l.Release()
			}
		})
	}
	// Resize underneath the workers, staying within [1, maxLimit].
	resizer := make(chan struct{})
	go func() {
		defer close(resizer)
		for i := range 100 {
			l.SetLimit(1 + i%maxLimit)
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	<-resizer
	assert.LessOrEqual(t, peak.Load(), int64(maxLimit),
		"concurrent holders exceeded the highest limit ever set")
	assert.Equal(t, int64(0), live.Load())
}
