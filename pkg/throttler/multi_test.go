package throttler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewMultiThrottler_Zero(t *testing.T) {
	throttler := NewMultiThrottler()
	require.IsType(t, &Noop{}, throttler)
}

func TestNewMultiThrottler_One(t *testing.T) {
	single := &Noop{}
	throttler := NewMultiThrottler(single)
	require.Same(t, single, throttler)
}

func TestNewMultiThrottler_Multiple(t *testing.T) {
	throttler := NewMultiThrottler(&Noop{}, &Noop{})
	require.IsType(t, &multiThrottler{}, throttler)
}

// testThrottler is a configurable throttler for testing.
type testThrottler struct {
	throttled    atomic.Bool
	openErr      error
	closeErr     error
	updateLagErr error
	opened       atomic.Bool
	closed       atomic.Bool
	blockWaited  atomic.Bool
}

func (t *testThrottler) Open(_ context.Context) error {
	t.opened.Store(true)
	return t.openErr
}

func (t *testThrottler) Close() error {
	t.closed.Store(true)
	return t.closeErr
}

func (t *testThrottler) IsThrottled() bool {
	return t.throttled.Load()
}

func (t *testThrottler) BlockWait(ctx context.Context) {
	t.blockWaited.Store(true)
	// Simulate waiting then becoming unthrottled
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	t.throttled.Store(false)
}

func (t *testThrottler) UpdateLag(_ context.Context) error {
	return t.updateLagErr
}

func TestMultiThrottler_Open(t *testing.T) {
	t1 := &testThrottler{}
	t2 := &testThrottler{}
	multi := NewMultiThrottler(t1, t2)

	require.NoError(t, multi.Open(t.Context()))
	require.True(t, t1.opened.Load())
	require.True(t, t2.opened.Load())
}

func TestMultiThrottler_Open_Error(t *testing.T) {
	t1 := &testThrottler{openErr: errors.New("connection refused")}
	t2 := &testThrottler{}
	multi := NewMultiThrottler(t1, t2)

	err := multi.Open(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
	// Second throttler should not have been opened
	require.False(t, t2.opened.Load())
}

func TestMultiThrottler_Open_PartialFailure_CleansUp(t *testing.T) {
	// First throttler opens successfully, second fails.
	// The first should be closed on cleanup.
	t1 := &testThrottler{}
	t2 := &testThrottler{openErr: errors.New("connection refused")}
	multi := NewMultiThrottler(t1, t2)

	err := multi.Open(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
	// First throttler was opened successfully and should have been closed during cleanup
	require.True(t, t1.opened.Load())
	require.True(t, t1.closed.Load())
	// Second throttler's Open was attempted but failed
	require.False(t, t2.closed.Load(), "failed throttler should not be closed")
}

func TestMultiThrottler_Close(t *testing.T) {
	t1 := &testThrottler{}
	t2 := &testThrottler{}
	multi := NewMultiThrottler(t1, t2)

	require.NoError(t, multi.Close())
	require.True(t, t1.closed.Load())
	require.True(t, t2.closed.Load())
}

func TestMultiThrottler_Close_CollectsErrors(t *testing.T) {
	t1 := &testThrottler{closeErr: errors.New("err1")}
	t2 := &testThrottler{closeErr: errors.New("err2")}
	multi := NewMultiThrottler(t1, t2)

	err := multi.Close()
	require.Error(t, err)
	require.Contains(t, err.Error(), "err1")
	require.Contains(t, err.Error(), "err2")
	// Both should still be closed
	require.True(t, t1.closed.Load())
	require.True(t, t2.closed.Load())
}

func TestMultiThrottler_IsThrottled_NoneThrottled(t *testing.T) {
	t1 := &testThrottler{}
	t2 := &testThrottler{}
	multi := NewMultiThrottler(t1, t2)

	require.False(t, multi.IsThrottled())
}

func TestMultiThrottler_IsThrottled_OneThrottled(t *testing.T) {
	t1 := &testThrottler{}
	t2 := &testThrottler{}
	t2.throttled.Store(true)
	multi := NewMultiThrottler(t1, t2)

	require.True(t, multi.IsThrottled())
}

func TestMultiThrottler_IsThrottled_AllThrottled(t *testing.T) {
	t1 := &testThrottler{}
	t2 := &testThrottler{}
	t1.throttled.Store(true)
	t2.throttled.Store(true)
	multi := NewMultiThrottler(t1, t2)

	require.True(t, multi.IsThrottled())
}

func TestMultiThrottler_BlockWait_OnlyBlocksThrottled(t *testing.T) {
	t1 := &testThrottler{}
	t2 := &testThrottler{}
	t2.throttled.Store(true)
	multi := NewMultiThrottler(t1, t2)

	multi.BlockWait(t.Context())

	// Only t2 should have been waited on
	require.False(t, t1.blockWaited.Load())
	require.True(t, t2.blockWaited.Load())
}

func TestMultiThrottler_BlockWait_RespectsContext(t *testing.T) {
	t1 := &testThrottler{}
	t1.throttled.Store(true)
	multi := NewMultiThrottler(t1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	// Should return quickly without hanging
	multi.BlockWait(ctx)
}

func TestMultiThrottler_UpdateLag(t *testing.T) {
	t1 := &testThrottler{}
	t2 := &testThrottler{}
	multi := NewMultiThrottler(t1, t2)

	require.NoError(t, multi.UpdateLag(t.Context()))
}

func TestMultiThrottler_UpdateLag_ReturnsFirstError(t *testing.T) {
	t1 := &testThrottler{updateLagErr: errors.New("lag error")}
	t2 := &testThrottler{}
	multi := NewMultiThrottler(t1, t2)

	err := multi.UpdateLag(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "lag error")
}
