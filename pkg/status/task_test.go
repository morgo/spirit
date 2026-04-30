package status

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockTask implements Task for testing checkpoint behavior.
type mockTask struct {
	mu               sync.Mutex
	checkpointCalls  atomic.Int32
	progress         Progress
	dumpCheckpointFn func(ctx context.Context) error
	cancelled        atomic.Bool
}

func (m *mockTask) Progress() Progress {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.progress
}

func (m *mockTask) Status() string {
	return "mock status"
}

func (m *mockTask) DumpCheckpoint(ctx context.Context) error {
	m.checkpointCalls.Add(1)
	if m.dumpCheckpointFn != nil {
		return m.dumpCheckpointFn(ctx)
	}
	return nil
}

func (m *mockTask) Cancel() {
	m.cancelled.Store(true)
}

func TestContinuallyDumpCheckpoint_ImmediateWrite(t *testing.T) {
	// Verify that DumpCheckpoint is called immediately when the routine starts,
	// without waiting for the ticker interval.
	mock := &mockTask{
		progress: Progress{CurrentState: CopyRows},
	}

	ctx, cancel := context.WithCancel(t.Context())
	logger := slog.Default()

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(ctx, mock, logger)
	}()

	// Wait briefly for the immediate checkpoint to be written.
	// This should happen almost instantly, well before the 50s ticker.
	assert.Eventually(t, func() bool {
		return mock.checkpointCalls.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond, "expected immediate checkpoint write")

	cancel()
	<-done // Wait for goroutine to exit
}

func TestContinuallyDumpCheckpoint_ImmediateWatermarkNotReady(t *testing.T) {
	// When the immediate checkpoint returns ErrWatermarkNotReady,
	// the routine should continue to the ticker loop (not cancel).
	var calls atomic.Int32
	mock := &mockTask{
		progress: Progress{CurrentState: CopyRows},
		dumpCheckpointFn: func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				return ErrWatermarkNotReady
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	logger := slog.Default()

	// Use a short checkpoint interval for the test.
	origInterval := CheckpointDumpInterval
	CheckpointDumpInterval = 50 * time.Millisecond
	defer func() { CheckpointDumpInterval = origInterval }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(ctx, mock, logger)
	}()

	// The first call should fail with ErrWatermarkNotReady, but the routine
	// should continue and succeed on the next ticker-driven call.
	assert.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond, "expected retry after watermark not ready")

	assert.False(t, mock.cancelled.Load(), "task should not be cancelled for ErrWatermarkNotReady")

	cancel()
	<-done // Wait for goroutine to exit
}

func TestContinuallyDumpCheckpoint_ContextCanceled_ReturnsImmediately(t *testing.T) {
	// When the context is already canceled, the routine should return immediately
	// without entering the ticker loop.
	mock := &mockTask{
		progress: Progress{CurrentState: CopyRows},
		dumpCheckpointFn: func(ctx context.Context) error {
			return context.Canceled
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel before starting

	logger := slog.Default()

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(ctx, mock, logger)
	}()

	// Should exit quickly
	select {
	case <-done:
		// Good — exited promptly
	case <-time.After(2 * time.Second):
		t.Fatal("continuallyDumpCheckpoint did not exit after context canceled")
	}

	assert.False(t, mock.cancelled.Load(), "task should not be cancelled for context.Canceled")
}
