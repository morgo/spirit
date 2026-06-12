package throttler

import (
	"context"
	"time"
)

// Mock is an always-throttled test throttler. It is deliberately binary (no
// GradualThrottler): tests that exercise the autoscaler's continuous signal
// use their own GradualThrottler stub instead.
type Mock struct {
}

var _ Throttler = &Mock{}

func (t *Mock) Open(_ context.Context) error {
	return nil
}

func (t *Mock) Close() error {
	return nil
}

func (t *Mock) IsThrottled() bool {
	return true
}

func (t *Mock) BlockWait(ctx context.Context) {
	// Use a timer with context cancellation for interruptible sleep
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		return
	}
}

func (t *Mock) UpdateLag(ctx context.Context) error {
	return nil
}
