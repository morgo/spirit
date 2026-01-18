package throttler

import (
	"context"
	"time"
)

type Test struct {
}

var _ Throttler = &Test{}

func (t *Test) Open(_ context.Context) error {
	return nil
}

func (t *Test) Close() error {
	return nil
}

func (t *Test) IsThrottled() bool {
	return true
}

func (t *Test) BlockWait(ctx context.Context) {
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

func (t *Test) UpdateLag(ctx context.Context) error {
	return nil
}
