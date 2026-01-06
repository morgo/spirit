package throttler

import (
	"context"
	"time"
)

type Noop struct {
	currentLag   time.Duration // used for testing
	lagTolerance time.Duration // used for testing
}

var _ Throttler = &Noop{}

func (t *Noop) Open(_ context.Context) error {
	return nil
}

func (t *Noop) Close() error {
	return nil
}

func (t *Noop) IsThrottled() bool {
	return t.currentLag > t.lagTolerance
}

func (t *Noop) BlockWait() {
}

func (t *Noop) UpdateLag(ctx context.Context) error {
	return nil
}
