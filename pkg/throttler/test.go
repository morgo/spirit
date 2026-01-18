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

func (t *Test) BlockWait() {
	time.Sleep(time.Second)
}

func (t *Test) UpdateLag(ctx context.Context) error {
	return nil
}
