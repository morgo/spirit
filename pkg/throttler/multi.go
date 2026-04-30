package throttler

import (
	"context"
	"errors"
)

// multiThrottler wraps multiple throttlers and throttles if any child is throttled.
// It is used to support multiple replica endpoints or combining different
// throttling strategies (e.g., replica lag + commit latency).
type multiThrottler struct {
	throttlers []Throttler
}

var _ Throttler = &multiThrottler{}

// NewMultiThrottler creates a throttler that wraps multiple child throttlers.
// If zero throttlers are provided, it returns a Noop. If one is provided,
// it is returned directly without wrapping.
func NewMultiThrottler(throttlers ...Throttler) Throttler {
	switch len(throttlers) {
	case 0:
		return &Noop{}
	case 1:
		return throttlers[0]
	default:
		return &multiThrottler{throttlers: throttlers}
	}
}

func (m *multiThrottler) Open(ctx context.Context) error {
	for _, t := range m.throttlers {
		if err := t.Open(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiThrottler) Close() error {
	var errs []error
	for _, t := range m.throttlers {
		if err := t.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// IsThrottled returns true if any child throttler is throttled.
func (m *multiThrottler) IsThrottled() bool {
	for _, t := range m.throttlers {
		if t.IsThrottled() {
			return true
		}
	}
	return false
}

// BlockWait blocks until all child throttlers are unthrottled.
// It delegates to the slowest child — once the slowest is done waiting,
// all others should already be unthrottled.
func (m *multiThrottler) BlockWait(ctx context.Context) {
	for _, t := range m.throttlers {
		if t.IsThrottled() {
			t.BlockWait(ctx)
		}
	}
}

// UpdateLag updates lag on all child throttlers.
// Returns the first error encountered but continues updating all.
func (m *multiThrottler) UpdateLag(ctx context.Context) error {
	var firstErr error
	for _, t := range m.throttlers {
		if err := t.UpdateLag(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
