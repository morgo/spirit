package throttler

import (
	"context"
	"errors"
	"sync"
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
	var opened []Throttler
	for _, t := range m.throttlers {
		if err := t.Open(ctx); err != nil {
			// Close any already-opened throttlers to avoid resource leaks.
			for i := len(opened) - 1; i >= 0; i-- {
				_ = opened[i].Close()
			}
			return err
		}
		opened = append(opened, t)
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
// It waits on all throttled children concurrently so the total wait time
// is bounded by the slowest replica, not the sum of all replicas.
func (m *multiThrottler) BlockWait(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range m.throttlers {
		if t.IsThrottled() {
			wg.Go(func() {
				t.BlockWait(ctx)
			})
		}
	}
	wg.Wait()
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
