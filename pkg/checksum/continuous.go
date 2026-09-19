package checksum

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/change"
)

var _ ContinuousChecker = (*SingleChecker)(nil)
var _ ContinuousChecker = (*DistributedChecker)(nil)
var _ ContinuousChecker = (*locklessChecker)(nil)
var _ ContinuousChecker = (*MockChecker)(nil)

func (c *SingleChecker) RunContinuous(ctx context.Context) error {
	c.resume.continuous.Store(true)
	return runContinuousSnapshot(ctx, c, []change.Source{c.feed}, &c.resume, func() error {
		return c.resume.restart(&c.differencesFound, c.chunker.Reset)
	})
}

func (c *DistributedChecker) RunContinuous(ctx context.Context) error {
	c.resume.continuous.Store(true)
	return runContinuousSnapshot(ctx, c, c.feeds, &c.resume, func() error {
		return c.resume.restart(&c.differencesFound, c.chunker.Reset)
	})
}

// Flush during pacing, but stop before Run acquires snapshot setup locks.
// Each finite Run owns flushing after those locks have been released.
func runContinuousSnapshot(ctx context.Context, checker Checker, feeds []change.Source, resume *snapshotResume, reset func() error) error {
	var duration time.Duration
	for {
		for _, feed := range feeds {
			feed.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
		}
		ready := waitForChecksum(ctx, LocklessMinPassInterval-duration)
		for _, feed := range feeds {
			feed.StopPeriodicFlush()
		}
		if !ready {
			return nil
		}
		if err := reset(); err != nil {
			return fmt.Errorf("reset continuous checksum: %w", err)
		}
		before := resume.observed.Load()
		started := time.Now()
		err := checker.Run(ctx)
		if err != nil {
			// A retry can reset DifferencesFound even after a repair was interrupted.
			// Use the monotonic observation count for this entire Run instead.
			if ctx.Err() != nil && checksumCanceled(err) && resume.observed.Load() == before {
				return nil
			}
			return err
		}
		duration = time.Since(started)
	}
}

func waitForChecksum(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(max(0, delay))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// Accept wrapped cancellation, but not a joined cancellation plus a real error.
func checksumCanceled(err error) bool {
	var joined interface{ Unwrap() []error }
	return !errors.As(err, &joined) && errors.Is(err, context.Canceled)
}
