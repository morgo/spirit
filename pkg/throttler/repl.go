package throttler

import (
	"context"
	"database/sql"
	"log/slog"
	"sync/atomic"
	"time"
)

var blockWaitInterval = 1 * time.Second

type Repl struct {
	replica        *sql.DB
	lagTolerance   time.Duration
	currentLagInMs int64
	logger         *slog.Logger
}

func (l *Repl) IsThrottled() bool {
	return atomic.LoadInt64(&l.currentLagInMs) >= l.lagTolerance.Milliseconds()
}

// BlockWait blocks until the lag is within the tolerance, or up to 60s
// to allow some progress to be made. It respects context cancellation.
func (l *Repl) BlockWait(ctx context.Context) {
	timer := time.NewTimer(blockWaitInterval)
	defer timer.Stop()

	for range 60 {
		if atomic.LoadInt64(&l.currentLagInMs) < l.lagTolerance.Milliseconds() {
			return
		}

		timer.Reset(blockWaitInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Continue checking
		}
	}
	l.logger.Warn("lag monitor timed out", "lag_ms", atomic.LoadInt64(&l.currentLagInMs), "tolerance", l.lagTolerance)
}
