package throttler

import (
	"sync/atomic"
	"time"
)

// Staleness guard for polled gradual signals.
//
// The Aurora throttlers sample on a background loop and cache the result;
// Utilization() only reads the cache. If sampling fails persistently (monitor
// connections partitioned, grants revoked mid-migration, a failover the pool
// won't reconnect from), the poll loop logs and continues — and the cached
// value freezes. A value frozen below the autoscaler's low watermark would add
// a write thread every cooldown until the 2x cap, ramping blind on a dead
// signal, with the binary IsThrottled hard-stop equally frozen. The guard
// breaks that failure mode: once the last successful sample is older than
// staleSignalThreshold, Utilization() reports StaleUtilizationHold — a value
// inside the autoscaler's dead band — so the controller freezes scaling in
// place (no growth, no shrink) until sampling recovers.
//
// IsThrottled()/BlockWait() are deliberately NOT touched by the guard: they
// keep reporting the last computed state exactly as before, so the binary
// throttle semantics are unchanged whether the signal is fresh or stale.
const (
	// staleSignalThreshold is how old the last successful sample may be
	// before Utilization() stops trusting the cached value. Three poll
	// intervals (commitLatencyPollInterval and activeThreadsPollInterval are
	// both 5s): one or two failed or slow polls — a brief failover blip, one
	// stalled perf-schema query — don't flap the guard, but the signal is
	// declared stale before the autoscaler can take more than one blind step,
	// since its increases are spaced (acCooldownTicks+1)*acTick = 15s apart.
	staleSignalThreshold = 15 * time.Second

	// StaleUtilizationHold is the utilization reported while the signal is
	// stale. It must sit inside the autoscaler's dead band [low, high) —
	// currently [0.5, 0.9) — so a stale signal holds the write-thread count
	// steady rather than ramping it or shrinking it. 0.7 is the midpoint,
	// keeping maximum distance from both watermarks if they ever move.
	// Exported so the autoscaler tests can pin this invariant.
	StaleUtilizationHold = 0.7
)

// staleGuard tracks the freshness of a polled signal. It is embedded by the
// gradual throttlers (CommitLatency, ActiveThreads): applySample marks the
// signal fresh, Utilization checks it. All methods are safe for concurrent
// use.
type staleGuard struct {
	lastSampleAt atomic.Int64 // unixnano of the last successful sample; 0 = never sampled
	warned       atomic.Bool  // true once the current stale period has been logged
}

// markFresh records a successful sample at the current time. It returns true
// when the signal was previously declared stale — i.e. this sample is a
// recovery — so the caller can log the transition exactly once.
func (s *staleGuard) markFresh() (recovered bool) {
	s.lastSampleAt.Store(time.Now().UnixNano())
	return s.warned.Swap(false)
}

// check reports whether the signal is stale (no successful sample within
// threshold). entering is true only for the first check that finds the signal
// stale, so callers can log a warning once per stale period rather than on
// every call.
//
// A guard that has never seen a sample is not stale: the throttlers fail
// Open() if their very first sample fails, so "no sample yet" means the
// throttler isn't open, not that a working signal died.
func (s *staleGuard) check(threshold time.Duration) (stale, entering bool) {
	last := s.lastSampleAt.Load()
	if last == 0 {
		return false, false
	}
	if time.Since(time.Unix(0, last)) < threshold {
		return false, false
	}
	return true, s.warned.CompareAndSwap(false, true)
}

// age returns the time since the last successful sample, for logging.
func (s *staleGuard) age() time.Duration {
	return time.Since(time.Unix(0, s.lastSampleAt.Load()))
}
