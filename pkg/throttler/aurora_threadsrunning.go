package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"
)

// Threads-running throttling — the second Aurora signal, from issue #831.
//
// Like gh-ost and pt-osc, we use a "threads running" count to detect server
// busyness and back off when the box is under load. What's different from
// those tools is that we compare the count to the instance vCPU count — which
// on Aurora we read deterministically from @@innodb_buffer_pool_instances —
// and that we feed it to the write-thread autoscaler as a continuous
// utilization signal, not only as a binary stop/go.
//
// History (issue #831): this began as an "active CPU threads" signal that
// joined performance_schema.threads to events_waits_current to subtract
// threads parked on redo-log flush, on the theory that those are blocked on IO
// and not consuming CPU, so the redo log could be safely oversubscribed. That
// refinement can genuinely help when commit latency is very high — many
// threads stalled on the log while CPU sits idle — but in staging the
// subtraction never fired (the event name didn't match on Aurora, and the
// instrument may not even be enabled), so it was carrying cost and an extra
// grant requirement without delivering the benefit. To start with we use the
// simpler, more conservative approach: count all running threads via
// Threads_running from global_status. It needs no grant beyond the
// global_status access that IsAurora and the commit-latency throttler already
// require — so this throttler no longer needs SELECT on
// performance_schema.threads / events_waits_current, and is always available
// once Aurora is detected. Reviving the redo-aware variant later (with a
// verified event name) remains an option if commit-bound workloads call for
// it.
//
// Counting all running threads means redo-log waiters now read as load, so the
// signal caps concurrency near vCPUs rather than oversubscribing the log on a
// commit-bound workload. The commit-latency throttler, reading the same
// global_status, watches storage saturation directly, so that case is still
// covered.
const (
	// threadsRunningQuery reads the Threads_running status variable: the count
	// of non-sleeping threads. It comes from performance_schema.global_status
	// (the same source as the commit-latency throttler and IsAurora), so it
	// needs no privilege beyond what those already require — there is no
	// separate grant probe.
	//
	// No self-exclusion: the monitor's own polling query counts itself as one
	// running thread (and the commit-latency poller may add another). On the
	// 4+ vCPU instances where the autoscaler engages, ±1–2 is in the noise and
	// the EWMA smooths it, so we accept the small over-count rather than carry
	// machinery to subtract it.
	threadsRunningQuery = `SELECT CAST(VARIABLE_VALUE AS UNSIGNED)
	FROM performance_schema.global_status
	WHERE VARIABLE_NAME = 'Threads_running'`

	// auroraVCPUsQuery reads the vCPU count from @@innodb_buffer_pool_instances,
	// which Aurora pins to the instance vCPU count (see issue #831).
	//
	// Do NOT use @@innodb_purge_threads for this: Aurora sizes it by a
	// stepped formula, not the vCPU count (r7g family: large=1, xlarge=1,
	// 2xlarge=3, 4xlarge=3, 8xlarge=6, 16xlarge=12), so it badly
	// under-reads CPU capacity at every size. On community MySQL
	// innodb_buffer_pool_instances is sized by buffer pool size / CPU
	// hints instead — we gate the throttler on IsAurora, so this query is
	// only ever issued there.
	auroraVCPUsQuery = `SELECT @@innodb_buffer_pool_instances`
)

// DefaultWriteThreads is the fallback apply (write) thread count used when
// auto-sizing is requested (write-threads=0) but the target offers no reliable
// vCPU signal to size from — i.e. a non-Aurora server. It must match the
// `default:"4"` kong tags on the WriteThreads CLI flags (see Migration and
// Move), so that requesting auto-sizing on non-Aurora lands on the same value
// as not requesting it at all.
const DefaultWriteThreads = 4

// AuroraVCPUs reads the instance vCPU count from @@innodb_buffer_pool_instances,
// which Aurora pins to the vCPU count (issue #831). It returns an error if the
// value is non-positive. This is only meaningful on Aurora — callers should gate
// on IsAurora first.
func AuroraVCPUs(ctx context.Context, db *sql.DB) (int, error) {
	var vCPUs int
	if err := db.QueryRowContext(ctx, auroraVCPUsQuery).Scan(&vCPUs); err != nil {
		return 0, fmt.Errorf("reading @@innodb_buffer_pool_instances for vCPU count: %w", err)
	}
	if vCPUs <= 0 {
		return 0, fmt.Errorf("@@innodb_buffer_pool_instances returned non-positive value %d", vCPUs)
	}
	return vCPUs, nil
}

// ResolveWriteThreads resolves the number of apply (write) threads to use
// against a target. A positive requested value is returned unchanged. Zero
// means "auto-size": on Aurora it resolves to the instance vCPU count
// (@@innodb_buffer_pool_instances); on non-Aurora there is no reliable vCPU
// signal to size from, so it falls back to DefaultWriteThreads (logging that it
// did so).
// A negative value is rejected.
func ResolveWriteThreads(ctx context.Context, db *sql.DB, requested int, logger *slog.Logger) (int, error) {
	if requested < 0 {
		return 0, fmt.Errorf("write threads must be non-negative, got %d", requested)
	}
	if requested > 0 {
		return requested, nil
	}
	// Auto-sizing requires probing the server, so a usable DB is mandatory
	// here (a positive requested value above never reaches this point).
	if db == nil {
		return 0, errors.New("cannot auto-size write threads: no database connection provided (set write-threads to a positive value)")
	}
	isAurora, err := IsAurora(ctx, db)
	if err != nil {
		return 0, err
	}
	if !isAurora {
		// No reliable vCPU signal off Aurora, so we can't honor auto-sizing.
		// Rather than fail, fall back to the default the user would have got
		// had they not asked to auto-size at all.
		logger.Info("write-threads=0 requested auto-sizing, but the target is not Aurora (no reliable vCPU signal); using the default instead",
			"write_threads", DefaultWriteThreads)
		return DefaultWriteThreads, nil
	}
	return AuroraVCPUs(ctx, db)
}

// MinAutoscaleVCPUs is the smallest instance size (in vCPUs) on which the
// write-thread autoscaler is allowed to engage. Below this the utilization
// signal is too coarse to control on: one thread is half or a third of the
// whole scale, so there is no dead band wide enough to rest in and the
// controller can only oscillate. Observed in staging on r6g.large (2 vCPUs):
// the thread count ping-ponged 1↔2 indefinitely (issue #831). At 4+ vCPUs the
// worst-case per-thread utilization step (0.25) fits inside the autoscaler's
// dead band.
const MinAutoscaleVCPUs = 4

// ResolveMaxWriteThreads resolves the upper bound the write-thread autoscaler
// may scale to. When autoscaling is disabled the cap equals start, so the
// thread count cannot move. When enabled the cap is fixed at 2 × start —
// deliberately not configurable for now, to keep the experimental surface
// small. See issue #831.
func ResolveMaxWriteThreads(start int, autoscaleEnabled bool) int {
	if !autoscaleEnabled {
		return start
	}
	return 2 * start
}

// threadsRunningPollInterval mirrors commitLatencyPollInterval — fast enough to
// catch sustained pressure without hammering global_status. Var (not const) so
// tests can shorten it.
var threadsRunningPollInterval = 5 * time.Second

// threadsRunningEWMAAlpha is the smoothing factor for the exponentially
// weighted moving average that Utilization() reports. Threads_running is an
// instantaneous gauge with high variance — it blips on brief latch/lock waits
// and on spirit's own housekeeping (checkpoints, GTID flushes, status
// queries). The autoscaler must control on the sustained average, not the
// variance — the commit-latency sibling signal is already window-averaged for
// the same reason. 0.3 gives a time constant of ~3 samples (~15s at the 5s
// poll), matching the autoscaler's cooldown spacing so the controller never
// acts twice on effectively the same information.
//
// Only Utilization() is smoothed. IsThrottled/BlockWait stay on the raw
// sample: the hard-stop protects the production workload and must react
// within one poll.
const threadsRunningEWMAAlpha = 0.3

// ThreadsRunning throttles when the server's Threads_running count exceeds the
// instance vCPU count. Aurora-only — depends on @@innodb_buffer_pool_instances
// matching vCPUs.
type ThreadsRunning struct {
	db     *sql.DB
	logger *slog.Logger

	// vCPUs is the throttle threshold, captured once at Open and treated as
	// immutable for the migration's lifetime. Aurora instance type changes
	// require a restart so it cannot move under us.
	vCPUs int64

	isThrottled atomic.Bool
	isClosed    atomic.Bool

	// lastThreadsRunning holds the most recent observation for logging and the
	// raw hard-stop comparison.
	lastThreadsRunning atomic.Int64

	// smoothedBits holds math.Float64bits of the EWMA of Threads_running that
	// Utilization() reports (see threadsRunningEWMAAlpha). ewmaSeeded records
	// whether smoothedBits holds a real value yet — 0.0 is a valid average,
	// so a sentinel won't do.
	smoothedBits atomic.Uint64
	ewmaSeeded   atomic.Bool

	// stale guards Utilization() against the cached value freezing when
	// sampling fails persistently. See stale.go.
	stale staleGuard
}

var _ GradualThrottler = (*ThreadsRunning)(nil)

// NewThreadsRunningThrottler returns a Throttler that polls the Threads_running
// status variable and throttles when it exceeds the instance vCPU count.
func NewThreadsRunningThrottler(db *sql.DB, logger *slog.Logger) (*ThreadsRunning, error) {
	if db == nil {
		return nil, errors.New("threads-running throttler requires a non-nil DB")
	}
	return &ThreadsRunning{
		db:     db,
		logger: logger,
	}, nil
}

func (a *ThreadsRunning) Open(ctx context.Context) error {
	vCPUs, err := AuroraVCPUs(ctx, a.db)
	if err != nil {
		return err
	}
	a.vCPUs = int64(vCPUs)
	a.logger.Info("Aurora threads-running throttler enabled", "vCPUs", a.vCPUs)
	if err := a.UpdateLag(ctx); err != nil {
		return err
	}
	go a.run(ctx)
	return nil
}

func (a *ThreadsRunning) run(ctx context.Context) {
	ticker := time.NewTicker(threadsRunningPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if a.isClosed.Load() {
				return
			}
			if err := a.UpdateLag(ctx); err != nil {
				a.logger.Error("error sampling Aurora Threads_running", "error", err)
			}
		}
	}
}

func (a *ThreadsRunning) Close() error {
	a.isClosed.Store(true)
	return nil
}

func (a *ThreadsRunning) IsThrottled() bool {
	return a.isThrottled.Load()
}

// Utilization reports the smoothed (EWMA) Threads_running count as a fraction
// of the instance vCPU count. 1.0 means the sustained average equals vCPUs —
// around where the raw signal trips IsThrottled. Returns 0 if vCPUs is not yet
// known. The smoothing exists for the autoscaler, this method's only consumer:
// it must see sustained load, not single-sample spikes (see
// threadsRunningEWMAAlpha). The binary hard-stop deliberately does not share
// it.
//
// When sampling has failed for longer than staleSignalThreshold the cached
// average is no longer trustworthy, so this reports StaleUtilizationHold
// instead — parking the autoscaler in its dead band rather than letting a
// frozen value drive scaling. IsThrottled/BlockWait are unaffected.
func (a *ThreadsRunning) Utilization() float64 {
	if stale, entering := a.stale.check(staleSignalThreshold); stale {
		if entering {
			a.logger.Warn("threads-running signal is stale; holding autoscaler utilization steady until sampling recovers",
				"last_successful_sample_age", a.stale.age(),
				"hold_utilization", StaleUtilizationHold)
		}
		return StaleUtilizationHold
	}
	if a.vCPUs <= 0 {
		return 0
	}
	return math.Float64frombits(a.smoothedBits.Load()) / float64(a.vCPUs)
}

// BlockWait blocks until Threads_running falls to or below vCPUs, or up to
// 60s. Matches the commit-latency throttler's loop shape so the multi-
// throttler waits uniformly across signals.
func (a *ThreadsRunning) BlockWait(ctx context.Context) {
	timer := time.NewTimer(blockWaitInterval)
	defer timer.Stop()

	for range 60 {
		if !a.isThrottled.Load() {
			return
		}
		timer.Reset(blockWaitInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
	a.logger.Warn("threads-running throttler timed out",
		"threads_running", a.lastThreadsRunning.Load(),
		"vCPUs", a.vCPUs)
}

// UpdateLag samples Threads_running and updates throttled state.
func (a *ThreadsRunning) UpdateLag(ctx context.Context) error {
	var running int64
	if err := a.db.QueryRowContext(ctx, threadsRunningQuery).Scan(&running); err != nil {
		return fmt.Errorf("sampling Aurora Threads_running: %w", err)
	}
	a.applySample(running)
	return nil
}

// applySample updates state from a single observation. Split out so tests can
// drive the calculation without a real Aurora.
//
// Threads_running is an instantaneous gauge (the status variable is a current
// count, not a delta of cumulative counters), so a failover cannot produce a
// bogus value — only an error, which the staleness guard covers. There is
// therefore no counter-reset case to handle here.
func (a *ThreadsRunning) applySample(running int64) {
	// Capture whether this sample arrives after a stale gap before marking it
	// fresh: the EWMA must be reseeded then, not extended — blending a live
	// sample into pre-outage history would report a fiction made of dead data.
	staleGap := a.stale.gapExceeds(staleSignalThreshold)
	if a.stale.markFresh() {
		a.logger.Info("threads-running sampling recovered; resuming live utilization signal")
	}
	a.lastThreadsRunning.Store(running)

	if sample := float64(running); !a.ewmaSeeded.Load() || staleGap {
		a.smoothedBits.Store(math.Float64bits(sample))
		a.ewmaSeeded.Store(true)
	} else {
		prev := math.Float64frombits(a.smoothedBits.Load())
		a.smoothedBits.Store(math.Float64bits(prev + threadsRunningEWMAAlpha*(sample-prev)))
	}

	// The hard-stop compares the raw sample, not the EWMA: it protects the
	// production workload and must engage within one poll of a load spike.
	throttled := running > a.vCPUs
	prev := a.isThrottled.Swap(throttled)
	if throttled && !prev {
		a.logger.Warn("Threads_running exceeds vCPUs, throttling",
			"threads_running", running,
			"vCPUs", a.vCPUs)
	}
}
