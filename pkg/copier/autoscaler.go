package copier

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
)

// Control-loop tunables. The shape is "gentle in the normal regime, abrupt
// only in emergencies": ±1 thread at a time, cooldown-gated, with a
// multiplicative halving reserved for utilization at/above the point where
// the hard-stop engages anyway.
//
// Two properties of the utilization signal dictate this shape (issue #831;
// observed in staging):
//
//   - It is largely self-induced. On a quiet server the active-thread count
//     is mostly our own write workers, so the controller's output feeds its
//     own input. Classic AIMD halving — built for congestion signals
//     dominated by other parties' traffic — overshoots badly here: each
//     halving cuts the signal roughly in half too, and the controller
//     sawtooths between the watermark and half of it indefinitely.
//   - The marginal utilization of one more thread is unpredictable. Write
//     workers spend a workload-dependent fraction of their time parked on
//     redo-log waits, which the active-threads signal deliberately excludes,
//     so a thread's on-CPU duty cycle — and therefore the effect of adding
//     or removing one — can't be computed ahead. Small steps with a pause to
//     observe are the only reliable way to converge.
//
// Zones, evaluated each tick against the (smoothed) utilization:
//
//	util < acLowWatermark                  add one thread (cooldown-gated)
//	[acLowWatermark, acHighWatermark)      hold
//	[acHighWatermark, acPanicThreshold)    shed one thread (cooldown-gated)
//	util >= acPanicThreshold               halve (first breach immediate)
//
// Because increases stop the moment util reaches the low watermark, the
// self-induced steady state parks just above it: acLowWatermark is the
// effective setpoint, and the band up to acHighWatermark is hysteresis
// headroom for noise and for the production workload. Parking around 40-50%
// of vCPUs is deliberately conservative — the primary OLTP workload gets the
// remaining capacity, and leaving copy throughput on the table is fine.
// Responsiveness to genuine overload is not traded away: that is the
// BlockWait hard-stop's job, which none of this touches.
const (
	// acLowWatermark is the effective setpoint: below it there is headroom,
	// so we may add a thread (subject to cooldown).
	acLowWatermark = 0.4
	// acHighWatermark starts the additive back-off. The dead band between the
	// watermarks must be wider than the utilization step of a single thread
	// (at most 1/vCPUs, and >= 0.25 only when vCPUs < MinAutoscaleVCPUs,
	// where the runner disables autoscaling entirely) — otherwise one +1 can
	// vault across the band and ping-pong with the -1 path.
	acHighWatermark = 0.7
	// acPanicThreshold is where back-off turns multiplicative. 1.0 is where
	// the raw active-threads signal trips the BlockWait hard-stop: the copy
	// is pausing anyway, and halving sheds enough pressure that the resume is
	// gentle. This compares the gradual utilization, NOT IsThrottled() — on a
	// multi-throttler that would include binary children like replica lag,
	// and halving on those is unguided (they already pause the copy, which
	// makes the worker count moot while tripped).
	acPanicThreshold = 1.0
	// acCooldownTicks is how many ticks a direction holds after a change before
	// it may fire again: a change at tick T allows the next at tick T+3, i.e.
	// 15s apart at acTick, giving the change time to register in the signal
	// first. Increases and decreases hold independent cooldowns — see tick().
	acCooldownTicks = 2
)

// acTick is how often the controller re-evaluates. Aligned with the
// throttler poll interval (5s) — sampling faster than the signal updates
// just adds noise. Var (not const) so tests can shorten it; production
// never mutates it.
var acTick = 5 * time.Second

// writeScaler is the optional capability the autoscaler drives. The
// SingleTargetApplier implements it; the ShardedApplier does not (yet), so the
// copier type-asserts it and skips autoscaling when it's absent.
type writeScaler interface {
	SetWriteWorkers(n int)
}

// autoScaler runs a control loop that adjusts the applier's live write-worker
// count based on the throttler's continuous utilization signal: additive ±1
// steps in the normal regime, halving only at the panic threshold (see the
// zone table above). It never touches the binary BlockWait() hard-stop, which
// remains the safety net underneath — the controller's goal is to keep
// utilization parked in the [low, high) dead-band so the hard-stop is rarely
// hit.
type autoScaler struct {
	throttler          throttler.GradualThrottler
	scaler             writeScaler
	min, max           int
	current            int
	low, high, panicAt float64
	// upCooldown gates increases; downCooldown gates decreases. They are
	// separate so a fresh overload can halve immediately even right after an
	// increase (which likely caused it), while consecutive halvings are still
	// spaced out enough for the signal to reflect the previous cut.
	upCooldown, downCooldown int
	logger                   *slog.Logger
	metricsSink              metrics.Sink
}

// newAutoScaler builds a controller. start is the resolved write-thread count
// (the applier was started at this value); maxThreads is the cap. The minimum
// is always 1 so the copy keeps making progress. Requiring a GradualThrottler
// (not just a Throttler) is what guarantees there is a continuous signal to
// control on — the caller asserts for it and skips autoscaling otherwise.
func newAutoScaler(t throttler.GradualThrottler, s writeScaler, start, maxThreads int, logger *slog.Logger, sink metrics.Sink) *autoScaler {
	if maxThreads < start {
		maxThreads = start
	}
	return &autoScaler{
		throttler:   t,
		scaler:      s,
		min:         1,
		max:         maxThreads,
		current:     start,
		low:         acLowWatermark,
		high:        acHighWatermark,
		panicAt:     acPanicThreshold,
		logger:      logger,
		metricsSink: sink,
	}
}

// run drives the control loop until ctx is cancelled.
func (a *autoScaler) run(ctx context.Context) {
	ticker := time.NewTicker(acTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tick(ctx)
		}
	}
}

// tick performs a single control step. Split out so tests can drive it directly
// without real time.
func (a *autoScaler) tick(ctx context.Context) {
	util := a.throttler.Utilization()

	acted := false
	switch {
	case util >= a.panicAt:
		// Panic: multiplicative backoff, at most once per cooldown window.
		// The first breach halves immediately — it is never delayed by an
		// increase's cooldown, since the increase likely caused the overload.
		// Consecutive halvings wait out the window: the signal updates on the
		// same ~5s cadence we tick on, so reacting to every tick would halve
		// repeatedly on one stale window. The BlockWait hard-stop engages in
		// this zone too, so the copy is already paused — the halve is about
		// resuming gently, not about stopping the bleeding.
		if a.downCooldown == 0 {
			a.set(ceilDiv(a.current, 2))
			a.downCooldown = acCooldownTicks
			a.upCooldown = acCooldownTicks
			acted = true
		}
	case util >= a.high:
		// Soft overload: additive decrease, the mirror image of the increase
		// path. Like the panic path it is gated only by the down cooldown, so
		// the first shed is never delayed by a recent increase's cooldown.
		// Shedding one thread at a time avoids the halve-and-reclimb sawtooth
		// on a signal our own workers largely produce.
		if a.downCooldown == 0 {
			a.set(a.current - 1)
			a.downCooldown = acCooldownTicks
			a.upCooldown = acCooldownTicks
			acted = true
		}
	case util < a.low && a.upCooldown == 0:
		// Cautious, cooldown-gated additive increase.
		a.set(a.current + 1)
		a.upCooldown = acCooldownTicks
		acted = true
	}
	if !acted {
		// Dead-band, or waiting out a cooldown after a recent change.
		if a.upCooldown > 0 {
			a.upCooldown--
		}
		if a.downCooldown > 0 {
			a.downCooldown--
		}
	}

	a.emit(ctx, util)
}

// set clamps target to [min, max] and applies it only when it actually changes,
// logging the transition at Info.
func (a *autoScaler) set(target int) {
	if target < a.min {
		target = a.min
	}
	if target > a.max {
		target = a.max
	}
	if target == a.current {
		return
	}
	a.logger.Info("autoscaler adjusting write threads",
		"from", a.current, "to", target, "min", a.min, "max", a.max)
	a.current = target
	a.scaler.SetWriteWorkers(target)
}

// emit reports the current thread count and observed utilization every tick.
func (a *autoScaler) emit(ctx context.Context, util float64) {
	if a.metricsSink == nil {
		return
	}
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{Name: metrics.WriteThreadsMetricName, Type: metrics.GAUGE, Value: float64(a.current)},
			{Name: metrics.ThrottlerUtilizationMetricName, Type: metrics.GAUGE, Value: util},
		},
	}
	sendCtx, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()
	if err := a.metricsSink.Send(sendCtx, m); err != nil {
		a.logger.Debug("autoscaler metrics send failed", "error", err)
	}
}

// ceilDiv returns ceil(n/d) for positive integers — used for the multiplicative
// halving so that, e.g., 3 backs off to 2 rather than 1.
func ceilDiv(n, d int) int {
	return (n + d - 1) / d
}
