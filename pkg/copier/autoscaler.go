package copier

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
)

// AIMD (additive-increase / multiplicative-decrease) tunables. The shape is
// "cautious up, fast down": we add one thread at a time and only after a
// cooldown, but halve as soon as load approaches the hard-stop (further
// halvings then wait out the cooldown so the signal can catch up). See
// issue #831.
const (
	// acTick is how often the controller re-evaluates. Aligned with the
	// throttler poll interval (5s) — sampling faster than the signal updates
	// just adds noise.
	acTick = 5 * time.Second
	// acLowWatermark: below this utilization there is headroom on every signal,
	// so we may add a thread (subject to cooldown). acHighWatermark: at or above
	// this we back off immediately. The band between them is the dead-zone where
	// we hold steady. High is below 1.0 so we ease off *before* tripping the
	// hard-stop BlockWait rather than relying on it.
	acLowWatermark  = 0.5
	acHighWatermark = 0.9
	// acCooldownTicks is how many ticks a direction holds after a change before
	// it may fire again: a change at tick T allows the next at tick T+3, i.e.
	// 15s apart at acTick, giving the change time to register in the signal
	// first. Increases and decreases hold independent cooldowns — see tick().
	acCooldownTicks = 2
)

// writeScaler is the optional capability the autoscaler drives. The
// SingleTargetApplier implements it; the ShardedApplier does not (yet), so the
// copier type-asserts it and skips autoscaling when it's absent.
type writeScaler interface {
	SetWriteWorkers(n int)
}

// autoScaler runs an AIMD control loop that adjusts the applier's live
// write-worker count based on the throttler's continuous utilization signal.
// It never touches the binary BlockWait() hard-stop, which remains the safety
// net underneath — the controller's goal is to keep utilization parked in the
// [low, high) dead-band so the hard-stop is rarely hit.
type autoScaler struct {
	throttler throttler.GradualThrottler
	scaler    writeScaler
	min, max  int
	current   int
	low, high float64
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
	case util >= a.high:
		// Multiplicative backoff, at most once per cooldown window. The first
		// breach halves immediately — it is never delayed by an increase's
		// cooldown, since the increase likely caused the overload. Consecutive
		// halvings wait out the window: the signal updates on the same ~5s
		// cadence we tick on, so reacting to every tick would halve repeatedly
		// on one stale window. If load keeps climbing in the meantime, the
		// BlockWait hard-stop at 1.0 remains the emergency brake.
		if a.downCooldown == 0 {
			a.set(ceilDiv(a.current, 2))
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
