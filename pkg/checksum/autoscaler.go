package checksum

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
)

// csTick is how often the checksum controller re-evaluates. Var (not const) so
// tests can shorten it; production never mutates it.
var csTick = autoscale.Tick

const (
	// csBacklogVetoDeltas is the pending-change count above which the change
	// feed is considered to be losing ground. It matches the figure
	// pkg/change uses to call a backlog "trivial" (binlogTrivialThreshold):
	// below it, flushing is cheap enough that a rising count is just noise.
	csBacklogVetoDeltas = 10000
	// csBacklogPersistTicks is how many consecutive ticks the backlog must be
	// both large and rising before it vetoes. A single rising sample is
	// expected — the feed buffers between flushes, so depth naturally sawtooths
	// on the flush interval. Sustained growth across ticks is the real signal.
	csBacklogPersistTicks = 2
)

// checksumScaler adjusts the checksum phase's live worker count for the
// duration of one pass.
//
// It applies the same utilization zone law as the copier's autoscaler (see
// pkg/autoscale), with two differences that follow from what the checksum
// phase actually is:
//
//  1. One pool, no arbitration. The copier's dual controller uses the applier
//     queue to decide whether to grow the read or the write side. The checksum
//     has no such queue: each chunk's read and aggregate happen inside one
//     goroutine, server-side, so there is only one pool to size.
//
//  2. A change-feed backlog veto, which works everywhere. The utilization
//     signal comes only from the Aurora throttlers (they are the ones that
//     implement throttler.GradualThrottler), so on stock MySQL there is
//     nothing continuous to control on and the controller cannot grow. But
//     every deployment can observe the one thing that matters during a
//     checksum: whether the change feed is keeping up. The feed flushes
//     concurrently with the checksum, and its backlog is what gates cut-over
//     (and, if it grows unboundedly, what risks the binlogs being purged
//     before a resume can replay them). A backlog that is both large and
//     sustainedly rising means our reads are winning the race against the
//     writes that actually need to finish, so we shed — on Aurora and stock
//     MySQL alike.
//
// So the capability matrix is:
//
//	                      hard stop   shed on backlog   grow
//	stock MySQL           yes         yes               no (back to start only)
//	Aurora + autoscaling  yes         yes               yes (utilization law)
//
// The hard stop (throttler.BlockWait) is not this type's job — the dispatch
// loop calls it directly, and it remains the safety net underneath whatever
// the controller decides.
type checksumScaler struct {
	// gradual is the continuous signal, or nil when the throttler does not
	// provide one (stock MySQL, replica-lag-only, or Noop). nil means growth
	// above start is impossible; everything else still works.
	gradual throttler.GradualThrottler
	// backlog reports the change feed's pending-change count. nil disables the
	// veto.
	backlog func() int
	limiter *autoscale.Limiter

	min, start, max int
	current         int
	// upCooldown gates increases; downCooldown gates decreases. Separate so a
	// fresh overload can shed immediately even right after an increase (which
	// likely caused it), while consecutive sheds are still spaced out enough
	// for the signal to reflect the previous cut.
	upCooldown, downCooldown int
	// backlogTicks counts consecutive ticks the backlog has been large and
	// rising; lastBacklog is the previous sample. See observeBacklog.
	backlogTicks int
	lastBacklog  int

	logger      *slog.Logger
	metricsSink metrics.Sink
}

// newChecksumScaler builds a controller over limiter. start is the count the
// pass began at, maxThreads the ceiling it may grow to (which the caller must
// have provisioned transactions for — see the snapshot note on
// SingleChecker.initConnPool). t may be any throttler; the continuous signal
// is used only if it provides one.
func newChecksumScaler(t throttler.Throttler, limiter *autoscale.Limiter, backlog func() int, start, maxThreads int, logger *slog.Logger, sink metrics.Sink) *checksumScaler {
	start = max(start, 1)
	if maxThreads < start {
		maxThreads = start
	}
	s := &checksumScaler{
		backlog:     backlog,
		limiter:     limiter,
		min:         1,
		start:       start,
		max:         maxThreads,
		current:     start,
		logger:      logger,
		metricsSink: sink,
	}
	// A GradualThrottler is what makes growth possible; without one the
	// controller is shed-and-recover only. multiThrottler forwards the
	// assertion to its children, so a mixed replica-lag + Aurora setup still
	// yields a signal.
	if g, ok := t.(throttler.GradualThrottler); ok {
		s.gradual = g
	}
	return s
}

// run drives the control loop until ctx is cancelled. Callers start it per
// pass, so a yield/resume cycle gets a fresh controller at start.
func (s *checksumScaler) run(ctx context.Context) {
	ticker := time.NewTicker(csTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// tick performs a single control step. Split out so tests can drive it
// directly without real time.
//
// The backlog veto is evaluated first and outranks everything: utilization can
// look like there is plenty of headroom while the feed still falls behind,
// because the feed's writes are a small share of server load but a hard
// prerequisite for finishing the migration.
func (s *checksumScaler) tick(ctx context.Context) {
	util := 0.0
	if s.gradual != nil {
		util = s.gradual.Utilization()
	}

	acted := false
	switch {
	case s.observeBacklog():
		// The feed is losing ground. Shed one worker to hand read capacity
		// back to it. This is the only lever available on stock MySQL.
		if s.downCooldown == 0 {
			s.set(s.current-1, "change-feed backlog rising")
			s.downCooldown = autoscale.CooldownTicks
			s.upCooldown = autoscale.CooldownTicks
			acted = true
		}
	case s.gradual == nil:
		// No continuous signal, so no guided growth. Recover toward the
		// configured start once the backlog is healthy again, otherwise a
		// single transient backlog would permanently halve checksum
		// throughput for the rest of the pass.
		if s.current < s.start && s.upCooldown == 0 {
			s.set(s.current+1, "backlog recovered")
			s.upCooldown = autoscale.CooldownTicks
			acted = true
		}
	default:
		acted = s.actOnUtilization(util)
	}

	if !acted {
		// Dead band, or waiting out a cooldown after a recent change.
		if s.upCooldown > 0 {
			s.upCooldown--
		}
		if s.downCooldown > 0 {
			s.downCooldown--
		}
	}
	s.emit(ctx, util)
}

// actOnUtilization applies the zone law for one sample, returning whether it
// changed anything. Only reached when a continuous signal exists.
func (s *checksumScaler) actOnUtilization(util float64) bool {
	switch autoscale.Classify(util) {
	case autoscale.Halve:
		// The hard stop is typically already firing on raw samples in this
		// zone, so the pass is being paused anyway; halving is about resuming
		// gently. Gated on the down cooldown only, so the first breach is
		// never delayed by a recent increase's cooldown.
		if s.downCooldown == 0 {
			s.set(autoscale.CeilDiv(s.current, 2), "utilization at panic threshold")
			s.downCooldown = autoscale.CooldownTicks
			s.upCooldown = autoscale.CooldownTicks
			return true
		}
	case autoscale.Shed:
		if s.downCooldown == 0 {
			s.set(s.current-1, "utilization above high watermark")
			s.downCooldown = autoscale.CooldownTicks
			s.upCooldown = autoscale.CooldownTicks
			return true
		}
	case autoscale.Grow:
		if s.upCooldown == 0 {
			s.set(s.current+1, "utilization below low watermark")
			s.upCooldown = autoscale.CooldownTicks
			return true
		}
	case autoscale.Hold:
	}
	return false
}

// observeBacklog samples the change feed and folds it into the persistence
// tracking, reporting whether the veto is in force this tick: true once the
// backlog has been both above csBacklogVetoDeltas and rising for
// csBacklogPersistTicks consecutive ticks.
func (s *checksumScaler) observeBacklog() bool {
	if s.backlog == nil {
		return false
	}
	n := s.backlog()
	rising := n > s.lastBacklog
	s.lastBacklog = n
	if n >= csBacklogVetoDeltas && rising {
		s.backlogTicks++
	} else {
		s.backlogTicks = 0
	}
	return s.backlogTicks >= csBacklogPersistTicks
}

// set clamps target to [min, max] and applies it only when it actually
// changes, logging the transition and why at Info.
func (s *checksumScaler) set(target int, reason string) {
	target = min(max(target, s.min), s.max)
	if target == s.current {
		return
	}
	s.logger.Info("autoscaler adjusting checksum threads",
		"from", s.current, "to", target, "min", s.min, "max", s.max, "reason", reason)
	s.current = target
	s.limiter.SetLimit(target)
}

// emit reports the live thread count and observed utilization every tick.
func (s *checksumScaler) emit(ctx context.Context, util float64) {
	if s.metricsSink == nil {
		return
	}
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{Name: metrics.ChecksumThreadsMetricName, Type: metrics.GAUGE, Value: float64(s.current)},
		},
	}
	// Utilization is only meaningful when a continuous signal exists; emitting
	// a hard zero on stock MySQL would read as "completely idle" on dashboards
	// rather than "not measured".
	if s.gradual != nil {
		m.Values = append(m.Values,
			metrics.MetricValue{Name: metrics.ThrottlerUtilizationMetricName, Type: metrics.GAUGE, Value: util})
	}
	sendCtx, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()
	if err := s.metricsSink.Send(sendCtx, m); err != nil {
		s.logger.Debug("checksum autoscaler metrics send failed", "error", err)
	}
}
