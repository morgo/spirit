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
	// csBacklogVetoDeltas is the post-flush residual above which the change feed
	// is considered capable of losing ground. It matches the figure pkg/change
	// uses to call a backlog "trivial" (binlogTrivialThreshold), which is the
	// mark BlockWait waits for before allowing cut-over: below it, the feed is
	// already drained enough for the migration to finish, so a residual under it
	// is not worth shedding a worker over however it is trending.
	csBacklogVetoDeltas = 10000
	// csBacklogHysteresisFlushes is how many consecutive flushes must agree
	// before the verdict changes, in either direction.
	//
	// The exit condition matters as much as the entry one, and asymmetrically so.
	// Growth is gated on a 2-tick cooldown (one step per 10s) while shedding is
	// one step per flush (30s), so growth is three times faster. If a single
	// favourable flush cleared the verdict, a feed that is losing ground on
	// balance but not monotonically would have its sheds more than undone by the
	// grows in between, and the controller would net-grow while the feed fell
	// further behind.
	csBacklogHysteresisFlushes = 2
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
//     before a resume can replay them). If the feed is losing ground, our
//     reads are winning a race against writes that actually have to finish,
//     so we shed — on Aurora and stock MySQL alike.
//
//     "Losing ground" is specifically a rising *post-flush residual*, not a
//     rising backlog, and it is read from change.Source.FlushResidual rather
//     than polled. The raw count rises between every pair of flushes on any
//     busy table, so its slope carries no information, and polling for its
//     troughs does not recover the residual either — a poll lands some fixed
//     fraction of an interval after the flush and so reads the residual plus
//     that fraction's worth of writes. What a healthy feed guarantees is that
//     each flush returns the backlog to near zero, and only the feed itself can
//     say what a flush left behind.
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
	// backlog reports what the feed's most recent flush left behind and a
	// monotonic flush counter, per change.Source.FlushResidual. nil disables the
	// veto.
	backlog func() (residual, flushes int)
	limiter *autoscale.Limiter

	min, start, max int
	current         int
	// upCooldown gates increases; downCooldown gates decreases. Separate so a
	// fresh overload can shed immediately even right after an increase (which
	// likely caused it), while consecutive sheds are still spaced out enough
	// for the signal to reflect the previous cut.
	upCooldown, downCooldown int
	// Residual-tracking state, all owned by observeBacklog. lastFlushes is the
	// flush counter at the most recent evaluation, so a residual is compared only
	// once per flush; prevResidual the previous flush's residual, or -1 before any
	// flush has been seen; rising/falling count consecutive agreeing flushes for
	// the hysteresis.
	lastFlushes     int
	prevResidual    int
	rising, falling int
	// backlogLosing is the current verdict, and gates recovery as well as
	// shedding — a pass must not climb back up while the feed is losing ground.
	backlogLosing bool
	// shedForFlush records that a worker has already been given up for the
	// current flush, so one flush costs at most one worker however many ticks
	// elapse before the next.
	shedForFlush bool

	logger      *slog.Logger
	metricsSink metrics.Sink
}

// newChecksumScaler builds a controller over limiter. start is the count the
// pass began at, maxThreads the ceiling it may grow to (which the caller must
// have provisioned transactions for — see the snapshot note on
// SingleChecker.initConnPool). t may be any throttler; the continuous signal
// is used only if it provides one.
func newChecksumScaler(t throttler.Throttler, limiter *autoscale.Limiter, backlog func() (residual, flushes int), start, maxThreads int, logger *slog.Logger, sink metrics.Sink) *checksumScaler {
	start = max(start, 1)
	if maxThreads < start {
		maxThreads = start
	}
	s := &checksumScaler{
		backlog: backlog,
		limiter: limiter,
		min:     1,
		start:   start,
		max:     maxThreads,
		current: start,
		// No flush has been observed yet, so there is nothing to compare against.
		prevResidual: -1,
		logger:       logger,
		metricsSink:  sink,
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
// Precedence, highest first:
//
//  1. The utilization panic zone. Both this and the backlog veto shed, and
//     this sheds faster (multiplicatively), so it has to be evaluated first:
//     if the veto were checked ahead of it, a tick spent inside the veto's
//     cooldown would consume the panic response and downgrade backoff to -1
//     per cooldown at exactly the moment the server is most overloaded.
//  2. The backlog veto, which therefore outranks Grow, Hold and Shed.
//     Utilization can show plenty of headroom while the feed still falls
//     behind, because the feed's writes are a small share of server load but a
//     hard prerequisite for finishing the migration.
//  3. The utilization zone law.
//
// The Hold case doubles as the recovery path. Without it a transient backlog
// would permanently cost throughput for the rest of the pass: on stock MySQL
// there is no other way back up at all, and on Aurora the dead band would sit
// on the reduced count indefinitely. Recovery stops at the configured start —
// growing beyond what the operator asked for requires a positive headroom
// signal, not merely the absence of a negative one.
func (s *checksumScaler) tick(ctx context.Context) {
	util := 0.0
	zone := autoscale.Hold
	if s.gradual != nil {
		util = s.gradual.Utilization()
		zone = autoscale.Classify(util)
	}
	backlogVeto := s.observeBacklog()

	acted := false
	switch {
	case zone == autoscale.Halve:
		// The hard stop is typically already firing on raw samples in this
		// zone, so the pass is being paused anyway; halving is about resuming
		// gently. Gated on the down cooldown only, so the first breach is
		// never delayed by a recent increase's cooldown.
		if s.downCooldown == 0 {
			s.set(autoscale.CeilDiv(s.current, 2), "utilization at panic threshold")
			s.shedCooldowns()
			acted = true
		}
	case backlogVeto:
		// The feed is losing ground. Shed one worker to hand read capacity
		// back to it. This is the only shedding lever on stock MySQL.
		if s.downCooldown == 0 {
			s.set(s.current-1, "change-feed backlog not draining")
			s.shedForFlush = true
			s.shedCooldowns()
			acted = true
		}
	case zone == autoscale.Shed:
		if s.downCooldown == 0 {
			s.set(s.current-1, "utilization above high watermark")
			s.shedCooldowns()
			acted = true
		}
	case zone == autoscale.Grow:
		// Also gated on backlogLosing. The veto above is deliberately one-shot
		// per window, so without this the other ticks of a losing window would
		// grow straight back into the shed and the two signals would cancel out.
		if !s.backlogLosing && s.upCooldown == 0 {
			s.set(s.current+1, "utilization below low watermark")
			s.upCooldown = autoscale.CooldownTicks
			acted = true
		}
	default: // Hold, or no continuous signal at all.
		// Gated on backlogLosing for the same reason, not on the one-shot veto:
		// the veto clears as soon as it is acted on, so recovering on its absence
		// would hand the worker straight back while the feed is still behind.
		if !s.backlogLosing && s.current < s.start && s.upCooldown == 0 {
			s.set(s.current+1, "recovering toward configured thread count")
			s.upCooldown = autoscale.CooldownTicks
			acted = true
		}
	}

	if !acted {
		// Nothing to do, or waiting out a cooldown after a recent change.
		if s.upCooldown > 0 {
			s.upCooldown--
		}
		if s.downCooldown > 0 {
			s.downCooldown--
		}
	}
	s.emit(ctx, util)
}

// shedCooldowns is applied after any decrease. It sets the up cooldown too, so
// a shed is not immediately undone by a growth signal that has not yet had time
// to reflect the cut.
func (s *checksumScaler) shedCooldowns() {
	s.downCooldown = autoscale.CooldownTicks
	s.upCooldown = autoscale.CooldownTicks
}

// observeBacklog samples the change feed and reports whether the veto is in
// force this tick.
//
// The measured quantity is the residual the feed itself recorded at the end of
// each flush, compared across consecutive flushes. On a feed that is keeping up
// every flush drains the backlog to near zero, so successive residuals stay
// near zero however heavy the write load and however steeply the count climbs
// in between. A residual that is both large and above the previous flush's is
// the feed reporting it could not finish what it started: work is surviving
// flushes and accumulating.
//
// Ticks are much more frequent than flushes, so most calls have no new
// information; those return the standing verdict, actionable only if it has not
// already been paid for. Latching this way means a cooldown cannot swallow the
// signal, while shedForFlush caps the cost of one flush at one worker.
func (s *checksumScaler) observeBacklog() bool {
	if s.backlog == nil {
		return false
	}
	residual, flushes := s.backlog()
	if flushes == s.lastFlushes {
		return s.backlogLosing && !s.shedForFlush
	}
	s.lastFlushes = flushes

	// prevResidual < 0 on the first flush observed: establish the baseline only.
	if s.prevResidual >= 0 && residual >= csBacklogVetoDeltas && residual > s.prevResidual {
		s.rising++
		s.falling = 0
	} else {
		s.falling++
		s.rising = 0
	}
	s.prevResidual = residual
	switch {
	case s.rising >= csBacklogHysteresisFlushes:
		s.backlogLosing = true
	case s.falling >= csBacklogHysteresisFlushes:
		s.backlogLosing = false
	}
	s.shedForFlush = false
	return s.backlogLosing
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
