// Package autoscale holds the primitives shared by spirit's phase-level
// thread-count controllers.
//
// Everything here is needed by more than one phase and belongs to none of them:
//
//   - The utilization zone law: the watermark/cooldown constants, Classify, and
//     MinVCPUs (the instance size below which the signal is too coarse for the
//     law to work at all). The copier's write/read autoscaler and the checksum
//     controller apply the same law to different pools; defining the thresholds
//     once means they cannot silently drift apart.
//   - Gate (in controller.go), which turns one tick's signals into a Plan. It
//     owns the precedence between the zones, a caller-supplied veto, and the
//     cooldown bookkeeping — the part most likely to drift if each phase kept its
//     own copy, and the hardest to notice when it does.
//   - The plumbing every controller repeats: Ceiling for a scalable pool's upper
//     bound, RunTicker for the tick loop, Emit for best-effort gauges.
//   - Limiter, a concurrency gate whose limit can change while work is in
//     flight. errgroup.SetLimit cannot: the errgroup contract forbids
//     modifying the limit while any goroutine in the group is active, so a
//     phase that wants to be resized mid-pass needs its own gate.
//
// What stays with each phase is what is genuinely phase-specific: how big a step
// is, which pool it lands on, and what may veto one. The copier apportions a
// step between its read and write pools using the applier queue; the checksum has
// one pool and a change-feed backlog veto.
//
// The law's rationale — why additive steps with a multiplicative panic
// backoff, and why the dead band has hysteresis — is documented at length on
// the copier's autoScaler, which was where it was first derived (issue #831).
// This package deliberately holds only the mechanism, not that history.
package autoscale

import (
	"context"
	"sync"
	"time"
)

const (
	// LowWatermark is the effective setpoint: below it there is headroom, so a
	// pool may add a thread (subject to cooldown).
	LowWatermark = 0.4
	// HighWatermark starts the additive back-off. The dead band between the
	// watermarks must be wider than the utilization step of a single thread,
	// otherwise one +1 can vault across the band and ping-pong with the -1
	// path.
	HighWatermark = 0.7
	// PanicThreshold is where back-off turns multiplicative. At 1.0 the
	// smoothed signal has reached the throttle point, where the binary
	// hard-stop is typically already firing on raw samples — so halving is
	// about resuming gently, not about stopping the bleeding.
	PanicThreshold = 1.0
	// CooldownTicks is how many ticks a direction holds after a change before
	// it may fire again, giving the change time to register in the signal.
	// Increases and decreases hold independent cooldowns.
	CooldownTicks = 2
	// MinVCPUs is the smallest instance size (in vCPUs) on which a controller is
	// allowed to engage at all. It is a property of the law above rather than of
	// any one phase: the utilization signal's denominator is the vCPU count, so
	// below this one thread is half or a third of the whole scale and no dead band
	// is wide enough to rest in — the controller can only oscillate. Observed in
	// staging on r6g.large (2 vCPUs): the write-thread count ping-ponged 1↔2
	// indefinitely (issue #831). At 4+ vCPUs the worst-case per-thread step (0.25)
	// fits inside the dead band.
	//
	// The migration runner enforces it once, at setup, by disabling autoscaling
	// for the whole migration; the controllers themselves never see a small
	// instance.
	MinVCPUs = 4

	// VCPUReserve is how many vCPUs a pool sized from the instance leaves free,
	// so spirit never nominally claims the whole server: the other pool, the
	// server's own background work, and the application all need room. Both the
	// write pool (max(1, vCPUs-VCPUReserve)) and the read-side starting point
	// (see ReadBounds) subtract it.
	VCPUReserve = 2

	// MinReadStartThreads is the floor on a read-side pool's starting size.
	// ReadBounds' divisor drives small instances down to 1, which would make the
	// copy single-threaded until the controller has ramped for 15s a step; two
	// threads is the smallest start that still overlaps read and apply work from
	// the first chunk.
	MinReadStartThreads = 2

	// readStartDivisor makes the read side start at roughly a quarter of the
	// instance. See ReadBounds for why the read and write sides are asymmetric.
	readStartDivisor = 4
)

// Tick is how often a controller should re-evaluate. Aligned with the
// throttler poll interval — sampling faster than the signal updates just adds
// noise. Callers copy this into their own var so tests can shorten it without
// racing other packages.
const Tick = 5 * time.Second

// Action is the decision the zone law reaches for one sample of the
// utilization signal.
type Action int

const (
	// Hold means the signal is inside the dead band: change nothing.
	Hold Action = iota
	// Grow means there is headroom to add one thread.
	Grow
	// Shed means soft overload: remove one thread.
	Shed
	// Halve means the signal has reached the throttle point: multiplicative
	// backoff.
	Halve
)

// Classify maps one utilization sample onto the zone law:
//
//	util < LowWatermark                    Grow
//	[LowWatermark, HighWatermark)          Hold
//	[HighWatermark, PanicThreshold)        Shed
//	util >= PanicThreshold                 Halve
//
// It is deliberately pure and cooldown-free: callers own the cooldown state,
// because which cooldown gates which action differs between the write-only,
// dual-pool, and checksum controllers.
func Classify(util float64) Action {
	switch {
	case util >= PanicThreshold:
		return Halve
	case util >= HighWatermark:
		return Shed
	case util < LowWatermark:
		return Grow
	default:
		return Hold
	}
}

// CeilDiv returns ceil(n/d) for positive integers — used for multiplicative
// halving so that, e.g., 3 backs off to 2 rather than 1.
func CeilDiv(n, d int) int {
	return (n + d - 1) / d
}

// ReadBounds returns the starting size and ceiling for a read-side pool — the
// copier's read workers and the checksum's workers — on an instance of the given
// vCPU count. Callers must have already established that the instance is at
// least MinVCPUs; below that no controller engages at all.
//
// The read side starts at about a quarter of the instance and may grow to all of
// it. That is deliberately not the write side's shape (start at vCPUs-VCPUReserve,
// grow to 2x that), because the two pools are limited by different things. Write
// threads spend most of their life parked on a redo-log flush, so a count above
// the vCPU count is not oversubscription — it is what keeps the log busy, and it
// is why the redo-aware load signal excludes those waiters. A read thread
// scanning a table that is already in the buffer pool is pure CPU, so the same
// count really does compete with the application for cores: oversubscribing here
// is how a checksum ends up degrading the workload it was supposed to be
// invisible to. Hence start small and let the load signal earn the way up,
// stopping at the physical limit.
func ReadBounds(vCPUs int) (start, ceiling int) {
	ceiling = max(vCPUs, MinReadStartThreads)
	start = max(MinReadStartThreads, CeilDiv(vCPUs-VCPUReserve, readStartDivisor))
	return min(start, ceiling), ceiling
}

// Limiter is a counting semaphore whose limit may change while permits are
// held. It is the resizable stand-in for errgroup.SetLimit, which may not be
// modified while goroutines in the group are active.
//
// Lowering the limit never interrupts work in flight: the reduction is
// absorbed by subsequent Releases, so callers holding a permit always run to
// completion. That property is what makes shedding cheap for phases whose
// unit of work is expensive to redo — a cancelled chunk is wasted I/O, a
// parked worker is not.
//
// The zero value is not usable; call NewLimiter.
type Limiter struct {
	mu       sync.Mutex
	limit    int
	inFlight int
	// ready is a broadcast channel, closed and replaced whenever a permit may
	// have become available (a Release, or a limit increase). Waiters select
	// on the generation they observed, so no wakeup is lost and none is
	// missed: a waiter that reads the channel under the lock cannot have the
	// close happen between its check and its select.
	ready chan struct{}
}

// NewLimiter returns a Limiter admitting at most limit concurrent holders.
// A limit below 1 is raised to 1 — a limiter that admits nobody would
// deadlock its caller rather than throttle it.
func NewLimiter(limit int) *Limiter {
	return &Limiter{
		limit: max(limit, 1),
		ready: make(chan struct{}),
	}
}

// Acquire blocks until a permit is available or ctx is done, returning
// ctx.Err() in the latter case. On success the caller must call Release
// exactly once.
func (l *Limiter) Acquire(ctx context.Context) error {
	for {
		// Check ctx first so a cancelled context cannot be starved by a
		// permanently available permit.
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		if l.inFlight < l.limit {
			l.inFlight++
			l.mu.Unlock()
			return nil
		}
		wait := l.ready
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

// Release returns a permit. It must be called exactly once per successful
// Acquire.
func (l *Limiter) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight > 0 {
		l.inFlight--
	}
	l.broadcastLocked()
}

// SetLimit changes the number of permits. Raising it wakes waiters; lowering
// it takes effect as holders release, never by interrupting them. A limit
// below 1 is raised to 1, matching NewLimiter.
func (l *Limiter) SetLimit(n int) {
	n = max(n, 1)
	l.mu.Lock()
	defer l.mu.Unlock()
	grew := n > l.limit
	l.limit = n
	if grew {
		l.broadcastLocked()
	}
}

// Limit reports the current permit ceiling.
func (l *Limiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

// InFlight reports how many permits are currently held. It can exceed Limit
// transiently, right after a reduction that in-flight holders have not yet
// absorbed.
func (l *Limiter) InFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inFlight
}

// broadcastLocked wakes every current waiter. Caller must hold l.mu.
func (l *Limiter) broadcastLocked() {
	close(l.ready)
	l.ready = make(chan struct{})
}
