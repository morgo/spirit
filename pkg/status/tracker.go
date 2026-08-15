package status

import (
	"sync"
	"time"
)

// Tracker owns the current State plus per-state wall-clock timing. Runners
// embed it in place of a bare State field so that state transitions and phase
// timing cannot drift apart, and so per-phase durations no longer need ad-hoc
// fields on the runner (copyDuration, sentinelWaitStartTime, ...).
//
// The zero value is ready for use.
//
// Phases with a clear extent run under Do, which times exactly the function it
// brackets. Set remains the primitive for transitions whose "phase" has no
// meaningful end from the setter's perspective (Close, ErrCleanup); it closes
// out the previous state's running interval, matching the historical "one
// state ends when the next starts" semantics.
//
// Tracker assumes spirit's linear execution model: a single goroutine advances
// through the phases in order, and the only concurrent transition is a fatal
// Set (ErrCleanup) racing an open bracket. In that case time accrues to the
// bracketed state up to the fatal transition and the bracket's own exit
// becomes a no-op. It is not designed for concurrent or overlapping phases.
type Tracker struct {
	state State // atomic; safe to read via Get concurrently with Do/Set

	mu        sync.Mutex
	startedAt time.Time               // first transition; the zero point for TotalElapsed
	enteredAt time.Time               // when the current state was entered
	open      bool                    // the current state has a running interval
	durations map[State]time.Duration // closed time attributed per state
}

// Begin marks the start of a run: it resets all timing (start time, per-state
// durations) and enters Initial, so setup work before the first phase is
// attributed to Initial and TotalElapsed measures from here. Runners call it
// at the top of Run, where they previously recorded a startTime field. Calling
// Begin again starts a fresh run rather than extending the previous one.
func (t *Tracker) Begin() {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startedAt = now
	t.enteredAt = now
	t.open = true
	t.durations = nil
	t.state.set(Initial)
}

// Get returns the current state.
func (t *Tracker) Get() State {
	return t.state.get()
}

// Set transitions to state without a bracket: it closes the previous state's
// still-open interval (if a completed Do already closed it, the gap since is
// left unattributed) and state begins accruing now. Prefer Do wherever the
// phase has a clear extent.
func (t *Tracker) Set(state State) {
	t.enter(state)
}

// Do runs fn as the given state: it transitions to state, runs fn, and
// attributes fn's wall-clock time (panic inclusive) to state. The state
// remains current after Do returns — as with Set, the next state begins only
// when it is entered.
func (t *Tracker) Do(state State, fn func() error) error {
	t.enter(state)
	defer t.exit(state)
	return fn()
}

// StartTime returns when the run began: Begin, or the first transition if
// Begin was never called. It is the zero time before either, and stable for
// the life of a run — migration derives timestamped _old table names from it.
func (t *Tracker) StartTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.startedAt
}

// TotalElapsed returns how long the tracker has been running: the time since
// Begin (or, if Begin was never called, since the first transition). It
// reports 0 before either. This is the value to render as "total-time".
func (t *Tracker) TotalElapsed() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.startedAt.IsZero() {
		return 0
	}
	return time.Since(t.startedAt)
}

// Elapsed returns how long the current state has been current. It reports 0
// before the first transition. This is the value to render on the status
// block's header ("copier-time", "sentinel-wait-time", ...).
func (t *Tracker) Elapsed() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.enteredAt.IsZero() {
		return 0
	}
	return time.Since(t.enteredAt)
}

// Duration returns the total time attributed to state so far, including the
// still-running interval when state is current. States visited more than once
// accumulate. Note that once a bracket completes its interval is closed:
// Duration(state) freezes while Elapsed keeps growing until the next
// transition — they answer different questions.
func (t *Tracker) Duration(state State) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	d := t.durations[state]
	if t.open && t.state.get() == state {
		d += time.Since(t.enteredAt)
	}
	return d
}

func (t *Tracker) enter(state State) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.startedAt.IsZero() {
		t.startedAt = now
	}
	if t.open {
		t.accrueLocked(now)
	}
	t.state.set(state)
	t.enteredAt = now
	t.open = true
}

// exit closes the bracket opened by Do for state. If a nested Do or a Set has
// already transitioned away, the interval was closed at that transition and
// exit is a no-op — time between an inner bracket's end and the outer's end is
// deliberately unattributed rather than double counted.
func (t *Tracker) exit(state State) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.open && t.state.get() == state {
		t.accrueLocked(now)
	}
}

// accrueLocked adds the running interval to the current state's total and
// closes it. Callers must hold t.mu.
func (t *Tracker) accrueLocked(now time.Time) {
	if t.durations == nil {
		t.durations = make(map[State]time.Duration)
	}
	t.durations[t.state.get()] += now.Sub(t.enteredAt)
	t.open = false
}
