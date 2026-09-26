package change

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/block/spirit/pkg/table"
)

// Parking exists for one caller: the lockless checksum, verifying a row that is
// written continuously.
//
// Such a row cannot be verified by reading both sides. Any SQL comparison is
// between a source image at one position and a target state at a later one, and
// closing that window means stopping the writes. But the stream already carries
// the answer. With binlog_row_image=FULL, an event's after-image *is* the
// source's value for that row at that position — MySQL says so, no read
// required. So the verification is:
//
//	park the feed; wait for the next event for this row; park at it;
//	flush, so the target holds exactly that image; compare the target to it.
//
// A mismatch there is a real inconsistency: the feed just delivered that image
// and the flush just applied it, and nothing further has been admitted.
//
// This terminates in the opposite direction from a lock: the more often the row
// is written, the sooner the event arrives. The rows that defeat every
// read-and-compare strategy are exactly the rows this resolves fastest.
//
// One verification runs at a time. The checksum's hot ranges are rare by
// construction (a range reaches this only after exhausting its ordinary retries)
// and serializing them keeps the reader's park state a single piece of shared
// state rather than a set of overlapping holds.
//
// This park is unrelated to the memory-backpressure park, which blocks inside
// HasChanged when a subscription is over its soft limit. The two interact in
// one place. A watch is offered the change *before* HasChanged, so it does fire
// under backpressure — but the dispatch then blocks in HasChanged, and the
// verification is not woken until the change is buffered. So the verification
// times out and defers (correct, if useless: the caller was already deferring
// the range), and the dispatch reaches the park long after the verification is
// gone. That is what rowWaiter.abandon exists for.

// ErrRowRewritten means the watched row was written again before the
// verification could read the target, so the image the caller was handed is no
// longer what the target holds. The caller should retry.
var ErrRowRewritten = errors.New("change: watched row was rewritten during verification")

// ErrFlushIncomplete means the parked flush could not empty the buffer, so the
// watched row's change may not have reached the target. The caller should
// retry; it must not read the target and call the difference a divergence.
var ErrFlushIncomplete = errors.New("change: parked flush did not drain the buffer")

// RowWatch names the rows a verification is waiting for. Match is called with
// the row's primary key values as the binlog decoded them, in the source
// table's key order; returning true fires the watch.
type RowWatch struct {
	Schema string
	Table  string
	Match  func(key []any) bool
}

// RowVerifier is handed the event the feed parked at: the row's key, its
// after-image (nil for a delete), and whether it was a delete. It runs with the
// reader parked and the change flushed, so the target holds that image and
// nothing beyond it. Returning an error fails the verification; the reader is
// unparked either way.
type RowVerifier func(ctx context.Context, key []any, image []any, deleted bool) error

// parkGate holds a reader goroutine between events. It is advisory in one
// direction only: arming it does not interrupt an event already being
// dispatched, it stops the next one from starting. That is the guarantee the
// verification needs — "nothing after this event is delivered" — and it is why
// the gate is checked after the event is read from the stream but before it is
// acted on, so no event is consumed and discarded.
type parkGate struct {
	mu     sync.Mutex
	parked chan struct{} // non-nil while parked; closed to release
}

// park arms the gate. Calling it while already parked is a no-op, so a caller
// that parks from inside a dispatch (see rowWaiter) cannot deadlock against one
// parking from outside.
func (g *parkGate) park() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.parked == nil {
		g.parked = make(chan struct{})
	}
}

// unpark releases a parked reader. Safe to call when not parked.
func (g *parkGate) unpark() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.parked != nil {
		close(g.parked)
		g.parked = nil
	}
}

// wait blocks the reader while the gate is armed. It returns ctx.Err() if the
// context ends first, which the reader treats as a shutdown.
func (g *parkGate) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		parked := g.parked
		g.mu.Unlock()
		if parked == nil {
			return nil
		}
		select {
		case <-parked:
			// Re-check rather than return: the gate may have been re-armed
			// between the close and this wakeup.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// rowWaiter is the armed half of a verification. At most one is armed at a time;
// the mutex on the client that owns it enforces that.
type rowWaiter struct {
	watch RowWatch

	mu        sync.Mutex
	fired     bool
	abandoned bool
	key       []any
	image     []any
	deleted   bool
	rewrites  int

	releaseOnce sync.Once
	ch          chan struct{}
}

func newRowWaiter(watch RowWatch) *rowWaiter {
	return &rowWaiter{watch: watch, ch: make(chan struct{})}
}

// observe is called for every row change *before* it is buffered, and reports
// whether the watch is interested in it.
//
// Recording before the buffer is what makes the rewrite count trustworthy, and
// the ordering is load-bearing. A verification reads result() after its flush.
// If a second change to the watched key were recorded after being buffered, a
// flush could carry it to the target while result() still read zero rewrites —
// and the comparison would then be against an image the target has already
// moved past, reported as a divergence. Recording first makes that impossible:
// anything a flush can carry was counted before it could be carried.
//
// Waking the verification is deliberately *not* done here — see release.
func (w *rowWaiter) observe(schema, tbl string, key, image []any, deleted bool) bool {
	if w.watch.Schema != schema || w.watch.Table != tbl || w.watch.Match == nil || !w.watch.Match(key) {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fired {
		// rewrites counts matching changes after the one that fired. Any of
		// them makes the captured image stale, because a flush that carries
		// the newer one moves the target past what we were handed.
		w.rewrites++
		return true
	}
	w.fired = true
	w.key, w.image, w.deleted = key, image, deleted
	return true
}

// parkAndRelease holds the reader at this change and wakes the verification. It
// is called from the dispatch, once the change has been buffered. Waking from
// observe instead would let the verification flush before the watched change
// was in the buffer, so the target would not hold the image it was about to be
// compared against.
//
// The abandoned check is what makes it safe to park the reader from inside a
// dispatch. The dispatch and the verification that armed the watch run on
// different goroutines, and the verification can be gone by the time the
// dispatch gets here: HasChanged blocks on the subscription's soft limit, for
// as long as the backpressure lasts, which is easily longer than the caller's
// budget. Parking for a verification that has returned stops the feed for the
// rest of the run, because that verification was the only thing that would have
// unparked it. Serializing against abandon on w.mu means one of the two always
// happens: either the park is skipped, or abandon undoes it.
func (w *rowWaiter) parkAndRelease(g *parkGate) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.abandoned {
		return
	}
	g.park()
	w.releaseOnce.Do(func() { close(w.ch) })
}

// abandon retires the waiter and releases the reader, whatever the verification
// parked or did not park. After it returns, no dispatch still in flight on this
// waiter can park the reader again. Every exit from a verification runs it,
// including the ones that never fired.
func (w *rowWaiter) abandon(g *parkGate) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.abandoned = true
	g.unpark()
}

// result reports what fired, and whether a later change made it stale.
func (w *rowWaiter) result() (key, image []any, deleted bool, rewritten bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.key, w.image, w.deleted, w.rewrites > 0
}

// flushParked applies what the stream has already buffered, with the reader
// parked.
//
// It is deliberately not the exported Flush. That one ends in BlockWait, which
// waits for the reader to reach the source's *current* position — and the
// reader is parked, by us, precisely so that nothing past the watched event is
// admitted. The wait could never succeed, so every verification would spend its
// budget in there and time out instead of reaching a verdict. What this path
// needs is the opposite of catching up: drain exactly what is held and stop.
//
// allFlushed is what makes the drain sufficient. The watched change was
// buffered before the reader parked, so an empty buffer means it reached the
// target. A buffer that will not empty — a batch that lost to lock contention,
// a key held behind the copier's watermark — may still be holding it, and
// comparing the target against an image that was never applied would report
// apply lag as a divergence, which is the one mistake this whole path exists to
// avoid. So that case retries rather than answering.
func flushParked(ctx context.Context, flush func(context.Context) error, allFlushed func() bool) error {
	// With the reader parked nothing new arrives, so the buffer only shrinks.
	// One drain normally empties it; the retries cover a change the reader
	// buffered between a drain's swap and the park, and a batch that can
	// succeed on a second attempt.
	for range 3 {
		if err := flush(ctx); err != nil {
			return err
		}
		if allFlushed() {
			return nil
		}
	}
	return ErrFlushIncomplete
}

// verifyRowAtNextChange is the shared body of Source.VerifyRowAtNextChange for
// both clients. The
// client supplies its own gate, waiter slot and flush, because those are the
// only parts that differ between them.
//
// The order is the whole contract, so it lives here rather than in each client:
// arm before unparking (or the change that fires the watch could be delivered
// while nothing is listening), park from inside the dispatch (so nothing after
// that event is admitted), flush only once parked (so the flush cannot carry a
// later change), and re-check for a rewrite after the flush (a multi-row event
// finishes dispatching after the gate is armed, so it can still add one).
func verifyRowAtNextChange(
	ctx context.Context,
	gate *parkGate,
	arm func(*rowWaiter),
	disarm func(),
	flush func(context.Context) error,
	watch RowWatch,
	verify RowVerifier,
) error {
	waiter := newRowWaiter(watch)
	arm(waiter)
	defer func() {
		// Disarm first, so no further dispatch can pick the waiter up; abandon
		// then covers the ones already holding it, and releases the reader.
		disarm()
		waiter.abandon(gate)
	}()

	// Anything already parked would keep the watched change from ever
	// arriving, so the reader runs until it fires.
	gate.unpark()

	select {
	case <-waiter.ch:
	case <-ctx.Done():
		return ctx.Err()
	}

	// The dispatch parked the reader as it fired. Flushing now carries every
	// change up to and including that event to the target, and no change after
	// it.
	if err := flush(ctx); err != nil {
		return err
	}
	key, image, deleted, rewritten := waiter.result()
	if rewritten {
		return ErrRowRewritten
	}
	if err := verify(ctx, key, image, deleted); err != nil {
		return err
	}
	// Read the count again, because the verifier ran with the target readable
	// by everything else. The gate stops the *next* event, not the rest of this
	// one, so a multi-row event (an ODKU listing the key twice, a PK-shifting
	// UPDATE) can still buffer a second change to the watched key — and a
	// periodic flush, which is not serialized with this, can apply it while the
	// verifier is mid-read. A verdict reached against a target that moved is
	// not a verdict, so report the rewrite and let the caller retry.
	if _, _, _, rewritten = waiter.result(); rewritten {
		return ErrRowRewritten
	}
	return nil
}

// watchRow offers a row change to an armed verification *before* the change is
// buffered, and returns the waiter when it is one being watched. The caller
// must then buffer the change, park the reader, and call release — in that
// order. dispatchRow on each client is the only caller; see there for why each
// step is where it is.
//
// Returning nil (no verification armed, or not this row) is the overwhelmingly
// common case, and costs one atomic load.
func watchRow(slot *atomic.Pointer[rowWaiter], tbl *table.TableInfo, key, image []any, deleted bool) *rowWaiter {
	w := slot.Load()
	if w == nil || tbl == nil {
		return nil
	}
	if !w.observe(tbl.SchemaName, tbl.TableName, key, image, deleted) {
		return nil
	}
	return w
}
