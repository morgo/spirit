package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/block/spirit/pkg/status"
)

// Lifecycle is the state a runner's Run invocation shares with the methods an
// API caller or a background goroutine calls while it runs: the function that
// cancels it, and the correctness evidence (status.WorkflowResult) it leaves
// behind. The zero value is ready to use. A runner owns one as a named field
// and forwards its own Cancel, Abort and Result to it, so the runner's public
// API does not grow the methods that record evidence.
type Lifecycle struct {
	mu     sync.Mutex
	cancel context.CancelCauseFunc

	// Atomic so a caller may read Result as soon as a Run goroutine returns.
	durableMutation   atomic.Bool
	terminalOwnership atomic.Uint32
}

// Begin starts a Run invocation. It derives a context that Cancel cancels,
// publishes its cancel function, and clears the evidence the previous
// invocation left. Run must defer the returned function with the address of
// its named error result:
//
//	ctx, end := r.lifecycle.Begin(ctx)
//	defer end(&retErr)
//
// end replaces a context.Canceled result with a fatal-abort cause (see
// status.AbortCause), records the evidence the result carries (see
// RecordError), and only then cancels the context, so the cause it reads is
// the one that stopped the run.
func (l *Lifecycle) Begin(ctx context.Context) (context.Context, func(*error)) {
	ctx, cancel := context.WithCancelCause(ctx)
	l.SetCancel(cancel)
	l.durableMutation.Store(false)
	l.terminalOwnership.Store(uint32(status.WorkflowTerminalOwnershipNone))
	return ctx, func(retErr *error) {
		*retErr = status.AbortCause(ctx, *retErr)
		l.RecordError(*retErr)
		cancel(nil)
	}
}

// SetCancel replaces the cancel function. Begin calls it; tests that drive a
// runner's phases without Run call it to observe or supply cancellation.
func (l *Lifecycle) SetCancel(cancel context.CancelCauseFunc) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cancel = cancel
}

// Cancel cancels the running invocation with cause. A nil cause is an operator
// cancellation: Run returns context.Canceled. A cause marked with
// status.FatalAbort is returned by Run instead. It does nothing before Begin
// (early setup, or test paths that bypass Run).
func (l *Lifecycle) Cancel(cause error) {
	l.mu.Lock()
	cancel := l.cancel
	l.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

// MarkDurableMutation records that this invocation changed something that a
// retry cannot undo.
func (l *Lifecycle) MarkDurableMutation() {
	l.durableMutation.Store(true)
}

// SetTerminalOwnership records which side owns the table or traffic when the
// invocation ends.
func (l *Lifecycle) SetTerminalOwnership(o status.WorkflowTerminalOwnership) {
	l.terminalOwnership.Store(uint32(o))
}

// RecordError records the evidence err carries: status.ErrDurableMutation and
// status.ErrOwnershipAmbiguous.
func (l *Lifecycle) RecordError(err error) {
	if errors.Is(err, status.ErrDurableMutation) {
		l.MarkDurableMutation()
	}
	if errors.Is(err, status.ErrOwnershipAmbiguous) {
		l.SetTerminalOwnership(status.WorkflowTerminalOwnershipAmbiguous)
	}
}

// Result returns the evidence retained from the most recent invocation.
func (l *Lifecycle) Result() status.WorkflowResult {
	return status.WorkflowResult{
		DurableMutation:   l.durableMutation.Load(),
		TerminalOwnership: status.WorkflowTerminalOwnership(l.terminalOwnership.Load()),
	}
}
