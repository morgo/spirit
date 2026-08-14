package status

import (
	"sync/atomic"
	"time"
)

// LastEvent records when something last happened so a runner can report it as
// an age on its periodic status line ("since-checkpoint=12s") instead of the
// event logging a line of its own each time it occurs. See
// github.com/block/spirit/issues/329.
//
// The zero value is ready to use and reports "never". Safe for concurrent use:
// the recorder and the status goroutine are always different goroutines.
//
// Contains a sync/atomic value, so it must not be copied after first use.
type LastEvent struct {
	nanos atomic.Int64
}

// Record marks the event as having just happened.
func (e *LastEvent) Record() {
	e.nanos.Store(time.Now().UnixNano())
}

// At returns when the event last happened, or the zero time if it never has.
func (e *LastEvent) At() time.Time {
	nanos := e.nanos.Load()
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

// Age renders how long ago the event happened, rounded to the second, or
// "never" if it has not happened yet.
func (e *LastEvent) Age() string {
	at := e.At()
	if at.IsZero() {
		return "never"
	}
	return time.Since(at).Round(time.Second).String()
}
