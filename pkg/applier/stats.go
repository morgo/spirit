package applier

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// timingRingSize is the number of most-recent chunklet timings retained for
// the rolling percentiles reported by Stats(). It matches defaultBufferSize,
// so at full occupancy the window covers roughly one buffer's worth of
// writes.
const timingRingSize = 128

// Stats is a point-in-time snapshot of an applier's write pipeline. It exists
// so status lines and metrics can distinguish a read-limited pipeline (queue
// near empty) from a write-limited one (queue pegged at capacity with
// queue-wait far above write time) — without this, write-side saturation is
// invisible: the copier's end-to-end chunk feedback misattributes it to the
// read side. All fields are approximate; they are read without pausing the
// pipeline.
type Stats struct {
	// QueueDepth is the number of chunklets currently waiting in the
	// buffer(s) — summed across shards for the sharded applier.
	QueueDepth int
	// QueueCap is the total buffer capacity (summed across shards).
	QueueCap int
	// PendingWork is the number of chunks accepted by Apply() whose
	// callback has not fired yet (queued + in-flight).
	PendingWork int
	// ActiveWorkers is the number of live write workers.
	ActiveWorkers int
	// RowsPerChunklet is the mean rows per chunklet since the applier started.
	// splitRowsIntoChunklets cuts on whichever of chunkletMaxRows or
	// MaxStatementSizeBytes binds first, and which one that is depends on the
	// table's width and column types — so it cannot be predicted from the
	// config, only measured. It matters because the chunklet is the unit of
	// nearly everything on the write path: one statement, one completion, one
	// handoff. A value at chunkletMaxRows means the row cap binds and raising
	// MaxStatementSizeBytes would do nothing; below it means the byte cap binds
	// and raising chunkletMaxRows would do nothing.
	//
	// A mean rather than a percentile: the question is which cap is in force,
	// which the mean answers, and the last chunklet of every chunk is a short
	// remainder that would skew a low percentile.
	RowsPerChunklet float64

	// Rolling percentiles over the last timingRingSize chunklets. Zero when no
	// chunklet has completed yet. Together these account for a write worker's
	// whole cycle, which matters when the pipeline stops responding to more
	// workers: each phase is limited by something different, and only one of
	// them is the target's write capacity.
	//
	// QueueWait is the time a chunklet spent between Apply() offering it to
	// the buffer and a write worker dequeueing it (including send-side
	// backpressure when the buffer is full).
	QueueWaitP50 time.Duration
	QueueWaitP90 time.Duration
	// BuildTime is the client-side cost of turning the chunklet's rows into an
	// INSERT statement — a datum conversion and a string format per value, so
	// it scales with rows × columns and is spent on spirit's own CPU while
	// holding no connection. It is a *component of* WriteTime, not additional
	// to it; subtract it to get time actually spent at the server. A BuildTime
	// approaching WriteTime means the client is the bottleneck, which no
	// server-side signal (CPU, commit latency, Threads_running) can report and
	// which more write workers cannot fix.
	BuildTimeP50 time.Duration
	BuildTimeP90 time.Duration
	// WriteTime is the time spent turning the chunklet into a statement and
	// executing it against the target(s) — BuildTime plus the round trip,
	// including any retry backoff inside it.
	WriteTimeP50 time.Duration
	WriteTimeP90 time.Duration
	// Handoff is the time a write worker spent publishing its completion after
	// the write finished. A single feedbackCoordinator goroutine drains those
	// completions and invokes the chunk callback inline, so this is where a
	// slow callback shows up as backpressure on every write worker at once.
	// Non-trivial Handoff with the queue pegged means workers are blocked
	// behind the completion path rather than the target, and adding workers
	// will not help.
	HandoffP50 time.Duration
	HandoffP90 time.Duration
}

// String renders the snapshot in the kebab-case key=value style used by the
// runner status lines, so migrate and move report identical fields. Durations
// are rounded to the millisecond — finer precision is noise at status cadence.
// Only the p50 of build and handoff is rendered, to keep the line readable;
// both p90s are in Stats and in the emitted metrics.
func (s Stats) String() string {
	return fmt.Sprintf("applier-queue=%d/%d applier-pending=%d applier-workers=%d applier-rows-per-chunklet=%.0f applier-queue-wait-p50=%v applier-queue-wait-p90=%v applier-build-p50=%v applier-write-p50=%v applier-write-p90=%v applier-handoff-p50=%v",
		s.QueueDepth,
		s.QueueCap,
		s.PendingWork,
		s.ActiveWorkers,
		s.RowsPerChunklet,
		s.QueueWaitP50.Round(time.Millisecond),
		s.QueueWaitP90.Round(time.Millisecond),
		s.BuildTimeP50.Round(time.Millisecond),
		s.WriteTimeP50.Round(time.Millisecond),
		s.WriteTimeP90.Round(time.Millisecond),
		s.HandoffP50.Round(time.Millisecond),
	)
}

// StatusSuffix renders a's Stats() for appending to a runner status line: a
// leading space plus Stats().String(), or "" when a is nil. Runner Status()
// can be called before the applier is constructed, so this must be nil-safe.
func StatusSuffix(a Applier) string {
	if a == nil {
		return ""
	}
	return " " + a.Stats().String()
}

// splitCounter accumulates how many chunklets a chunk's rows were cut into, so
// Stats can report the mean chunklet size. Counted at split time rather than at
// write time so it reflects the split decision even for chunklets that later
// fail, and so it is populated before any worker has finished.
type splitCounter struct {
	chunklets atomic.Int64
	rows      atomic.Int64
}

func (c *splitCounter) record(chunklets, rows int) {
	c.chunklets.Add(int64(chunklets))
	c.rows.Add(int64(rows))
}

// mean returns rows per chunklet, or 0 before anything has been split.
func (c *splitCounter) mean() float64 {
	chunklets := c.chunklets.Load()
	if chunklets == 0 {
		return 0
	}
	return float64(c.rows.Load()) / float64(chunklets)
}

// chunkletTiming is one completed chunklet's queue-wait, build, write and
// completion-handoff durations. buildTime is contained within writeTime.
type chunkletTiming struct {
	queueWait time.Duration
	buildTime time.Duration
	writeTime time.Duration
	handoff   time.Duration
}

// timingPercentiles is the p50/p90 of each phase over a ring's contents.
type timingPercentiles struct {
	queueWaitP50, queueWaitP90 time.Duration
	buildP50, buildP90         time.Duration
	writeP50, writeP90         time.Duration
	handoffP50, handoffP90     time.Duration
}

// timingRing is a fixed-size ring of the most recent chunklet timings.
// record is called by write workers on the hot path — one mutex acquire and
// one slot write per chunklet; percentiles are computed on read, which is
// infrequent (status/metrics cadence).
type timingRing struct {
	mu      sync.Mutex
	entries [timingRingSize]chunkletTiming
	next    int  // next write position
	full    bool // true once the ring has wrapped
}

func (r *timingRing) record(queueWait, buildTime, writeTime, handoff time.Duration) {
	r.mu.Lock()
	r.entries[r.next] = chunkletTiming{
		queueWait: queueWait,
		buildTime: buildTime,
		writeTime: writeTime,
		handoff:   handoff,
	}
	r.next++
	if r.next == timingRingSize {
		r.next = 0
		r.full = true
	}
	r.mu.Unlock()
}

// percentiles returns the p50/p90 of each phase over the entries recorded so
// far. All zeros when nothing has been recorded.
func (r *timingRing) percentiles() timingPercentiles {
	r.mu.Lock()
	n := r.next
	if r.full {
		n = timingRingSize
	}
	if n == 0 {
		r.mu.Unlock()
		return timingPercentiles{}
	}
	queueWaits := make([]time.Duration, n)
	builds := make([]time.Duration, n)
	writes := make([]time.Duration, n)
	handoffs := make([]time.Duration, n)
	for i := range n {
		queueWaits[i] = r.entries[i].queueWait
		builds[i] = r.entries[i].buildTime
		writes[i] = r.entries[i].writeTime
		handoffs[i] = r.entries[i].handoff
	}
	r.mu.Unlock()

	var p timingPercentiles
	p.queueWaitP50, p.queueWaitP90 = p50p90(queueWaits)
	p.buildP50, p.buildP90 = p50p90(builds)
	p.writeP50, p.writeP90 = p50p90(writes)
	p.handoffP50, p.handoffP90 = p50p90(handoffs)
	return p
}

// p50p90 sorts s in place and returns its 50th and 90th percentiles.
func p50p90(s []time.Duration) (p50, p90 time.Duration) {
	slices.Sort(s)
	return percentile(s, 50), percentile(s, 90)
}

// percentile returns the p-th percentile of a sorted slice using the
// nearest-rank method (ceil(n*p/100), 1-indexed).
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := max((len(sorted)*p+99)/100, 1)
	return sorted[idx-1]
}
