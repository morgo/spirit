package table

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countingHandler is a minimal slog.Handler that tallies emitted records by
// (level, message) so tests can assert on logging volume.
type countingHandler struct {
	counts map[string]int
}

func newCountingHandler() *countingHandler {
	return &countingHandler{counts: make(map[string]int)}
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.counts[r.Level.String()+":"+r.Message]++
	return nil
}
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

// TestBoundaryCheckTargetChunkSize verifies the dynamic-chunk-sizer
// clamping rules. The 1.5x per-step growth cap is checked first and
// usually dominates; the MaxDynamicRowSize cap only kicks in when
// chunkSize is already at or near the cap. MinDynamicRowSize is a
// hard floor.
func TestBoundaryCheckTargetChunkSize(t *testing.T) {
	d := &dynamicChunkSizer{chunkSize: 1000}
	require.Equal(t, uint64(1500), d.boundaryCheckTargetChunkSize(10_000),
		"growth must be clamped to 1.5x per step")
	require.Equal(t, uint64(MinDynamicRowSize), d.boundaryCheckTargetChunkSize(0),
		"target below MinDynamicRowSize must floor to the floor")

	// To exercise the MaxDynamicRowSize cap we need a chunkSize where
	// the 1.5x growth would otherwise exceed it.
	d.chunkSize = MaxDynamicRowSize
	require.Equal(t, uint64(MaxDynamicRowSize), d.boundaryCheckTargetChunkSize(MaxDynamicRowSize*2),
		"target above MaxDynamicRowSize must clamp to the cap")
}

// TestUpdateChunkerTarget verifies that applying a new target clamps the
// value and resets the chunk timing history.
func TestUpdateChunkerTarget(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:       1000,
		chunkTimingInfo: []time.Duration{time.Second, 2 * time.Second},
	}
	d.updateChunkerTarget(10_000)

	require.Equal(t, uint64(1500), d.chunkSize,
		"chunkSize must equal the clamped target")
	require.Empty(t, d.chunkTimingInfo,
		"timing history must be reset so future p90 reflects the new chunk size")
}

// TestCalculateNewTargetChunkSize verifies the row-target formula
// (chunkSize * ChunkerTarget / p90) and that the raw p90 is returned so
// callers can react to extreme cases (the optimistic chunker uses it to
// switch to prefetch mode).
func TestCalculateNewTargetChunkSize(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:     1000,
		ChunkerTarget: 500 * time.Millisecond,
		// p90 of [100ms]*10 is 100ms → newTarget = 1000 * 500ms / 100ms = 5000
		chunkTimingInfo: make([]time.Duration, 10),
	}
	for i := range d.chunkTimingInfo {
		d.chunkTimingInfo[i] = 100 * time.Millisecond
	}

	newTarget, p90 := d.calculateNewTargetChunkSize()
	require.Equal(t, 100*time.Millisecond, p90)
	require.Equal(t, uint64(5000), newTarget,
		"newTarget = chunkSize * ChunkerTarget / p90")
}

const highChunkMsg = "INFO:high chunk processing time"
const pinnedMsg = "WARN:chunk size pinned at minimum; rows may be too wide to meet target-chunk-time"

// TestPanicShrinkAboveFloorLogs verifies that while the chunk size can still
// shrink, panicShrink emits the per-shrink "high chunk processing time" line
// and reduces the chunk size.
func TestPanicShrinkAboveFloorLogs(t *testing.T) {
	h := newCountingHandler()
	d := &dynamicChunkSizer{chunkSize: 1000, ChunkerTarget: 100 * time.Millisecond}

	d.panicShrink(slog.New(h), time.Second)

	require.Equal(t, uint64(100), d.chunkSize,
		"newTarget = 1000/(DynamicPanicFactor*2) = 100, still above the floor")
	require.Equal(t, 1, h.counts[highChunkMsg], "should log the shrink once")
	require.Zero(t, h.counts[pinnedMsg], "not at the floor yet")
	require.False(t, d.pinnedAtFloor)
}

// TestPanicShrinkAtFloorSuppressesFlood is the regression test for the log
// flood: once the chunk size is pinned at MinDynamicRowSize, repeated panic
// feedback (one call per copied chunk) must NOT emit a line per chunk. Exactly
// one Warn is emitted for the whole stuck period.
func TestPanicShrinkAtFloorSuppressesFlood(t *testing.T) {
	h := newCountingHandler()
	logger := slog.New(h)
	d := &dynamicChunkSizer{chunkSize: MinDynamicRowSize, ChunkerTarget: 100 * time.Millisecond}

	// Simulate a wide-row table where every 10-row chunk blows past the panic
	// threshold — the copier calls Feedback (→ panicShrink) once per chunk.
	for range 10_000 {
		d.panicShrink(logger, time.Second)
	}

	require.Equal(t, uint64(MinDynamicRowSize), d.chunkSize, "stays pinned at the floor")
	require.Zero(t, h.counts[highChunkMsg],
		"the per-chunk Info line must be suppressed at the floor")
	require.Equal(t, 1, h.counts[pinnedMsg],
		"exactly one Warn for the whole stuck period, not one per chunk")
	require.True(t, d.pinnedAtFloor)
}

// TestPanicShrinkReArmsAfterRecovery verifies that if the chunk size climbs
// back above the floor, a later relapse is reported again (the warning is not
// permanently silenced).
func TestPanicShrinkReArmsAfterRecovery(t *testing.T) {
	h := newCountingHandler()
	logger := slog.New(h)
	d := &dynamicChunkSizer{chunkSize: MinDynamicRowSize, ChunkerTarget: 100 * time.Millisecond}

	d.panicShrink(logger, time.Second) // pin + warn once
	require.Equal(t, 1, h.counts[pinnedMsg])
	require.True(t, d.pinnedAtFloor)

	// Recover: normal feedback grows the chunk size back above the floor.
	d.updateChunkerTarget(1000)
	require.False(t, d.pinnedAtFloor, "climbing off the floor re-arms the warning")

	// Relapse straight back to the floor and stay there.
	d.panicShrink(logger, time.Second) // 1000/10 = 100, above floor → Info
	d.chunkSize = MinDynamicRowSize    // simulate reaching the floor again
	d.panicShrink(logger, time.Second) // at floor → Warn again

	require.Equal(t, 2, h.counts[pinnedMsg], "relapse must warn again")
}
