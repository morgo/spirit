package table

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

// TestBumpWatermarkInOrder verifies the happy path: chunks arrive in
// order, each becoming the new watermark immediately and not buffering.
func TestBumpWatermarkInOrder(t *testing.T) {
	w := &watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)}

	// First chunk has no LowerBound — it becomes the initial watermark.
	first := &Chunk{
		UpperBound: &Boundary{Value: []Datum{{Val: uint64(100), Tp: unsignedType}}, Inclusive: false},
	}
	w.bumpWatermark(first, slog.Default())
	require.Equal(t, first, w.watermark)
	require.Empty(t, w.lowerBoundWatermarkMap)

	// Second chunk's LowerBound aligns with first.UpperBound — it
	// becomes the new watermark.
	second := &Chunk{
		LowerBound: &Boundary{Value: []Datum{{Val: uint64(100), Tp: unsignedType}}, Inclusive: true},
		UpperBound: &Boundary{Value: []Datum{{Val: uint64(200), Tp: unsignedType}}, Inclusive: false},
	}
	w.bumpWatermark(second, slog.Default())
	require.Equal(t, second, w.watermark)
	require.Empty(t, w.lowerBoundWatermarkMap)
}

// TestBumpWatermarkOutOfOrder verifies that an out-of-order chunk is
// buffered in lowerBoundWatermarkMap and applied later when a preceding
// chunk closes the gap.
func TestBumpWatermarkOutOfOrder(t *testing.T) {
	w := &watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)}

	// First chunk: 0..100.
	first := &Chunk{
		UpperBound: &Boundary{Value: []Datum{{Val: uint64(100), Tp: unsignedType}}, Inclusive: false},
	}
	w.bumpWatermark(first, slog.Default())

	// Out-of-order: 200..300 arrives before 100..200. It's buffered.
	outOfOrder := &Chunk{
		LowerBound: &Boundary{Value: []Datum{{Val: uint64(200), Tp: unsignedType}}, Inclusive: true},
		UpperBound: &Boundary{Value: []Datum{{Val: uint64(300), Tp: unsignedType}}, Inclusive: false},
	}
	w.bumpWatermark(outOfOrder, slog.Default())
	require.Equal(t, first, w.watermark, "watermark must not advance for an unaligned chunk")
	require.Len(t, w.lowerBoundWatermarkMap, 1)

	// Gap-closer 100..200 arrives. It bumps the watermark, and then the
	// drain loop pulls the buffered 200..300 in.
	gap := &Chunk{
		LowerBound: &Boundary{Value: []Datum{{Val: uint64(100), Tp: unsignedType}}, Inclusive: true},
		UpperBound: &Boundary{Value: []Datum{{Val: uint64(200), Tp: unsignedType}}, Inclusive: false},
	}
	w.bumpWatermark(gap, slog.Default())
	require.Equal(t, outOfOrder, w.watermark, "drain must advance past the buffered chunk")
	require.Empty(t, w.lowerBoundWatermarkMap, "buffered chunk must be removed after drain")
}
