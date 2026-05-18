package table

import (
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
