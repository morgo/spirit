package table

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

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
