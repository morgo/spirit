package table

import (
	"fmt"
	"log/slog"
	"time"
)

// dynamicChunkSizer holds the time-based chunk-sizing state shared by the
// optimistic and composite chunkers. The chunker target is "spend
// roughly ChunkerTarget per chunk"; chunkTimingInfo accumulates per-chunk
// durations, and the next chunk's row count is derived from the p90 of
// the history vs the target.
//
// Embed into a chunker struct so call sites can continue to read fields
// as t.chunkSize / t.chunkTimingInfo / t.ChunkerTarget without changing.
type dynamicChunkSizer struct {
	chunkSize             uint64
	chunkTimingInfo       []time.Duration
	ChunkerTarget         time.Duration // e.g. 500ms target per chunk
	disableDynamicChunker bool          // only used by the test suite
}

// updateChunkerTarget applies a recalculated row target after clamping
// it to safe bounds (no more than 1.5x growth per step, capped at
// MaxDynamicRowSize, floored at MinDynamicRowSize). Resets the timing
// history so the next p90 reflects the new chunk size. Caller must hold
// the chunker's mutex.
func (d *dynamicChunkSizer) updateChunkerTarget(newTarget uint64) {
	d.chunkSize = d.boundaryCheckTargetChunkSize(newTarget)
	d.chunkTimingInfo = []time.Duration{}
}

// boundaryCheckTargetChunkSize clamps a proposed row count to the
// dynamic-chunking bounds. Extracted so tests and the prefetch-switch
// path in the optimistic chunker can verify the same clamping logic.
func (d *dynamicChunkSizer) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
	newTargetRows := float64(newTarget)

	// Cap growth at 1.5x per step. Prior chunks may have had "gaps" that
	// made them complete faster than expected; we don't want a single
	// fast chunk to balloon the next one.
	if newTargetRows > float64(d.chunkSize)*MaxDynamicStepFactor {
		newTargetRows = float64(d.chunkSize) * MaxDynamicStepFactor
	}

	if newTargetRows > MaxDynamicRowSize {
		newTargetRows = MaxDynamicRowSize
	}
	if newTargetRows < MinDynamicRowSize {
		newTargetRows = MinDynamicRowSize
	}
	return uint64(newTargetRows)
}

// calculateNewTargetChunkSize returns the row target derived from the
// p90 of the chunkTimingInfo history vs ChunkerTarget, plus the raw p90
// so a caller can react to extreme cases (the optimistic chunker uses
// the p90 to decide whether to switch to prefetch mode). Caller must
// hold the chunker's mutex.
func (d *dynamicChunkSizer) calculateNewTargetChunkSize() (newTargetRows uint64, p90 time.Duration) {
	p90 = LazyFindP90(d.chunkTimingInfo)
	target := float64(d.ChunkerTarget)
	rows := float64(d.chunkSize) * (target / float64(p90))
	return uint64(rows), p90
}

// watermarkTracker holds the "minimum value safely copied" plus the
// out-of-order chunk-alignment map used by both chunker implementations.
// bumpWatermark advances the watermark by feeding in newly-completed
// chunks; if a chunk doesn't align with the current watermark.UpperBound
// it is buffered in lowerBoundWatermarkMap and applied later when a
// preceding chunk closes the gap.
//
// checkpointHighPtr is set on resume-from-checkpoint and used by
// KeyAboveHighWatermark before chunkPtr advances, to prevent re-applying
// changes for keys that were already copied in a previous run.
type watermarkTracker struct {
	watermark              *Chunk
	lowerBoundWatermarkMap map[string]*Chunk
	checkpointHighPtr      Datum
}

// isSpecialRestoredChunk reports whether `chunk` is the first chunk
// dispatched after resume-from-checkpoint. The restored chunk shares
// its lower bound with the saved watermark, which the alignment rules
// below would otherwise reject as already-seen. Caller must hold the
// chunker's mutex.
func (w *watermarkTracker) isSpecialRestoredChunk(chunk *Chunk) bool {
	if chunk.LowerBound == nil || chunk.UpperBound == nil ||
		w.watermark == nil || w.watermark.LowerBound == nil || w.watermark.UpperBound == nil {
		return false // restored checkpoints always have both.
	}
	return chunk.LowerBound.comparesTo(w.watermark.LowerBound)
}

// waterMarkMapNotEmpty reports whether there are buffered out-of-order
// chunks waiting to be applied.
func (w *watermarkTracker) waterMarkMapNotEmpty() bool {
	return len(w.lowerBoundWatermarkMap) != 0
}

// bumpWatermark advances the watermark using a newly-completed chunk.
// Chunks can complete out of order under parallel copy, so the algorithm
// is:
//   - If the chunk does not align with the current watermark, buffer it
//     in lowerBoundWatermarkMap keyed by its lowerBound string.
//   - If it does align (or this is the first/restored chunk), bump the
//     watermark to the chunk's UpperBound. Then drain the map of any
//     chunks whose lowerBound now aligns with the new watermark.
//
// Caller must hold the chunker's mutex. The logger is passed in because
// the helper doesn't own one — see chunker_helpers.go's package doc.
func (w *watermarkTracker) bumpWatermark(chunk *Chunk, logger *slog.Logger) {
	if chunk.UpperBound == nil {
		return
	}
	// First chunk, or the special restored chunk: set and drain stored chunks.
	if (w.watermark == nil && chunk.LowerBound == nil) || w.isSpecialRestoredChunk(chunk) {
		w.watermark = chunk
		w.drainAlignedChunks()
		return
	}

	// Past the first-chunk case, every subsequent chunk must have a
	// lower bound. A nil here would mean the chunker dispatched a second
	// open-bounded chunk, which is a bug.
	if chunk.LowerBound == nil {
		errMsg := fmt.Sprintf("watermarkTracker.bumpWatermark: nil lowerBound value encountered more than once: %v", chunk)
		logger.Error(errMsg)
		panic(errMsg) // Fatal equivalent - log and panic
	}

	// Out-of-order chunk: buffer it for later alignment.
	if w.watermark == nil || !w.watermark.UpperBound.comparesTo(chunk.LowerBound) {
		w.lowerBoundWatermarkMap[chunk.LowerBound.valuesString()] = chunk
		return
	}

	// Chunk aligns with the current watermark.UpperBound: it becomes the
	// new watermark.
	w.watermark = chunk
	w.drainAlignedChunks()
}

// drainAlignedChunks pulls chunks out of lowerBoundWatermarkMap whose
// lowerBound matches the current watermark.UpperBound, advancing the
// watermark each time. Stops when nothing aligns or the map is empty.
func (w *watermarkTracker) drainAlignedChunks() {
	for w.waterMarkMapNotEmpty() && w.watermark.UpperBound != nil {
		key := w.watermark.UpperBound.valuesString()
		next, ok := w.lowerBoundWatermarkMap[key]
		if !ok {
			return
		}
		w.watermark = next
		delete(w.lowerBoundWatermarkMap, key)
	}
}
