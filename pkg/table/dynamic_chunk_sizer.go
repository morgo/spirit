package table

import "time"

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
