package checksum

import (
	"context"
	"log/slog"

	"github.com/block/spirit/pkg/table"
)

const (
	// csRepairSplitParts is how many pieces a mismatching chunk is cut into per
	// narrowing round. Each piece costs one boundary lookup plus one checksum
	// comparison, so the round costs roughly one extra scan of the range no
	// matter how this is set; a larger value simply buys a finer result from
	// that same scan. 8 keeps the boundary OFFSET queries cheap while cutting a
	// worst-case chunk down by two orders of magnitude over csRepairMaxDepth
	// rounds.
	csRepairSplitParts = 8

	// csRepairMinSplitRows is the range size below which narrowing is not worth
	// its own cost: recopying this many rows is a short, bounded stall, and the
	// narrowing scan would be a comparable amount of work for no benefit.
	csRepairMinSplitRows = 2000

	// csRepairMaxDepth bounds the recursion. Two rounds take the largest chunk
	// the dynamic sizer will produce (table.MaxDynamicRowSize, 100k rows) down
	// to ~1.5k rows, which is already below csRepairMinSplitRows. The bound
	// exists so that a pathological key distribution cannot turn one mismatch
	// into an unbounded tree of queries.
	csRepairMaxDepth = 2
)

// rangeCounts is what verifying one key range reports back.
type rangeCounts struct {
	// sourceRows and targetRows are the row counts the comparison read on each
	// side. Both are needed, not just the larger: they are what the caller sums
	// to prove the sub-ranges partition their parent exactly.
	sourceRows, targetRows uint64
	// mismatched is true when source and target disagree over this range.
	mismatched bool
	// splitter is the connection whose row distribution should be used to cut
	// this range up, and must belong to the snapshot the counts were read in.
	// For a sharded source it is the shard holding the most of this range, which
	// is why each range carries its own rather than inheriting the parent's: a
	// range's rows can live mostly on a different shard than its parent's did.
	// A nil splitter means the range cannot be cut and must be repaired whole.
	splitter table.Splitter
}

// rows is the range's size for the purpose of deciding whether to narrow it: the
// larger side, so that a wholly-missing target still narrows.
func (r rangeCounts) rows() uint64 {
	return max(r.sourceRows, r.targetRows)
}

// rangeVerifier re-compares source and target over a sub-range.
//
// It MUST run in the same snapshot that observed the original mismatch. That is
// what makes narrowing sound: BIT_XOR is associative and the counts are additive,
// so over an exact partition of the range the sub-results must reproduce the
// whole-range result. Read from a different snapshot, a sub-range could appear
// clean because a concurrent write happened to fix it, and the repair would be
// skipped on the strength of an observation that never applied to the chunk we
// were asked to fix.
type rangeVerifier func(ctx context.Context, chunk *table.Chunk) (rangeCounts, error)

// rangeRepairer recopies a range from source to target. It must be idempotent
// and safe to call on a sub-range of a chunk, which both checkers' chunk-replace
// methods are: they work purely from chunk.String().
type rangeRepairer func(ctx context.Context, chunk *table.Chunk) error

// narrowRepair repairs a chunk whose checksum did not match, first narrowing it
// to the sub-ranges that actually differ.
//
// Chunks are sized by how long they take to *read* during checksumming, which
// says nothing about how long they take to recopy — a repair DELETEs and then
// re-writes every row in the range. As checksum chunks grow (they are sized for
// read-ahead, not for latency), recopying a whole chunk to fix a handful of rows
// becomes a large write burst, a long lock hold and a wide window in which the
// target range is deleted but not yet rewritten. Narrowing first turns that into
// a few small repairs.
//
// Narrowing is best-effort in one direction only. Anything that makes it
// unusable — a key range that cannot be cut, a boundary query that fails, a
// subdivision whose row counts do not add up, a set of pieces that all differ
// (or none of which do) — falls back to repairing the whole range, because
// leaving a known difference in place is not an option. Verification *failures*
// are not swallowed: those mean the snapshot itself is gone, so the pass should
// fail and be retried rather than repair on stale information. The narrow window
// where Split succeeds and verification is then cancelled leaves the difference
// unrepaired, which is a change from the unconditional repair this replaces; it
// is safe because differencesFound has already been incremented by the caller,
// so the run cannot report a pass.
func narrowRepair(
	ctx context.Context,
	chunk *table.Chunk,
	counts rangeCounts,
	verify rangeVerifier,
	repair rangeRepairer,
	logger *slog.Logger,
) error {
	return narrowRepairAtDepth(ctx, chunk, counts, verify, repair, logger, 0)
}

func narrowRepairAtDepth(
	ctx context.Context,
	chunk *table.Chunk,
	counts rangeCounts,
	verify rangeVerifier,
	repair rangeRepairer,
	logger *slog.Logger,
	depth int,
) error {
	if counts.rows() < csRepairMinSplitRows || depth >= csRepairMaxDepth || counts.splitter == nil {
		return repair(ctx, chunk)
	}
	subs, err := chunk.Split(ctx, counts.splitter, csRepairSplitParts)
	if err != nil {
		logger.Warn("could not subdivide chunk for repair, recopying it whole",
			"chunk", chunk.String(), "error", err)
		return repair(ctx, chunk)
	}
	if len(subs) < 2 {
		// Too few distinct key values in the range to cut it up.
		return repair(ctx, chunk)
	}

	// Verify every sub-range before repairing any of them. Repairing as we go
	// would mean a mid-loop verification error left the chunk partially fixed
	// *and* stopped us from learning about the rest of it; this way the fallbacks
	// below still have the whole picture.
	results := make([]rangeCounts, len(subs))
	var sourceRows, targetRows uint64
	differing := 0
	for i, sub := range subs {
		r, err := verify(ctx, sub)
		if err != nil {
			return err
		}
		results[i] = r
		sourceRows += r.sourceRows
		targetRows += r.targetRows
		if r.mismatched {
			differing++
		}
	}

	// Everything below rests on the sub-ranges partitioning the parent exactly,
	// and table.Chunk.Split cannot promise that for every key type — a key whose
	// collation order differs from its byte order, or a NULL key value, produces
	// an overlap or a gap. Rather than reason about types, check it: the counts
	// were read in the same snapshot as the parent's, so over an exact partition
	// they must add up on both sides. A gap would let a differing row escape
	// repair entirely, so a mismatch here has to mean the whole chunk.
	if sourceRows != counts.sourceRows || targetRows != counts.targetRows {
		logger.Warn("subdividing a chunk did not partition it exactly, recopying it whole",
			"chunk", chunk.String(), "parts", len(subs),
			"sourceRows", sourceRows, "expectedSourceRows", counts.sourceRows,
			"targetRows", targetRows, "expectedTargetRows", counts.targetRows)
		return repair(ctx, chunk)
	}

	// The whole range mismatched, so at least one sub-range must: the sub-ranges
	// partition it exactly, BIT_XOR is associative and the counts are additive.
	// If that does not hold, something we believe about the range is wrong, so
	// fall back to the repair we would have done anyway rather than silently
	// decline to fix a known difference.
	if differing == 0 {
		logger.Warn("subdividing a mismatching chunk found no differing sub-range, recopying it whole",
			"chunk", chunk.String(), "parts", len(subs))
		return repair(ctx, chunk)
	}

	// Every piece differs, so narrowing bought nothing: the same rows will be
	// rewritten either way. Doing it as one statement pair rather than one per
	// piece is the cheaper and shorter-lived option, and it keeps the systematic
	// cases (a lossy ALTER, a wrong column mapping, a truncated target) from
	// fanning a single chunk out into dozens of transactions.
	if differing == len(subs) {
		logger.Warn("every sub-range of a mismatching chunk differs, recopying it whole",
			"chunk", chunk.String(), "parts", len(subs), "rows", counts.rows())
		return repair(ctx, chunk)
	}

	logger.Warn("narrowed chunk repair",
		"chunk", chunk.String(),
		"rows", counts.rows(),
		"parts", len(subs),
		"differing", differing,
		"depth", depth)

	for i, sub := range subs {
		if !results[i].mismatched {
			continue
		}
		if err := narrowRepairAtDepth(ctx, sub, results[i], verify, repair, logger, depth+1); err != nil {
			return err
		}
	}
	return nil
}
