package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/table"
)

// A hot range is one whose source keeps changing under the reader. The snapshot
// fallback (lockless_snapshot.go) freezes the source row images for such a range
// and waits for the target to show them, which converges for a range that is
// merely busy — but not for a row that is written continuously. There, the
// frozen image is stale before the first poll, every attempt observes a
// different source, and the range is deferred to the next pass with no verdict
// at all. It is deferred again on the next pass for the same reason. A genuinely
// diverged hot row and a merely busy one are indistinguishable, forever.
//
// Settling is the terminal step for exactly that case, and it works by giving up
// on reading the source at all.
//
// Any comparison between a SELECT of the source and a read of the target is
// between a source image at one position and a target state at a later one.
// Closing that window means stopping the writes, and for a row that is written
// continuously the window is never empty. But the change stream already carries
// the answer: with binlog_row_image=FULL, an event's after-image *is* the
// source's value for that row at that position. MySQL says so; no read is
// needed, and nothing has to hold still.
//
// So for each row still outstanding:
//
//  1. Ask the feed to wait for the next change to that row and park there.
//  2. Flush, so the target holds exactly that image and nothing past it.
//  3. Compare the target row to the event's image.
//
// A mismatch at step 3 is a real inconsistency: the feed delivered that image
// and the flush applied it, so apply lag cannot explain a difference.
//
// This terminates in the opposite direction from a lock: the more often the row
// is written, the sooner its next event arrives. The rows that defeat every
// read-and-compare strategy are exactly the ones this settles fastest, and a row
// quiet enough that no event arrives inside the budget is one the ordinary poll
// was already converging on.
//
// Rows are settled one at a time. The escalation is rare by construction — a
// range reaches it only after exhausting MaxHotAttempts — and serializing keeps
// the feed's park a single piece of state rather than a set of overlapping holds.
const (
	// settleBudget bounds the whole escalation: every row's wait for its next
	// change, each flush, and each comparison. Overrunning it is not an error,
	// it defers, exactly as before this path existed.
	settleBudget = 5 * time.Second

	// settleRowBudget bounds one row's wait. A genuinely hot row produces a
	// change in milliseconds, so a row that does not is not the case this
	// exists for — and spending the whole budget on it would starve the rows
	// behind it.
	settleRowBudget = time.Second
)

// settleVerdict is the outcome of trying to settle a hot range.
type settleVerdict int

const (
	// settleUnavailable means no verdict was reached: the feed cannot park, no
	// change arrived inside the budget, or a change landed in the middle of the
	// comparison. The caller defers the range exactly as it did before this
	// path existed.
	settleUnavailable settleVerdict = iota

	// settleClean means every outstanding row was observed holding the image
	// the stream had just delivered for it. The range is verified.
	settleClean

	// settleDiverged means at least one was not. The feed had just applied that
	// row's image, so nothing could still be in flight for it.
	settleDiverged
)

func (v settleVerdict) String() string {
	switch v {
	case settleUnavailable:
		return "unavailable"
	case settleClean:
		return "clean"
	case settleDiverged:
		return "diverged"
	default:
		return fmt.Sprintf("settleVerdict(%d)", int(v))
	}
}

// rowSettler performs the escalation described at the top of this file. It is
// deliberately a type of its own rather than more methods on LocklessChecker:
// settling needs the source, a feed that can park, and somewhere to log, and
// nothing else about a running check.
type rowSettler struct {
	sourceDB *sql.DB
	parker   change.RowParker
	logger   *slog.Logger
}

// newRowSettler returns nil when the feed cannot park, which is not a failure:
// parking is the whole mechanism, library callers may have no feed at all, and
// a settler that cannot park has nothing to offer over the poll the caller has
// already been doing. A nil settler answers settleUnavailable to everything.
func newRowSettler(sourceDB *sql.DB, feed change.Source, logger *slog.Logger) *rowSettler {
	parker, ok := feed.(change.RowParker)
	if !ok {
		return nil
	}
	return &rowSettler{sourceDB: sourceDB, parker: parker, logger: logger}
}

// settle returns settleUnavailable rather than an error for every condition
// that only means "not this time" — a feed that cannot park, a row that went
// quiet, a change that landed mid-comparison — because the caller's response to
// all of them is the deferral it would have done anyway. An error is reserved
// for a read that failed in a way worth surfacing.
func (s *rowSettler) settle(ctx context.Context, snapshot *hotSnapshot) (settleVerdict, error) {
	if s == nil || len(snapshot.pending) == 0 {
		return settleUnavailable, nil
	}

	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, settleBudget)
	defer cancel()

	// Settled rows leave snapshot.pending as they are verified, so a run that
	// gives up part way still banks the progress: the next attempt waits only
	// for what is left.
	for key, row := range snapshot.pending {
		verdict, err := s.settleRow(ctx, parent, snapshot, row)
		if err != nil {
			return settleUnavailable, err
		}
		switch verdict {
		case settleClean:
			delete(snapshot.pending, key)
		case settleDiverged:
			return settleDiverged, nil
		case settleUnavailable:
			return settleUnavailable, nil
		}
	}
	return settleClean, nil
}

// settleRow waits for one row's next change and verifies the target against it.
// parent is the caller's context, used only to tell our own budget expiring
// (defer) from the run being cancelled (propagate).
func (s *rowSettler) settleRow(ctx, parent context.Context, snapshot *hotSnapshot, row hotSnapshotRow) (settleVerdict, error) {
	chunk := snapshot.chunk
	matcher, err := keyMatcher(chunk.Table, chunk.Key, row.key)
	if err != nil {
		return settleUnavailable, err
	}
	rowCtx, cancel := context.WithTimeout(ctx, settleRowBudget)
	defer cancel()

	var verdict settleVerdict
	err = s.parker.VerifyRowAtNextChange(rowCtx, change.RowWatch{
		Schema: chunk.Table.SchemaName,
		Table:  chunk.Table.TableName,
		Match:  matcher,
	}, func(ctx context.Context, _, image []any, deleted bool) error {
		var err error
		verdict, err = s.compareRowToImage(ctx, snapshot, row, image, deleted)
		return err
	})
	switch {
	case parent.Err() != nil:
		// The run itself is going away; that is not a verdict and not ours to
		// swallow.
		return settleUnavailable, parent.Err()
	case errors.Is(err, change.ErrRowRewritten):
		s.logger.Debug("lockless checksum: watched row was rewritten mid-verification; deferring",
			"chunk", chunk.String())
		return settleUnavailable, nil
	case errors.Is(err, change.ErrFlushIncomplete):
		// The image may not have reached the target, so any difference we
		// found would be apply lag wearing a divergence's clothes.
		s.logger.Debug("lockless checksum: feed could not drain while parked; deferring",
			"chunk", chunk.String())
		return settleUnavailable, nil
	case errors.Is(err, context.DeadlineExceeded):
		s.logger.Debug("lockless checksum: no change arrived for the watched row inside its budget; deferring",
			"chunk", chunk.String())
		return settleUnavailable, nil
	case err != nil:
		return settleUnavailable, err
	}
	return verdict, nil
}

// compareRowToImage answers the only question the parked feed leaves open: does
// the target hold what the stream just delivered?
//
// A delete is the simple half — the row must be gone. For an image, the expected
// value is computed by evaluating the *same* checksum expressions the rest of the
// checker uses against the image itself (see expectedImageCRC), so a type change
// or a column rename is normalised exactly as it is everywhere else rather than
// by a second, parallel notion of equality written in Go.
func (s *rowSettler) compareRowToImage(ctx context.Context, snapshot *hotSnapshot, row hotSnapshotRow, image []any, deleted bool) (settleVerdict, error) {
	chunk := snapshot.chunk
	predicate, err := pointPredicate(chunk, row.key)
	if err != nil {
		return settleUnavailable, err
	}
	actual, _, oversized, err := readHotSnapshotRows(ctx, snapshot.targetDB, chunk, chunk.NewTable, snapshot.targetColumns, predicate, 2)
	if err != nil {
		return settleUnavailable, fmt.Errorf("read target row while settling %s: %w", chunk.String(), err)
	}
	if oversized || len(actual) > 1 {
		// A key representation that changed under collation can match the
		// predicate and come back different. Never read that as a verdict.
		return settleUnavailable, nil
	}
	if deleted {
		if len(actual) == 0 {
			return settleClean, nil
		}
		return settleDiverged, nil
	}
	if len(actual) == 0 {
		return settleDiverged, nil // the stream just wrote it; the target has no row
	}
	expected, err := s.expectedImageCRC(ctx, chunk, image)
	if err != nil {
		return settleUnavailable, err
	}
	for _, got := range actual {
		if got.crc != expected {
			return settleDiverged, nil
		}
	}
	return settleClean, nil
}

// expectedImageCRC evaluates the source side of the checksum expressions against
// a binlog row image, giving the CRC the target must hold for that row.
//
// The image is rendered as a one-row derived table whose column *types* come
// from the real table: the first UNION branch selects the mapped columns from it
// with a false predicate, so it contributes names and types and no rows, and the
// second supplies the values. That matters because ChecksumExprs casts every
// column to the target's type before hashing — the same normalisation an
// ordinary chunk read gets — and those casts have to be applied to a column of
// the right type, not to whatever a bare parameter would be typed as.
func (s *rowSettler) expectedImageCRC(ctx context.Context, chunk *table.Chunk, image []any) (uint64, error) {
	sourceExprs, _, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return 0, err
	}
	columns, _ := chunk.ColumnMapping.ColumnsSlice()
	ordinals := chunk.ColumnMapping.SourceOrdinalIndices()
	values := make([]any, len(ordinals))
	for i, ordinal := range ordinals {
		if ordinal >= len(image) {
			return 0, fmt.Errorf("binlog row image has %d columns, need ordinal %d", len(image), ordinal)
		}
		values[i] = image[ordinal]
	}
	query := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) FROM (SELECT %s FROM %s WHERE 1=0 UNION ALL SELECT %s) AS img",
		sourceExprs, table.QuoteColumns(columns), chunk.Table.QuotedTableName,
		strings.TrimSuffix(strings.Repeat("?,", len(values)), ","))
	var crc uint64
	if err := s.sourceDB.QueryRowContext(ctx, query, values...).Scan(&crc); err != nil {
		return 0, fmt.Errorf("evaluate checksum over binlog row image: %w", err)
	}
	return crc, nil
}

// keyMatcher decides whether a binlog event's key is the row being waited for.
// The two sides arrive differently — the stream decodes Go values, the snapshot
// holds Datums read back from MySQL — so both go through the key column's
// declared type before being compared.
//
// It is only a matcher: a false negative costs another wait, which is why it
// fails closed by returning false on a value it cannot convert rather than
// guessing.
func keyMatcher(info *table.TableInfo, keyColumns []string, want []table.Datum) (func([]any) bool, error) {
	if len(keyColumns) != len(want) {
		return nil, fmt.Errorf("key has %d columns but snapshot holds %d", len(keyColumns), len(want))
	}
	types := make([]string, len(keyColumns))
	for i, column := range keyColumns {
		tp, ok := info.GetColumnMySQLType(column)
		if !ok {
			return nil, fmt.Errorf("missing key column type for %s", column)
		}
		types[i] = tp
	}
	return func(key []any) bool {
		if len(key) != len(want) {
			return false
		}
		for i, value := range key {
			got, err := table.NewDatumFromValue(value, types[i])
			if err != nil {
				return false
			}
			below, err := got.LessThan(want[i])
			if err != nil || below {
				return false
			}
			above, err := got.GreaterThan(want[i])
			if err != nil || above {
				return false
			}
		}
		return true
	}, nil
}

// pointPredicate renders a single key as a chunk predicate, which is how every
// other read in this package addresses one row.
func pointPredicate(chunk *table.Chunk, key []table.Datum) (string, error) {
	if len(key) == 0 {
		return "", errors.New("cannot address a row without a key")
	}
	point := *chunk
	point.LowerBound = &table.Boundary{Value: key, Inclusive: true}
	point.UpperBound = &table.Boundary{Value: key, Inclusive: true}
	return point.String(), nil
}
