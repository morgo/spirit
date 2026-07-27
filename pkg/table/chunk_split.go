package table

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// Splitter is the query surface Chunk.Split needs. Both *sql.DB and *sql.Tx
// satisfy it.
//
// Callers that split a chunk in order to decide something about its contents
// should pass the *transaction* they made that observation in, not the pool:
// boundaries cut from a different snapshot than the one that found a
// discrepancy would not line up with it.
type Splitter interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Split divides the chunk's key range into up to parts contiguous sub-chunks of
// roughly equal row count, and returns them in key order.
//
// The first sub-chunk inherits the original's lower bound and the last its upper
// bound — including whether those are inclusive — so an unbounded end stays
// unbounded. Interior split points appear as an exclusive upper bound on one
// sub-chunk and an inclusive lower bound on the next, which is the same
// convention the chunkers use and is what makes the partition exact for
// composite keys as well as single-column ones. AdditionalConditions, Key,
// Table, NewTable and ColumnMapping are carried across unchanged; ChunkSize and
// ActualBytes are not, since they describe the original chunk's sizing rather
// than its range.
//
// # Exactness
//
// The sub-chunks are an exact partition of the original — union equal to the
// original range, no overlaps — provided the key's comparison order matches the
// order the boundaries were read in. Boundaries come from ORDER BY, while the
// range predicates use >= and <, and MySQL does not always agree with itself
// about those two:
//
//   - ENUM sorts by declaration ordinal but compares against a string literal
//     lexically, so 'zebra' can sort before 'mango' and still compare after it.
//   - A case- or accent-insensitive collation orders strings by collation weight
//     while Datum comparison (used for the monotonicity check below) compares
//     bytes.
//   - A NULL key value satisfies neither >= nor <, so it belongs to no
//     sub-chunk. In practice chunk keys are PRIMARY KEY columns and cannot be
//     NULL, and the chunkers' own bounds already exclude NULL-keyed rows.
//
// Cuts that are not strictly increasing under Datum comparison are dropped,
// which removes the ENUM case and every ordinal-versus-lexical case where byte
// order is the truth. It cannot cover the collation cases, so a caller that
// depends on exactness must verify it — for example by checking that the
// sub-chunks' row counts sum to the original's, as checksum.narrowRepair does,
// and treating a shortfall or excess as "operate on the whole chunk".
//
// Returns nil (not an error) when the range cannot usefully be divided: parts
// is less than 2, or the range holds too few rows to yield more than one piece.
// Callers should treat a nil result as "operate on the whole chunk".
//
// Split finds boundaries the same way the composite chunker does, with
// ORDER BY ... LIMIT 1 OFFSET n against the source table, so it costs one query
// per interior boundary and the OFFSET scan cost that implies. It is intended
// for the rare path (narrowing a discrepancy), not for steady-state chunking.
func (c *Chunk) Split(ctx context.Context, q Splitter, parts int) ([]*Chunk, error) {
	if parts < 2 {
		return nil, nil
	}
	rows, err := c.countRows(ctx, q)
	if err != nil {
		return nil, err
	}
	// Fewer rows than pieces means at least one piece would be empty, and a
	// single row cannot be divided at all.
	if rows < 2 {
		return nil, nil
	}
	if uint64(parts) > rows {
		parts = int(rows)
	}

	// Interior boundaries only: parts pieces need parts-1 cuts.
	stride := rows / uint64(parts)
	if stride == 0 {
		return nil, nil
	}
	// The smallest key in the range. Every cut has to be strictly greater than
	// this, or the first sub-chunk would be empty — which happens when the key
	// has ties and the whole first stride shares the minimum value. Split does
	// not require a unique key, so this is reachable.
	prev, err := c.boundaryAtOffset(ctx, q, 0)
	if err != nil {
		return nil, err
	}
	var cuts [][]Datum
	for i := 1; i < parts; i++ {
		offset := stride * uint64(i)
		if offset >= rows {
			break
		}
		datums, err := c.boundaryAtOffset(ctx, q, offset)
		if err != nil {
			return nil, err
		}
		// No row at this offset (concurrent change, or an AdditionalConditions
		// filter we cannot predict): stop cutting and use what we have.
		if datums == nil {
			break
		}
		// Each cut must be strictly greater than the one before it (and than the
		// range minimum). A repeated value would produce an empty sub-chunk or one
		// whose inclusive lower bound equals its exclusive upper bound; a value
		// that went *backwards* would make two sub-chunks overlap. Both happen for
		// real: ties on a non-unique key give equal cuts, and a key whose sort
		// order differs from its comparison order (ENUM) gives decreasing ones.
		// Skipping the cut merges it into the previous piece, which is still
		// contiguous.
		if prev != nil && !datumsIncreasing(prev, datums) {
			continue
		}
		prev = datums
		cuts = append(cuts, datums)
	}
	if len(cuts) == 0 {
		return nil, nil
	}

	subs := make([]*Chunk, 0, len(cuts)+1)
	for i := range len(cuts) + 1 {
		sub := &Chunk{
			Key:                  c.Key,
			AdditionalConditions: c.AdditionalConditions,
			Table:                c.Table,
			NewTable:             c.NewTable,
			ColumnMapping:        c.ColumnMapping,
		}
		if i == 0 {
			sub.LowerBound = c.LowerBound
		} else {
			sub.LowerBound = &Boundary{Value: cuts[i-1], Inclusive: true}
		}
		if i == len(cuts) {
			sub.UpperBound = c.UpperBound
		} else {
			sub.UpperBound = &Boundary{Value: cuts[i], Inclusive: false}
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

// countRows returns how many source rows fall in the chunk's range. Used to
// decide the number of pieces, so that a range far smaller than parts is not
// cut into empty slivers.
func (c *Chunk) countRows(ctx context.Context, q Splitter) (uint64, error) {
	query := "SELECT COUNT(*) FROM " + c.Table.QuotedTableName + " WHERE " + c.String()
	var n uint64
	if err := q.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("failed to count rows for chunk split: %w", err)
	}
	return n, nil
}

// boundaryAtOffset returns the key values of the row at the given offset within
// the chunk's range, ordered by the chunk key, or nil if there is no such row.
func (c *Chunk) boundaryAtOffset(ctx context.Context, q Splitter, offset uint64) ([]Datum, error) {
	quotedKeys := QuoteColumns(c.Key)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1 OFFSET %d",
		quotedKeys,
		c.Table.QuotedTableName,
		c.String(),
		quotedKeys,
		offset,
	)
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to find split boundary: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	raw := make([]sql.RawBytes, len(columnNames))
	ptrs := make([]any, len(columnNames))
	for i := range columnNames {
		ptrs[i] = &raw[i]
	}
	if !rows.Next() {
		return nil, rows.Err()
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	// Scanned as raw bytes, so convert to the column's datum type — the same
	// conversion chunkerComposite.nextQueryToDatums performs.
	datums := make([]Datum, 0, len(columnNames))
	for i, name := range columnNames {
		val := reflect.ValueOf(raw[i]).Interface().(sql.RawBytes)
		tp, err := c.Table.datumTp(name)
		if err != nil {
			return nil, fmt.Errorf("looking up type for column %s: %w", name, err)
		}
		d, err := NewDatum(string(val), tp)
		if err != nil {
			return nil, fmt.Errorf("failed to create datum for column %s: %w", name, err)
		}
		datums = append(datums, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return datums, nil
}

// datumsIncreasing reports whether key tuple b sorts strictly after a, comparing
// lexicographically across the key columns the way a composite range predicate
// does.
//
// A datum pair that cannot be compared (mismatched or unsupported types) is
// treated as increasing: refusing every cut would disable splitting outright,
// whereas an out-of-order cut only costs the caller's exactness check, which is
// the backstop for exactly this case.
func datumsIncreasing(a, b []Datum) bool {
	if len(a) != len(b) {
		return true
	}
	for i := range a {
		cmp, err := b[i].compare(a[i])
		if err != nil {
			return true
		}
		if cmp != 0 {
			return cmp > 0
		}
	}
	return false // identical tuples
}
