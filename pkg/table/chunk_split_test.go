package table

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// splitFixture creates a table with rows 1..n and returns its TableInfo.
func splitFixture(t *testing.T, name string, n int) *TableInfo {
	t.Helper()
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+name)
	testutils.RunSQL(t, "CREATE TABLE "+name+" (id INT NOT NULL PRIMARY KEY, pad VARCHAR(10))")
	if n > 0 {
		testutils.RunSQL(t, fmt.Sprintf(
			"INSERT INTO %s (id, pad) WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < %d) SELECT i, 'x' FROM s",
			name, n))
	}
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ti := NewTableInfo(db, "test", name)
	require.NoError(t, ti.SetInfo(t.Context()))
	return ti
}

// countIn returns how many rows of ti fall inside the chunk's range, which is
// how the tests verify a partition covers exactly what it should.
func countIn(t *testing.T, ti *TableInfo, c *Chunk) int {
	t.Helper()
	var n int
	require.NoError(t, ti.db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+ti.QuotedTableName+" WHERE "+c.String()).Scan(&n))
	return n
}

func TestChunkSplitPartitionsExactly(t *testing.T) {
	ti := splitFixture(t, "csplit1", 1000)
	whole := &Chunk{Key: []string{"id"}, Table: ti, NewTable: ti}
	require.Equal(t, 1000, countIn(t, ti, whole))

	subs, err := whole.Split(t.Context(), ti.db, 4)
	require.NoError(t, err)
	require.Len(t, subs, 4)

	// The union must be exactly the original, with no double-counting: if the
	// interior boundaries were both inclusive, or both exclusive, this total
	// would come out over or under 1000.
	total := 0
	for _, s := range subs {
		n := countIn(t, ti, s)
		assert.Positive(t, n, "no sub-chunk may be empty")
		total += n
	}
	assert.Equal(t, 1000, total, "sub-chunks must partition the range exactly")

	// Unbounded ends stay unbounded, so the partition still covers rows outside
	// any boundary the caller never set.
	assert.Nil(t, subs[0].LowerBound, "first sub-chunk inherits the open lower bound")
	assert.Nil(t, subs[len(subs)-1].UpperBound, "last sub-chunk inherits the open upper bound")
}

func TestChunkSplitPreservesBoundsAndConditions(t *testing.T) {
	ti := splitFixture(t, "csplit2", 500)
	lo, err := NewDatum(100, signedType)
	require.NoError(t, err)
	hi, err := NewDatum(300, signedType)
	require.NoError(t, err)
	// [100, 300) with an extra filter, i.e. the shape a real chunk has.
	whole := &Chunk{
		Key:                  []string{"id"},
		Table:                ti,
		NewTable:             ti,
		LowerBound:           &Boundary{Value: []Datum{lo}, Inclusive: true},
		UpperBound:           &Boundary{Value: []Datum{hi}, Inclusive: false},
		AdditionalConditions: "id % 2 = 0",
	}
	before := countIn(t, ti, whole)
	require.Equal(t, 100, before, "even ids in [100,300)")

	subs, err := whole.Split(t.Context(), ti.db, 5)
	require.NoError(t, err)
	require.NotEmpty(t, subs)

	total := 0
	for _, s := range subs {
		assert.Equal(t, "id % 2 = 0", s.AdditionalConditions, "the filter must ride along")
		assert.Equal(t, whole.Key, s.Key)
		assert.Equal(t, ti, s.Table)
		assert.Equal(t, ti, s.NewTable)
		total += countIn(t, ti, s)
	}
	assert.Equal(t, before, total, "splitting must not change what the range covers")

	// The outer edges are the original's, inclusivity included — otherwise a
	// repair driven off these sub-chunks would miss or overreach a boundary row.
	assert.Equal(t, whole.LowerBound, subs[0].LowerBound)
	assert.Equal(t, whole.UpperBound, subs[len(subs)-1].UpperBound)
}

func TestChunkSplitCompositeKey(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS csplit3")
	testutils.RunSQL(t, "CREATE TABLE csplit3 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))")
	testutils.RunSQL(t, `INSERT INTO csplit3 (a, b)
		WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 40)
		SELECT x.i, y.i FROM s x, s y`)
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ti := NewTableInfo(db, "test", "csplit3")
	require.NoError(t, ti.SetInfo(t.Context()))

	whole := &Chunk{Key: []string{"a", "b"}, Table: ti, NewTable: ti}
	require.Equal(t, 1600, countIn(t, ti, whole))

	subs, err := whole.Split(t.Context(), ti.db, 8)
	require.NoError(t, err)
	require.Len(t, subs, 8)

	// Composite keys are where an inclusive/exclusive slip is easiest to make,
	// because the comparison expands into nested OR terms rather than a simple
	// range test.
	total := 0
	for i, s := range subs {
		if i > 0 {
			require.Len(t, s.LowerBound.Value, 2, "interior cuts bound both key parts")
		}
		total += countIn(t, ti, s)
	}
	assert.Equal(t, 1600, total)
}

func TestChunkSplitRefusesWhenNotUseful(t *testing.T) {
	ti := splitFixture(t, "csplit4", 3)
	whole := &Chunk{Key: []string{"id"}, Table: ti, NewTable: ti}

	for _, parts := range []int{0, 1} {
		subs, err := whole.Split(t.Context(), ti.db, parts)
		require.NoError(t, err)
		assert.Nil(t, subs, "parts=%d is not a split", parts)
	}

	// More pieces requested than rows available: clamped rather than returning
	// empty slivers.
	subs, err := whole.Split(t.Context(), ti.db, 50)
	require.NoError(t, err)
	require.Len(t, subs, 3, "3 rows can only be cut into 3 pieces")
	for _, s := range subs {
		assert.Positive(t, countIn(t, ti, s))
	}

	// A single row cannot be divided, and an empty range has nothing to divide.
	single := splitFixture(t, "csplit5", 1)
	subs, err = (&Chunk{Key: []string{"id"}, Table: single, NewTable: single}).Split(t.Context(), single.db, 4)
	require.NoError(t, err)
	assert.Nil(t, subs)

	empty := splitFixture(t, "csplit6", 0)
	subs, err = (&Chunk{Key: []string{"id"}, Table: empty, NewTable: empty}).Split(t.Context(), empty.db, 4)
	require.NoError(t, err)
	assert.Nil(t, subs)
}

func TestChunkSplitHandlesKeyTies(t *testing.T) {
	// Split does not require a unique key. With heavy ties, consecutive offsets
	// can land on the same key value; emitting both as cuts would create an
	// empty sub-chunk and, worse, a sub-chunk whose lower bound equals its
	// exclusive upper bound.
	testutils.RunSQL(t, "DROP TABLE IF EXISTS csplit7")
	testutils.RunSQL(t, "CREATE TABLE csplit7 (id INT NOT NULL PRIMARY KEY, grp INT NOT NULL, KEY (grp))")
	testutils.RunSQL(t, `INSERT INTO csplit7 (id, grp)
		WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 100)
		SELECT i, i % 3 FROM s`)
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ti := NewTableInfo(db, "test", "csplit7")
	require.NoError(t, ti.SetInfo(t.Context()))

	// Chunking on grp, which has only 3 distinct values across 100 rows.
	whole := &Chunk{Key: []string{"grp"}, Table: ti, NewTable: ti}
	subs, err := whole.Split(t.Context(), ti.db, 10)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(subs), 3, "cannot cut a 3-valued key into more than 3 pieces")

	total := 0
	for _, s := range subs {
		n := countIn(t, ti, s)
		assert.Positive(t, n, "a tie must not yield an empty sub-chunk")
		total += n
	}
	assert.Equal(t, 100, total, "ties must still partition exactly")
}

// TestChunkSplitEnumKeyStaysExact covers a key whose sort order is not its
// comparison order. MySQL orders ENUM by declaration ordinal but compares it
// against a string literal lexically, so boundaries read in ORDER BY order can
// come back *decreasing* in the order the range predicates use — which would
// make two sub-chunks overlap and get the same rows deleted and rewritten twice.
func TestChunkSplitEnumKeyStaysExact(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS csplit8")
	// Declaration order is deliberately not lexicographic.
	testutils.RunSQL(t, "CREATE TABLE csplit8 (e ENUM('apple','zebra','mango') NOT NULL PRIMARY KEY)")
	testutils.RunSQL(t, "INSERT INTO csplit8 (e) VALUES ('apple'), ('zebra'), ('mango')")
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ti := NewTableInfo(db, "test", "csplit8")
	require.NoError(t, ti.SetInfo(t.Context()))

	whole := &Chunk{Key: []string{"e"}, Table: ti, NewTable: ti}
	require.Equal(t, 3, countIn(t, ti, whole))

	subs, err := whole.Split(t.Context(), ti.db, 3)
	require.NoError(t, err)

	total := 0
	for _, s := range subs {
		n := countIn(t, ti, s)
		assert.Positive(t, n, "no sub-chunk may be empty")
		total += n
	}
	assert.Equal(t, 3, total, "a decreasing cut must be dropped, not emitted as an overlap")
}
