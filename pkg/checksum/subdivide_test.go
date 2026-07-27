package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// narrowFixtureRows is chosen so that two rounds of narrowing are reachable:
// 19600 / 8 = 2450 rows per first-round piece, which is still above
// csRepairMinSplitRows, so a differing piece is cut again.
const narrowFixtureRows = 19600

// narrowFixture builds a source table of narrowFixtureRows rows (ids 1..N) and
// returns a chunk covering all of it. The recursive CTE is kept shallow and
// cross-joined because cte_max_recursion_depth defaults to 1000.
func narrowFixture(t *testing.T) (*sql.DB, *table.Chunk) {
	t.Helper()
	testutils.RunSQL(t, "DROP TABLE IF EXISTS narrowsub_t1, narrowsub_t2")
	testutils.RunSQL(t, "CREATE TABLE narrowsub_t1 (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL)")
	testutils.RunSQL(t, "CREATE TABLE narrowsub_t2 (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL)")
	testutils.RunSQL(t, `INSERT INTO narrowsub_t1 (id, pad)
		WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM s WHERE i < 139)
		SELECT x.i*140 + y.i + 1, 0 FROM s x, s y`)
	testutils.RunSQL(t, "INSERT INTO narrowsub_t2 SELECT * FROM narrowsub_t1")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	t1 := table.NewTableInfo(db, "test", "narrowsub_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "narrowsub_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM narrowsub_t1").Scan(&n))
	require.Equal(t, narrowFixtureRows, n)

	return db, &table.Chunk{
		Key:           t1.KeyColumns,
		Table:         t1,
		NewTable:      t2,
		ColumnMapping: table.NewColumnMapping(t1, t2, nil),
	}
}

// rowsIn counts source rows inside a chunk's range.
func rowsIn(t *testing.T, db *sql.DB, c *table.Chunk) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM narrowsub_t1 WHERE "+c.String()).Scan(&n))
	return n
}

// wholeCounts describes the fixture chunk as its parent verification would.
func wholeCounts(db *sql.DB) rangeCounts {
	return rangeCounts{sourceRows: narrowFixtureRows, targetRows: narrowFixtureRows, splitter: db}
}

// verifierForIDs returns a rangeVerifier that reports a range as differing when
// it contains any of the given ids — i.e. it stands in for a real source/target
// comparison, without needing corrupted data.
func verifierForIDs(t *testing.T, db *sql.DB, ids ...int) rangeVerifier {
	t.Helper()
	return func(ctx context.Context, sub *table.Chunk) (rangeCounts, error) {
		rows := rowsIn(t, db, sub)
		counts := rangeCounts{sourceRows: rows, targetRows: rows, splitter: db}
		for _, id := range ids {
			var hit int
			q := fmt.Sprintf("SELECT COUNT(*) FROM narrowsub_t1 WHERE id = %d AND (%s)", id, sub.String())
			if err := db.QueryRowContext(ctx, q).Scan(&hit); err != nil {
				return rangeCounts{}, err
			}
			if hit > 0 {
				counts.mismatched = true
				break
			}
		}
		return counts, nil
	}
}

func recordingRepairer(out *[]*table.Chunk) rangeRepairer {
	return func(_ context.Context, c *table.Chunk) error {
		*out = append(*out, c)
		return nil
	}
}

func TestNarrowRepairNarrowsToTheDifferingRange(t *testing.T) {
	db, whole := narrowFixture(t)
	var repaired []*table.Chunk

	require.NoError(t, narrowRepair(t.Context(), whole, wholeCounts(db),
		verifierForIDs(t, db, 7777), recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

	require.Len(t, repaired, 1, "one differing row means exactly one range to repair")
	// Two rounds of 8 leaves ~1/64th of the chunk. The point of the exercise is
	// that a single bad row does not rewrite 19600 rows.
	got := rowsIn(t, db, repaired[0])
	assert.Positive(t, got)
	assert.Less(t, got, uint64(csRepairMinSplitRows),
		"the narrowed range should be smaller than the point at which narrowing stops")

	// And it has to be the *right* range.
	var hit int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM narrowsub_t1 WHERE id = 7777 AND ("+repaired[0].String()+")").Scan(&hit))
	assert.Equal(t, 1, hit, "the repaired range must contain the differing row")
}

func TestNarrowRepairHandlesSeveralDifferingRanges(t *testing.T) {
	db, whole := narrowFixture(t)
	var repaired []*table.Chunk

	// Three rows spread across the range: first piece, middle, last piece.
	require.NoError(t, narrowRepair(t.Context(), whole, wholeCounts(db),
		verifierForIDs(t, db, 5, 9800, narrowFixtureRows), recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

	require.Len(t, repaired, 3)
	total := uint64(0)
	for _, c := range repaired {
		total += rowsIn(t, db, c)
	}
	assert.Less(t, total, uint64(3*csRepairMinSplitRows))

	// The repaired ranges must not overlap, or the same rows would be deleted
	// and rewritten more than once.
	for i := range repaired {
		for j := i + 1; j < len(repaired); j++ {
			var both int
			q := fmt.Sprintf("SELECT COUNT(*) FROM narrowsub_t1 WHERE (%s) AND (%s)",
				repaired[i].String(), repaired[j].String())
			require.NoError(t, db.QueryRowContext(t.Context(), q).Scan(&both))
			assert.Zero(t, both, "repaired ranges %d and %d overlap", i, j)
		}
	}
}

func TestNarrowRepairSkipsSmallChunks(t *testing.T) {
	db, whole := narrowFixture(t)
	var repaired []*table.Chunk

	// A chunk this size is cheaper to recopy than to analyse.
	small := rangeCounts{sourceRows: csRepairMinSplitRows - 1, targetRows: csRepairMinSplitRows - 1, splitter: db}
	require.NoError(t, narrowRepair(t.Context(), whole, small,
		func(context.Context, *table.Chunk) (rangeCounts, error) {
			t.Fatal("a chunk below the threshold must not be verified piecewise")
			return rangeCounts{}, nil
		}, recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

	require.Len(t, repaired, 1)
	assert.Same(t, whole, repaired[0], "the whole chunk is repaired unchanged")
}

// TestNarrowRepairFallsBackWhenNoSubRangeDiffers covers the case that should be
// impossible: the whole range mismatched, so an exact partition of it must
// contain a mismatching piece. If the invariant is ever violated we must still
// perform the repair rather than quietly decline to fix a known difference.
func TestNarrowRepairFallsBackWhenNoSubRangeDiffers(t *testing.T) {
	db, whole := narrowFixture(t)
	var repaired []*table.Chunk

	require.NoError(t, narrowRepair(t.Context(), whole, wholeCounts(db),
		func(_ context.Context, sub *table.Chunk) (rangeCounts, error) {
			rows := rowsIn(t, db, sub)
			return rangeCounts{sourceRows: rows, targetRows: rows, splitter: db}, nil
		}, recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

	require.Len(t, repaired, 1)
	assert.Same(t, whole, repaired[0])
}

func TestNarrowRepairFallsBackWhenSplitFails(t *testing.T) {
	db, whole := narrowFixture(t)
	// A condition that cannot be evaluated makes the boundary queries fail.
	whole.AdditionalConditions = "no_such_column = 1"
	var repaired []*table.Chunk

	require.NoError(t, narrowRepair(t.Context(), whole, wholeCounts(db),
		func(context.Context, *table.Chunk) (rangeCounts, error) {
			t.Fatal("verification cannot run if the range could not be split")
			return rangeCounts{}, nil
		}, recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

	require.Len(t, repaired, 1)
	assert.Same(t, whole, repaired[0], "an unsplittable range is repaired whole")
}

// TestNarrowRepairPropagatesVerifyError asserts a verification failure is not
// swallowed: it means the snapshot we were reasoning about is gone, so the pass
// must fail and be retried rather than repair on stale information.
func TestNarrowRepairPropagatesVerifyError(t *testing.T) {
	db, whole := narrowFixture(t)
	sentinel := errors.New("snapshot gone")
	var repaired []*table.Chunk

	err := narrowRepair(t.Context(), whole, wholeCounts(db),
		func(context.Context, *table.Chunk) (rangeCounts, error) {
			return rangeCounts{}, sentinel
		}, recordingRepairer(&repaired), slog.New(slog.DiscardHandler))

	require.ErrorIs(t, err, sentinel)
	assert.Empty(t, repaired, "nothing should be recopied on an unusable snapshot")
}

// TestNarrowAndReplaceChunkRepairsOnlyWhatDiffers drives the real comparison and
// the real recopy against real data.
//
// The probe is a target-only column (as an "add column" migration would have):
// it is invisible to the checksum, which compares intersecting columns only, but
// replaceChunk resets it to its default on every row it rewrites. Counting the
// rows whose marker was cleared tells us exactly how much of the chunk was
// recopied to fix one bad row.
func TestNarrowAndReplaceChunkRepairsOnlyWhatDiffers(t *testing.T) {
	const badID = 7777
	testutils.RunSQL(t, "DROP TABLE IF EXISTS narrowe2e_t1, _narrowe2e_t1_new")
	testutils.RunSQL(t, "CREATE TABLE narrowe2e_t1 (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL)")
	testutils.RunSQL(t, "CREATE TABLE _narrowe2e_t1_new (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL, marker INT NOT NULL DEFAULT 0)")
	testutils.RunSQL(t, `INSERT INTO narrowe2e_t1 (id, pad)
		WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM s WHERE i < 139)
		SELECT x.i*140 + y.i + 1, x.i FROM s x, s y`)
	testutils.RunSQL(t, "INSERT INTO _narrowe2e_t1_new (id, pad, marker) SELECT id, pad, 1 FROM narrowe2e_t1")
	testutils.RunSQL(t, fmt.Sprintf("UPDATE _narrowe2e_t1_new SET pad = pad + 1000 WHERE id = %d", badID))

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "narrowe2e_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_narrowe2e_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	checker := &SingleChecker{
		db:             db,
		dbConfig:       dbconn.NewDBConfig(),
		logger:         slog.New(slog.DiscardHandler),
		fixDifferences: true,
	}
	chunk := &table.Chunk{
		Key:           t1.KeyColumns,
		Table:         t1,
		NewTable:      t2,
		ColumnMapping: table.NewColumnMapping(t1, t2, nil),
	}

	// A REPEATABLE READ snapshot stands in for the one a checksum worker holds
	// from the transaction pool.
	trx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = trx.Rollback() }()

	require.NoError(t, checker.narrowAndReplaceChunk(t.Context(), trx, chunk, narrowFixtureRows, narrowFixtureRows))

	// The difference is gone.
	var diffs int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM narrowe2e_t1 a
		JOIN _narrowe2e_t1_new b USING (id) WHERE a.pad <> b.pad`).Scan(&diffs))
	assert.Zero(t, diffs, "the repair must reconcile the differing row")
	var srcRows, tgtRows int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM narrowe2e_t1").Scan(&srcRows))
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _narrowe2e_t1_new").Scan(&tgtRows))
	assert.Equal(t, srcRows, tgtRows)

	// ...and it was fixed by rewriting a small slice of the chunk, not all of it.
	var rewritten int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM _narrowe2e_t1_new WHERE marker = 0").Scan(&rewritten))
	assert.Positive(t, rewritten, "something must have been rewritten")
	assert.Less(t, rewritten, csRepairMinSplitRows,
		"narrowing should have kept the recopy well under the whole %d-row chunk", narrowFixtureRows)

	var badRowRewritten int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM _narrowe2e_t1_new WHERE id = %d AND marker = 0", badID)).Scan(&badRowRewritten))
	assert.Equal(t, 1, badRowRewritten, "the differing row must be one of the rewritten ones")
}

// TestDistributedNarrowAndReplaceChunkRepairsOnlyWhatDiffers is the N:M analog
// of the test above. The repair path differs completely — DELETE on every target
// followed by a re-apply through the applier — so it gets its own coverage. The
// marker column is the same probe: the applier writes the source's non-generated
// columns only, so a rewritten row loses its marker.
func TestDistributedNarrowAndReplaceChunkRepairsOnlyWhatDiffers(t *testing.T) {
	const badID = 7777
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	newDBName, _ := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS narrowdist_t1")
	testutils.RunSQL(t, "CREATE TABLE narrowdist_t1 (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL)")
	testutils.RunSQL(t, `INSERT INTO narrowdist_t1 (id, pad)
		WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM s WHERE i < 139)
		SELECT x.i*140 + y.i + 1, x.i FROM s x, s y`)
	testutils.RunSQL(t, "CREATE TABLE "+newDBName+".narrowdist_t1 (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL, marker INT NOT NULL DEFAULT 0)")
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".narrowdist_t1 (id, pad, marker) SELECT id, pad, 1 FROM test.narrowdist_t1")
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s.narrowdist_t1 SET pad = pad + 1000 WHERE id = %d", newDBName, badID))

	destCfg := cfg.Clone()
	destCfg.DBName = newDBName
	src, err := dbconn.New(cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(src)
	dest, err := dbconn.New(destCfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(dest)

	t1 := table.NewTableInfo(src, "test", "narrowdist_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(dest, newDBName, "narrowdist_t1")
	require.NoError(t, t2.SetInfo(t.Context()))

	app, err := applier.NewSingleTargetApplier(
		applier.Target{DB: dest, KeyRange: "0", Config: destCfg}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	feed := change.NewBinlogClient(src, cfg.Addr, cfg.User, cfg.Passwd, app, change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.Applier = app
	config.Logger = slog.New(slog.DiscardHandler)
	checker, err := NewChecker([]*sql.DB{src}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	dc, ok := checker.(*DistributedChecker)
	require.True(t, ok)
	// Run() would build these; we are driving one chunk directly so that the
	// chunker's own sizing does not decide whether narrowing is reachable.
	dc.sourcePools = []sourcePool{{db: src}}
	// Run() also starts the applier's workers; the recopy's Apply calls queue
	// forever without them.
	require.NoError(t, app.Start(t.Context()))
	defer func() { _ = app.Stop() }()

	chunk := &table.Chunk{
		Key:           t1.KeyColumns,
		Table:         t1,
		NewTable:      t2,
		ColumnMapping: table.NewColumnMapping(t1, t2, nil),
	}
	srcTrx, err := src.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = srcTrx.Rollback() }()
	tgtTrx, err := dest.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tgtTrx.Rollback() }()

	require.NoError(t, dc.narrowAndReplaceChunk(t.Context(), []*sql.Tx{srcTrx}, []*sql.Tx{tgtTrx}, chunk,
		rangeCounts{sourceRows: narrowFixtureRows, targetRows: narrowFixtureRows, splitter: srcTrx}))

	var diffs int
	require.NoError(t, dest.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM test.narrowdist_t1 a
		JOIN narrowdist_t1 b USING (id) WHERE a.pad <> b.pad`).Scan(&diffs))
	assert.Zero(t, diffs, "the repair must reconcile the differing row")
	var tgtRows int
	require.NoError(t, dest.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM narrowdist_t1").Scan(&tgtRows))
	assert.Equal(t, narrowFixtureRows, tgtRows, "no rows may be lost by the delete-then-reapply")

	var rewritten int
	require.NoError(t, dest.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM narrowdist_t1 WHERE marker = 0").Scan(&rewritten))
	assert.Positive(t, rewritten)
	assert.Less(t, rewritten, csRepairMinSplitRows,
		"narrowing should have kept the re-apply well under the whole %d-row chunk", narrowFixtureRows)
}

// TestNarrowRepairFallsBackWhenEverySubRangeDiffers covers systematic corruption
// — a lossy ALTER, a wrong column mapping, a truncated target. Every piece needs
// rewriting, so narrowing has bought nothing, and fanning the chunk out into one
// repair per piece would only multiply the transaction count and the span of the
// fix's uncancellable window.
func TestNarrowRepairFallsBackWhenEverySubRangeDiffers(t *testing.T) {
	db, whole := narrowFixture(t)
	var repaired []*table.Chunk

	require.NoError(t, narrowRepair(t.Context(), whole, wholeCounts(db),
		func(_ context.Context, sub *table.Chunk) (rangeCounts, error) {
			rows := rowsIn(t, db, sub)
			return rangeCounts{sourceRows: rows, targetRows: rows, mismatched: true, splitter: db}, nil
		}, recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

	require.Len(t, repaired, 1, "an all-differing chunk is recopied in one go")
	assert.Same(t, whole, repaired[0])
}

// TestNarrowRepairFallsBackWhenCountsDoNotAddUp is the guard against a
// subdivision that is not an exact partition. table.Chunk.Split cannot promise
// exactness for every key type — a collation whose order differs from byte order,
// or a NULL key value, yields an overlap or a gap — and a gap would let a
// differing row escape repair entirely. The row counts, read in the same snapshot
// as the parent's, are the check.
func TestNarrowRepairFallsBackWhenCountsDoNotAddUp(t *testing.T) {
	db, whole := narrowFixture(t)

	for _, tc := range []struct {
		name  string
		short bool // report fewer rows (a gap) rather than more (an overlap)
	}{
		{"gap", true},
		{"overlap", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var repaired []*table.Chunk
			first := true
			require.NoError(t, narrowRepair(t.Context(), whole, wholeCounts(db),
				func(_ context.Context, sub *table.Chunk) (rangeCounts, error) {
					rows := rowsIn(t, db, sub)
					// Perturb one piece so the totals cannot reconcile.
					if first {
						first = false
						if tc.short {
							rows--
						} else {
							rows++
						}
					}
					return rangeCounts{sourceRows: rows, targetRows: rows, mismatched: true, splitter: db}, nil
				}, recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

			require.Len(t, repaired, 1)
			assert.Same(t, whole, repaired[0], "an inexact partition must not be trusted")
		})
	}
}

// TestNarrowRepairFallsBackWithoutASplitter covers the distributed case where no
// source shard holds any of the range: there are no boundaries to cut from.
func TestNarrowRepairFallsBackWithoutASplitter(t *testing.T) {
	_, whole := narrowFixture(t)
	var repaired []*table.Chunk

	counts := rangeCounts{sourceRows: 0, targetRows: narrowFixtureRows, splitter: nil}
	require.NoError(t, narrowRepair(t.Context(), whole, counts,
		func(context.Context, *table.Chunk) (rangeCounts, error) {
			t.Fatal("without a splitter there is nothing to verify piecewise")
			return rangeCounts{}, nil
		}, recordingRepairer(&repaired), slog.New(slog.DiscardHandler)))

	require.Len(t, repaired, 1)
	assert.Same(t, whole, repaired[0])
}

func TestWidestSource(t *testing.T) {
	a, b, c := &sql.Tx{}, &sql.Tx{}, &sql.Tx{}
	trxs := []*sql.Tx{a, b, c}

	assert.Equal(t, table.Splitter(b), widestSource(trxs, []uint64{10, 500, 20}))
	assert.Equal(t, table.Splitter(a), widestSource(trxs, []uint64{500, 500, 20}), "ties take the first shard")
	assert.Nil(t, widestSource(trxs, []uint64{0, 0, 0}), "no shard holds the range")
	assert.Nil(t, widestSource(nil, nil))
	assert.Nil(t, widestSource(trxs, nil))
	// A count slice longer than the transaction slice must not index out of range.
	assert.Equal(t, table.Splitter(a), widestSource([]*sql.Tx{a}, []uint64{5, 999}))
}

// TestNarrowAndReplaceChunkUsesTheMismatchSnapshot is the test that fails if the
// sub-range verification is moved off the snapshot transaction.
//
// The setup makes live data and snapshot data disagree: the mismatch is observed
// in the snapshot, then repaired behind its back. A verifier reading live data
// would find every sub-range clean, conclude the partition was inexact or the
// mismatch imaginary, and fall back to recopying the whole chunk. Reading the
// snapshot, it still sees the difference and narrows to it.
func TestNarrowAndReplaceChunkUsesTheMismatchSnapshot(t *testing.T) {
	const badID = 7777
	testutils.RunSQL(t, "DROP TABLE IF EXISTS narrowsnap_t1, _narrowsnap_t1_new")
	testutils.RunSQL(t, "CREATE TABLE narrowsnap_t1 (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL)")
	testutils.RunSQL(t, "CREATE TABLE _narrowsnap_t1_new (id INT NOT NULL PRIMARY KEY, pad INT NOT NULL, marker INT NOT NULL DEFAULT 0)")
	testutils.RunSQL(t, `INSERT INTO narrowsnap_t1 (id, pad)
		WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM s WHERE i < 139)
		SELECT x.i*140 + y.i + 1, x.i FROM s x, s y`)
	testutils.RunSQL(t, "INSERT INTO _narrowsnap_t1_new (id, pad, marker) SELECT id, pad, 1 FROM narrowsnap_t1")
	testutils.RunSQL(t, fmt.Sprintf("UPDATE _narrowsnap_t1_new SET pad = pad + 1000 WHERE id = %d", badID))

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	t1 := table.NewTableInfo(db, "test", "narrowsnap_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_narrowsnap_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	checker := &SingleChecker{
		db:             db,
		dbConfig:       dbconn.NewDBConfig(),
		logger:         slog.New(slog.DiscardHandler),
		fixDifferences: true,
	}
	chunk := &table.Chunk{
		Key:           t1.KeyColumns,
		Table:         t1,
		NewTable:      t2,
		ColumnMapping: table.NewColumnMapping(t1, t2, nil),
	}

	trx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = trx.Rollback() }()

	// Observe the mismatch, which also pins the snapshot.
	srcCRC, tgtCRC, srcCount, tgtCount, err := checker.checksumRange(t.Context(), trx, chunk)
	require.NoError(t, err)
	require.True(t, compareChunk(srcCRC, tgtCRC, srcCount, tgtCount).mismatched())

	// Now make the live rows agree, invisibly to the snapshot.
	testutils.RunSQL(t, fmt.Sprintf(
		"UPDATE _narrowsnap_t1_new SET pad = pad - 1000 WHERE id = %d", badID))

	require.NoError(t, checker.narrowAndReplaceChunk(t.Context(), trx, chunk, srcCount, tgtCount))

	var rewritten int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM _narrowsnap_t1_new WHERE marker = 0").Scan(&rewritten))
	assert.Positive(t, rewritten, "the snapshot still shows a difference, so something must be rewritten")
	assert.Less(t, rewritten, csRepairMinSplitRows,
		"verification must read the snapshot; reading live data would find every sub-range clean and recopy the whole chunk")
}
