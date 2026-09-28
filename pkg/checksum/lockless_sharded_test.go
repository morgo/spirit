package checksum

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysql "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// shardedFixture is N source schemas and M target schemas on the test server,
// each holding table t1, wired the way pkg/move wires them: one feed and one
// chunker per source, a MultiChunker over the chunkers, and one ShardedApplier
// routing even ids to the first target and odd ids to the last.
type shardedFixture struct {
	sources, targets []*sql.DB
	feeds            []change.Source
	chunker          table.Chunker
	applier          applier.Applier
}

const shardedTableDDL = "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)"

// newShardedFixture creates the schemas, runs the per-schema setup SQL (index
// i of sourceSQL runs on source i, likewise targetSQL), and starts the feeds.
func newShardedFixture(t *testing.T, sourceSQL, targetSQL []string) *shardedFixture {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	connect := func(setup string) (*sql.DB, *mysql.Config) {
		name, _ := testutils.CreateUniqueTestDatabase(t)
		testutils.RunSQLInDatabase(t, name, shardedTableDDL)
		if setup != "" {
			testutils.RunSQLInDatabase(t, name, setup)
		}
		dbCfg := cfg.Clone()
		dbCfg.DBName = name
		db, err := dbconn.New(dbCfg.FormatDSN(), dbconn.NewDBConfig())
		require.NoError(t, err)
		t.Cleanup(func() { utils.CloseAndLog(db) })
		return db, dbCfg
	}

	f := &shardedFixture{}
	var targets []applier.Target
	ranges := map[int][]string{1: {"-"}, 2: {"-80", "80-"}}[len(targetSQL)]
	require.NotNil(t, ranges, "fixture supports one or two targets")
	for i, setup := range targetSQL {
		db, dbCfg := connect(setup)
		f.targets = append(f.targets, db)
		targets = append(targets, applier.Target{DB: db, KeyRange: ranges[i], Config: dbCfg})
	}
	app, err := applier.NewShardedApplier(targets, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	f.applier = app

	var chunkers []table.Chunker
	for _, setup := range sourceSQL {
		db, dbCfg := connect(setup)
		info := table.NewTableInfo(db, dbCfg.DBName, "t1")
		require.NoError(t, info.SetInfo(t.Context()))
		info.ShardingColumn = "id"
		info.HashFunc = testutils.EvenOddHasher
		feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, app, change.NewClientDefaultConfig())
		t.Cleanup(feed.Close)
		chunker, err := table.NewChunker(info, table.ChunkerConfig{NewTable: info})
		require.NoError(t, err)
		require.NoError(t, feed.AddSubscription(info, info, chunker))
		require.NoError(t, feed.Start(t.Context()))
		f.sources = append(f.sources, db)
		f.feeds = append(f.feeds, feed)
		chunkers = append(chunkers, chunker)
	}
	f.chunker = table.NewMultiChunker(chunkers...)
	require.NoError(t, f.chunker.Open())
	return f
}

func (f *shardedFixture) checker(t *testing.T, fix bool) Checker {
	t.Helper()
	config := NewCheckerDefaultConfig()
	config.Lockless = true
	config.Applier = f.applier
	config.FixDifferences = fix
	config.RetryDelay = 10 * time.Millisecond
	checker, err := NewChecker(f.sources, f.chunker, f.feeds, config)
	require.NoError(t, err)
	return checker
}

// rows returns every (id, name) across dbs, which for the targets is the
// logical table the shards hold between them.
func rowsAcross(t *testing.T, dbs []*sql.DB) map[int]string {
	t.Helper()
	all := map[int]string{}
	for _, db := range dbs {
		rows, err := db.QueryContext(t.Context(), "SELECT id, name FROM t1")
		require.NoError(t, err)
		for rows.Next() {
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			_, dup := all[id]
			require.False(t, dup, "id %d is on more than one server", id)
			all[id] = name
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return all
}

// A 1:M reshard: one source, its rows split across two targets by the
// applier's hash. Every chunk aggregates across both targets.
func TestShardedOneToMany(t *testing.T) {
	var src, even, odd string
	src = "INSERT INTO t1 VALUES "
	for id := 1; id <= 200; id++ {
		v := fmt.Sprintf("(%d, 'row%d')", id, id)
		if id > 1 {
			src += ","
		}
		src += v
	}
	even = "INSERT INTO t1 SELECT id, CONCAT('row', id) FROM (SELECT 2*n AS id FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < 100) SELECT n FROM s) x) y"
	odd = "INSERT INTO t1 SELECT id, CONCAT('row', id) FROM (SELECT 2*n-1 AS id FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < 100) SELECT n FROM s) x) y"
	f := newShardedFixture(t, []string{src}, []string{even, odd})

	checker := f.checker(t, false)
	require.NoError(t, checker.Run(t.Context()))
	require.Zero(t, checker.DifferencesFound())
}

// An N:M reshard: two sources (ids 1-4 and 5-8) resharded to two targets by
// parity, so every target holds rows from both sources.
func TestShardedNtoM(t *testing.T) {
	f := newShardedFixture(t,
		[]string{
			"INSERT INTO t1 VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')",
			"INSERT INTO t1 VALUES (5,'five'),(6,'six'),(7,'seven'),(8,'eight')",
		},
		[]string{
			"INSERT INTO t1 VALUES (2,'two'),(4,'four'),(6,'six'),(8,'eight')",
			"INSERT INTO t1 VALUES (1,'one'),(3,'three'),(5,'five'),(7,'seven')",
		})
	checker := f.checker(t, false)
	require.NoError(t, checker.Run(t.Context()))
	require.Zero(t, checker.DifferencesFound())
}

// The same row on both sources (a disjointness violation) cancels out of the
// source-side BIT_XOR, so the aggregated CRC matches an empty target. Only
// the summed row counts (2 vs 0) catch it.
func TestShardedPairCancellation(t *testing.T) {
	f := newShardedFixture(t,
		[]string{"INSERT INTO t1 VALUES (1,'dup')", "INSERT INTO t1 VALUES (1,'dup')"},
		[]string{""})
	err := f.checker(t, false).Run(t.Context())
	require.ErrorIs(t, err, ErrPermanentDivergence, "row-count mismatch from pair-cancellation must be detected")
}

// With FixDifferences, a divergence anywhere in an N:M topology is repaired
// by deleting the range on every target and re-applying every source's rows
// through the sharded applier — which re-routes each row to its shard, so a
// row that landed on the wrong target is moved rather than duplicated.
func TestShardedRepairNtoM(t *testing.T) {
	f := newShardedFixture(t,
		[]string{
			"INSERT INTO t1 VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')",
			"INSERT INTO t1 VALUES (5,'five'),(6,'six'),(7,'seven'),(8,'eight')",
		},
		[]string{
			// 4 is missing; 6 is wrong; 7 is on the wrong shard.
			"INSERT INTO t1 VALUES (2,'two'),(6,'SIX'),(7,'seven'),(8,'eight')",
			// 9 exists on no source.
			"INSERT INTO t1 VALUES (1,'one'),(3,'three'),(5,'five'),(9,'nine')",
		})
	checker := f.checker(t, true)
	require.NoError(t, checker.Run(t.Context()))
	require.Positive(t, checker.DifferencesFound())
	require.Equal(t, rowsAcross(t, f.sources), rowsAcross(t, f.targets))

	var misplaced int
	require.NoError(t, f.targets[0].QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1 WHERE MOD(id, 2) = 1").Scan(&misplaced))
	require.Zero(t, misplaced, "the repair re-routed the odd row to its own shard")
}

// A 1:1 copy on another server, reached only through the applier (the shape
// of a move with one source and one target), is repaired through it.
func TestShardedFixCorruptOneToOne(t *testing.T) {
	f := newShardedFixture(t,
		[]string{"INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')"},
		[]string{"INSERT INTO t1 VALUES (1,'a'),(3,'wrong')"})
	checker := f.checker(t, true)
	require.Equal(t, "0/3 0.00%", checker.GetProgress().String())
	require.NoError(t, checker.Run(t.Context()))
	require.Equal(t, rowsAcross(t, f.sources), rowsAcross(t, f.targets))
}
