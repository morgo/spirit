package move

// Isolated tests for the reverse-window data plane (ReverseFeed), exercised
// WITHOUT any forward move or cutover: set up databases already in the
// post-forward-copy state, then drive writes on the (former) target side and
// assert they flow back to the (former) source.
//
// Layout: poc_rf_u = the unsharded source U (reverse target). poc_rf_s0 /
// poc_rf_s1 = two former target shards S (reverse sources) holding DISJOINT,
// globally-unique PKs — so U is exactly their union and the N:1 merge back is
// collision-free (the sequences/globally-unique-PK precondition from the design).

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// openSchemaConn opens a connection whose DEFAULT database is `schema`, which
// TableInfo.SetInfo requires (it resolves columns via DATABASE(), not the
// SchemaName argument). Registered for cleanup so goleak stays clean.
func openSchemaConn(t *testing.T, schema string) *sql.DB {
	t.Helper()
	db, err := dbconn.New(testutils.DSNForDatabase(schema), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db
}

func reverseTableInfo(t *testing.T, db *sql.DB, schema, name string) *table.TableInfo {
	t.Helper()
	ti := table.NewTableInfo(db, schema, name)
	require.NoError(t, ti.SetInfo(t.Context()))
	return ti
}

// setupFanIn builds U + two shards in the post-forward-copy state (U == s0 ∪ s1).
func setupFanIn(t *testing.T) {
	t.Helper()
	for _, s := range []string{"poc_rf_u", "poc_rf_s0", "poc_rf_s1"} {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+s)
		testutils.RunSQL(t, "CREATE DATABASE "+s)
		testutils.RunSQL(t, "CREATE TABLE "+s+".t1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	}
	testutils.RunSQL(t, "INSERT INTO poc_rf_s0.t1 VALUES (1,'one'),(3,'three'),(5,'five')")
	testutils.RunSQL(t, "INSERT INTO poc_rf_s1.t1 VALUES (2,'two'),(4,'four'),(6,'six')")
	testutils.RunSQL(t, "INSERT INTO poc_rf_u.t1  VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'),(6,'six')")
}

// newFanInFeed wires a reverse feed s0,s1 → U. positions[i], when non-empty,
// starts source i from that captured position instead of the current head.
func newFanInFeed(t *testing.T, positions []string) (*ReverseFeed, *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	uDB := openSchemaConn(t, "poc_rf_u")
	uTbl := reverseTableInfo(t, uDB, "poc_rf_u", "t1")

	mkSource := func(schema string, idx int) ReverseSource {
		sDB := openSchemaConn(t, schema)
		pos := ""
		if idx < len(positions) {
			pos = positions[idx]
		}
		return ReverseSource{
			DB:       sDB,
			Addr:     cfg.Addr,
			User:     cfg.User,
			Password: cfg.Passwd,
			Tables:   []*table.TableInfo{reverseTableInfo(t, sDB, schema, "t1")},
			Position: pos,
		}
	}

	feed, err := NewReverseFeed(ReverseFeedConfig{
		Sources:      []ReverseSource{mkSource("poc_rf_s0", 0), mkSource("poc_rf_s1", 1)},
		Target:       applier.Target{DB: uDB, KeyRange: "0"},
		TargetTables: map[string]*table.TableInfo{"t1": uTbl},
	})
	require.NoError(t, err)
	return feed, uDB
}

func dumpRows(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var out []string
	for rows.Next() {
		var id int
		var val string
		require.NoError(t, rows.Scan(&id, &val))
		out = append(out, fmt.Sprintf("%d=%s", id, val))
	}
	require.NoError(t, rows.Err())
	return out
}

// assertConverged asserts U equals the union of the two shards' current state.
func assertConverged(t *testing.T, db *sql.DB) {
	t.Helper()
	union := dumpRows(t, db, "SELECT id,val FROM poc_rf_s0.t1 UNION ALL SELECT id,val FROM poc_rf_s1.t1 ORDER BY id")
	got := dumpRows(t, db, "SELECT id,val FROM poc_rf_u.t1 ORDER BY id")
	require.Equal(t, union, got, "U must equal the union of all shards")
}

// TestReverseFeedFanInConverges: writes to BOTH shards after cutover (insert,
// update, delete) merge back into the single U. This is the N:1 case the
// original PoC did not cover.
func TestReverseFeedFanInConverges(t *testing.T) {
	setupFanIn(t)
	feed, uDB := newFanInFeed(t, nil)
	defer feed.Close()
	require.NoError(t, feed.Start(t.Context()))

	// "App writes" landing on each shard after cutover.
	testutils.RunSQL(t, "INSERT INTO poc_rf_s0.t1 VALUES (7,'seven')")
	testutils.RunSQL(t, "UPDATE poc_rf_s0.t1 SET val='ONE' WHERE id=1")
	testutils.RunSQL(t, "DELETE FROM poc_rf_s0.t1 WHERE id=3")
	testutils.RunSQL(t, "INSERT INTO poc_rf_s1.t1 VALUES (8,'eight')")
	testutils.RunSQL(t, "UPDATE poc_rf_s1.t1 SET val='TWO' WHERE id=2")
	testutils.RunSQL(t, "DELETE FROM poc_rf_s1.t1 WHERE id=4")

	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB)
}

// TestReverseFeedIdempotentReplay: resuming a fresh feed from an EARLIER
// captured position re-applies a window of events; idempotent REPLACE keeps U
// correct. Confirms the imprecise-cutover-handoff safety, now per-source (N:1).
func TestReverseFeedIdempotentReplay(t *testing.T) {
	setupFanIn(t)
	feed, uDB := newFanInFeed(t, nil)
	require.NoError(t, feed.Start(t.Context()))

	// Batch 1, then capture positions (non-empty after a flush).
	testutils.RunSQL(t, "INSERT INTO poc_rf_s0.t1 VALUES (7,'seven')")
	testutils.RunSQL(t, "INSERT INTO poc_rf_s1.t1 VALUES (8,'eight')")
	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB)

	pos := feed.Positions()
	require.Len(t, pos, 2)
	for i, p := range pos {
		require.NotEmpty(t, p, "source %d position", i)
	}

	// Batch 2 (applied by the live feed), then close.
	testutils.RunSQL(t, "UPDATE poc_rf_s0.t1 SET val='ONE' WHERE id=1")
	testutils.RunSQL(t, "DELETE FROM poc_rf_s1.t1 WHERE id=4")
	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB)
	feed.Close()

	// A fresh feed resumes from the batch-1 positions, replaying batch 2. U is
	// already up to date, so idempotent apply must leave it converged.
	replay, uDB2 := newFanInFeed(t, pos)
	defer replay.Close()
	require.NoError(t, replay.Start(t.Context()))
	require.NoError(t, replay.Flush(t.Context()))
	assertConverged(t, uDB2)
}

// TestReverseFeedWindowHoldsAndReturns: Run opens the feeds, holds for the
// window, and returns nil (feeds healthy, no fatal). The window-loop skeleton
// the eventual cutover integration will drive.
func TestReverseFeedWindowHoldsAndReturns(t *testing.T) {
	setupFanIn(t)
	feed, _ := newFanInFeed(t, nil)
	defer feed.Close()

	start := time.Now()
	require.NoError(t, feed.Run(t.Context(), 500*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 450*time.Millisecond, "Run must hold for ~the window")
}

// TestReverseFeedStartCleanupOnPartialFailure: when Start fails partway (here
// the second source has a bogus resume position), the feed it already started
// for the first source must be torn down, not left running. There is
// deliberately no feed.Close() — a correct Start already cleaned up, and the
// package's goleak TestMain fails if the first source's binlog-reader or
// periodic-flush goroutines survive.
func TestReverseFeedStartCleanupOnPartialFailure(t *testing.T) {
	setupFanIn(t)
	// Source 0 starts from head (succeeds, spawning goroutines); source 1
	// resumes from an unparseable position and fails, tripping Start's cleanup.
	feed, _ := newFanInFeed(t, []string{"", "not-a-valid-position"})
	err := feed.Start(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "start source 1", "the second source must be the one that fails")
}
