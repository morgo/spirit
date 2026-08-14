package change

import (
	"context"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// statsFeed is a minimal change.Source double. Only FeedStats carries
// behaviour; reporting is opt-in so a source with reports=false exercises the
// "does not implement StatsReporter" path.
type statsFeed struct {
	stats FeedStats
}

var _ Source = (*statsFeed)(nil)

func (f *statsFeed) FeedStats() FeedStats { return f.stats }

// plainFeed is a Source that deliberately does NOT implement StatsReporter,
// standing in for an out-of-tree source. Embedding the interface satisfies
// Source without supplying FeedStats; StatusRow only type-asserts, so the nil
// embedded value is never called.
type plainFeed struct{ Source }

func (f *statsFeed) AddSubscription(_, _ *table.TableInfo, _ table.MappedChunker) error { return nil }
func (f *statsFeed) Start(context.Context) error                                        { return nil }
func (f *statsFeed) StartFromPosition(context.Context, string) error                    { return nil }
func (f *statsFeed) Position() string                                                   { return "" }
func (f *statsFeed) CurrentPosition(context.Context) (string, error)                    { return "", nil }
func (f *statsFeed) Flush(context.Context) error                                        { return nil }
func (f *statsFeed) FlushUnderTableLock(context.Context, []*dbconn.TableLock) error     { return nil }
func (f *statsFeed) BlockWait(context.Context) error                                    { return nil }
func (f *statsFeed) GetDeltaLen() int                                                   { return 0 }
func (f *statsFeed) FlushResidual() (int, int)                                          { return 0, 0 }
func (f *statsFeed) SetWatermarkOptimization(context.Context, bool) error               { return nil }
func (f *statsFeed) StartPeriodicFlush(context.Context, time.Duration)                  {}
func (f *statsFeed) StopPeriodicFlush()                                                 {}
func (f *statsFeed) AllChangesFlushed() bool                                            { return true }
func (f *statsFeed) Stop()                                                              {}
func (f *statsFeed) Close()                                                             {}

func TestFeedStatsStringNeverFlushed(t *testing.T) {
	require.Equal(t,
		"rotations=0 (0 forced)  never flushed",
		FeedStats{}.String())
}

func TestFeedStatsString(t *testing.T) {
	s := FeedStats{
		LastFlushAt:       time.Now().Add(-10 * time.Second),
		LastFlushDuration: 2854617 * time.Nanosecond,
		LastFlushRows:     5583,
		Rotations:         4,
		ForcedRotations:   1,
	}
	require.Equal(t,
		"rotations=4 (1 forced)  flushed 10s ago (took 2.855ms, 5583 rows)",
		s.String())
}

func TestStatusRowNoReporter(t *testing.T) {
	// A nil source, and a source that cannot report, both contribute nothing
	// rather than printing empty fields.
	require.Empty(t, StatusRow())
	require.Empty(t, StatusRow(nil))
	require.Empty(t, StatusRow(&plainFeed{}))
}

func TestStatusRowSingleSource(t *testing.T) {
	src := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-3 * time.Second),
		LastFlushDuration: 5 * time.Millisecond,
		LastFlushRows:     12,
		Rotations:         2,
		ForcedRotations:   1,
	}}
	require.Equal(t,
		"rotations=2 (1 forced)  flushed 3s ago (took 5ms, 12 rows)",
		StatusRow(src))
}

// A sharded move reads one feed per source. The fields are merged into one
// set: counters sum, and the flush figures come from the feed that flushed
// least recently, because that is the one holding the position back.
func TestStatusRowMergesSources(t *testing.T) {
	recent := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-time.Second),
		LastFlushDuration: time.Millisecond,
		LastFlushRows:     1,
		Rotations:         2,
		ForcedRotations:   1,
	}}
	stale := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-90 * time.Second),
		LastFlushDuration: 7 * time.Millisecond,
		LastFlushRows:     900,
		Rotations:         3,
		ForcedRotations:   0,
	}}
	require.Equal(t,
		"rotations=5 (1 forced)  flushed 1m30s ago (took 7ms, 900 rows)",
		StatusRow(recent, stale))
	// Order must not matter.
	require.Equal(t, StatusRow(recent, stale), StatusRow(stale, recent))
}

// A feed that has never flushed is the stalest of all: it must not be masked
// by a sibling that has.
func TestStatusRowNeverFlushedWins(t *testing.T) {
	flushed := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-time.Second),
		LastFlushDuration: time.Millisecond,
		LastFlushRows:     5,
		Rotations:         1,
	}}
	never := &statsFeed{stats: FeedStats{Rotations: 1}}
	require.Equal(t,
		"rotations=2 (0 forced)  never flushed",
		StatusRow(flushed, never))
	require.Equal(t, StatusRow(flushed, never), StatusRow(never, flushed))
}

// The feed records real flushes and real rotations, which is what the runner
// status block reports in place of the per-flush and per-rotation log lines.
func TestFeedStatsFromLiveFeed(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS feedstatst1, feedstatst2")
	testutils.RunSQL(t, "CREATE TABLE feedstatst1 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE feedstatst2 (a INT NOT NULL, b INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "feedstatst1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "feedstatst2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Nothing flushed yet.
	require.True(t, client.FeedStats().LastFlushAt.IsZero())
	require.Contains(t, StatusRow(client), "never flushed")

	testutils.RunSQL(t, "INSERT INTO feedstatst1 (a, b) VALUES (1, 2), (3, 4)")
	require.NoError(t, client.BlockWait(t.Context()))
	// A single flush, as the periodic flush loop performs it.
	require.NoError(t, client.flush(t.Context(), false, nil))

	stats := client.FeedStats()
	require.False(t, stats.LastFlushAt.IsZero(), "a completed flush must be recorded")
	require.Equal(t, 1, stats.Flushes)
	require.Equal(t, 2, stats.LastFlushRows, "batch size is the pending count at the start of the flush")
	require.Zero(t, stats.Residual, "the flush drained everything")

	// The public Flush loops until the backlog is trivial and then flushes
	// once more, so it ends on an empty batch. That is reported honestly: a
	// feed that is keeping up has nothing to flush.
	require.NoError(t, client.Flush(t.Context()))
	stats = client.FeedStats()
	require.Greater(t, stats.Flushes, 1)
	require.Zero(t, stats.LastFlushRows)

	// Rotating the source binlog advances the rotation counter. Only the
	// lower bound is asserted: the test server is shared, so anything else
	// running against it can rotate the log too.
	before := client.FeedStats().Rotations
	testutils.RunSQL(t, "FLUSH BINARY LOGS")
	testutils.RunSQL(t, "INSERT INTO feedstatst1 (a, b) VALUES (5, 6)")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Eventually(t, func() bool {
		return client.FeedStats().Rotations > before
	}, 10*time.Second, 50*time.Millisecond,
		"rotation was not counted (still %d)", client.FeedStats().Rotations)
}
