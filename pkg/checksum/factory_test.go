package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

type resumeChunker struct {
	*testChunker
	opened       string
	watermark    string
	watermarkErr error
}

func (c *resumeChunker) OpenAtWatermark(w string) error   { c.opened = w; return nil }
func (c *resumeChunker) GetLowWatermark() (string, error) { return c.watermark, c.watermarkErr }

type lifecycleFeed struct {
	fakeFeed
	starts, stops int
}

func (f *lifecycleFeed) StartPeriodicFlush(context.Context, time.Duration) { f.starts++ }
func (f *lifecycleFeed) StopPeriodicFlush()                                { f.stops++ }

func TestFactoryVerificationResume(t *testing.T) {
	for _, mode := range []string{"single", "distributed", "lockless"} {
		t.Run(mode, func(t *testing.T) {
			chunker := &resumeChunker{testChunker: newTestChunker(0), watermark: "verified-prefix"}
			cfg := NewCheckerDefaultConfig()
			cfg.Watermark = "saved-prefix"
			cfg.Applier = &spyApplier{}
			switch mode {
			case "distributed":
				cfg.Algorithm = Sharded
			case "lockless":
				cfg.Algorithm = Lockless
			}
			checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{&fakeFeed{}}, cfg)
			require.NoError(t, err)
			wm, err := checker.ResumeWatermark()
			require.NoError(t, err)
			// Saved evidence is honoured by every algorithm: a watermark means
			// the prefix below it was observed equal, whichever checker
			// observed it.
			require.Equal(t, "saved-prefix", chunker.opened)
			require.Equal(t, "verified-prefix", wm)
			if mode == "lockless" {
				// Optimistic verification has no per-attempt difference counter
				// to invalidate evidence with; what keeps the watermark honest
				// is that a repaired chunk is never fed back at all. See
				// TestLocklessRepairParksResumeWatermark.
				return
			}
			switch c := checker.(type) {
			case *SingleChecker:
				c.differencesFound.Add(1)
			case *DistributedChecker:
				c.differencesFound.Add(1)
			}
			wm, err = checker.ResumeWatermark()
			require.NoError(t, err)
			require.Empty(t, wm, "repairs must invalidate resume evidence")
		})
	}
}

func TestFactoryLocklessConfigAndLifecycle(t *testing.T) {
	chunker := newTestChunker(0)
	feed := &lifecycleFeed{}
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 2
	cfg.Autoscale = AutoscaleConfig{MaxThreads: 3}
	cfg.Algorithm = Lockless
	checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{feed}, cfg)
	require.NoError(t, err)
	finite := checker.(*LocklessChecker)
	require.Equal(t, 2, finite.cfg.Concurrency)
	require.Equal(t, cfg.Autoscale, finite.cfg.Autoscale)
	// FixDifferences was not set, so a confirmed divergence is an error rather
	// than something to repair, and no recopier was built.
	require.Nil(t, finite.recopier)
	checker.SetThrottler(&throttler.Noop{})
	require.Contains(t, StatusRow(checker), "scanning")
	for range 2 {
		require.NoError(t, checker.Run(t.Context()))
		require.Contains(t, StatusRow(checker), "verified")
		require.Equal(t, StatusSummary(checker), StatusRow(checker))
		require.False(t, checker.StartTime().IsZero())
		require.Positive(t, checker.ExecTime())
	}
	require.Equal(t, 1, chunker.resets)
	require.Equal(t, 2, feed.starts)
	require.Equal(t, feed.starts, feed.stops)
}

// Repair policy is derived from FixDifferences for every algorithm, and it is
// the same derivation: the recopier the checker repairs through is built by the
// factory, so a divergence is answered identically whichever checker the config
// selected. The write path it is built over is the caller's, and it is required
// when — and only when — repairs were asked for.
func TestFactoryDerivesRepairPolicy(t *testing.T) {
	for _, mode := range []string{"single", "distributed", "lockless"} {
		t.Run(mode, func(t *testing.T) {
			newCfg := func() *CheckerConfig {
				cfg := NewCheckerDefaultConfig()
				switch mode {
				case "distributed":
					cfg.Algorithm = Sharded
					// Sharded needs an applier to reach its targets at all, so
					// it always has one; whether it *repairs* through it is
					// still FixDifferences' call, same as everywhere else.
					cfg.Applier = &spyApplier{}
				case "lockless":
					cfg.Algorithm = Lockless
				}
				return cfg
			}
			recopierOf := func(c Checker) Recopier {
				switch c := c.(type) {
				case *SingleChecker:
					return c.recopier
				case *DistributedChecker:
					return c.recopier
				default:
					return c.(*LocklessChecker).recopier
				}
			}

			// Without FixDifferences there is no repair path at all, and no
			// applier is demanded for one.
			checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, newCfg())
			require.NoError(t, err)
			require.Nil(t, recopierOf(checker), "a divergence is an error, not something to rewrite")

			cfg := newCfg()
			cfg.FixDifferences = true
			cfg.Applier = &spyApplier{}
			checker, err = NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
			require.NoError(t, err)
			require.NotNil(t, recopierOf(checker))

			if mode == "distributed" {
				// Sharded already demanded its applier at validation, so there
				// is nothing further to demand here.
				return
			}
			cfg = newCfg()
			cfg.FixDifferences = true
			_, err = NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
			require.ErrorContains(t, err, "applier must be non-nil to repair differences")
		})
	}
}

// TargetDB is the only thing that says "the copy being verified is on another
// server", and it decides both halves of that: which handle the reads compare
// against, and which repair implementation is built. It is lockless-only,
// because the snapshot algorithms reach their target through a table lock and a
// REPEATABLE READ snapshot, neither of which spans two servers.
func TestFactoryCrossServerTarget(t *testing.T) {
	source, target := &sql.DB{}, &sql.DB{}
	newCfg := func() *CheckerConfig {
		cfg := NewCheckerDefaultConfig()
		cfg.Algorithm = Lockless
		cfg.FixDifferences = true
		cfg.Applier = &spyApplier{}
		return cfg
	}

	// Same server: the repair goes through the mapping-aware single-server
	// path, which is what makes a migration's repair correct across a rename.
	checker, err := NewChecker([]*sql.DB{source}, newTestChunker(0), []change.Source{&fakeFeed{}}, newCfg())
	require.NoError(t, err)
	lockless := checker.(*LocklessChecker)
	require.Same(t, source, lockless.targetDB, "the target defaults to the server being read from")
	require.IsType(t, &chunkRepairer{}, lockless.recopier)

	cfg := newCfg()
	cfg.TargetDB = target
	checker, err = NewChecker([]*sql.DB{source}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
	require.NoError(t, err)
	lockless = checker.(*LocklessChecker)
	require.Same(t, source, lockless.sourceDB)
	require.Same(t, target, lockless.targetDB)
	recopier, ok := lockless.recopier.(*mysqlRecopier)
	require.True(t, ok, "a cross-server repair reads one server and writes the other")
	require.Same(t, source, recopier.sourceDB)
	require.Same(t, target, recopier.targetDB)

	for _, algorithm := range []Algorithm{Single, Sharded} {
		t.Run(algorithm.String(), func(t *testing.T) {
			cfg := newCfg()
			cfg.Algorithm = algorithm
			cfg.TargetDB = target
			_, err := NewChecker([]*sql.DB{source}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
			require.ErrorContains(t, err, algorithm.String()+" verification cannot span two servers")
		})
	}
}

// A checker owns the feed's periodic flush for the duration of a run, unless
// the caller runs one for longer than any single run and says so.
func TestFactoryExternalFlushLoop(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprintf("external=%t", external), func(t *testing.T) {
			cfg := NewCheckerDefaultConfig()
			cfg.Algorithm = Lockless
			cfg.ExternalFlushLoop = external
			checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
			require.NoError(t, err)
			require.Equal(t, !external, checker.(*LocklessChecker).ownsFeedFlush)
		})
	}
}

// The finite until-clean loop must terminate, so the factory bounds it even
// when the caller did not.
func TestFactoryBoundsLocklessPasses(t *testing.T) {
	cfg := NewCheckerDefaultConfig()
	cfg.Algorithm = Lockless
	checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
	require.NoError(t, err)
	require.Positive(t, checker.(*LocklessChecker).cfg.MaxPasses)
	require.Zero(t, cfg.MaxPasses, "factory must not mutate the caller's config")
}

func TestFactoryRejectsUnsupportedLocklessTopology(t *testing.T) {
	for _, mode := range []string{"sources", "feeds", "nil-source", "nil-feed"} {
		t.Run(mode, func(t *testing.T) {
			cfg := NewCheckerDefaultConfig()
			cfg.Algorithm = Lockless
			sources := []*sql.DB{{}}
			feeds := []change.Source{&fakeFeed{}}
			switch mode {
			case "sources":
				sources = append(sources, &sql.DB{})
			case "feeds":
				feeds = append(feeds, &fakeFeed{})
			case "nil-source":
				sources[0] = nil
			case "nil-feed":
				feeds[0] = nil
			}
			_, err := NewChecker(sources, newTestChunker(0), feeds, cfg)
			require.Error(t, err)
		})
	}
}

// The algorithm is named, not inferred, so an applier no longer selects one.
// Supplying one to a single-server or lockless checker means "repair through
// this" and nothing else; a sharded checker needs one whether or not it
// repairs; and an unnamed algorithm is refused rather than silently defaulted.
func TestFactoryAlgorithmSelection(t *testing.T) {
	newCfg := func(a Algorithm) *CheckerConfig {
		cfg := NewCheckerDefaultConfig()
		cfg.Algorithm = a
		cfg.Applier = &spyApplier{}
		return cfg
	}
	build := func(t *testing.T, cfg *CheckerConfig) (Checker, error) {
		t.Helper()
		return NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
	}

	for _, tc := range []struct {
		algorithm Algorithm
		want      Checker
		name      string
	}{
		{Single, (*SingleChecker)(nil), "single"},
		{Sharded, (*DistributedChecker)(nil), "sharded"},
		{Lockless, (*LocklessChecker)(nil), "lockless"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.name, tc.algorithm.String())
			checker, err := build(t, newCfg(tc.algorithm))
			require.NoError(t, err)
			require.IsType(t, tc.want, checker)
		})
	}

	cfg := newCfg(Sharded)
	cfg.Applier = nil
	_, err := build(t, cfg)
	require.ErrorContains(t, err, "sharded verification requires an applier")

	_, err = build(t, newCfg(Algorithm(99)))
	require.ErrorContains(t, err, "unknown checksum algorithm")
}

func TestSnapshotResumeWatermarkNotReady(t *testing.T) {
	chunker := &resumeChunker{testChunker: newTestChunker(0), watermarkErr: table.ErrWatermarkNotReady}
	checker := &SingleChecker{chunker: chunker}
	_, err := checker.ResumeWatermark()
	require.ErrorIs(t, err, table.ErrWatermarkNotReady)
}

func TestStatusFallback(t *testing.T) {
	require.Equal(t, "Checksum Progress="+(unpacedChecker{}).GetProgress().String(), StatusSummary(unpacedChecker{}))
	require.NotContains(t, StatusRow(unpacedChecker{}), "lockless")
}

// Empty multi-table watermarks are valid checkpoints: no child may have
// finished a chunk when the checkpoint was written. Both children must open.
func TestFactoryResumeWithoutChildWatermarks(t *testing.T) {
	for _, optimistic := range []bool{false, true} {
		name := "snapshot"
		if optimistic {
			name = "lockless"
		}
		t.Run(name, func(t *testing.T) {
			var children []table.Chunker
			for _, tableName := range []string{"factory_resume_a", "factory_resume_b"} {
				tt := testutils.NewTestTable(t, tableName, "CREATE TABLE "+tableName+" (id BIGINT PRIMARY KEY)")
				info := table.NewTableInfo(tt.DB, "test", tableName)
				require.NoError(t, info.SetInfo(t.Context()))
				child, err := table.NewChunker(info, table.ChunkerConfig{NewTable: info})
				require.NoError(t, err)
				children = append(children, child)
			}
			chunker := table.NewMultiChunker(children...)
			t.Cleanup(func() { require.NoError(t, chunker.Close()) })
			cfg := NewCheckerDefaultConfig()
			cfg.Watermark = "{}"
			cfg.Applier = &spyApplier{}
			if optimistic {
				cfg.Algorithm = Lockless
			}
			_, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{&fakeFeed{}}, cfg)
			require.NoError(t, err)
			for _, child := range children {
				_, err := child.Next()
				require.NoError(t, err, "every child must be open from the beginning")
			}
		})
	}
}
