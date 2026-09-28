package checksum

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/applier"
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

func TestFactoryVerificationResume(t *testing.T) {
	for _, mode := range []string{"single", "lockless"} {
		t.Run(mode, func(t *testing.T) {
			chunker := &resumeChunker{testChunker: newTestChunker(0), watermark: "verified-prefix"}
			cfg := NewCheckerDefaultConfig()
			cfg.Watermark = "saved-prefix"
			cfg.Applier = &applier.MockApplier{}
			if mode == "lockless" {
				cfg.Lockless = true
			}
			checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{&change.MockSource{}}, cfg)
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
			checker.(*SingleChecker).differencesFound.Add(1)
			wm, err = checker.ResumeWatermark()
			require.NoError(t, err)
			require.Empty(t, wm, "repairs must invalidate resume evidence")
		})
	}
}

func TestFactoryLocklessConfigAndLifecycle(t *testing.T) {
	chunker := newTestChunker(0)
	feed := &change.MockSource{}
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 2
	cfg.Autoscale = AutoscaleConfig{MaxThreads: 3}
	cfg.Lockless = true
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
	require.Equal(t, 2, feed.PeriodicFlushStarts())
	require.Equal(t, feed.PeriodicFlushStarts(), feed.PeriodicFlushStops())
}

// Repair policy is derived from FixDifferences for every algorithm, and it is
// the same derivation: the recopier the checker repairs through is built by the
// factory, so a divergence is answered identically whichever checker the config
// selected. The write path it is built over is the caller's, and it is required
// when — and only when — repairs were asked for.
func TestFactoryDerivesRepairPolicy(t *testing.T) {
	for _, mode := range []string{"single", "lockless"} {
		t.Run(mode, func(t *testing.T) {
			newCfg := func() *CheckerConfig {
				cfg := NewCheckerDefaultConfig()
				if mode == "lockless" {
					cfg.Lockless = true
				}
				return cfg
			}
			recopierOf := func(c Checker) Recopier {
				if c, ok := c.(*SingleChecker); ok {
					return c.recopier
				}
				return c.(*LocklessChecker).recopier
			}

			// Without FixDifferences there is no repair path at all, and no
			// applier is demanded for one.
			checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&change.MockSource{}}, newCfg())
			require.NoError(t, err)
			require.Nil(t, recopierOf(checker), "a divergence is an error, not something to rewrite")

			cfg := newCfg()
			cfg.FixDifferences = true
			cfg.Applier = &applier.MockApplier{}
			checker, err = NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&change.MockSource{}}, cfg)
			require.NoError(t, err)
			require.NotNil(t, recopierOf(checker))

			cfg = newCfg()
			cfg.FixDifferences = true
			_, err = NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&change.MockSource{}}, cfg)
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
		cfg.Lockless = true
		cfg.FixDifferences = true
		cfg.Applier = &applier.MockApplier{}
		return cfg
	}

	// Same server: the repair goes through the mapping-aware single-server
	// path, which is what makes a migration's repair correct across a rename.
	checker, err := NewChecker([]*sql.DB{source}, newTestChunker(0), []change.Source{&change.MockSource{}}, newCfg())
	require.NoError(t, err)
	lockless := checker.(*LocklessChecker)
	require.Equal(t, []*sql.DB{source}, lockless.targetDBs, "the target defaults to the server being read from")
	require.IsType(t, &chunkRepairer{}, lockless.recopier)

	cfg := newCfg()
	cfg.TargetDB = target
	checker, err = NewChecker([]*sql.DB{source}, newTestChunker(0), []change.Source{&change.MockSource{}}, cfg)
	require.NoError(t, err)
	lockless = checker.(*LocklessChecker)
	require.Equal(t, []*sql.DB{source}, lockless.sourceDBs)
	require.Equal(t, []*sql.DB{target}, lockless.targetDBs)
	recopier, ok := lockless.recopier.(*mysqlRecopier)
	require.True(t, ok, "a cross-server repair reads one server and writes the other")
	require.Same(t, source, recopier.sourceDB)
	require.Same(t, target, recopier.targetDB)

	cfg = newCfg()
	cfg.Lockless = false
	cfg.TargetDB = target
	_, err = NewChecker([]*sql.DB{source}, newTestChunker(0), []change.Source{&change.MockSource{}}, cfg)
	require.ErrorContains(t, err, "single verification cannot span two servers")
}

// TestFactoryRejectsExtraSourcesForSingleSource: a caller that passes N sources
// and an applier without setting Lockless would otherwise build a
// SingleChecker, verify sourceDBs[0], and report the whole topology clean.
// Verification that passes by not looking is the one failure mode worth a hard
// error, so the shape is rejected.
func TestFactoryRejectsExtraSourcesForSingleSource(t *testing.T) {
	cfg := NewCheckerDefaultConfig()
	cfg.Applier = &applier.MockApplier{}
	sources := []*sql.DB{{}, {}}
	feeds := []change.Source{&change.MockSource{}, &change.MockSource{}}

	_, err := NewChecker(sources, newTestChunker(0), feeds[:1], cfg)
	require.ErrorContains(t, err, "single verification requires one source and one feed, got 2 and 1")

	_, err = NewChecker(sources[:1], newTestChunker(0), feeds, cfg)
	require.ErrorContains(t, err, "single verification requires one source and one feed, got 1 and 2")

	// A nil entry is the same class of mistake and must not reach the
	// checker as a usable handle either.
	_, err = NewChecker([]*sql.DB{nil}, newTestChunker(0), feeds[:1], cfg)
	require.ErrorContains(t, err, "single verification requires a non-nil source and feed")
}

// A checker owns the feed's periodic flush for the duration of a run, unless
// the caller runs one for longer than any single run and says so.
func TestFactoryExternalFlushLoop(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprintf("external=%t", external), func(t *testing.T) {
			cfg := NewCheckerDefaultConfig()
			cfg.Lockless = true
			cfg.ExternalFlushLoop = external
			checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&change.MockSource{}}, cfg)
			require.NoError(t, err)
			require.Equal(t, !external, checker.(*LocklessChecker).ownsFeedFlush)
		})
	}
}

// The finite until-clean loop must terminate, so the factory bounds it even
// when the caller did not.
func TestFactoryBoundsLocklessPasses(t *testing.T) {
	cfg := NewCheckerDefaultConfig()
	cfg.Lockless = true
	checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&change.MockSource{}}, cfg)
	require.NoError(t, err)
	require.Positive(t, checker.(*LocklessChecker).cfg.MaxPasses)
	require.Zero(t, cfg.MaxPasses, "factory must not mutate the caller's config")
}

// A lockless checker reads every source and every target, so the factory
// must pair each source with its feed and must know where the copy is: the
// TargetDB, else the applier's targets, else beside a lone source. N sources
// with nowhere named for their rows would verify one shard's slice and report
// the topology clean, so that shape is refused.
func TestFactoryLocklessTopology(t *testing.T) {
	a, b, c := &sql.DB{}, &sql.DB{}, &sql.DB{}
	feeds := []change.Source{&change.MockSource{}, &change.MockSource{}}
	build := func(sources []*sql.DB, feeds []change.Source, app applier.Applier) (*LocklessChecker, error) {
		cfg := NewCheckerDefaultConfig()
		cfg.Lockless = true
		if app != nil {
			cfg.Applier = app
		}
		checker, err := NewChecker(sources, newTestChunker(0), feeds, cfg)
		if err != nil {
			return nil, err
		}
		return checker.(*LocklessChecker), nil
	}

	_, err := build([]*sql.DB{a, b}, feeds, nil)
	require.ErrorContains(t, err, "lockless verification of 2 sources requires an applier to name the targets")

	_, err = build([]*sql.DB{a, b}, feeds, &applier.MockApplier{})
	require.ErrorContains(t, err, "requires an applier to name the targets", "an applier with no targets names nothing")

	_, err = build([]*sql.DB{a, b}, feeds[:1], nil)
	require.ErrorContains(t, err, "one feed per source, got 2 sources and 1 feeds")

	_, err = build([]*sql.DB{a}, feeds, nil)
	require.ErrorContains(t, err, "one feed per source, got 1 sources and 2 feeds")

	_, err = build([]*sql.DB{nil}, feeds[:1], nil)
	require.ErrorContains(t, err, "source 0 has none")

	_, err = build([]*sql.DB{a}, []change.Source{nil}, nil)
	require.ErrorContains(t, err, "source 0 has none")

	_, err = build([]*sql.DB{a}, feeds[:1], &applier.MockApplier{Targets: []applier.Target{{}}})
	require.ErrorContains(t, err, "applier target 0 has no connection")

	cfg := NewCheckerDefaultConfig()
	cfg.Lockless = true
	cfg.TargetDB = c
	_, err = NewChecker([]*sql.DB{a, b}, newTestChunker(0), feeds, cfg)
	require.ErrorContains(t, err, "TargetDB requires exactly one source, got 2")

	checker, err := build([]*sql.DB{a}, feeds[:1], nil)
	require.NoError(t, err)
	require.Equal(t, []*sql.DB{a}, checker.targetDBs, "a lone source is compared with itself")
	require.True(t, checker.sameServer())
	require.NotNil(t, checker.settlingFeed())

	checker, err = build([]*sql.DB{a}, feeds[:1], &applier.MockApplier{Targets: []applier.Target{{DB: a}}})
	require.NoError(t, err)
	require.True(t, checker.sameServer(), "a migration's applier targets the source's own handle")

	checker, err = build([]*sql.DB{a, b}, feeds, &applier.MockApplier{Targets: []applier.Target{{DB: c}, {DB: a}}})
	require.NoError(t, err)
	require.Equal(t, []*sql.DB{a, b}, checker.sourceDBs)
	require.Equal(t, []*sql.DB{c, a}, checker.targetDBs)
	require.False(t, checker.sameServer())
	require.Nil(t, checker.settlingFeed(), "no single feed orders a multi-source range")
}

// Lockless alone selects the checker; an applier does not. Supplying one to a
// single-server checker means "repair through this" and nothing else.
func TestFactoryCheckerSelection(t *testing.T) {
	newCfg := func(lockless bool) *CheckerConfig {
		cfg := NewCheckerDefaultConfig()
		cfg.Lockless = lockless
		cfg.Applier = &applier.MockApplier{}
		return cfg
	}
	build := func(t *testing.T, cfg *CheckerConfig) (Checker, error) {
		t.Helper()
		return NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&change.MockSource{}}, cfg)
	}

	for _, tc := range []struct {
		lockless bool
		want     Checker
		name     string
	}{
		{false, (*SingleChecker)(nil), "single"},
		{true, (*LocklessChecker)(nil), "lockless"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checker, err := build(t, newCfg(tc.lockless))
			require.NoError(t, err)
			require.IsType(t, tc.want, checker)
		})
	}
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
			cfg.Applier = &applier.MockApplier{}
			if optimistic {
				cfg.Lockless = true
			}
			_, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{&change.MockSource{}}, cfg)
			require.NoError(t, err)
			for _, child := range children {
				_, err := child.Next()
				require.NoError(t, err, "every child must be open from the beginning")
			}
		})
	}
}
