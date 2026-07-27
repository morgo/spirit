package checksum

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingThrottler records BlockWait calls so a test can prove the checksum
// consults the throttler at all. It never actually blocks.
type countingThrottler struct {
	throttler.Noop
	calls atomic.Int64
}

func (c *countingThrottler) BlockWait(_ context.Context) { c.calls.Add(1) }

// gateThrottler blocks in BlockWait until released, standing in for a
// throttler that is holding the phase back. It signals entry once so a test
// can synchronise on the pass having reached the throttle check.
type gateThrottler struct {
	throttler.Noop
	release chan struct{}
	entered chan struct{}
	once    atomic.Bool
}

func newGateThrottler() *gateThrottler {
	return &gateThrottler{
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
}

func (g *gateThrottler) BlockWait(ctx context.Context) {
	if g.once.CompareAndSwap(false, true) {
		close(g.entered)
	}
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

// waitEntered fails the test if the checksum never reaches the throttle check.
func (g *gateThrottler) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("checksum never consulted the throttler")
	}
}

// checksumFixture builds two identical tables of approximately rows rows and
// returns a checker over them, wired to a real binlog feed.
func checksumFixture(t *testing.T, name string, rows int, cfg *CheckerConfig) Checker {
	t.Helper()
	newName := "_" + name + "_new"
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+name+", "+newName+", _"+name+"_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE "+name+" (a INT NOT NULL AUTO_INCREMENT, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE "+newName+" (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _"+name+"_chkpnt (a INT)") // for binlog advancement
	// Seed by doubling, the same shape as testutils.TestTable.SeedRows.
	testutils.RunSQL(t, "INSERT INTO "+name+" (b) SELECT 1 FROM dual")
	for n := 1; n < rows; n *= 2 {
		testutils.RunSQL(t, "INSERT INTO "+name+" (b) SELECT b FROM "+name)
	}
	testutils.RunSQL(t, "INSERT INTO "+newName+" (a, b) SELECT a, b FROM "+name)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	t1 := table.NewTableInfo(db, "test", name)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", newName)
	require.NoError(t, t2.SetInfo(t.Context()))

	dsn, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, dsn.Addr, dsn.User, dsn.Passwd,
		applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	t.Cleanup(feed.Close)

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, cfg)
	require.NoError(t, err)
	return checker
}

func TestChecksumConsultsThrottler(t *testing.T) {
	checker := checksumFixture(t, "checksum_throttle_counts", 4096, NewCheckerDefaultConfig())

	thr := &countingThrottler{}
	aware, ok := checker.(ThrottleAware)
	require.True(t, ok, "SingleChecker must expose the ThrottleAware capability")
	aware.SetThrottler(thr)

	require.NoError(t, checker.Run(t.Context()))
	// Before this existed the checksum ignored throttling entirely, so a
	// nonzero count is the whole point of the assertion.
	assert.Positive(t, thr.calls.Load(), "checksum must consult the throttler while dispatching")
}

func TestChecksumThrottlerBlocksDispatch(t *testing.T) {
	checker := checksumFixture(t, "checksum_throttle_blocks", 4096, NewCheckerDefaultConfig())
	thr := newGateThrottler()
	checker.(ThrottleAware).SetThrottler(thr)

	done := make(chan error, 1)
	go func() { done <- checker.Run(t.Context()) }()

	thr.waitEntered(t)
	select {
	case err := <-done:
		t.Fatalf("checksum completed while throttled: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// Releasing lets it finish normally — throttling pauses the pass, it does
	// not fail it.
	close(thr.release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("checksum did not finish after the throttler released")
	}
}

func TestChecksumThrottledPassIsCancellable(t *testing.T) {
	checker := checksumFixture(t, "checksum_throttle_cancel", 1024, NewCheckerDefaultConfig())
	thr := newGateThrottler() // never released
	checker.(ThrottleAware).SetThrottler(thr)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- checker.Run(ctx) }()

	thr.waitEntered(t)
	// A checksum parked in BlockWait must still honour cancellation, otherwise
	// a throttled pass would be unkillable.
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("cancelling a throttled checksum did not return")
	}
}

func TestChecksumCancelledWhileThrottledNeverReportsPass(t *testing.T) {
	// Regression guard for the worst failure mode this phase can have: a
	// checksum that verified only part of the table but returned nil, which
	// Run reports as "checksum passed" and a caller may act on by cutting over.
	//
	// The dispatch loop stops on a done context, so a cancellation landing
	// while no chunk is in flight leaves the errgroup with no error to report.
	// Without the chunker.IsRead() guard in runChecksum this returns nil.
	checker := checksumFixture(t, "checksum_cancel_no_false_pass", 8192, NewCheckerDefaultConfig())
	thr := newGateThrottler() // never released, so not one chunk is dispatched
	checker.(ThrottleAware).SetThrottler(thr)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- checker.Run(ctx) }()

	thr.waitEntered(t)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err, "a checksum that verified nothing must not report success")
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("cancelling a throttled checksum did not return")
	}
}

func TestChecksumScalesConcurrencyDuringPass(t *testing.T) {
	// End-to-end proof that the pool is genuinely resizable mid-pass: the
	// transaction pool is provisioned at MaxThreads, so growing the limiter
	// past the starting concurrency must still find a transaction rather than
	// failing with "no transactions in pool".
	//
	// The gate throttler makes this deterministic: the pass is held at its
	// first throttle check while the limiter is resized, so there is no race
	// against the checksum finishing first.
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 2
	cfg.Autoscale = AutoscaleConfig{Enabled: true, MaxThreads: 6}
	checker := checksumFixture(t, "checksum_scale_up", 8192, cfg)

	single, ok := checker.(*SingleChecker)
	require.True(t, ok)

	thr := newGateThrottler()
	single.SetThrottler(thr)

	done := make(chan error, 1)
	go func() { done <- checker.Run(t.Context()) }()

	thr.waitEntered(t)
	limiter := single.currentLimiter()
	require.NotNil(t, limiter, "the limiter must be published before dispatch begins")
	assert.Equal(t, 2, limiter.Limit(), "pass starts at Concurrency")

	limiter.SetLimit(6)
	close(thr.release)

	require.NoError(t, <-done, "checksum must succeed after scaling up mid-pass")
	assert.Equal(t, 6, limiter.Limit())
}

func TestChecksumProvisionsPoolAtMaxThreads(t *testing.T) {
	// The pool cannot be grown after the table lock is released, so it has to
	// be provisioned at the ceiling up front regardless of whether scaling is
	// enabled. Guards against a future change sizing it at Concurrency.
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 2
	cfg.Autoscale = AutoscaleConfig{Enabled: false, MaxThreads: 5}
	checker := checksumFixture(t, "checksum_pool_size", 1024, cfg)

	single, ok := checker.(*SingleChecker)
	require.True(t, ok)
	assert.Equal(t, 2, single.concurrency)
	assert.Equal(t, 5, single.maxConcurrency)
	require.NoError(t, checker.Run(t.Context()))
}

func TestCheckerMaxThreadsNeverBelowConcurrency(t *testing.T) {
	// A MaxThreads below Concurrency would provision fewer transactions than
	// the starting worker count, starving the pass on trxPool.Get().
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 4
	cfg.Autoscale = AutoscaleConfig{Enabled: true, MaxThreads: 1}
	checker := checksumFixture(t, "checksum_max_floor", 128, cfg)

	single := checker.(*SingleChecker)
	assert.Equal(t, 4, single.concurrency)
	assert.Equal(t, 4, single.maxConcurrency)
	require.NoError(t, checker.Run(t.Context()))
}

func TestCheckerDefaultsToNoopThrottler(t *testing.T) {
	// Callers that never set a throttler must get a working checksum, not a
	// nil-pointer dereference in the dispatch loop.
	checker := checksumFixture(t, "checksum_noop_throttler", 128, NewCheckerDefaultConfig())
	single := checker.(*SingleChecker)
	require.NotNil(t, single.throttler)
	require.NoError(t, checker.Run(t.Context()))
}

func TestCheckerZeroConcurrencyIsUsable(t *testing.T) {
	// A zero here used to produce a transaction pool of zero transactions and a
	// checksum that could not run at all.
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 0
	checker := checksumFixture(t, "checksum_zero_concurrency", 128, cfg)
	single := checker.(*SingleChecker)
	assert.Equal(t, 1, single.concurrency)
	assert.Equal(t, 1, single.maxConcurrency)
	require.NoError(t, checker.Run(t.Context()))
}

func TestStatusSuffixReportsPacing(t *testing.T) {
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 3
	checker := checksumFixture(t, "checksum_status_suffix", 128, cfg)

	// Before the first pass the limiter does not exist yet, so Threads falls
	// back to the configured concurrency rather than reporting zero.
	assert.Equal(t, " checksum-threads=3 checksum-is-throttled=false", StatusSuffix(checker))

	// A throttled checker must say so, which is the whole point of the field.
	checker.(ThrottleAware).SetThrottler(&throttler.Mock{})
	assert.Contains(t, StatusSuffix(checker), "checksum-is-throttled=true")
}

func TestStatusSuffixEmptyForUnpacedChecker(t *testing.T) {
	// Callers append the suffix unconditionally, so a checker that does not
	// report pacing must contribute nothing rather than a partial field.
	assert.Empty(t, StatusSuffix(unpacedChecker{}))
}

// unpacedChecker is a Checker that implements neither Paced nor ThrottleAware.
type unpacedChecker struct{}

func (unpacedChecker) Run(context.Context) error            { return nil }
func (unpacedChecker) GetProgress() status.ChecksumProgress { return status.ChecksumProgress{} }
func (unpacedChecker) StartTime() time.Time                 { return time.Time{} }
func (unpacedChecker) ExecTime() time.Duration              { return 0 }
func (unpacedChecker) DifferencesFound() uint64             { return 0 }
