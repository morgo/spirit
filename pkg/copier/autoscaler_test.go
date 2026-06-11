package copier

import (
	"context"
	"io"
	"log/slog"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// fakeScaler records SetWriteWorkers calls and reports them back.
type fakeScaler struct {
	n int
}

func (f *fakeScaler) SetWriteWorkers(n int) { f.n = n }

// utilThrottler is a GradualThrottler stub whose Utilization is scripted by
// the test. Only Utilization is exercised by the autoscaler; the rest satisfy
// the interface. The value is stored atomically so the integration test can
// move it while the autoscaler goroutine reads it concurrently (-race).
type utilThrottler struct{ utilBits atomic.Uint64 }

var _ throttler.GradualThrottler = &utilThrottler{}

func (u *utilThrottler) setUtil(v float64)               { u.utilBits.Store(math.Float64bits(v)) }
func (u *utilThrottler) Open(context.Context) error      { return nil }
func (u *utilThrottler) Close() error                    { return nil }
func (u *utilThrottler) IsThrottled() bool               { return u.Utilization() >= 1.0 }
func (u *utilThrottler) Utilization() float64            { return math.Float64frombits(u.utilBits.Load()) }
func (u *utilThrottler) BlockWait(context.Context)       {}
func (u *utilThrottler) UpdateLag(context.Context) error { return nil }

// fakeScalingApplier satisfies applier.Applier (embedded, never called) plus
// the writeScaler capability, mimicking the SingleTargetApplier for gate tests.
type fakeScalingApplier struct {
	applier.Applier
	fakeScaler
}

func newTestScaler(start, max int) (*autoScaler, *fakeScaler, *utilThrottler) {
	fs := &fakeScaler{n: start}
	ut := &utilThrottler{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	as := newAutoScaler(ut, fs, start, max, logger, &metrics.NoopSink{})
	return as, fs, ut
}

func TestAutoScaler_IncreasesBelowLowWatermarkAfterCooldown(t *testing.T) {
	as, fs, ut := newTestScaler(2, 8)
	ut.setUtil(0.2) // well below low watermark

	// First sub-low tick increases immediately (cooldown starts at 0).
	as.tick(t.Context())
	require.Equal(t, 3, as.current)
	require.Equal(t, 3, fs.n)

	// Cooldown is now in effect: the next ticks hold despite continued headroom.
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "should hold during cooldown tick 1")
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "should hold during cooldown tick 2")

	// Cooldown elapsed → increase again.
	as.tick(t.Context())
	require.Equal(t, 4, as.current)
}

func TestAutoScaler_DecreasesImmediatelyAtHighWatermark(t *testing.T) {
	as, fs, ut := newTestScaler(8, 16)
	ut.setUtil(0.95) // at/above high watermark

	as.tick(t.Context())
	require.Equal(t, 4, as.current, "8 should halve to 4 immediately")
	require.Equal(t, 4, fs.n)

	// Consecutive halvings are cooldown-spaced: the signal updates on the same
	// cadence we tick on, so reacting every tick would halve repeatedly on one
	// stale window. Sustained overload halves again only after the cooldown.
	as.tick(t.Context())
	require.Equal(t, 4, as.current, "should hold during cooldown tick 1")
	as.tick(t.Context())
	require.Equal(t, 4, as.current, "should hold during cooldown tick 2")

	as.tick(t.Context())
	require.Equal(t, 2, as.current, "cooldown elapsed, halve again")
}

func TestAutoScaler_DecreaseNotBlockedByIncreaseCooldown(t *testing.T) {
	// An increase's cooldown must not delay a backoff: if the increase tipped
	// the server over the high watermark, the very next tick halves.
	as, _, ut := newTestScaler(4, 8)
	ut.setUtil(0.2)
	as.tick(t.Context())
	require.Equal(t, 5, as.current, "increase under low watermark")

	ut.setUtil(0.95)
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "halve immediately despite increase cooldown: ceil(5/2)=3")
}

func TestAutoScaler_HoldsInDeadBand(t *testing.T) {
	as, _, ut := newTestScaler(4, 16)
	ut.setUtil(0.7) // between low (0.5) and high (0.9)

	for range 5 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current, "dead-band should hold steady")
}

func TestAutoScaler_ClampsAtMax(t *testing.T) {
	as, _, ut := newTestScaler(3, 4)
	ut.setUtil(0.0) // maximum headroom, always wants to increase

	// Drive many ticks; should climb to the cap and stop.
	for range 30 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current)
}

func TestAutoScaler_ClampsAtMinOne(t *testing.T) {
	as, _, ut := newTestScaler(2, 8)
	ut.setUtil(1.5) // way over

	for range 10 {
		as.tick(t.Context())
	}
	require.Equal(t, 1, as.current, "must never drop below 1")
}

func TestAutoScaler_MaxFlooredAtStart(t *testing.T) {
	// A max below the start value is nonsensical; it must be floored at start so
	// we never scale below where we began except via the >high backoff path.
	as, _, _ := newTestScaler(6, 2)
	require.Equal(t, 6, as.max)
}

// TestAutoScaler_DeadBandBoundaries pins the documented [low, high) edge
// semantics of the dead band: tick() uses `util < low` for increases and
// `util >= high` for decreases, so exactly-low must HOLD and exactly-high
// must HALVE. The epsilon cases guard against either comparison being
// accidentally flipped to <= / >.
func TestAutoScaler_DeadBandBoundaries(t *testing.T) {
	const eps = 1e-9
	tests := []struct {
		name string
		util float64
		want int // expected current after one tick, starting from 4
	}{
		{"just below low increases", acLowWatermark - eps, 5},
		{"exactly low holds", acLowWatermark, 4},
		{"just below high holds", acHighWatermark - eps, 4},
		{"exactly high halves", acHighWatermark, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			as, _, ut := newTestScaler(4, 16)
			ut.setUtil(tc.util)
			as.tick(t.Context())
			require.Equal(t, tc.want, as.current)
		})
	}
}

// TestAutoScaler_StaleHoldValueParksInDeadBand pins the cross-package
// invariant the staleness guard depends on: the utilization a stale throttler
// reports (throttler.StaleUtilizationHold) must sit inside this controller's
// dead band [low, high), so a dead signal freezes the thread count rather
// than ramping it to the cap or shrinking it to 1. If either side moves and
// breaks the relationship, this fails loudly.
func TestAutoScaler_StaleHoldValueParksInDeadBand(t *testing.T) {
	require.GreaterOrEqual(t, throttler.StaleUtilizationHold, acLowWatermark,
		"stale hold below the low watermark would scale up blind on a dead signal")
	require.Less(t, throttler.StaleUtilizationHold, acHighWatermark,
		"stale hold at/above the high watermark would halve on a dead signal")

	as, _, ut := newTestScaler(4, 16)
	ut.setUtil(throttler.StaleUtilizationHold)
	for range 5 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current, "stale hold utilization must freeze the thread count")
}

func TestCeilDiv(t *testing.T) {
	require.Equal(t, 1, ceilDiv(1, 2))
	require.Equal(t, 1, ceilDiv(2, 2))
	require.Equal(t, 2, ceilDiv(3, 2))
	require.Equal(t, 2, ceilDiv(4, 2))
	require.Equal(t, 3, ceilDiv(5, 2))
}

// TestAutoscalerIfEnabled_Gating covers the three conditions that must all
// hold for the autoscaler to engage: the flag is on, the applier supports
// dynamic write threads, and the throttler provides a continuous load signal
// (GradualThrottler). Missing any one of them means a fixed pool.
func TestAutoscalerIfEnabled_Gating(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gradual := &utilThrottler{}
	scalingApplier := &fakeScalingApplier{}

	// Disabled (the default): no autoscaler.
	c := &buffered{logger: logger, throttler: gradual, applier: scalingApplier}
	require.Nil(t, c.autoscalerIfEnabled())

	// Enabled + scaling applier + gradual throttler: engages with the
	// configured bounds.
	c.autoscale = AutoscaleConfig{Enabled: true, StartThreads: 2, MaxThreads: 4}
	as := c.autoscalerIfEnabled()
	require.NotNil(t, as)
	require.Equal(t, 2, as.current)
	require.Equal(t, 4, as.max)

	// Binary-only throttler (Noop here; replica lag and Mock behave the same):
	// no continuous signal to control on, so the pool stays fixed.
	c.throttler = &throttler.Noop{}
	require.Nil(t, c.autoscalerIfEnabled())

	// Applier without the dynamic-scaling capability: stays fixed.
	c.throttler = gradual
	c.applier = nil
	require.Nil(t, c.autoscalerIfEnabled())
}

// gatedUtilThrottler extends utilThrottler with a BlockWait that parks the
// copier's read workers until the test closes the gate. That keeps the copy
// alive (without burning rows) while the autoscaler — which ticks
// independently of chunk flow — makes its scaling decisions, so the test can
// observe them deterministically before letting the copy finish.
type gatedUtilThrottler struct {
	utilThrottler
	gate chan struct{}
}

func (g *gatedUtilThrottler) BlockWait(ctx context.Context) {
	select {
	case <-g.gate:
	case <-ctx.Done():
	}
}

// TestAutoScalerIntegrationEngaged runs the autoscaler for real: a buffered
// copy of a real table through a real SingleTargetApplier, with the
// autoscaler goroutine (run/tick on a ticker) driving SetWriteWorkers from a
// test-controlled utilization signal. It asserts the live worker pool grows
// under low utilization, halves at the high watermark, and that the copy then
// completes correctly. goleak in TestMain verifies nothing leaks.
func TestAutoScalerIntegrationEngaged(t *testing.T) {
	// Shorten the control-loop tick (production default 5s) so scaling
	// happens in milliseconds. Copier tests do not run in parallel, so
	// mutating the package var with a restore is safe.
	prevTick := acTick
	acTick = 20 * time.Millisecond
	t.Cleanup(func() { acTick = prevTick })

	testutils.RunSQL(t, "DROP TABLE IF EXISTS autoscale_src, autoscale_dst")
	testutils.RunSQL(t, "CREATE TABLE autoscale_src (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64) NOT NULL)")
	testutils.RunSQL(t, "CREATE TABLE autoscale_dst (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64) NOT NULL)")
	// Seed a few thousand small rows by doubling: 2^12 = 4096.
	testutils.RunSQL(t, "INSERT INTO autoscale_src (val) VALUES ('seed')")
	for range 12 {
		testutils.RunSQL(t, "INSERT INTO autoscale_src (val) SELECT val FROM autoscale_src")
	}

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Resolve the schema from the DSN rather than hardcoding it, so the test
	// works against any test database.
	dsnCfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	t1 := table.NewTableInfo(db, dsnCfg.DBName, "autoscale_src")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, dsnCfg.DBName, "autoscale_dst")
	require.NoError(t, t2.SetInfo(t.Context()))

	const start, maxThreads = 2, 4 // mirrors ResolveMaxWriteThreads: cap = 2x start

	applierCfg := applier.NewApplierDefaultConfig()
	applierCfg.Threads = start
	app, err := applier.NewSingleTargetApplier(applier.Target{DB: db, KeyRange: "0"}, applierCfg)
	require.NoError(t, err)

	gated := &gatedUtilThrottler{gate: make(chan struct{})}
	gated.setUtil(0.2) // below the low watermark: the controller wants to grow

	cfg := NewCopierDefaultConfig()
	cfg.Applier = app
	cfg.Throttler = gated
	cfg.Concurrency = 2
	cfg.Autoscale = AutoscaleConfig{Enabled: true, StartThreads: start, MaxThreads: maxThreads}

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: cfg.TargetChunkTime, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(db, chunker, cfg)
	require.NoError(t, err)

	copyDone := make(chan error, 1)
	go func() { copyDone <- copier.Run(t.Context()) }()

	// Phase 1: sustained low utilization → additive +1 per cooldown until the
	// live worker pool reaches the cap. ActiveWriteWorkers observes the real
	// goroutine pool, so this proves run() drove SetWriteWorkers on the
	// applier (not just controller-internal state).
	require.Eventually(t, func() bool { return app.ActiveWriteWorkers() == maxThreads },
		10*time.Second, 5*time.Millisecond,
		"autoscaler should grow the live worker pool to the cap under low utilization")

	// Phase 2: utilization at/above the high watermark → multiplicative
	// backoff. Parked workers exit asynchronously, so wait for convergence.
	// (Sustained overload may halve again, cooldown-spaced, hence <=.)
	gated.setUtil(0.95)
	require.Eventually(t, func() bool { return app.ActiveWriteWorkers() <= maxThreads/2 },
		10*time.Second, 5*time.Millisecond,
		"autoscaler should halve the live worker pool at the high watermark")

	// Park the signal in the dead band and release the gate: the copy now
	// proceeds to completion with the scaled-down pool.
	gated.setUtil(0.7)
	close(gated.gate)
	select {
	case err := <-copyDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Minute):
		t.Fatal("copy did not complete after releasing the throttler gate")
	}

	// The copy must be complete and correct despite the mid-copy rescaling.
	var srcRows, dstRows int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM autoscale_src").Scan(&srcRows))
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM autoscale_dst").Scan(&dstRows))
	require.Equal(t, 4096, srcRows)
	require.Equal(t, srcRows, dstRows, "destination row count must match source after autoscaled copy")

	var checksumSrc, checksumDst string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT BIT_XOR(CRC32(CONCAT(id, val))) FROM autoscale_src").Scan(&checksumSrc))
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT BIT_XOR(CRC32(CONCAT(id, val))) FROM autoscale_dst").Scan(&checksumDst))
	require.Equal(t, checksumSrc, checksumDst, "checksum mismatch between source and destination")
	testutils.RunSQL(t, "DROP TABLE IF EXISTS autoscale_src, autoscale_dst")
}
