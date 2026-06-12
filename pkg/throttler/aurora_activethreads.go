package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// Active-thread throttling — the second Aurora signal, from issue #831.
//
// This is similar to gh-ost and pt-osc using a "threads running" metric to
// detect CPU pressure and back off when the MySQL server is busy.
//
// However, our implementation has one important difference:
// We exclude the threads that are waiting on redo-log flush.
//
// The thought process here is that redo-log waiters are parked anyway, and thus
// not consuming CPU. We can also oversubscribe them because they will group-commit
// to the underlying storage anyway.
//
// In previous experiments I have tried to also count redo-log threads at 50% etc,
// but not including them seems to be the most robust heuristic.
//
// Also different from previous tools use of "threads running" is that we compare
// the threads to the instance vCPU count, which we can deterministically get on
// Aurora via @@innodb_buffer_pool_instances.
//
// This is called *aurora* active threads, but the only thing aurora specific is that
// the vCPU count is retrievable from a variable. I've tried on MySQL to find a consistent
// way to get the vCPU count, but there doesn't seem to be a reliable way. So for now,
// it misses out on this functionality.
const (
	// activeThreadsQuery counts query-running threads on the server and
	// subtracts those parked on redo-log waits. LEFT JOIN handles the case
	// where events_waits_current has no row for a thread (e.g., wait
	// consumer disabled) — that thread is then counted as on-CPU, which is
	// the safe-conservative behavior.
	//
	// The sampling connection excludes itself (CONNECTION_ID()) — without
	// this the monitor's own query always counts as one active thread,
	// which on small instances (vCPUs=2) eats half the headroom and can
	// throttle a migration with no other activity on the server.
	activeThreadsQuery = `SELECT
		GREATEST(COUNT(*) - COALESCE(SUM(CASE WHEN ewc.EVENT_NAME = 'wait/io/redo_log_flush' AND ewc.END_EVENT_ID IS NULL THEN 1 ELSE 0 END), 0), 0) AS active_cpu_threads
	FROM performance_schema.threads pps
	LEFT JOIN performance_schema.events_waits_current ewc ON pps.THREAD_ID = ewc.THREAD_ID
	WHERE pps.PROCESSLIST_ID IS NOT NULL
	  AND pps.PROCESSLIST_ID <> CONNECTION_ID()
	  AND pps.PROCESSLIST_COMMAND = 'Query'`

	// auroraVCPUsQuery reads the vCPU count from @@innodb_buffer_pool_instances,
	// which Aurora pins to the instance vCPU count (see issue #831).
	//
	// Do NOT use @@innodb_purge_threads for this: Aurora sizes it by a
	// stepped formula, not the vCPU count (r7g family: large=1, xlarge=1,
	// 2xlarge=3, 4xlarge=3, 8xlarge=6, 16xlarge=12), so it badly
	// under-reads CPU capacity at every size. On community MySQL
	// innodb_buffer_pool_instances is sized by buffer pool size / CPU
	// hints instead — we gate the throttler on IsAurora, so this query is
	// only ever issued there.
	auroraVCPUsQuery = `SELECT @@innodb_buffer_pool_instances`
)

// DefaultWriteThreads is the fallback apply (write) thread count used when
// auto-sizing is requested (write-threads=0) but the target offers no reliable
// vCPU signal to size from — i.e. a non-Aurora server. It must match the
// `default:"4"` kong tags on the WriteThreads CLI flags (see Migration and
// Move), so that requesting auto-sizing on non-Aurora lands on the same value
// as not requesting it at all.
const DefaultWriteThreads = 4

// auroraVCPUs reads the instance vCPU count from @@innodb_buffer_pool_instances,
// which Aurora pins to the vCPU count (issue #831). It returns an error if the
// value is non-positive. This is only meaningful on Aurora — callers should gate
// on IsAurora first.
func auroraVCPUs(ctx context.Context, db *sql.DB) (int, error) {
	var vCPUs int
	if err := db.QueryRowContext(ctx, auroraVCPUsQuery).Scan(&vCPUs); err != nil {
		return 0, fmt.Errorf("reading @@innodb_buffer_pool_instances for vCPU count: %w", err)
	}
	if vCPUs <= 0 {
		return 0, fmt.Errorf("@@innodb_buffer_pool_instances returned non-positive value %d", vCPUs)
	}
	return vCPUs, nil
}

// ResolveWriteThreads resolves the number of apply (write) threads to use
// against a target. A positive requested value is returned unchanged. Zero
// means "auto-size": on Aurora it resolves to the instance vCPU count
// (@@innodb_buffer_pool_instances); on non-Aurora there is no reliable vCPU
// signal to size from, so it falls back to DefaultWriteThreads (logging that it
// did so).
// A negative value is rejected.
func ResolveWriteThreads(ctx context.Context, db *sql.DB, requested int, logger *slog.Logger) (int, error) {
	if requested < 0 {
		return 0, fmt.Errorf("write threads must be non-negative, got %d", requested)
	}
	if requested > 0 {
		return requested, nil
	}
	// Auto-sizing requires probing the server, so a usable DB is mandatory
	// here (a positive requested value above never reaches this point).
	if db == nil {
		return 0, errors.New("cannot auto-size write threads: no database connection provided (set write-threads to a positive value)")
	}
	isAurora, err := IsAurora(ctx, db)
	if err != nil {
		return 0, err
	}
	if !isAurora {
		// No reliable vCPU signal off Aurora, so we can't honor auto-sizing.
		// Rather than fail, fall back to the default the user would have got
		// had they not asked to auto-size at all.
		logger.Info("write-threads=0 requested auto-sizing, but the target is not Aurora (no reliable vCPU signal); using the default instead",
			"write_threads", DefaultWriteThreads)
		return DefaultWriteThreads, nil
	}
	return auroraVCPUs(ctx, db)
}

// ResolveMaxWriteThreads resolves the upper bound the write-thread autoscaler
// may scale to. When autoscaling is disabled the cap equals start, so the
// thread count cannot move. When enabled the cap is fixed at 2 × start —
// deliberately not configurable for now, to keep the experimental surface
// small. See issue #831.
func ResolveMaxWriteThreads(start int, autoscaleEnabled bool) int {
	if !autoscaleEnabled {
		return start
	}
	return 2 * start
}

// activeThreadsPollInterval mirrors commitLatencyPollInterval — we want fast
// enough to catch sustained CPU pressure without hammering the perf-schema
// join. Var (not const) so tests can shorten it.
var activeThreadsPollInterval = 5 * time.Second

// CanReadActiveThreads probes whether the current user can run the active-
// threads query. Returns nil when the query runs cleanly, a wrapped error
// when it fails (typically: SELECT on performance_schema.threads or
// events_waits_current denied). Called from runner setup so we can skip the
// throttler quietly on accounts without perf-schema read access — IsAurora
// already proved global_status is readable, but perf-schema table grants are
// independent.
func CanReadActiveThreads(ctx context.Context, db *sql.DB) error {
	var n int64
	if err := db.QueryRowContext(ctx, activeThreadsQuery).Scan(&n); err != nil {
		return fmt.Errorf("probing active-threads query (requires SELECT on performance_schema.threads, events_waits_current): %w", err)
	}
	return nil
}

// ActiveThreads throttles when the count of query-running threads (excluding
// those waiting on the redo log) exceeds the instance vCPU count. Aurora-
// only — depends on @@innodb_buffer_pool_instances matching vCPUs.
type ActiveThreads struct {
	db     *sql.DB
	logger *slog.Logger

	// vCPUs is the throttle threshold, captured once at Open and treated as
	// immutable for the migration's lifetime. Aurora instance type changes
	// require a restart so it cannot move under us.
	vCPUs int64

	isThrottled atomic.Bool
	isClosed    atomic.Bool

	// lastActiveThreads holds the most recent observation for logging.
	lastActiveThreads atomic.Int64

	// stale guards Utilization() against the cached value freezing when
	// sampling fails persistently. See stale.go.
	stale staleGuard
}

var _ GradualThrottler = (*ActiveThreads)(nil)

// NewActiveThreadsThrottler returns a Throttler that polls performance_schema
// to count on-CPU query threads and throttles when that count exceeds the
// instance vCPU count.
func NewActiveThreadsThrottler(db *sql.DB, logger *slog.Logger) (*ActiveThreads, error) {
	if db == nil {
		return nil, errors.New("active-threads throttler requires a non-nil DB")
	}
	return &ActiveThreads{
		db:     db,
		logger: logger,
	}, nil
}

func (a *ActiveThreads) Open(ctx context.Context) error {
	vCPUs, err := auroraVCPUs(ctx, a.db)
	if err != nil {
		return err
	}
	a.vCPUs = int64(vCPUs)
	a.logger.Info("Aurora active-threads throttler enabled", "vCPUs", a.vCPUs)
	if err := a.UpdateLag(ctx); err != nil {
		return err
	}
	go a.run(ctx)
	return nil
}

func (a *ActiveThreads) run(ctx context.Context) {
	ticker := time.NewTicker(activeThreadsPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if a.isClosed.Load() {
				return
			}
			if err := a.UpdateLag(ctx); err != nil {
				a.logger.Error("error sampling Aurora active threads", "error", err)
			}
		}
	}
}

func (a *ActiveThreads) Close() error {
	a.isClosed.Store(true)
	return nil
}

func (a *ActiveThreads) IsThrottled() bool {
	return a.isThrottled.Load()
}

// Utilization reports the most recent active-CPU-thread count as a fraction of
// the instance vCPU count. 1.0 means active threads equal vCPUs (the point at
// which one more thread trips IsThrottled). Returns 0 if vCPUs is not yet known.
//
// When sampling has failed for longer than staleSignalThreshold the cached
// count is no longer trustworthy, so this reports StaleUtilizationHold
// instead — parking the autoscaler in its dead band rather than letting a
// frozen value drive scaling. IsThrottled/BlockWait are unaffected.
func (a *ActiveThreads) Utilization() float64 {
	if stale, entering := a.stale.check(staleSignalThreshold); stale {
		if entering {
			a.logger.Warn("active-threads signal is stale; holding autoscaler utilization steady until sampling recovers",
				"last_successful_sample_age", a.stale.age(),
				"hold_utilization", StaleUtilizationHold)
		}
		return StaleUtilizationHold
	}
	if a.vCPUs <= 0 {
		return 0
	}
	return float64(a.lastActiveThreads.Load()) / float64(a.vCPUs)
}

// BlockWait blocks until active CPU threads fall to or below vCPUs, or up to
// 60s. Matches the commit-latency throttler's loop shape so the multi-
// throttler waits uniformly across signals.
func (a *ActiveThreads) BlockWait(ctx context.Context) {
	timer := time.NewTimer(blockWaitInterval)
	defer timer.Stop()

	for range 60 {
		if !a.isThrottled.Load() {
			return
		}
		timer.Reset(blockWaitInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
	a.logger.Warn("active-threads throttler timed out",
		"active_threads", a.lastActiveThreads.Load(),
		"vCPUs", a.vCPUs)
}

// UpdateLag samples active CPU threads and updates throttled state.
func (a *ActiveThreads) UpdateLag(ctx context.Context) error {
	var active int64
	if err := a.db.QueryRowContext(ctx, activeThreadsQuery).Scan(&active); err != nil {
		return fmt.Errorf("sampling Aurora active threads: %w", err)
	}
	a.applySample(active)
	return nil
}

// applySample updates state from a single observation. Split out so tests can
// drive the calculation without a real Aurora.
//
// Unlike the commit-latency throttler there is no counter-reset case to
// handle here: the active-thread count is an instantaneous gauge (the query
// clamps at >= 0), not a delta of cumulative counters, so a failover cannot
// produce a bogus value — only an error, which the staleness guard covers.
func (a *ActiveThreads) applySample(active int64) {
	if a.stale.markFresh() {
		a.logger.Info("active-threads sampling recovered; resuming live utilization signal")
	}
	a.lastActiveThreads.Store(active)
	throttled := active > a.vCPUs
	prev := a.isThrottled.Swap(throttled)
	if throttled && !prev {
		a.logger.Warn("active CPU threads exceed vCPUs, throttling",
			"active_threads", active,
			"vCPUs", a.vCPUs)
	}
}
