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
// Aurora via @@innodb_purge_threads.
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
	activeThreadsQuery = `SELECT
		GREATEST(COUNT(*) - COALESCE(SUM(CASE WHEN ewc.EVENT_NAME = 'wait/io/redo_log_flush' AND ewc.END_EVENT_ID IS NULL THEN 1 ELSE 0 END), 0), 0) AS active_cpu_threads
	FROM performance_schema.threads pps
	LEFT JOIN performance_schema.events_waits_current ewc ON pps.THREAD_ID = ewc.THREAD_ID
	WHERE pps.PROCESSLIST_ID IS NOT NULL
	  AND pps.PROCESSLIST_COMMAND = 'Query'`

	// auroraVCPUsQuery reads the vCPU count from @@innodb_purge_threads.
	// Aurora pins this to the instance vCPU count (see issue #831). On RDS
	// MySQL the var also exists but its value tracks Aurora-style sizing
	// only on Aurora — we gate the throttler on IsAurora, so this query is
	// only ever issued there.
	auroraVCPUsQuery = `SELECT @@innodb_purge_threads`
)

// auroraVCPUs reads the instance vCPU count from @@innodb_purge_threads, which
// Aurora pins to the vCPU count (issue #831). It returns an error if the value
// is non-positive. This is only meaningful on Aurora — callers should gate on
// IsAurora first.
func auroraVCPUs(ctx context.Context, db *sql.DB) (int, error) {
	var vCPUs int
	if err := db.QueryRowContext(ctx, auroraVCPUsQuery).Scan(&vCPUs); err != nil {
		return 0, fmt.Errorf("reading @@innodb_purge_threads for vCPU count: %w", err)
	}
	if vCPUs <= 0 {
		return 0, fmt.Errorf("@@innodb_purge_threads returned non-positive value %d", vCPUs)
	}
	return vCPUs, nil
}

// ResolveWriteThreads resolves the number of apply (write) threads to use
// against a target. A positive requested value is returned unchanged. Zero
// means "auto-size": on Aurora it resolves to the instance vCPU count
// (@@innodb_purge_threads); on non-Aurora there is no reliable vCPU signal to
// size from, so it is an error — callers must pass an explicit positive value.
// A negative value is rejected.
func ResolveWriteThreads(ctx context.Context, db *sql.DB, requested int) (int, error) {
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
		return 0, errors.New("write threads cannot be auto-sized on a non-Aurora target: set write-threads to a positive value")
	}
	return auroraVCPUs(ctx, db)
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
// only — depends on @@innodb_purge_threads matching vCPUs.
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
}

var _ Throttler = (*ActiveThreads)(nil)

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
func (a *ActiveThreads) applySample(active int64) {
	a.lastActiveThreads.Store(active)
	throttled := active > a.vCPUs
	prev := a.isThrottled.Swap(throttled)
	if throttled && !prev {
		a.logger.Warn("active CPU threads exceed vCPUs, throttling",
			"active_threads", active,
			"vCPUs", a.vCPUs)
	}
}
