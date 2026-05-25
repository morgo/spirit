package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-sql-driver/mysql"
)

// MySQL error codes for "you don't have permission" failures. The active-
// threads probe distinguishes these from other errors so the log message
// can suggest a concrete fix (grant SELECT) when it's actually a grants
// problem, and avoid that misleading suggestion otherwise.
const (
	errAccessDenied         = 1045 // ER_ACCESS_DENIED_ERROR
	errDBAccessDenied       = 1044 // ER_DBACCESS_DENIED_ERROR
	errTableAccessDenied    = 1142 // ER_TABLEACCESS_DENIED_ERROR (SELECT on a table denied)
	errSpecificAccessDenied = 1227 // ER_SPECIFIC_ACCESS_DENIED_ERROR
)

// AuroraSetup orchestrates probing for Aurora and assembling the Aurora-
// specific throttlers (commit-latency + active-threads). It exists so both
// the migration runner and the move runner can wire up the same throttlers
// without duplicating the IsAurora / monitor-pool / probe / construct dance.
//
// The throttler package intentionally does not import dbconn — opening the
// monitor pool happens via the caller-supplied OpenMonitor closure, which
// lets the caller own DSN, TLS, and pool sizing.
//
// The two Aurora throttlers are independent signals and have independent
// gates. Disabling one does not disable the other — see Build for details.
type AuroraSetup struct {
	// Source is the caller's main *sql.DB. Used only for the one-shot
	// IsAurora and CanReadActiveThreads probes — these are cheap and run
	// once at setup, so making them share the main pool is fine.
	Source *sql.DB

	// OpenMonitor opens a dedicated *sql.DB used exclusively by the Aurora
	// throttlers for recurring polls. Called at most once, only after
	// IsAurora has returned true and at least one Aurora throttler is
	// going to be constructed, so non-Aurora callers never pay the
	// connect cost. The caller owns closing the returned DB — see
	// AuroraResult.MonitorDB.
	OpenMonitor func() (*sql.DB, error)

	// CommitLatencyThreshold gates the commit-latency throttler. A non-
	// positive value disables that throttler only — the active-threads
	// throttler is independent and is still enabled when Aurora is
	// detected and the privilege probe succeeds.
	CommitLatencyThreshold time.Duration

	Logger *slog.Logger
}

// AuroraResult is the output of AuroraSetup.Build. When Throttlers is empty
// MonitorDB is nil — there's no pool to close. When Throttlers is non-empty
// MonitorDB is non-nil and the caller owns its lifecycle.
type AuroraResult struct {
	Throttlers []Throttler
	MonitorDB  *sql.DB
}

// Build probes the source for Aurora and assembles the Aurora throttlers.
//
// The two Aurora throttlers have independent gates:
//   - Commit-latency is enabled when CommitLatencyThreshold > 0.
//   - Active-threads is enabled when Aurora is detected and the user has
//     SELECT on performance_schema.threads and events_waits_current.
//
// Returns a zero AuroraResult (nil throttlers, nil monitor DB, nil error) in
// any of these benign cases:
//   - IsAurora probe failed (non-Aurora source, or perf_schema not readable —
//     logged at Debug so the non-Aurora common case stays quiet)
//   - IsAurora returned false
//   - Aurora was detected but neither gate produced a throttler (commit-
//     latency disabled by threshold AND active-threads probe failed for
//     any reason — privilege denied or otherwise, both downgraded to Info)
//
// Returns a non-nil error only for setup failures the caller almost
// certainly wants to surface: nil required fields, OpenMonitor failing, or
// throttler construction failing. The active-threads privilege probe is
// best-effort by design — its failure (whatever the cause) disables that
// throttler but never aborts the migration.
func (s AuroraSetup) Build(ctx context.Context) (AuroraResult, error) {
	// Validate required fields up-front. AuroraSetup is an exported struct
	// and these are all dereferenced unconditionally inside Build; a
	// descriptive error beats a nil-pointer panic.
	if s.Source == nil {
		return AuroraResult{}, errors.New("AuroraSetup.Source is required")
	}
	if s.OpenMonitor == nil {
		return AuroraResult{}, errors.New("AuroraSetup.OpenMonitor is required")
	}
	if s.Logger == nil {
		return AuroraResult{}, errors.New("AuroraSetup.Logger is required")
	}

	isAurora, err := IsAurora(ctx, s.Source)
	switch {
	case err != nil:
		// Non-Aurora MySQL with locked-down perf_schema lands here too;
		// keep it at Debug so the common case isn't noisy.
		s.Logger.Debug("Aurora probe failed, skipping Aurora throttlers", "error", err)
		return AuroraResult{}, nil
	case !isAurora:
		return AuroraResult{}, nil
	}

	// Decide independently which throttlers will be built before opening
	// the monitor pool, so we don't open a pool we'd immediately discard.
	enableCommitLatency := s.CommitLatencyThreshold > 0
	enableActiveThreads := true
	if probeErr := CanReadActiveThreads(ctx, s.Source); probeErr != nil {
		// Surface at Info because Aurora is confirmed and the operator
		// likely expected this throttler to be enabled. Distinguish
		// "looks like a grants problem" from other failures so the log
		// message only suggests `GRANT SELECT` when that's plausibly
		// the fix — a transient network error shouldn't send operators
		// down a fruitless permissions investigation.
		if isPrivilegeDeniedError(probeErr) {
			s.Logger.Info("Aurora active-threads throttler disabled: grant SELECT on performance_schema.threads and performance_schema.events_waits_current to enable",
				"error", probeErr)
		} else {
			s.Logger.Info("Aurora active-threads throttler disabled: probe failed",
				"error", probeErr)
		}
		enableActiveThreads = false
	}
	if !enableCommitLatency && !enableActiveThreads {
		return AuroraResult{}, nil
	}

	monitorDB, err := s.OpenMonitor()
	if err != nil {
		return AuroraResult{}, fmt.Errorf("could not open monitor DB for Aurora throttlers: %w", err)
	}

	var throttlers []Throttler

	if enableCommitLatency {
		cl, err := NewCommitLatencyThrottler(monitorDB, s.CommitLatencyThreshold, s.Logger)
		if err != nil {
			_ = monitorDB.Close()
			return AuroraResult{}, fmt.Errorf("could not create commit-latency throttler: %w", err)
		}
		s.Logger.Info("Aurora detected, enabling commit-latency throttler",
			"threshold", s.CommitLatencyThreshold)
		throttlers = append(throttlers, cl)
	}

	if enableActiveThreads {
		at, err := NewActiveThreadsThrottler(monitorDB, s.Logger)
		if err != nil {
			_ = monitorDB.Close()
			return AuroraResult{}, fmt.Errorf("could not create active-threads throttler: %w", err)
		}
		throttlers = append(throttlers, at)
	}

	return AuroraResult{Throttlers: throttlers, MonitorDB: monitorDB}, nil
}

// isPrivilegeDeniedError reports whether err looks like the MySQL server
// refusing the query for permissions reasons (vs. network, syntax, or
// missing-table errors). Used to tailor the active-threads probe-failure
// log message.
func isPrivilegeDeniedError(err error) bool {
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		return false
	}
	switch me.Number {
	case errAccessDenied, errDBAccessDenied, errTableAccessDenied, errSpecificAccessDenied:
		return true
	default:
		return false
	}
}
