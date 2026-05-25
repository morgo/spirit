package throttler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
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
//     latency disabled by threshold AND active-threads denied by privilege)
//
// Returns a non-nil error only when something the caller almost certainly
// wants to surface goes wrong: OpenMonitor itself fails, or constructing a
// throttler fails for a reason other than "missing privileges" (which is
// expected and downgraded to Info — see CanReadActiveThreads handling).
//
// On a successful Aurora build, the monitor pool is opened and the enabled
// throttlers are returned. The caller composes them via NewMultiThrottler
// with whatever other throttlers it has and is responsible for calling
// Close on each throttler AND Close on MonitorDB at shutdown.
func (s AuroraSetup) Build(ctx context.Context) (AuroraResult, error) {
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
	if ok, probeErr := CanReadActiveThreads(ctx, s.Source); probeErr != nil {
		// Surface at Info because Aurora is confirmed and the operator
		// likely expected this throttler to be enabled.
		s.Logger.Info("Aurora active-threads throttler disabled: grant SELECT on performance_schema.threads and performance_schema.events_waits_current to enable",
			"error", probeErr)
		enableActiveThreads = false
	} else if !ok {
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
