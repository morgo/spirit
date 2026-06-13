package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// AuroraSetup orchestrates probing for Aurora and assembling the Aurora-
// specific throttlers (commit-latency + threads-running). It exists so both
// the migration runner and the move runner can wire up the same throttlers
// without duplicating the IsAurora / monitor-pool / construct dance.
//
// The throttler package intentionally does not import dbconn — opening the
// monitor pool happens via the caller-supplied OpenMonitor closure, which
// lets the caller own DSN, TLS, and pool sizing.
//
// The two Aurora throttlers are independent signals and have independent
// gates. Disabling one does not disable the other — see Build for details.
type AuroraSetup struct {
	// Source is the caller's main *sql.DB. Used only for the one-shot
	// IsAurora probe — cheap and run once at setup, so sharing the main pool
	// is fine.
	Source *sql.DB

	// OpenMonitor opens a dedicated *sql.DB used exclusively by the Aurora
	// throttlers for recurring polls. Called at most once, only after
	// IsAurora has returned true and at least one Aurora throttler is
	// going to be constructed, so non-Aurora callers never pay the
	// connect cost. The caller owns closing the returned DB — see
	// AuroraResult.MonitorDB.
	OpenMonitor func() (*sql.DB, error)

	// CommitLatencyThreshold gates the commit-latency throttler. A non-
	// positive value disables that throttler only — the threads-running
	// throttler is independent and is always enabled once Aurora is detected
	// (it reads only global_status, which the IsAurora probe already proved
	// readable).
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
// On a confirmed Aurora source the threads-running throttler is always built;
// the commit-latency throttler is built only when CommitLatencyThreshold > 0.
// Both read only performance_schema.global_status, which the IsAurora probe
// already exercised, so there is no separate privilege probe.
//
// Returns a zero AuroraResult (nil throttlers, nil monitor DB, nil error) when
// the source is not Aurora — either the IsAurora probe failed (non-Aurora
// source, or perf_schema not readable; logged at Debug so the common case
// stays quiet) or it returned false. In those cases the monitor pool is never
// opened.
//
// Returns a non-nil error only for setup failures the caller almost
// certainly wants to surface: nil required fields, OpenMonitor failing, or
// throttler construction failing.
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

	// The threads-running throttler is always built on Aurora, so at least one
	// throttler is always produced here and opening the monitor pool is never
	// wasted. Commit-latency is the only gated one.
	enableCommitLatency := s.CommitLatencyThreshold > 0

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

	tr, err := NewThreadsRunningThrottler(monitorDB, s.Logger)
	if err != nil {
		_ = monitorDB.Close()
		return AuroraResult{}, fmt.Errorf("could not create threads-running throttler: %w", err)
	}
	throttlers = append(throttlers, tr)

	return AuroraResult{Throttlers: throttlers, MonitorDB: monitorDB}, nil
}
