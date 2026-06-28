// Package sentinel implements the "sentinel table" cutover gate shared by the
// migration and move runners.
//
// When a migration/move is run with deferred cutover, spirit creates a small
// marker table (the sentinel) and then blocks the final cutover until an
// operator drops it. While blocked, a continuous checksum re-verifies the
// copied data so a long human-paced wait does not let the shadow copy drift.
//
// Both runners previously carried near-identical copies of this logic; the
// only differences were which connection/schema the sentinel lives on and the
// two runner-specific callbacks (RunChecksum, InvalidateWatermark), which are
// injected into Wait rather than reimplemented here.
package sentinel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/dbconn"
)

// TableName is the fixed name of the sentinel table. It is intentionally a
// constant (not derived from the migrated table) so an operator always knows
// which table to drop to release a deferred cutover.
const TableName = "_spirit_sentinel"

// Create creates the sentinel table in schemaName on db if it does not already
// exist. Creation must be idempotent: the name is a constant, a resumed
// migration recreates it, and TestSentinelCreateNeverObservedAbsent relies on
// CREATE IF NOT EXISTS so a concurrent existence probe never sees it missing.
func Create(ctx context.Context, db *sql.DB, schemaName string) error {
	return dbconn.Exec(ctx, db, "CREATE TABLE IF NOT EXISTS %n.%n (id int NOT NULL PRIMARY KEY)", schemaName, TableName)
}

// Exists reports whether the sentinel table is present in schemaName on db.
func Exists(ctx context.Context, db *sql.DB, schemaName string) (bool, error) {
	const q = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var count int
	if err := db.QueryRowContext(ctx, q, schemaName, TableName).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// WaitConfig holds the dependencies of Wait. Exists, RunChecksum,
// InvalidateWatermark and Logger are required; WaitLimit and CheckInterval
// must be positive.
type WaitConfig struct {
	// Exists probes whether the sentinel table still exists. It is called once
	// up front (an absent sentinel means "proceed immediately") and then once
	// per CheckInterval.
	Exists func(ctx context.Context) (bool, error)

	// RunChecksum runs the continuous checksum for the duration of the wait.
	// It is spawned in its own goroutine with a child context that Wait
	// cancels on return, and is expected to filter a benign cancellation of
	// that context to a nil error; any non-nil return is treated as a real
	// verification failure and aborts the wait.
	RunChecksum func(ctx context.Context) error

	// InvalidateWatermark is invoked exactly once, after the continuous
	// checksum goroutine has fully stopped, to blank any persisted checksum
	// watermark if the checksum repaired a difference (so a resume re-verifies
	// from the start of the checksum phase). It runs even when the parent
	// context was cancelled.
	InvalidateWatermark func(ctx context.Context) error

	Logger *slog.Logger

	// WaitLimit bounds the total time spent waiting for the sentinel to be
	// dropped; CheckInterval is the existence-probe period.
	WaitLimit     time.Duration
	CheckInterval time.Duration

	// TableName is used only in log messages (callers pass the package
	// TableName, kept overridable so log output names the right table).
	TableName string
}

// Wait blocks until the sentinel table is dropped (proceed with cutover),
// WaitLimit elapses (error), or the continuous checksum exits on its own
// (error). It returns nil only when it is safe to proceed with cutover.
//
// For the lifetime of the wait it runs RunChecksum in the background. On
// return — by any path, including parent-context cancellation — it stops that
// goroutine and then calls InvalidateWatermark: if the continuous checksum
// repaired a chunk, the run is about to abort, and the persisted
// checksum_watermark must be blanked so a resume re-verifies rather than
// trusting a watermark that was recorded just before the difference was found.
func Wait(ctx context.Context, cfg WaitConfig) (retErr error) {
	if sentinelExists, err := cfg.Exists(ctx); err != nil {
		return err
	} else if !sentinelExists {
		// Sentinel table does not exist, we can proceed with cutover.
		return nil
	}

	cfg.Logger.Warn("cutover deferred while sentinel table exists; will wait",
		"sentinel-table", cfg.TableName,
		"wait-limit", cfg.WaitLimit.String(),
	)

	// Spawn the continuous checksum. It uses its own checker + chunker and is
	// not wired into the checkpoint — so a crash during sentinel wait does
	// not add mandatory checksum time on resume.
	continuousCtx, cancelContinuous := context.WithCancel(ctx)
	continuousDone := make(chan struct{})
	var continuousErr error
	go func() {
		defer close(continuousDone)
		continuousErr = cfg.RunChecksum(continuousCtx)
	}()

	// RunChecksum already filters harmless sentinel cancellations to nil, so
	// any non-nil continuousErr is one it intentionally chose to propagate —
	// surface it as retErr whenever the parent ctx itself has not been
	// cancelled (parent cancellation is its own error path).
	defer func() {
		cancelContinuous()
		<-continuousDone
		if retErr == nil && continuousErr != nil && ctx.Err() == nil {
			retErr = fmt.Errorf("continuous checksum failed: %w", continuousErr)
		}
		// If the continuous checker repaired any chunk, this run is about to
		// abort. The periodic dumper stops persisting a checksum_watermark the
		// instant the difference is recorded, but a dump whose conditions were
		// read just before that instant can still land a stale watermark row
		// afterwards. Rewrite the persisted rows here — strictly after the
		// continuous goroutine has exited (see <-continuousDone above) — so the
		// on-disk state after the abort forces full checksum re-verification on
		// resume. WithoutCancel: this cleanup must run even when the parent ctx
		// was already cancelled.
		if err := cfg.InvalidateWatermark(context.WithoutCancel(ctx)); err != nil {
			cfg.Logger.Error("failed to clear persisted checksum watermark after continuous checksum divergence", "error", err)
			// Join rather than suppress, even when the continuous-checksum abort
			// already set retErr: a failed invalidation means a stale
			// checksum_watermark may remain on disk, letting a resume skip the
			// full re-verification this abort exists to force. The operator must
			// see both failures.
			retErr = errors.Join(retErr, fmt.Errorf("failed to clear persisted checksum watermark: %w", err))
		}
	}()

	timer := time.NewTimer(cfg.WaitLimit)
	defer timer.Stop() // Ensure timer is always stopped to prevent goroutine leak

	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			sentinelExists, err := cfg.Exists(ctx)
			if err != nil {
				return err
			}
			if !sentinelExists {
				// Sentinel table has been dropped, we can proceed with cutover.
				// The defer above still observes continuousErr — if a continuous
				// pass was mid-recopy and surfaces a real drift error, that
				// overrides this nil return.
				cfg.Logger.Info("sentinel table dropped", "time", t)
				return nil
			}
		case <-timer.C:
			return errors.New("timed out waiting for sentinel table to be dropped")
		case <-continuousDone:
			// Continuous goroutine exited before the sentinel was dropped.
			// If our parent ctx is cancelled, the goroutine just propagated
			// that cancellation — surface the parent's error directly.
			if err := ctx.Err(); err != nil {
				return err
			}
			// A non-nil error means continuous detected a real failure. Return
			// it as a regular failure so the caller can re-run and resume from
			// the existing checkpoint (the caller decides not to invalidate it).
			if continuousErr != nil {
				return fmt.Errorf("continuous checksum failed: %w", continuousErr)
			}
			return errors.New("continuous checksum exited unexpectedly")
		}
	}
}
