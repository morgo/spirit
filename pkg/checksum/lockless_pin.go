package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/table"
)

// A hot range is one whose source keeps changing under the reader. The snapshot
// fallback (lockless_snapshot.go) freezes the source images for such a range and
// waits for the target to show them, which converges for a range that is merely
// busy — but not for a row that is written continuously. There, the frozen image
// is stale before the first poll, every attempt observes a different source, and
// the range is deferred to the next pass with no verdict at all. It is deferred
// again on the next pass for the same reason. A genuinely diverged hot row and a
// merely busy one are indistinguishable, forever.
//
// Settling is the terminal step for exactly that case. Rather than waiting for
// the source to stop moving, it holds it still — for one bounded moment, for the
// handful of rows still outstanding:
//
//  1. Take a shared lock on the outstanding source rows (SELECT ... FOR SHARE in
//     its own transaction) and read their images under it. No transaction can
//     now UPDATE or DELETE those rows.
//  2. BlockWait the change feed. It samples the source's live binlog position and
//     waits for the reader to reach it, so every write that ever touched these
//     rows has been consumed — and while the lock is held no further write can
//     commit to add one.
//  3. Flush the feed, applying that whole history to the target.
//  4. Read the target rows.
//
// At step 4 the target cannot move: there is no unapplied event for these rows
// and no new one can be produced. So the target must equal the images read at
// step 1, and a mismatch is a real inconsistency rather than apply lag or a race.
//
// This is not the table lock the snapshot checker takes. It is a row lock over
// at most hotSplitTargetRows rows, held for settleBudget, and the whole
// escalation is reached only after a range has already failed MaxHotAttempts
// observations. Anything that would make it expensive — a lock we cannot get
// quickly, a feed that cannot drain in the budget — abandons the attempt and
// falls back to deferring the range, which is what would have happened anyway.
const (
	// settleBudget bounds the whole pinned window: lock acquisition, the feed
	// drain, and both reads. It is the time application writes to these rows can
	// be blocked by the checksum, so it is chosen to be a stall an operator
	// would not notice rather than one that buys the best chance of success. A
	// budget overrun is not an error; it defers, as before.
	settleBudget = 5 * time.Second

	// settleLockWaitSeconds is the lock wait the pinning session asks for. A hot
	// row is by definition one other transactions are holding, so failing fast
	// and deferring is right: this path must never be the thing that queues up
	// behind application writes.
	settleLockWaitSeconds = 2
)

// pinnedVerdict is the outcome of trying to settle a hot range.
type pinnedVerdict int

const (
	// pinnedUnavailable means no verdict was reached: the escalation could not
	// be attempted, could not complete inside its budget, or covered an
	// obligation it cannot honestly pin. The caller defers the range exactly as
	// it did before this path existed.
	pinnedUnavailable pinnedVerdict = iota

	// pinnedClean means every outstanding obligation was observed satisfied with
	// the source held still and the feed fully drained. The range is verified.
	pinnedClean

	// pinnedDiverged means at least one was not. Nothing could have been in
	// flight, so this is a real inconsistency.
	pinnedDiverged
)

func (v pinnedVerdict) String() string {
	switch v {
	case pinnedUnavailable:
		return "unavailable"
	case pinnedClean:
		return "clean"
	case pinnedDiverged:
		return "diverged"
	default:
		return fmt.Sprintf("pinnedVerdict(%d)", int(v))
	}
}

// pinnedRows is what a settle attempt holds: the source images read under the
// lock, plus the release that drops it. release is never nil when err is nil.
type pinnedRows struct {
	rows    map[string]hotSnapshotRow
	release func()
}

// settleHotSnapshot is the escalation described at the top of this file. It
// returns pinnedUnavailable rather than an error for every condition that only
// means "not this time" — a missing feed, a lock we could not take, a drain that
// outran the budget — because the caller's fallback for all of them is the same
// deferral it would have done anyway. An error is reserved for a read that
// failed in a way worth surfacing.
func (c *LocklessChecker) settleHotSnapshot(ctx context.Context, s *hotSnapshot) (pinnedVerdict, error) {
	// The feed is what makes the drain meaningful. Library callers may not have
	// one (it is advisory throughout this checker), and without it there is no
	// way to know the target has seen everything the source has done.
	if c.feed == nil || c.pinSourceRows == nil || len(s.pending) == 0 {
		return pinnedUnavailable, nil
	}
	// An obligation that a row is *absent* on the source cannot be pinned: under
	// READ COMMITTED there is no lock on a row that is not there, so a
	// concurrent INSERT can still land between the drain and the read. Settling
	// one would be claiming a guarantee we do not have.
	keys := make([][]table.Datum, 0, len(s.pending))
	for _, row := range s.pending {
		if !row.present {
			return pinnedUnavailable, nil
		}
		keys = append(keys, row.key)
	}

	ctx, cancel := context.WithTimeout(ctx, settleBudget)
	defer cancel()

	pinned, err := c.pinSourceRows(ctx, s, keys)
	if err != nil {
		// Losing the race for the lock is the expected outcome on a busy row,
		// not a failure of the checksum.
		c.cfg.Logger.Debug("lockless checksum: could not pin hot rows; deferring",
			"chunk", s.chunk.String(), "rows", len(keys), "error", err)
		return pinnedUnavailable, nil
	}
	defer pinned.release()

	// A row that was present at capture but is gone under the lock has become an
	// absence obligation, which is the case above: we hold no lock on it, so a
	// concurrent INSERT could still put it back. Give up rather than guess.
	if len(pinned.rows) != len(keys) {
		return pinnedUnavailable, nil
	}

	// Everything committed before now is in the stream; nothing further can
	// commit for these rows while they are pinned.
	if err := c.feed.BlockWait(ctx); err != nil {
		c.cfg.Logger.Debug("lockless checksum: feed did not catch up inside the settle budget; deferring",
			"chunk", s.chunk.String(), "error", err)
		return pinnedUnavailable, nil
	}
	if err := c.feed.Flush(ctx); err != nil {
		c.cfg.Logger.Debug("lockless checksum: feed did not drain inside the settle budget; deferring",
			"chunk", s.chunk.String(), "error", err)
		return pinnedUnavailable, nil
	}

	actual, _, oversized, err := readHotSnapshotRows(ctx, s.targetDB, s.chunk, s.chunk.NewTable, s.targetColumns, s.pendingPredicate(), "", 2*int(hotSplitTargetRows))
	if err != nil {
		return pinnedUnavailable, fmt.Errorf("read target rows while settling hot range %s: %w", s.chunk.String(), err)
	}
	// Same reasoning as hotSnapshot.check: a key representation that changed
	// under collation can match the predicate and come back different, so an
	// unrecognised key means we are not looking at what we think we are.
	if oversized {
		return pinnedUnavailable, nil
	}
	for key := range actual {
		if _, known := pinned.rows[key]; !known {
			return pinnedUnavailable, nil
		}
	}

	for key, expected := range pinned.rows {
		got, exists := actual[key]
		if !exists || got.crc != expected.crc {
			return pinnedDiverged, nil
		}
	}
	return pinnedClean, nil
}

// pinSourceRows takes the shared lock and reads the source images under it. The
// returned release drops the lock; it is safe to call exactly once.
//
// The lock lives in its own transaction on its own connection, which is what
// lets the feed drain (on other connections) while it is held. It is explicitly
// READ COMMITTED: this transaction exists to hold row locks, not to establish a
// snapshot, and a REPEATABLE READ view here would be the long-lived read view
// the lockless checker was written to avoid.
func pinSourceRows(ctx context.Context, db *sql.DB, s *hotSnapshot, keys [][]table.Datum) (*pinnedRows, error) {
	trx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	release := func() {
		// The transaction only ever read, so there is nothing to commit and a
		// rollback is the cheaper way to drop the locks. A failure here means the
		// connection is already gone, which drops them too.
		_ = trx.Rollback()
	}
	// Fail fast rather than queue behind whoever is writing the row. Without
	// this the statement below would inherit the server's default (50s), which
	// is ten times the whole settle budget.
	if _, err := trx.ExecContext(ctx, fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", settleLockWaitSeconds)); err != nil {
		release()
		return nil, err
	}
	rows, _, oversized, err := readHotSnapshotRows(ctx, trx, s.chunk, s.chunk.Table, s.sourceColumns, s.pendingPredicate(), " FOR SHARE", 2*int(hotSplitTargetRows))
	if err != nil {
		release()
		return nil, err
	}
	if oversized {
		release()
		return nil, errors.New("pinned source rows exceeded the snapshot budget")
	}
	if len(rows) != len(keys) {
		// Reported through the caller's len check too, but returning early here
		// releases the lock without waiting for the drain.
		release()
		return &pinnedRows{rows: rows, release: func() {}}, nil
	}
	return &pinnedRows{rows: rows, release: release}, nil
}
