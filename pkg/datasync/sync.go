// Package datasync implements the `sync` command: a continuous,
// heterogeneous data sync.
//
// A sync performs an initial copy of the source tables into the target,
// then streams ongoing changes from the source to the target
// indefinitely until the context is cancelled. Unlike `move`, there is
// no checksum and no cutover — the sync simply keeps the target caught
// up with the source for as long as it runs. Cancelling the command
// (SIGINT/SIGTERM, or a caller-cancelled context) drains the in-flight
// backlog and exits cleanly.
//
// The source is either a built-in MySQL binlog client (constructed from
// SourceDSN) or a caller-injected change.Source — e.g. a Vitess /
// PlanetScale VStream. The target is written through an applier; today
// that is a MySQL SingleTargetApplier, but the applier abstraction is
// what makes the sync heterogeneous: a future Postgres applier would
// let this sync MySQL → Postgres without changing the runner.
//
// The package is deliberately named datasync (not sync) so it does not
// shadow the standard library's sync package. The CLI command is still
// `sync` (the kong field name drives the command name, the same way
// migration.Migration is exposed as `migrate`).
package datasync

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/utils"
)

// Sync is the configuration for a continuous data sync. The exported,
// kong-tagged fields are the CLI surface; the kong:"-" fields are for
// programmatic callers (e.g. strata's Vitess/PlanetScale import) that
// inject a non-MySQL change source and/or a custom applier.
type Sync struct {
	SourceDSN       string        `name:"source-dsn" help:"Where to sync the tables from." default:"spirit:spirit@tcp(127.0.0.1:3306)/src"`
	TargetDSN       string        `name:"target-dsn" help:"Where to sync the tables to." default:"spirit:spirit@tcp(127.0.0.1:3306)/dest"`
	TargetChunkTime time.Duration `name:"target-chunk-time" help:"How long each copy chunk should take." default:"5s"`
	Threads         int           `name:"threads" help:"How many chunks to copy in parallel during the initial copy." default:"4"`
	WriteThreads    int           `name:"write-threads" help:"How many concurrent write threads to use on the target." default:"4"`
	// FlushInterval controls how often buffered changes are applied to the
	// target during continuous sync — i.e. the replication latency vs.
	// batching trade-off. Defaults to change.DefaultFlushInterval.
	FlushInterval time.Duration `name:"flush-interval" help:"How often to flush buffered changes to the target during continuous sync." default:"30s"`

	// Force, when set, makes the runner drop and recreate the target database
	// at startup *unless* a resumable checkpoint exists — i.e. it only nukes
	// the target when the copy could not have resumed anyway. A resumable
	// run (checkpoint present) is left intact and resumes as normal. Intended
	// for testing/iterating, where a previous partial run can leave the target
	// non-empty with no usable checkpoint, otherwise tripping the fresh-sync
	// target-empty guard.
	Force bool `name:"force" help:"Drop and recreate the target database when the copy cannot resume from a checkpoint." default:"false"`

	// DisableContinuousChecksum turns off the post-copy eventually-consistent
	// checksum. The checker is on by default — it's what produces the
	// "data is known consistent" signal that programmatic callers (e.g.
	// the import feature) gate on. Disabling is intended for tests and
	// for source/target pairs whose schemas the checker cannot read (a
	// future heterogeneous-applier case). Hidden because operators
	// normally want it on.
	DisableContinuousChecksum bool `name:"disable-continuous-checksum" help:"Disable the post-copy continuous checksum (advanced)." default:"false" hidden:""`

	// Source optionally provides a pre-constructed change.Source to use
	// for replication instead of constructing a built-in MySQL-binlog
	// client from SourceDSN. When set, the runner uses this as the change
	// feed. SourceDSN is still required for source-side SQL (SHOW TABLES,
	// SHOW CREATE TABLE, the initial-copy SELECTs). Setting Source requires
	// setting Applier (see below).
	//
	// Intended for callers (e.g. strata's Vitess/PlanetScale import) that
	// need a non-MySQL-binlog change source.
	Source change.Source `kong:"-"`

	// Applier optionally provides a pre-constructed applier.Applier. When
	// set, the runner uses this instead of constructing a MySQL
	// SingleTargetApplier from the target. Required when Source is set: the
	// injected change.Source needs the same applier instance the copier
	// uses, so all writes flow through one logical apply path.
	Applier applier.Applier `kong:"-"`

	// Target optionally supplies a pre-opened target. When nil, a single
	// MySQL target is opened from TargetDSN (auto-creating the database if
	// it does not exist). Sync writes to exactly one logical target — there
	// is no N:M fan-out.
	Target *applier.Target `kong:"-"`
}

// Run is the kong CLI entry point. It runs the sync until the process
// receives SIGINT/SIGTERM, then drains and exits cleanly. Programmatic
// callers that want to supply their own context should construct a
// Runner via NewRunner and call Runner.Run directly.
//
// Signal handling is two-stage: the first SIGINT/SIGTERM cancels the
// context for a graceful drain, and a second forces an immediate exit.
// The force-quit matters because the change feed can be slow to unwind
// (e.g. a busy or repeatedly-reconnecting binlog stream), and without it
// a second Ctrl+C would be swallowed — making the command feel hung.
func (s *Sync) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		<-sigCh
		slog.Default().Info("sync: signal received, shutting down — press Ctrl+C again to force-quit")
		cancel()
		<-sigCh
		slog.Default().Warn("sync: second signal received, forcing exit")
		os.Exit(130)
	}()

	runner, err := NewRunner(s)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(runner)
	return runner.Run(ctx)
}
