package move

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/host"
	"github.com/block/spirit/pkg/throttler"
)

// moveAutoscaleBounds deliberately uses the smallest target, not the sum of
// target capacity: skewed input can route every row to one shard. Also divide
// the client write budget across targets, since SetWriteWorkers is per shard.
// redoAware and commitLatencyEnabled decide whether the write ceiling may
// exceed the start (see throttler.ResolveMaxWriteThreads).
func moveAutoscaleBounds(vcpus []int, groups []host.Group, clientCeiling int, redoAware, commitLatencyEnabled bool) (int, copier.AutoscaleConfig) {
	smallest, maxShardsPerHost, ok := moveCapacity(vcpus, groups)
	if !ok {
		return 0, copier.AutoscaleConfig{}
	}
	targetCount := 0
	for _, group := range groups {
		targetCount += len(group.Indices)
	}
	readStart, readMax := autoscale.ReadBounds(smallest)
	// A checksum reads every target concurrently for each chunk. Schemas
	// sharing a host must share its read budget too.
	readStart = max(1, readStart/maxShardsPerHost)
	readMax = max(1, readMax/maxShardsPerHost)
	readMax = min(readMax, max(1, clientCeiling))
	readStart = min(readStart, readMax)
	writeBudget := max(1, clientCeiling/max(1, targetCount))
	writeStart := min(max(1, autoscale.WriteStart(smallest)/maxShardsPerHost), writeBudget)
	return readStart, copier.AutoscaleConfig{
		Enabled: true, StartThreads: writeStart,
		MaxThreads:     min(throttler.ResolveMaxWriteThreads(writeStart, true, redoAware, commitLatencyEnabled), writeBudget),
		MaxReadThreads: readMax,
	}
}

// moveFlushBounds sizes each source feed's change flush, as migration and sync
// do from autoscale.FlushBounds. A flush batch fans out to every shard, so the
// busiest host sees width × sources × (shards on that host) concurrent
// REPLACEs. Dividing the smallest target's width by sources and co-located
// shards keeps that near what one migration puts on the instance. The floor is
// the change package's default width, which every move used before this, so the
// derivation only ever widens a flush; the batch size is re-paired so rows in
// flight per feed stay at autoscale.FlushRowsInFlight. The client ceiling caps
// the statements all feeds render at once, but never below that floor: a small
// client split across many sources keeps the width every move had before.
func moveFlushBounds(vcpus []int, groups []host.Group, sources, clientCeiling int) (concurrency, batchSize int) {
	smallest, maxShardsPerHost, ok := moveCapacity(vcpus, groups)
	if !ok {
		return 0, 0
	}
	sources = max(1, sources)
	width, _ := autoscale.FlushBounds(smallest)
	width = min(width/(sources*maxShardsPerHost), clientCeiling/sources)
	width = max(autoscale.MinFlushConcurrency, width)
	return width, autoscale.FlushBatchSize(width)
}

// moveCapacity returns the smallest target's vCPUs and the most shards on one
// host. ok is false when any target is below autoscale.MinVCPUs.
func moveCapacity(vcpus []int, groups []host.Group) (smallest, maxShardsPerHost int, ok bool) {
	if len(vcpus) == 0 {
		return 0, 0, false
	}
	smallest, maxShardsPerHost = vcpus[0], 1
	for _, group := range groups {
		maxShardsPerHost = max(maxShardsPerHost, len(group.Indices))
	}
	for _, n := range vcpus {
		if n < autoscale.MinVCPUs {
			return 0, 0, false
		}
		smallest = min(smallest, n)
	}
	return smallest, maxShardsPerHost, true
}

// setupThrottling is shared by fresh and resumed moves. Every Aurora target
// gets the load throttlers migration builds on its source — commit latency
// and threads — whether or not autoscaling is enabled, and they are combined
// into one signal: the copy pauses when any target is overloaded. Autoscaling
// then reads the same probe results, so the signal it scales against is the
// one throttling the move.
func (r *Runner) setupThrottling(ctx context.Context) error {
	r.setThrottler(&throttler.Noop{})
	groups := r.targetHosts()
	results := make([]throttler.AuroraResult, len(groups))
	for i, group := range groups {
		target := r.targets[group.Indices[0]]
		result, err := (throttler.AuroraSetup{
			Source: target.DB,
			OpenMonitor: func() (*sql.DB, error) {
				cfg := *r.dbConfig
				cfg.MaxOpenConnections = 2
				return dbconn.NewWithConnectionType(target.Config.FormatDSN(), &cfg, "move target monitor")
			},
			CommitLatencyThreshold: r.move.MaxCommitLatency,
			Logger:                 r.logger.With("target", targetKey(target)),
		}).Build(ctx)
		if err != nil {
			closeAuroraResults(results[:i])
			return err
		}
		results[i] = result
	}
	return r.applyAuroraResults(ctx, groups, results)
}

// applyAuroraResults installs the targets' combined load signal and then
// sizes autoscaling from the same results. It takes ownership of every
// result's throttlers and monitor pool, including on failure.
func (r *Runner) applyAuroraResults(ctx context.Context, groups []host.Group, results []throttler.AuroraResult) error {
	var signals []throttler.Throttler
	var monitors []*sql.DB
	for _, result := range results {
		signals = append(signals, result.Throttlers...)
		if result.MonitorDB != nil {
			monitors = append(monitors, result.MonitorDB)
		}
	}
	if len(signals) > 0 {
		composite := throttler.NewMultiThrottler(signals...)
		if err := composite.Open(ctx); err != nil {
			closeAuroraResults(results)
			return err
		}
		r.setThrottler(composite)
		r.monitorDBs = monitors
	}
	return r.setupAutoscaling(ctx, groups, results)
}

func closeAuroraResults(results []throttler.AuroraResult) {
	for _, result := range results {
		for _, t := range result.Throttlers {
			_ = t.Close()
		}
		if result.MonitorDB != nil {
			_ = result.MonitorDB.Close()
		}
	}
}

// setupAutoscaling derives thread counts from the targets. results holds each
// host group's probe from setupThrottling. All targets must supply a usable
// signal before we override the configured thread counts; move's only policy
// difference from migration is conservative, lockstep scaling across targets
// (#1212).
func (r *Runner) setupAutoscaling(ctx context.Context, groups []host.Group, results []throttler.AuroraResult) error {
	if !r.move.EnableExperimentalAutoscaling {
		return nil
	}
	redoAware := false
	for i, group := range groups {
		target := r.targets[group.Indices[0]]
		// The policy is the same either way — all targets or none, since the
		// controller scales them in lockstep — but the two causes are not. A
		// probe that failed is something an operator needs to act on (locked-down
		// perf_schema, an under-granted monitor user); a target that is simply
		// not Aurora is an ordinary configuration, so it does not warn.
		switch {
		case results[i].ProbeErr != nil:
			r.logger.Warn("move autoscaling disabled: could not determine whether the target is Aurora; thread counts stay as configured",
				"target", targetKey(target), "error", results[i].ProbeErr.Error())
			return nil
		case len(results[i].Throttlers) == 0:
			r.logger.Info("move autoscaling disabled: every target must provide an Aurora load signal; thread counts stay as configured",
				"target", targetKey(target))
			return nil
		}
		// One redo-aware target is enough to need the backstop: the composite
		// signal cannot see that target's redo log oversubscribed.
		redoAware = redoAware || results[i].RedoAware
	}
	vcpus := make([]int, len(groups))
	for i, group := range groups {
		target := r.targets[group.Indices[0]]
		var err error
		vcpus[i], err = throttler.AuroraVCPUs(ctx, target.DB)
		if err != nil {
			return fmt.Errorf("target %s CPU capacity: %w", targetKey(target), err)
		}
	}
	readStart, config := moveAutoscaleBounds(vcpus, groups, autoscale.ClientCeiling(), redoAware, r.move.MaxCommitLatency > 0)
	if !config.Enabled {
		r.logger.Warn("move autoscaling disabled: target too small", "vcpus", vcpus, "min_vcpus", autoscale.MinVCPUs)
		return nil
	}
	r.autoscale = config
	r.move.Threads = readStart
	r.move.WriteThreads = config.StartThreads
	r.flushConcurrency, r.flushBatchSize = moveFlushBounds(vcpus, groups, len(r.sources), autoscale.ClientCeiling())
	r.logger.Info("move autoscaling engaged: busiest target controls all shard pools; --threads and --write-threads are ignored",
		"threads", readStart, "max_read_threads", config.MaxReadThreads,
		"write_threads_per_target", config.StartThreads, "max_write_threads_per_target", config.MaxThreads,
		"flush_concurrency", r.flushConcurrency, "flush_batch_size", r.flushBatchSize)
	return nil
}

// targetHosts is the common host view for monitoring and DDL scheduling.
func (r *Runner) targetHosts() []host.Group {
	configs := make([]*mysql.Config, len(r.targets))
	for i, target := range r.targets {
		configs[i] = target.Config
	}
	return host.GroupConfigs(configs)
}

func (r *Runner) setThrottler(t throttler.Throttler) {
	r.throttlerMu.Lock()
	defer r.throttlerMu.Unlock()
	r.throttler = t
}

func (r *Runner) currentThrottler() throttler.Throttler {
	r.throttlerMu.RLock()
	defer r.throttlerMu.RUnlock()
	return r.throttler
}
