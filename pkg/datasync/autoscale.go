package datasync

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
)

func syncFlushBounds(vcpus, clientCeiling int) (int, int) {
	width, _ := autoscale.FlushBounds(vcpus)
	width = min(width, max(1, clientCeiling))
	return width, autoscale.FlushBatchSize(width)
}

// syncAutoscaleBounds derives the read and write bounds from the target's
// capacity. redoAware and commitLatencyEnabled decide whether the write
// ceiling may exceed the start (see throttler.ResolveMaxWriteThreads).
func syncAutoscaleBounds(vcpus, clientCeiling, connections int, redoAware, commitLatencyEnabled bool) (int, copier.AutoscaleConfig) {
	if vcpus < autoscale.MinVCPUs {
		return 0, copier.AutoscaleConfig{}
	}
	// Verification reads and repair writes share the target pool. Reserve
	// the entire change-feed flush plus six checkpoint/metadata connections,
	// then partition the remainder so both worker ceilings fit simultaneously.
	flush, _ := syncFlushBounds(vcpus, clientCeiling)
	available := connections - flush - 6
	if available < 2 {
		return 0, copier.AutoscaleConfig{}
	}
	readBudget := min(max(1, clientCeiling), available/2)
	writeBudget := min(max(1, clientCeiling), available-available/2)
	readStart, readMax := autoscale.ReadBounds(vcpus)
	writeStart := min(autoscale.WriteStart(vcpus), writeBudget)
	return min(readStart, readBudget), copier.AutoscaleConfig{
		Enabled: true, StartThreads: writeStart,
		MaxThreads:     min(throttler.ResolveMaxWriteThreads(writeStart, true, redoAware, commitLatencyEnabled), writeBudget),
		MaxReadThreads: min(readMax, readBudget),
	}
}

// setupThrottling builds the target's Aurora load throttlers — the same
// commit-latency and threads throttlers migration builds on its source — and
// publishes them as the load signal. It runs whether or not autoscaling is
// enabled: the copy pauses on an overloaded target and the change feed narrows
// its flush either way. Autoscaling then reads the same probe result, so the
// signal it scales against is the one throttling the sync.
func (r *Runner) setupThrottling(ctx context.Context) error {
	// Only the single target we monitor may be throttled or scaled. A
	// custom/sharded applier could write elsewhere, where this signal would
	// offer no protection and would pause the sync for unrelated load.
	if injected := r.sync.Applier; injected != nil {
		a, ok := injected.(*applier.SingleTargetApplier)
		if !ok || len(a.GetTargets()) != 1 || a.GetTargets()[0].DB != r.target.DB {
			if r.sync.EnableExperimentalAutoscaling {
				r.logger.Warn("sync autoscaling disabled: injected applier must use the monitored single target")
			}
			return nil
		}
	}
	if r.target.Config == nil {
		if r.sync.EnableExperimentalAutoscaling {
			r.logger.Warn("sync autoscaling disabled: target connection config unavailable")
		}
		return nil
	}
	result, err := (throttler.AuroraSetup{
		Source: r.target.DB,
		OpenMonitor: func() (*sql.DB, error) {
			cfg := *r.targetDBConfig
			cfg.MaxOpenConnections = 2
			return dbconn.NewWithConnectionType(r.target.Config.FormatDSN(), &cfg, "sync target monitor")
		},
		CommitLatencyThreshold: r.sync.MaxCommitLatency,
		Logger:                 r.logger,
	}).Build(ctx)
	if err != nil {
		return err
	}
	return r.applyAuroraResult(ctx, result)
}

// applyAuroraResult publishes the target's load signal and then sizes
// autoscaling from the same result. It takes ownership of the result's
// throttlers and monitor pool, including on failure.
func (r *Runner) applyAuroraResult(ctx context.Context, result throttler.AuroraResult) error {
	if err := r.openLoadSignal(ctx, result); err != nil {
		return err
	}
	return r.setupAutoscaling(ctx, result)
}

// openLoadSignal takes ownership of the new monitor, including on failure.
func (r *Runner) openLoadSignal(ctx context.Context, result throttler.AuroraResult) error {
	if len(result.Throttlers) == 0 {
		return nil
	}
	signal := throttler.NewMultiThrottler(result.Throttlers...)
	if err := signal.Open(ctx); err != nil {
		_ = signal.Close()
		if result.MonitorDB != nil {
			_ = result.MonitorDB.Close()
		}
		return err
	}
	r.progMu.Lock()
	r.loadSignal = signal
	r.progMu.Unlock()
	r.monitorDB = result.MonitorDB
	return nil
}

// setupAutoscaling derives thread counts from the target when autoscaling is
// enabled and the target provides a load signal. result is the probe that
// built that signal.
func (r *Runner) setupAutoscaling(ctx context.Context, result throttler.AuroraResult) error {
	if !r.sync.EnableExperimentalAutoscaling {
		return nil
	}
	switch {
	case result.ProbeErr != nil:
		r.logger.Warn("sync autoscaling disabled: target detection failed", "error", result.ProbeErr)
		return nil
	case len(result.Throttlers) == 0:
		r.logger.Info("sync autoscaling disabled: target is not Aurora")
		return nil
	}
	vcpus, err := throttler.AuroraVCPUs(ctx, r.target.DB)
	if err != nil {
		return fmt.Errorf("sync target CPU capacity: %w", err)
	}
	readStart, config := syncAutoscaleBounds(vcpus, autoscale.ClientCeiling(), r.sync.MaxConnections, result.RedoAware, r.sync.MaxCommitLatency > 0)
	if !config.Enabled {
		r.logger.Info("sync autoscaling disabled: target too small", "vcpus", vcpus)
		return nil
	}
	r.engageAutoscaling(readStart, config)
	r.flushConcurrency, r.flushBatchSize = syncFlushBounds(vcpus, autoscale.ClientCeiling())
	return nil
}

// engageAutoscaling hands the thread counts to the controllers. The load
// signal must already be open (openLoadSignal).
func (r *Runner) engageAutoscaling(readStart int, config copier.AutoscaleConfig) {
	r.autoscale = config
	r.sync.Threads, r.sync.WriteThreads = readStart, config.StartThreads
	r.logger.Info("sync autoscaling engaged; configured thread counts overridden",
		"threads", readStart, "max_read_threads", config.MaxReadThreads,
		"write_threads", config.StartThreads, "max_write_threads", config.MaxThreads)
}

func (r *Runner) currentLoadSignal() throttler.Throttler {
	r.progMu.RLock()
	defer r.progMu.RUnlock()
	if r.loadSignal == nil {
		return &throttler.Noop{}
	}
	return r.loadSignal
}

// TargetUnderLoad is safe before and during Run. Injected change sources can
// use it as their ClientConfig.UnderLoad callback, matching the built-in feed's
// adaptive flush control. Synchronous replication writes do not use the
// applier worker queue, so resizing that queue alone cannot control flushes.
func (r *Runner) TargetUnderLoad() bool {
	return throttler.GradualOnly(r.currentLoadSignal()).IsThrottled()
}

func (r *Runner) throttleStatus(state status.State) status.ThrottleStatus {
	if state != status.CopyRows && state != status.ApplyChangeset {
		return status.ThrottleStatus{}
	}
	paused, reason, util := throttler.Describe(r.currentLoadSignal())
	return status.ThrottleStatus{Throttled: paused, Reason: reason, Utilization: util}
}
