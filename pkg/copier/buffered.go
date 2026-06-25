package copier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

// The buffered copier implements a producer/consumer pattern
// where multiple reader goroutines read chunks from the source table,
// and then send them to an Applier which breaks them into chunklets.
// It closely matches the DBLog algorithm:
// https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b

type buffered struct {
	sync.Mutex

	db               *sql.DB
	applier          applier.Applier
	chunker          table.Chunker
	concurrency      int
	rowsPerSecond    atomic.Uint64
	isInvalid        atomic.Bool
	errMu            sync.Mutex // guards firstErr
	firstErr         error      // first error that invalidated the copy (any goroutine)
	startTime        time.Time
	throttler        throttler.Throttler
	dbConfig         *dbconn.DBConfig
	logger           *slog.Logger
	metricsSink      metrics.Sink
	copierEtaHistory *copierEtaHistory
	autoscale        AutoscaleConfig
}

// Assert that buffered implements the Copier interface
var _ Copier = (*buffered)(nil)

// readChunkData reads all rows from a chunk into memory
func (c *buffered) readChunkData(ctx context.Context, chunk *table.Chunk) ([][]any, error) {
	// Build the SELECT query to read full row data
	columnList, _ := chunk.ColumnMapping.Columns()
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		columnList,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)

	c.logger.Debug("reading chunk data", "chunk", chunk.String(), "query", query)

	// Use the chunk's table DB connection so each chunk reads from its own source.
	// This is important for N:M moves where chunks from different sources
	// need to read from different database connections.
	rows, err := chunk.Table.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunk data: %w", err)
	}
	defer utils.CloseAndLog(rows)

	// Get column count for scanning
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var rowDataList [][]any
	for rows.Next() {
		// Create slice to hold the row values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowDataList = append(rowDataList, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	c.logger.Debug("read rows from chunk", "rowCount", len(rowDataList), "chunk", chunk.String())
	return rowDataList, nil
}

func (c *buffered) isHealthy(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid.Load()
}

func (c *buffered) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *buffered) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.Lock()
	c.startTime = time.Now()
	c.Unlock()
	go c.estimateRowsPerSecondLoop(ctx) // estimate rows while copying

	// Start the applier
	if err := c.applier.Start(ctx); err != nil {
		return fmt.Errorf("failed to start applier: %w", err)
	}

	// Experimental: start the write-thread autoscaler. It runs for the lifetime
	// of the copy and stops when ctx is cancelled (deferred above). It only
	// engages when the applier supports dynamic scaling (SingleTargetApplier)
	// AND the throttler provides a continuous load signal (GradualThrottler);
	// otherwise the pool stays fixed.
	if as := c.autoscalerIfEnabled(); as != nil {
		go as.run(ctx)
	}

	// Start read workers
	g, errGrpCtx := errgroup.WithContext(ctx)
	c.logger.Debug("starting read workers", "count", c.concurrency)
	for range c.concurrency {
		g.Go(func() error {
			return c.readWorker(errGrpCtx)
		})
	}

	// Wait for all read workers to finish
	err := g.Wait()

	// Wait for the applier to finish processing all pending work
	// This ensures all callbacks have been invoked before we return
	if waitErr := c.applier.Wait(ctx); waitErr != nil && err == nil {
		err = waitErr
	}

	// "Stop" the applier. This will free up the gouroutines that
	// were used for copying, but it won't close the DB connections
	if closeErr := c.applier.Stop(); closeErr != nil && err == nil {
		err = closeErr
	}

	// A failure inside an async applier callback (e.g. a chunklet the target
	// rejected with a warning) is reported to the callback in the applier's own
	// goroutine — it never flows through errGrp or applier.Wait, both of which
	// return nil in that case. Surface the first captured error so callers get
	// the real root cause instead of a generic "copy failed" message (or, worse,
	// a nil error that looks like success). Read/apply errors that already came
	// back through errGrp take precedence and leave this untouched.
	if err == nil {
		err = c.getFirstErr()
	}
	return err
}

// autoscalerIfEnabled returns the experimental write-thread autoscaler to run
// for this copy, or nil when it should not engage: autoscaling disabled, an
// applier without dynamic scaling (ShardedApplier), or a throttler without a
// continuous load signal. Only GradualThrottler implementations (the Aurora
// throttlers, or a multi-throttler containing one) provide that signal —
// binary throttlers like replica lag protect via the hard-stop only, and
// scaling blind against them would just ramp to the maximum unguided.
func (c *buffered) autoscalerIfEnabled() *autoScaler {
	if !c.autoscale.Enabled {
		return nil
	}
	scaler, ok := c.applier.(writeScaler)
	if !ok {
		c.logger.Info("autoscaling enabled but this applier does not support dynamic write threads; running with a fixed pool")
		return nil
	}
	gradual, ok := c.throttler.(throttler.GradualThrottler)
	if !ok {
		c.logger.Warn("autoscaling enabled but no continuous load signal is available (requires an Aurora target); write threads stay fixed at the starting value",
			"write_threads", c.autoscale.StartThreads)
		return nil
	}
	c.logger.Info("starting experimental write-thread autoscaler",
		"start", c.autoscale.StartThreads, "max", c.autoscale.MaxThreads,
		"low_watermark", acLowWatermark, "high_watermark", acHighWatermark)
	return newAutoScaler(gradual, scaler, c.autoscale.StartThreads, c.autoscale.MaxThreads, c.logger, c.metricsSink)
}

// readWorker reads chunks and sends them to the applier
func (c *buffered) readWorker(ctx context.Context) error {
	c.logger.Debug("readWorker started", "isRead", c.chunker.IsRead())

	for !c.chunker.IsRead() && c.isHealthy(ctx) {
		c.throttler.BlockWait(ctx)

		c.logger.Debug("readWorker calling chunker.Next()")
		chunk, err := c.chunker.Next()
		if err != nil {
			if errors.Is(err, table.ErrTableIsRead) {
				c.logger.Debug("readWorker table is read, exiting")
				return nil
			}
			c.logger.Error("readWorker got error from chunker", "error", err)
			c.setInvalid(err)
			return err
		}
		c.logger.Debug("readWorker got chunk", "chunk", chunk.String())

		// Start timing from the beginning of the chunk processing (read + write)
		chunkStartTime := time.Now()
		rows, err := c.readChunkData(ctx, chunk)
		if err != nil {
			readErr := fmt.Errorf("failed to read chunk data: %w", err)
			c.setInvalid(readErr)
			return readErr
		}

		// Handle empty chunks immediately
		if len(rows) == 0 {
			totalTime := time.Since(chunkStartTime)
			c.logger.Debug("readWorker chunk is empty, sending immediate feedback", "chunk", chunk.String())
			c.chunker.Feedback(chunk, totalTime, 0)

			// Send metrics for empty chunk
			err := c.sendMetrics(ctx, totalTime, chunk.ChunkSize, 0)
			if err != nil {
				c.logger.Error("error sending metrics for empty chunk", "error", err)
			}
			continue
		}

		c.logger.Debug("readWorker sending rows to applier", "chunk", chunk.String(), "rowCount", len(rows))

		// Send rows to applier with callback
		// The callback will be invoked when all rows are safely flushed
		// Capture the loop variables to avoid data race when callback executes asynchronously
		capturedChunk := chunk
		capturedStartTime := chunkStartTime
		callback := func(affectedRows int64, err error) {
			if err != nil {
				// A context cancellation (Ctrl+C / graceful shutdown) tears down
				// in-flight chunklets deliberately — it is not a copy failure, so
				// log it quietly rather than alarming the user with an ERROR. We
				// still setInvalid to unwind Run; higher layers filter context
				// cancellation when deciding the command's exit status.
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					c.logger.Debug("applier callback cancelled", "chunk", capturedChunk.String(), "error", err)
				} else {
					c.logger.Error("applier callback received error", "chunk", capturedChunk.String(), "error", err)
				}
				c.setInvalid(err)
				return
			}

			c.logger.Debug("applier callback invoked",
				"table", capturedChunk.Table.TableName, "chunk", capturedChunk.String(),
				"affected_rows", affectedRows, "duration", time.Since(capturedStartTime).String())

			// Calculate total time from read start to callback completion (read + write)
			totalTime := time.Since(capturedStartTime)

			// Send feedback to chunker with total processing time
			c.chunker.Feedback(capturedChunk, totalTime, uint64(affectedRows))

			// Send metrics with total processing time
			metricsErr := c.sendMetrics(ctx, totalTime, capturedChunk.ChunkSize, uint64(affectedRows))
			if metricsErr != nil {
				c.logger.Error("error sending metrics from copier", "error", metricsErr)
			}
		}

		// Apply the rows
		if err := c.applier.Apply(ctx, chunk, rows, callback); err != nil {
			applyErr := fmt.Errorf("failed to apply rows: %w", err)
			c.setInvalid(applyErr)
			return applyErr
		}
	}

	c.logger.Debug("readWorker exiting main loop")
	return nil
}

// setInvalid marks the copy as failed and records the first error that caused
// it. It is called from the read workers and from async applier callbacks, so
// it captures only the first error and is safe for concurrent use.
func (c *buffered) setInvalid(err error) {
	if err != nil {
		c.errMu.Lock()
		if c.firstErr == nil {
			c.firstErr = err
		}
		c.errMu.Unlock()
	}
	c.isInvalid.Store(true)
}

// getFirstErr returns the first error that invalidated the copy, or nil.
func (c *buffered) getFirstErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.firstErr
}

func (c *buffered) SetThrottler(throttler throttler.Throttler) {
	c.Lock()
	defer c.Unlock()
	c.throttler = throttler
}

func (c *buffered) getCopyStats() (uint64, uint64, float64) {
	// Get progress from the chunker instead of calculating it ourselves
	rowsProcessed, _, totalRows := c.chunker.Progress()

	// Calculate percentage
	pct := float64(0)
	if totalRows > 0 {
		pct = float64(rowsProcessed) / float64(totalRows) * 100
	}

	return rowsProcessed, totalRows, pct
}

// GetProgress returns the progress of the copier
func (c *buffered) GetProgress() string {
	c.Lock()
	defer c.Unlock()
	copied, total, pct := c.getCopyStats()
	return fmt.Sprintf("%d/%d %.2f%%", copied, total, pct)
}

func (c *buffered) GetETA() string {
	c.Lock()
	defer c.Unlock()
	copiedRows, totalRows, pct := c.getCopyStats()
	estimate, st := etaEstimate(copiedRows, totalRows, pct, c.rowsPerSecond.Load(), c.startTime)
	switch st {
	case status.ETADue:
		return "DUE"
	case status.ETAMeasuring:
		return "TBD"
	case status.ETAReady, status.ETANone:
		// A ready estimate is formatted below; ETANone cannot occur during copy.
	}
	comparison := c.copierEtaHistory.addCurrentEstimateAndCompare(estimate)
	if comparison != "" {
		return fmt.Sprintf("%s (%s)", estimate.String(), comparison)
	}
	return estimate.String()
}

func (c *buffered) GetETAState() status.ETA {
	c.Lock()
	defer c.Unlock()
	copiedRows, totalRows, pct := c.getCopyStats()
	estimate, st := etaEstimate(copiedRows, totalRows, pct, c.rowsPerSecond.Load(), c.startTime)
	return status.ETA{State: st, Duration: estimate}
}

func (c *buffered) estimateRowsPerSecondLoop(ctx context.Context) {
	// We take >10 second averages because with parallel copy it bounces around a lot.
	// Get progress from chunker since we no longer track rows locally
	prevRowsCount, _, _ := c.chunker.Progress()
	ticker := time.NewTicker(copyEstimateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.isHealthy(ctx) {
				return
			}
			newRowsCount, _, _ := c.chunker.Progress()
			rowsPerInterval := float64(newRowsCount - prevRowsCount)
			intervalsDivisor := float64(copyEstimateInterval / time.Second) // should be something like 10 for 10 seconds
			rowsPerSecond := uint64(rowsPerInterval / intervalsDivisor)
			c.rowsPerSecond.Store(rowsPerSecond)
			prevRowsCount = newRowsCount
		}
	}
}

func (c *buffered) sendMetrics(ctx context.Context, processingTime time.Duration, logicalRowsCount uint64, affectedRowsCount uint64) error {
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{
				Name:  metrics.ChunkProcessingTimeMetricName,
				Type:  metrics.GAUGE,
				Value: float64(processingTime.Milliseconds()), // in milliseconds
			},
			{
				Name:  metrics.ChunkLogicalRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(logicalRowsCount),
			},
			{
				Name:  metrics.ChunkAffectedRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(affectedRowsCount),
			},
		},
	}

	contextWithTimeout, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()

	return c.metricsSink.Send(contextWithTimeout, m)
}

// GetChunker returns the chunker for accessing progress information
func (c *buffered) GetChunker() table.Chunker {
	return c.chunker
}

func (c *buffered) GetThrottler() throttler.Throttler {
	c.Lock()
	defer c.Unlock()
	return c.throttler
}
