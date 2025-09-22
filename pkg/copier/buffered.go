package copier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/siddontang/go-log/loggers"
	"golang.org/x/sync/errgroup"
)

type buffered struct {
	sync.Mutex
	db               *sql.DB
	chunker          table.Chunker
	concurrency      int
	finalChecksum    bool
	rowsPerSecond    uint64
	isInvalid        bool
	startTime        time.Time
	throttler        throttler.Throttler
	dbConfig         *dbconn.DBConfig
	logger           loggers.Advanced
	metricsSink      metrics.Sink
	copierEtaHistory *copierEtaHistory

	// Aurora algorithm specific fields - Producer/Consumer pattern
	sharedBuffer    chan bufferedChunk // Channel for buffering chunks between read and write pools
	bufferSize      int                // Size of the shared buffer
	readersFinished int32              // Atomic counter for finished readers
	workerIDCounter int32              // Atomic counter for assigning worker IDs
}

// rowData represents a single row with all its column values
type rowData struct {
	values []any
}

// bufferedChunk represents a chunk with its data moving through the pipeline
type bufferedChunk struct {
	chunk    *table.Chunk  // Original chunk for Feedback()
	rows     []rowData     // The actual row data
	readTime time.Duration // How long the read operation took
}

// Assert that buffered implements the Copier interface
var _ Copier = (*buffered)(nil)

// CopyChunk is maintained for interface compatibility but not used in producer-consumer pattern
func (c *buffered) CopyChunk(ctx context.Context, chunk *table.Chunk) error {
	return errors.New("not implemented in buffered copier, use Run()")
}

// readChunkData reads all rows from a chunk into memory
func (c *buffered) readChunkData(ctx context.Context, chunk *table.Chunk) ([]rowData, error) {
	// Build the SELECT query to read full row data
	columnList := utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable)
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		columnList,
		chunk.Table.QuotedName,
		chunk.String(),
	)

	c.logger.Debugf("reading chunk data: %s, query: %s", chunk.String(), query)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunk data: %w", err)
	}
	defer rows.Close()

	// Get column count for scanning
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var rowDataList []rowData
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

		rowDataList = append(rowDataList, rowData{values: values})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	c.logger.Debugf("read %d rows from chunk %s", len(rowDataList), chunk.String())
	return rowDataList, nil
}

// writeBufferedRows writes the buffered rows to the destination table using INSERT statements
func (c *buffered) writeBufferedRows(ctx context.Context, chunk *table.Chunk, rows []rowData) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Get the column list for the INSERT statement
	columnList := utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable)

	// Split rows into batches for parallel writing
	batchSize := 1000 // Configurable batch size
	var totalAffectedRows int64

	// Use higher parallelism for writes (5-10x more threads)
	writeWorkers := c.concurrency * 8 // 8x more write workers than read workers
	if writeWorkers > 50 {
		writeWorkers = 50 // Cap at reasonable limit
	}

	g, errGrpCtx := errgroup.WithContext(ctx)
	g.SetLimit(writeWorkers)

	var affectedRowsMutex sync.Mutex

	// Process rows in batches
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]
		g.Go(func() error {
			affectedRows, err := c.writeBatch(errGrpCtx, chunk, columnList, batch)
			if err != nil {
				return err
			}

			affectedRowsMutex.Lock()
			totalAffectedRows += affectedRows
			affectedRowsMutex.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return totalAffectedRows, nil
}

// writeBatch writes a batch of rows using INSERT IGNORE with VALUES
func (c *buffered) writeBatch(ctx context.Context, chunk *table.Chunk, columnList string, batch []rowData) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	// Build VALUES clauses
	var valuesClauses []string
	for _, row := range batch {
		var values []string
		for _, value := range row.values {
			values = append(values, c.formatSQLValue(value))
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// Build the INSERT statement
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		chunk.NewTable.QuotedName,
		columnList,
		strings.Join(valuesClauses, ", "),
	)

	c.logger.Debugf("writing batch of %d rows to %s", len(batch), chunk.NewTable.QuotedName)

	// Execute the batch insert
	result, err := dbconn.RetryableTransaction(ctx, c.db, c.finalChecksum, c.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute batch insert: %w", err)
	}

	return result, nil
}

func (c *buffered) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *buffered) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *buffered) Run(ctx context.Context) error {
	c.startTime = time.Now()
	go c.estimateRowsPerSecondLoop(ctx) // estimate rows while copying

	// Initialize producer-consumer pipeline
	c.bufferSize = c.concurrency // Buffer up to threads chunks between read and write pools
	c.sharedBuffer = make(chan bufferedChunk, c.bufferSize)

	g, errGrpCtx := errgroup.WithContext(ctx)
	g2, errGrpCtx2 := errgroup.WithContext(ctx)

	// Start read workers (producers)
	g.SetLimit(c.concurrency)
	for range c.concurrency {
		g.Go(func() error {
			return c.readWorker(errGrpCtx)
		})
	}

	// Start single write coordinator that fans out to parallel inserts
	// We want it to be a go-routine too.
	g2.Go(func() error {
		return c.writeCoordinator(errGrpCtx2)
	})

	// We wait for reads to finish, then we wait for writes to finish.
	err := g.Wait()
	if err != nil {
		return err
	}
	return g2.Wait()
}

// readWorker is a producer that reads chunks and puts them in the shared buffer
func (c *buffered) readWorker(ctx context.Context) error {
	// Get a unique worker ID
	workerID := atomic.AddInt32(&c.workerIDCounter, 1)
	c.logger.Infof("readWorker %d started", workerID)

	defer func() {
		// Atomically increment the finished readers counter
		finishedCount := atomic.AddInt32(&c.readersFinished, 1)
		c.logger.Infof("readWorker %d finished, total finished: %d/%d", workerID, finishedCount, c.concurrency)
		if finishedCount == int32(c.concurrency) {
			// Last reader to finish closes the channel
			c.logger.Infof("readWorker %d: closing shared buffer channel", workerID)
			close(c.sharedBuffer)
		}
	}()

	for !c.chunker.IsRead() && c.isHealthy(ctx) {
		c.throttler.BlockWait()

		c.logger.Debugf("readWorker %d: getting next chunk", workerID)
		chunk, err := c.chunker.Next()
		if err != nil {
			if err == table.ErrTableIsRead {
				c.logger.Infof("readWorker %d: table is read, exiting", workerID)
				return nil // Normal completion
			}
			c.setInvalid(true)
			return err
		}

		readStart := time.Now()
		rows, err := c.readChunkData(ctx, chunk)
		if err != nil {
			c.setInvalid(true)
			return fmt.Errorf("failed to read chunk data: %w", err)
		}
		readTime := time.Since(readStart)

		bufferedChunk := bufferedChunk{
			chunk:    chunk,
			rows:     rows,
			readTime: readTime,
		}

		// Send to shared buffer (blocks if buffer is full)
		c.logger.Debugf("readWorker %d: sending chunk %s to buffer", workerID, chunk.String())
		select {
		case c.sharedBuffer <- bufferedChunk:
			c.logger.Debugf("readWorker %d: buffered chunk %s with %d rows", workerID, chunk.String(), len(rows))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	c.logger.Infof("readWorker %d: exiting main loop (IsRead: %v, isHealthy: %v)", workerID, c.chunker.IsRead(), c.isHealthy(ctx))
	return nil
}

// writeCoordinator is a consumer that processes chunks from the shared buffer
func (c *buffered) writeCoordinator(ctx context.Context) error {
	for {
		c.logger.Debugf("writeCoordinator: waiting for next chunk from buffer (buffer size: %d)", c.bufferSize)
		select {
		case bufferedChunk, ok := <-c.sharedBuffer:
			if !ok {
				// Channel closed, no more chunks to process
				c.logger.Infof("writeCoordinator: channel closed, exiting")
				return nil
			}
			// Process the chunk
			affectedRows, err := c.writeBufferedRows(ctx, bufferedChunk.chunk, bufferedChunk.rows)
			if err != nil {
				c.setInvalid(true)
				return fmt.Errorf("failed to write buffered rows: %w", err)
			}

			// Send feedback with read time only (as requested)
			c.chunker.Feedback(bufferedChunk.chunk, bufferedChunk.readTime, uint64(affectedRows))

			// Send metrics with read time
			err = c.sendMetrics(ctx, bufferedChunk.readTime, bufferedChunk.chunk.ChunkSize, uint64(affectedRows))
			if err != nil {
				// we don't want to stop processing if metrics sending fails, log and continue
				c.logger.Errorf("error sending metrics from copier: %v", err)
			}

			c.logger.Debugf("completed chunk %s with %d affected rows (read time: %v)",
				bufferedChunk.chunk.String(), affectedRows, bufferedChunk.readTime)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *buffered) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
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
	rowsPerSecond := atomic.LoadUint64(&c.rowsPerSecond)
	if pct > 99.99 {
		return "DUE"
	}
	if rowsPerSecond == 0 || time.Since(c.startTime) < copyETAInitialWaitTime {
		return "TBD"
	}
	// divide the remaining rows by how many rows we copied in the last interval per second
	// "remainingRows" might be the actual rows or the logical rows since
	// c.getCopyStats() and rowsPerSecond change estimation method when the PK is auto-inc.
	remainingRows := totalRows - copiedRows
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))

	estimate := time.Duration(remainingSeconds * float64(time.Second))
	comparison := c.copierEtaHistory.addCurrentEstimateAndCompare(estimate)
	if comparison != "" {
		return fmt.Sprintf("%s (%s)", estimate.String(), comparison)
	}
	return estimate.String()
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
			atomic.StoreUint64(&c.rowsPerSecond, rowsPerSecond)
			prevRowsCount = newRowsCount
		}
	}
}

// The following funcs proxy to the chunker.
// This is done, so we don't need to export the chunker,
// KeyAboveHighWatermark returns true if the key is above where the chunker is currently at.
func (c *buffered) KeyAboveHighWatermark(key any) bool {
	return c.chunker.KeyAboveHighWatermark(key)
}

// GetLowWatermark returns the low watermark of the chunker, i.e. the lowest key that has been
// guaranteed to be written to the new table.
func (c *buffered) GetLowWatermark() (string, error) {
	return c.chunker.GetLowWatermark()
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

// Next4Test is typically only used in integration tests that don't want to actually migrate data,
// but need to advance the chunker.
func (c *buffered) Next4Test() (*table.Chunk, error) {
	return c.chunker.Next()
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

// formatSQLValue formats a Go value for use in SQL VALUES clause using proper SQL escaping
func (c *buffered) formatSQLValue(value any) string {
	// Use the proper SQL escaping library to handle all data types correctly
	escaped, err := sqlescape.EscapeSQL("%?", value)
	if err != nil {
		// Fallback to NULL if escaping fails
		c.logger.Warnf("failed to escape SQL value %v: %v, using NULL", value, err)
		return "NULL"
	}
	return escaped
}
