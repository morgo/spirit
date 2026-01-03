package applier

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/typeconv"
	_ "github.com/lib/pq"
)

// PostgresApplier implements the Applier interface for PostgreSQL targets
type PostgresApplier struct {
	target     Target
	dbConfig   *dbconn.DBConfig
	logger     *slog.Logger
	typeMapper typeconv.Mapper

	// Worker pool for parallel writes
	pendingWork chan *workItem
	workers     []*postgresWriter
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
	mu          sync.Mutex
}

// workItem represents a batch of work to be written
type workItem struct {
	chunk    *table.Chunk
	rows     [][]interface{}
	callback ApplyCallback
}

// postgresWriter handles writing to PostgreSQL
type postgresWriter struct {
	id       int
	db       *sql.DB
	logger   *slog.Logger
	mapper   typeconv.Mapper
	workChan <-chan *workItem
	ctx      context.Context
}

// NewPostgresApplier creates a new PostgreSQL applier
func NewPostgresApplier(target Target, dbConfig *dbconn.DBConfig, logger *slog.Logger, typeMapper typeconv.Mapper) *PostgresApplier {
	return &PostgresApplier{
		target:      target,
		dbConfig:    dbConfig,
		logger:      logger,
		typeMapper:  typeMapper,
		pendingWork: make(chan *workItem, 100),
	}
}

// Start initializes the applier and starts worker goroutines
func (a *PostgresApplier) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		return nil
	}

	a.ctx, a.cancel = context.WithCancel(ctx)

	// Start worker pool
	numWorkers := 8 // TODO: Make configurable
	a.workers = make([]*postgresWriter, numWorkers)

	for i := 0; i < numWorkers; i++ {
		writer := &postgresWriter{
			id:       i,
			db:       a.target.DB,
			logger:   a.logger,
			mapper:   a.typeMapper,
			workChan: a.pendingWork,
			ctx:      a.ctx,
		}
		a.workers[i] = writer

		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			writer.run()
		}()
	}

	a.started = true
	a.logger.Info("postgres applier started", "workers", numWorkers)
	return nil
}

// Apply sends rows to be written to the target
func (a *PostgresApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]interface{}, callback ApplyCallback) error {
	if !a.started {
		return fmt.Errorf("applier not started")
	}

	work := &workItem{
		chunk:    chunk,
		rows:     rows,
		callback: callback,
	}

	select {
	case a.pendingWork <- work:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
}

// DeleteKeys deletes rows by their key values
func (a *PostgresApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys []string, lock *dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Build DELETE statement
	// Keys are hashed strings, we need to unhash them
	var keyValues []interface{}
	for _, key := range keys {
		// Unhash the key to get the actual primary key value
		// This is a simplified version - in reality we'd need to properly unhash
		keyValues = append(keyValues, key)
	}

	// Build WHERE clause for primary key
	// Assuming single column primary key for now
	pkCol := targetTable.KeyColumns[0]
	placeholders := make([]string, len(keyValues))
	for i := range keyValues {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		targetTable.TableName,
		pkCol,
		strings.Join(placeholders, ","),
	)

	result, err := a.target.DB.ExecContext(ctx, query, keyValues...)
	if err != nil {
		return 0, fmt.Errorf("failed to delete keys: %w", err)
	}

	affected, _ := result.RowsAffected()
	return affected, nil
}

// UpsertRows performs an upsert (INSERT ... ON CONFLICT DO UPDATE)
func (a *PostgresApplier) UpsertRows(ctx context.Context, sourceTable, targetTable *table.TableInfo, rows []LogicalRow, lock *dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	var totalAffected int64

	for _, row := range rows {
		if row.IsDeleted {
			// Handle deletes
			// Build DELETE based on primary key
			pkCols := targetTable.KeyColumns
			whereClauses := make([]string, len(pkCols))
			pkValues := make([]interface{}, len(pkCols))

			for i, col := range pkCols {
				whereClauses[i] = fmt.Sprintf("%s = $%d", col, i+1)
				// Extract PK value from row
				// This is simplified - need proper column mapping
				pkValues[i] = row.RowImage[i]
			}

			query := fmt.Sprintf("DELETE FROM %s WHERE %s",
				targetTable.TableName,
				strings.Join(whereClauses, " AND "),
			)

			result, err := a.target.DB.ExecContext(ctx, query, pkValues...)
			if err != nil {
				return totalAffected, fmt.Errorf("failed to delete row: %w", err)
			}
			affected, _ := result.RowsAffected()
			totalAffected += affected
		} else {
			// Handle inserts/updates with ON CONFLICT
			columns := targetTable.Columns
			placeholders := make([]string, len(columns))
			for i := range columns {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
			}

			// Build update clause for ON CONFLICT
			updateClauses := make([]string, 0)
			for _, col := range columns {
				// Don't update primary key columns
				isPK := false
				for _, pkCol := range targetTable.KeyColumns {
					if col == pkCol {
						isPK = true
						break
					}
				}
				if !isPK {
					updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
				}
			}

			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
				targetTable.TableName,
				strings.Join(columns, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(targetTable.KeyColumns, ", "),
				strings.Join(updateClauses, ", "),
			)

			// Convert values using type mapper
			convertedValues := make([]interface{}, len(row.RowImage))
			for i, val := range row.RowImage {
				// Get column type from source table
				colName := sourceTable.Columns[i]
				colType, ok := sourceTable.GetColumnMySQLType(colName)
				if !ok {
					return totalAffected, fmt.Errorf("failed to get type for column %s", colName)
				}
				converted, err := a.typeMapper.MapValue(val, colType)
				if err != nil {
					return totalAffected, fmt.Errorf("failed to convert value: %w", err)
				}
				convertedValues[i] = converted
			}

			result, err := a.target.DB.ExecContext(ctx, query, convertedValues...)
			if err != nil {
				return totalAffected, fmt.Errorf("failed to upsert row: %w", err)
			}
			affected, _ := result.RowsAffected()
			totalAffected += affected
		}
	}

	return totalAffected, nil
}

// Wait blocks until all pending work is complete
func (a *PostgresApplier) Wait(ctx context.Context) error {
	a.mu.Lock()
	if !a.started {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()

	// Close the work channel to signal workers to finish
	close(a.pendingWork)

	// Wait for all workers to complete
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop shuts down the applier
func (a *PostgresApplier) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.started {
		return nil
	}

	if a.cancel != nil {
		a.cancel()
	}

	a.started = false
	a.logger.Info("postgres applier stopped")
	return nil
}

// GetTargets returns the target information
func (a *PostgresApplier) GetTargets() []Target {
	return []Target{a.target}
}

// run is the main loop for a postgres writer
func (w *postgresWriter) run() {
	for {
		select {
		case work, ok := <-w.workChan:
			if !ok {
				// Channel closed, exit
				return
			}
			w.processWork(work)
		case <-w.ctx.Done():
			return
		}
	}
}

// processWork writes a batch of rows to PostgreSQL
func (w *postgresWriter) processWork(work *workItem) {
	affected, err := w.writeRows(work.chunk, work.rows)
	if work.callback != nil {
		work.callback(affected, err)
	}
}

// writeRows writes rows to PostgreSQL using COPY protocol
func (w *postgresWriter) writeRows(chunk *table.Chunk, rows [][]interface{}) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// For now, use INSERT statements
	// TODO: Implement COPY protocol for better performance
	tableName := chunk.NewTable.TableName
	columns := chunk.NewTable.Columns

	// Build INSERT statement
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(chunk.NewTable.KeyColumns, ", "),
		w.buildUpdateClause(columns, chunk.NewTable.KeyColumns),
	)

	// Prepare statement
	stmt, err := w.db.Prepare(query)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	var totalAffected int64

	// Execute for each row
	for _, row := range rows {
		// Convert values using type mapper
		convertedValues := make([]interface{}, len(row))
		for i, val := range row {
			// Get column type from chunk table
			colName := chunk.Table.Columns[i]
			colType, ok := chunk.Table.GetColumnMySQLType(colName)
			if !ok {
				return totalAffected, fmt.Errorf("failed to get type for column %s", colName)
			}
			converted, err := w.mapper.MapValue(val, colType)
			if err != nil {
				return totalAffected, fmt.Errorf("failed to convert value: %w", err)
			}
			convertedValues[i] = converted
		}

		result, err := stmt.Exec(convertedValues...)
		if err != nil {
			w.logger.Error("failed to insert row", "error", err)
			return totalAffected, fmt.Errorf("failed to insert row: %w", err)
		}

		affected, _ := result.RowsAffected()
		totalAffected += affected
	}

	return totalAffected, nil
}

// buildUpdateClause builds the SET clause for ON CONFLICT DO UPDATE
func (w *postgresWriter) buildUpdateClause(columns []string, pkColumns []string) string {
	updateClauses := make([]string, 0)
	pkMap := make(map[string]bool)
	for _, pk := range pkColumns {
		pkMap[pk] = true
	}

	for _, col := range columns {
		if !pkMap[col] {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}

	return strings.Join(updateClauses, ", ")
}

// writeRowsCOPY writes rows using PostgreSQL's COPY protocol (future optimization)
func (w *postgresWriter) writeRowsCOPY(chunk *table.Chunk, rows [][]interface{}) (int64, error) {
	// TODO: Implement COPY protocol
	// This is significantly faster than INSERT for bulk loading
	//
	// Example implementation:
	// 1. Begin transaction
	// 2. Use COPY ... FROM STDIN
	// 3. Stream rows in PostgreSQL text format
	// 4. Commit transaction
	//
	// For now, we use INSERT statements which are simpler but slower

	return w.writeRows(chunk, rows)
}
