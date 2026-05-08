// Package table contains some common utilities for working with tables
// such as a 'Chunker' feature.
package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	lastChunkStatisticsThreshold = 10 * time.Second
)

var (
	ErrTableIsRead        = errors.New("table is read")
	ErrTableNotOpen       = errors.New("please call Open() first")
	ErrUnsupportedPKType  = errors.New("unsupported primary key type")
	ErrWatermarkNotReady  = errors.New("watermark not yet ready")
	ErrChunkerNotOpen     = errors.New("chunker is not open, call Open() first")
	ErrChunkerAlreadyOpen = errors.New("table is already open, did you mean to call Reset()?")
)

type TableInfo struct {
	sync.Mutex

	db                          *sql.DB
	EstimatedRows               uint64 // used by the composite chunker for Max
	SchemaName                  string
	TableName                   string
	QuotedTableName             string            // `table` - backtick-quoted table name without schema
	Columns                     []string          // all the column names
	NonGeneratedColumns         []string          // all the non-generated column names
	Indexes                     []string          // all the index names
	columnsMySQLTps             map[string]string // map from column name to MySQL type
	KeyColumns                  []string          // the column names of the primaryKey
	keyColumnsMySQLTp           []string          // the MySQL types of the primaryKey
	KeyIsAutoInc                bool              // if pk[0] is an auto_increment column
	keyDatums                   []datumTp         // the datum type of pk
	minValue                    Datum             // known minValue of pk[0] (using type of PK[0])
	maxValue                    Datum             // known maxValue of pk[0] (using type of PK[0])
	statisticsLastUpdated       time.Time
	statisticsLock              sync.Mutex
	DisableAutoUpdateStatistics atomic.Bool

	// Host is an optional identifier for the MySQL server this table belongs to.
	// It is used by MultiChunker to disambiguate tables with the same SchemaName
	// and TableName on different servers (e.g., in N:M move operations).
	// When empty, the multi-chunker keys by SchemaName.TableName only.
	Host string

	// Sharding configuration (for ShardedApplier)
	// These are set per-table when using multi-table migrations with different sharding keys
	ShardingColumn string   // Column name to extract and hash (e.g., "user_id")
	HashFunc       HashFunc // Hash function: value -> uint64
}

// HashFunc is a hash function that takes a single column value and returns a uint64 hash.
// This matches Vitess vindex behavior where the hash is used to determine shard placement.
// The hash value is then matched against key ranges to find the target shard.
type HashFunc func(value any) (uint64, error)

// QualifiedName returns a stable key for this table suitable for use in
// checkpoint watermarks. The format is "host.schema.table" when Host is set,
// or "schema.table" otherwise. This ensures uniqueness even when multiple
// servers have identically-named schemas and tables (N:M moves).
func (t *TableInfo) QualifiedName() string {
	if t.Host != "" {
		return t.Host + "." + t.SchemaName + "." + t.TableName
	}
	return t.SchemaName + "." + t.TableName
}

// DB returns the database connection associated with this table.
// This is used by components like the copier and checksum that need
// to read from the correct source database when multiple sources are in use.
func (t *TableInfo) DB() *sql.DB {
	return t.db
}

func NewTableInfo(db *sql.DB, schema, table string) *TableInfo {
	return &TableInfo{
		db:              db,
		SchemaName:      schema,
		TableName:       table,
		QuotedTableName: fmt.Sprintf("`%s`", table),
	}
}

// PrimaryKeyValues helps extract the PRIMARY KEY from a row image.
// It uses our knowledge of the ordinal position of columns to find the
// position of primary key columns (there might be more than one).
// For minimal row image, you need to send the before image to extract the PK.
// This is because in the after image, the PK might be nil.
func (t *TableInfo) PrimaryKeyValues(row any) ([]any, error) {
	var pkCols []any
	for _, pCol := range t.KeyColumns {
		for i, col := range t.Columns {
			if col == pCol {
				if row.([]any)[i] == nil {
					return nil, errors.New("primary key column is NULL, possibly a bug sending after-image instead of before")
				}
				pkCols = append(pkCols, row.([]any)[i])
			}
		}
	}
	return pkCols, nil
}

// SetInfo reads from MySQL metadata (usually infoschema) and sets the values in TableInfo.
func (t *TableInfo) SetInfo(ctx context.Context) error {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	if err := t.setRowEstimate(ctx); err != nil {
		return err
	}
	if err := t.setColumns(ctx); err != nil {
		return err
	}
	if err := t.setPrimaryKey(ctx); err != nil {
		return err
	}
	if err := t.setIndexes(ctx); err != nil {
		return err
	}
	return t.setMinMax(ctx)
}

// setRowEstimate is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
func (t *TableInfo) setRowEstimate(ctx context.Context) error {
	_, err := t.db.ExecContext(ctx, "ANALYZE TABLE "+t.QuotedTableName)
	if err != nil {
		return err
	}
	err = t.db.QueryRowContext(ctx, "SELECT IFNULL(table_rows,0) FROM information_schema.tables WHERE table_schema=? AND table_name=?", t.SchemaName, t.TableName).Scan(&t.EstimatedRows)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("table %s.%s does not exist", t.SchemaName, t.TableName)
		}
		return err
	}
	return nil
}

func (t *TableInfo) setIndexes(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=? AND table_name=? AND index_name != 'PRIMARY'",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	t.Indexes = []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		t.Indexes = append(t.Indexes, name)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func (t *TableInfo) setColumns(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT column_name, column_type, GENERATION_EXPRESSION FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	t.Columns = []string{}
	t.NonGeneratedColumns = []string{}
	t.columnsMySQLTps = make(map[string]string)
	for rows.Next() {
		var col, tp, expression string
		if err := rows.Scan(&col, &tp, &expression); err != nil {
			return err
		}
		t.Columns = append(t.Columns, col)
		t.columnsMySQLTps[col] = tp
		if expression == "" {
			t.NonGeneratedColumns = append(t.NonGeneratedColumns, col)
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

// DescIndex describes the columns in an index.
func (t *TableInfo) DescIndex(keyName string) ([]string, error) {
	cols := []string{}
	//nolint: noctx // too much refactoring to add context here
	rows, err := t.db.Query("SELECT column_name FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND index_name=? ORDER BY seq_in_index",
		t.SchemaName,
		t.TableName,
		keyName,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return cols, nil
}

// setPrimaryKey sets the primary key and also the primary key type.
// A primary key can contain multiple columns.
func (t *TableInfo) setPrimaryKey(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema=? and table_name=? and constraint_name='PRIMARY' ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	t.KeyColumns = []string{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.KeyColumns = append(t.KeyColumns, col)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if len(t.KeyColumns) == 0 {
		return errors.New("no primary key found (not supported)")
	}
	for i, col := range t.KeyColumns {
		// Get primary key type and auto_inc info.
		query := "SELECT column_type, extra FROM information_schema.columns WHERE table_schema=? AND table_name=? and column_name=?"
		var extra, pkType string
		err = t.db.QueryRowContext(ctx, query, t.SchemaName, t.TableName, col).Scan(&pkType, &extra)
		if err != nil {
			return err
		}
		pkType = removeWidth(pkType)
		t.keyColumnsMySQLTp = append(t.keyColumnsMySQLTp, pkType)
		t.keyDatums = append(t.keyDatums, mySQLTypeToDatumTp(pkType))
		if i == 0 {
			t.KeyIsAutoInc = (extra == "auto_increment")
		}
	}
	return nil
}

// PrimaryKeyIsMemoryComparable checks that the PRIMARY KEY type is compatible.
// We no longer need this check for the chunker, since it can
// handle any type of key in the composite chunker.
// But the migration still needs to verify this, because of the
// delta map feature, which requires binary comparable keys.
func (t *TableInfo) PrimaryKeyIsMemoryComparable() error {
	if len(t.KeyColumns) == 0 || len(t.keyDatums) == 0 {
		return errors.New("please call setInfo() first")
	}
	if slices.Contains(t.keyDatums, unknownType) {
		return ErrUnsupportedPKType
	}
	return nil
}

// setMinMax is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
// It only really applies to KeyColumns[0], since across composite keys
// there could be inter-dependencies between columns.
func (t *TableInfo) setMinMax(ctx context.Context) error {
	if t.keyDatums[0] == binaryType {
		return nil // we don't min/max binary types for now.
	}
	quotedKey := "`" + t.KeyColumns[0] + "`"
	query := fmt.Sprintf("SELECT IFNULL(min(%s),'0'), IFNULL(max(%s),'0') FROM %s", quotedKey, quotedKey, t.QuotedTableName)
	var minimum, maximum string
	err := t.db.QueryRowContext(ctx, query).Scan(&minimum, &maximum)
	if err != nil {
		return err
	}

	t.minValue, err = newDatumFromMySQL(minimum, t.keyColumnsMySQLTp[0])
	if err != nil {
		return err
	}
	t.maxValue, err = newDatumFromMySQL(maximum, t.keyColumnsMySQLTp[0])
	if err != nil {
		return err
	}
	return nil
}

// Close currently does nothing
func (t *TableInfo) Close() error {
	return nil
}

// AutoUpdateStatistics runs a loop that updates the table statistics every interval.
// This will continue until Close() is called on the tableInfo, or t.DisableAutoUpdateStatistics is set to true.
func (t *TableInfo) AutoUpdateStatistics(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if t.DisableAutoUpdateStatistics.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := t.updateTableStatistics(ctx); err != nil {
				logger.Error("error updating table statistics", "error", err)
			}
			logger.Info("table statistics updated",
				"estimated-rows", t.EstimatedRows,
				"pk[0].max-value", t.MaxValue())
		}
	}
}

// statisticsNeedUpdating returns true if the statistics are considered order than a threshold.
// this is useful for the chunker to synchronously check as it approaches the end of the table.
func (t *TableInfo) statisticsNeedUpdating() bool {
	threshold := time.Now().Add(-lastChunkStatisticsThreshold)
	return t.statisticsLastUpdated.Before(threshold)
}

// updateTableStatistics recalculates the min/max and row estimate.
func (t *TableInfo) updateTableStatistics(ctx context.Context) error {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	err := t.setMinMax(ctx)
	if err != nil {
		return err
	}
	err = t.setRowEstimate(ctx)
	if err != nil {
		return err
	}
	t.statisticsLastUpdated = time.Now()
	return nil
}

// MaxValue as a datum
func (t *TableInfo) MaxValue() Datum {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	return t.maxValue
}

func (t *TableInfo) wrapCastType(col string) (string, error) {
	tp, ok := t.columnsMySQLTps[col] // the tp keeps the width in this context.
	if !ok {
		return "", fmt.Errorf("column %q not found in table %s", col, t.TableName)
	}
	return fmt.Sprintf("CAST(`%s` AS %s)", col, castableTp(tp)), nil
}

// wrapCastTypeAs generates a CAST expression using sqlCol as the column reference
// in the SQL, but looks up the cast type from typeCol in this table's column types.
// This is used for column renames where the SQL column name differs from the
// type-lookup column name (e.g., source table uses old name, but cast type
// comes from the target table's new name).
func (t *TableInfo) wrapCastTypeAs(sqlCol, typeCol string) (string, error) {
	tp, ok := t.columnsMySQLTps[typeCol]
	if !ok {
		return "", fmt.Errorf("column %q not found for type lookup in table %s", typeCol, t.TableName)
	}
	return fmt.Sprintf("CAST(`%s` AS %s)", sqlCol, castableTp(tp)), nil
}

func (t *TableInfo) datumTp(col string) datumTp {
	tp, ok := t.columnsMySQLTps[col] // the tp keeps the width in this context.
	if !ok {
		panic("column not found, can not determine datumTp")
	}
	return mySQLTypeToDatumTp(tp)
}

// GetColumnMySQLType returns the MySQL type for a given column name
func (t *TableInfo) GetColumnMySQLType(col string) (string, bool) {
	tp, ok := t.columnsMySQLTps[col]
	return tp, ok
}

// GetColumnOrdinal returns the ordinal position (0-indexed) of a column by name.
// This is useful for extracting values from row slices where the position matters.
// Returns an error if the column is not found.
func (t *TableInfo) GetColumnOrdinal(columnName string) (int, error) {
	for i, col := range t.Columns {
		if col == columnName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", columnName, t.TableName)
}

// GetNonGeneratedColumnOrdinal returns the ordinal position (0-indexed) of a column by name
// within the NonGeneratedColumns slice. This is useful when working with row data that only
// contains non-generated columns (e.g., from SELECT statements that exclude generated columns).
// Returns an error if the column is not found or if it's a generated column.
func (t *TableInfo) GetNonGeneratedColumnOrdinal(columnName string) (int, error) {
	for i, col := range t.NonGeneratedColumns {
		if col == columnName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in non-generated columns of table %s", columnName, t.TableName)
}
