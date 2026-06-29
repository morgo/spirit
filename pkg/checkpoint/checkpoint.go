// Package checkpoint owns the "append-row" checkpoint table shared by the
// migration and move runners: its schema, lifecycle (create/drop), and row
// read/write. A checkpoint records where the row copy and (for migrate/move)
// the checksum got to, plus the change-feed position, so an interrupted run can
// resume instead of starting over.
//
// The table is append-only (one row per periodic dump, newest by id); resume
// reads the latest row. The package owns the table mechanics; callers own the
// interpretation of the watermarks and the resume policy (staleness max-age,
// statement match, table-name collision) via the returned Record — see Record's
// fields and Age.
//
// datasync uses a different, single-row checkpoint model and does not use this
// package.
package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/go-sql-driver/mysql"
)

// erNoSuchTable is MySQL error 1146 (ER_NO_SUCH_TABLE).
const erNoSuchTable = 1146

// ErrNotFound is returned by ReadLatest when there is no checkpoint row to
// resume from (the table is empty, or — in shared mode — holds no row for the
// requested statement). It is a normal state, distinct from a read failure.
var ErrNotFound = errors.New("checkpoint: no checkpoint found")

// Record is one checkpoint row. Watermarks and Position are opaque strings the
// caller produces and consumes; this package never interprets them.
type Record struct {
	// CopierWatermark is the row-copy resume point.
	CopierWatermark string
	// ChecksumWatermark is the checksum resume point ("" when not in / not past
	// the checksum phase, or deliberately blanked to force re-verification).
	ChecksumWatermark string
	// Position is the change-feed resume position: a single opaque position for
	// a single source (migration), or a JSON map of per-source positions for a
	// multi-source move. Stored in the binlog_position column.
	Position string
	// Statement is the migration's ALTER statement, used to match a resume to
	// its checkpoint and to scope a shared table. Empty for move.
	Statement string
	// OriginalTableName is the untruncated source table name, stored so a
	// single-table migration can detect the rare case where two long table
	// names truncate to the same checkpoint table name. Empty otherwise.
	OriginalTableName string
	// CreatedAt is when the row was written (UTC; spirit connections use
	// time_zone="+00:00").
	CreatedAt time.Time
}

// Age reports how long ago the checkpoint was written. Callers compare it to
// their configured max age to decide whether replaying from here is worthwhile.
func (r Record) Age() time.Duration { return time.Since(r.CreatedAt) }

// Table manages a checkpoint table on db at schema.name.
//
// In shared mode (multi-table migrations that share one checkpoint table per
// schema) Create/Drop/ReadLatest scope to a single statement instead of owning
// the whole table, so concurrent migrations in the same schema never clobber
// each other's rows.
type Table struct {
	db     *sql.DB
	schema string
	name   string
	shared bool
}

// NewTable returns a handle to the checkpoint table schema.name on db. Pass
// shared=true for the multi-table migration case (one table per schema, scoped
// by statement); false for a per-run table that this run owns outright.
func NewTable(db *sql.DB, schema, name string, shared bool) *Table {
	return &Table{db: db, schema: schema, name: name, shared: shared}
}

// SchemaName returns the table's schema.
func (t *Table) SchemaName() string { return t.schema }

// Name returns the table's name.
func (t *Table) Name() string { return t.name }

// tableDDL is the column list for CREATE TABLE. The schema is the union of what
// migrate and move need; move leaves statement and original_table_name empty.
const tableDDL = `(
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_position TEXT,
	statement TEXT,
	original_table_name VARCHAR(64) NOT NULL DEFAULT '',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

// Create prepares the checkpoint table for a fresh run of statement.
//
//   - Non-shared (single-table migration, move): DROP + CREATE, so the table
//     always matches this spirit version's schema.
//   - Shared (multi-table migration): CREATE IF NOT EXISTS — a concurrent
//     migration in the schema may already own the table, and a DROP would
//     destroy its live rows — then DELETE this statement's stale rows so a
//     fresh start is never mistaken for resumable progress.
//
// statement is used only in shared mode.
func (t *Table) Create(ctx context.Context, statement string) error {
	if t.shared {
		if err := dbconn.Exec(ctx, t.db, "CREATE TABLE IF NOT EXISTS %n.%n "+tableDDL, t.schema, t.name); err != nil {
			return err
		}
		if err := dbconn.Exec(ctx, t.db, "DELETE FROM %n.%n WHERE statement = %?", t.schema, t.name, statement); err != nil {
			return fmt.Errorf("could not clear stale rows from shared checkpoint table %q (if it was left behind by an incompatible spirit version, drop it manually): %w", t.name, err)
		}
		return nil
	}
	if err := dbconn.Exec(ctx, t.db, "DROP TABLE IF EXISTS %n.%n", t.schema, t.name); err != nil {
		return err
	}
	return dbconn.Exec(ctx, t.db, "CREATE TABLE %n.%n "+tableDDL, t.schema, t.name)
}

// Drop removes this run's checkpoint. Non-shared: DROP TABLE IF EXISTS. Shared:
// DELETE only this statement's rows (tolerating the table being already gone),
// so a concurrent migration's rows in the shared table survive.
func (t *Table) Drop(ctx context.Context, statement string) error {
	if t.shared {
		err := dbconn.Exec(ctx, t.db, "DELETE FROM %n.%n WHERE statement = %?", t.schema, t.name, statement)
		if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok && mysqlErr.Number == erNoSuchTable {
			return nil
		}
		return err
	}
	return dbconn.Exec(ctx, t.db, "DROP TABLE IF EXISTS %n.%n", t.schema, t.name)
}

// Write appends a checkpoint row. id and created_at are assigned by the server.
func (t *Table) Write(ctx context.Context, rec Record) error {
	return dbconn.Exec(ctx, t.db,
		"INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_position, statement, original_table_name) VALUES (%?, %?, %?, %?, %?)",
		t.schema, t.name,
		rec.CopierWatermark, rec.ChecksumWatermark, rec.Position, rec.Statement, rec.OriginalTableName,
	)
}

// ReadLatest returns the most recent checkpoint row. In shared mode it is
// filtered to statement, so another concurrent migration's newer row can't mask
// this one. Returns ErrNotFound when there is no matching row.
//
// Columns are selected explicitly (not SELECT *): a checkpoint table written by
// an incompatible spirit version that is missing a column surfaces as a read
// error, so resume fails safely rather than silently misreading.
func (t *Table) ReadLatest(ctx context.Context, statement string) (Record, error) {
	query := fmt.Sprintf(
		"SELECT copier_watermark, checksum_watermark, binlog_position, statement, original_table_name, created_at FROM `%s`.`%s`",
		t.schema, t.name)
	var args []any
	if t.shared {
		query += " WHERE statement = ?"
		args = append(args, statement)
	}
	query += " ORDER BY id DESC LIMIT 1"

	var rec Record
	var createdAt string
	err := t.db.QueryRowContext(ctx, query, args...).Scan(
		&rec.CopierWatermark, &rec.ChecksumWatermark, &rec.Position, &rec.Statement, &rec.OriginalTableName, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	// Spirit connections use time_zone="+00:00", so created_at is UTC.
	rec.CreatedAt, err = time.Parse(time.DateTime, createdAt)
	if err != nil {
		return Record{}, fmt.Errorf("could not parse checkpoint created_at timestamp: %w", err)
	}
	return rec, nil
}
