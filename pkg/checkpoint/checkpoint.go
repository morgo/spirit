// Package checkpoint owns the append-row checkpoint table shared by all three
// runners (migration, move, datasync): its schema, lifecycle (create / drop /
// exists), and row read/write. A checkpoint records where the row copy and
// (for migrate/move) the checksum got to, plus the change-feed position, so an
// interrupted run can resume instead of starting over.
//
// Single-owner tables (Owned, Persistent) keep one row, overwritten in place
// each dump; the multi-table Shared table appends one row per statement. Either
// way resume reads the newest row. Three Modes cover the runners' differing
// lifecycles — see Mode. The package owns the table mechanics; callers own the
// interpretation of the watermarks and the resume policy (staleness max-age,
// statement match, table-name collision, and — for datasync — using Exists as
// the resume signal) via the returned Record, Age, and Exists.
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

// Mode selects how a Table is created, cleared, and read, matching the three
// checkpoint lifecycles in spirit.
type Mode int

const (
	// Owned is a per-run table this run owns outright: Create DROPs and
	// recreates it (so it always matches this version's schema and carries no
	// stale rows), Write keeps a single row (overwrites in place), and Drop
	// drops it. Used by single-table migrations and by move.
	Owned Mode = iota
	// Shared is one table per schema shared by concurrently-running multi-table
	// migrations: Create is idempotent and clears only this statement's rows,
	// Write appends (one migration's row must not overwrite another's), Drop
	// deletes only this statement's rows, and ReadLatest is scoped to this
	// statement — so concurrent migrations never clobber or mask each other.
	Shared
	// Persistent is a long-lived table whose existence outlives any single run,
	// used by datasync: Create is idempotent and never clears, Write keeps a
	// single row, and Drop drops it. The caller decides fresh-vs-resume from
	// Exists and ReadLatest (not by recreating the table), so Create must never
	// destroy the rows a resume needs.
	Persistent
)

// Table manages a checkpoint table on db at schema.name. mode selects its
// create / clear / read behaviour; see Mode.
type Table struct {
	db     *sql.DB
	schema string
	name   string
	mode   Mode
}

// NewTable returns a handle to the checkpoint table schema.name on db, with the
// lifecycle selected by mode.
func NewTable(db *sql.DB, schema, name string, mode Mode) *Table {
	return &Table{db: db, schema: schema, name: name, mode: mode}
}

// SchemaName returns the table's schema.
func (t *Table) SchemaName() string { return t.schema }

// Name returns the table's name.
func (t *Table) Name() string { return t.name }

// tableDDL is the column list for CREATE TABLE. The schema is the union of what
// the runners need; move and datasync leave statement and original_table_name
// empty. id is the primary key single-owner modes REPLACE on to keep one row.
const tableDDL = `(
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_position TEXT,
	statement TEXT,
	original_table_name VARCHAR(64) NOT NULL DEFAULT '',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

// Create prepares the checkpoint table for a run. Behaviour depends on Mode:
//
//   - Owned (single-table migration, move): DROP + CREATE, so the table always
//     matches this spirit version's schema and carries no stale rows.
//   - Shared (multi-table migration): CREATE IF NOT EXISTS — a concurrent
//     migration in the schema may already own the table, and a DROP would
//     destroy its live rows — then DELETE this statement's stale rows so a
//     fresh start is never mistaken for resumable progress.
//   - Persistent (datasync): CREATE IF NOT EXISTS and nothing else. The table
//     is long-lived and its existence is the caller's resume signal, so it is
//     never dropped or cleared here.
//
// statement is used only in Shared mode.
func (t *Table) Create(ctx context.Context, statement string) error {
	switch t.mode {
	case Owned:
		if err := dbconn.Exec(ctx, t.db, "DROP TABLE IF EXISTS %n.%n", t.schema, t.name); err != nil {
			return err
		}
		return dbconn.Exec(ctx, t.db, "CREATE TABLE %n.%n "+tableDDL, t.schema, t.name)
	case Shared:
		if err := dbconn.Exec(ctx, t.db, "CREATE TABLE IF NOT EXISTS %n.%n "+tableDDL, t.schema, t.name); err != nil {
			return err
		}
		if err := dbconn.Exec(ctx, t.db, "DELETE FROM %n.%n WHERE statement = %?", t.schema, t.name, statement); err != nil {
			return fmt.Errorf("could not clear stale rows from shared checkpoint table %q (if it was left behind by an incompatible spirit version, drop it manually): %w", t.name, err)
		}
		return nil
	case Persistent:
		return dbconn.Exec(ctx, t.db, "CREATE TABLE IF NOT EXISTS %n.%n "+tableDDL, t.schema, t.name)
	default:
		return fmt.Errorf("checkpoint: unknown table mode %d", t.mode)
	}
}

// Drop removes this run's checkpoint. Owned/Persistent: DROP TABLE IF EXISTS.
// Shared: DELETE only this statement's rows (tolerating the table being already
// gone), so a concurrent migration's rows in the shared table survive.
func (t *Table) Drop(ctx context.Context, statement string) error {
	if t.mode == Shared {
		err := dbconn.Exec(ctx, t.db, "DELETE FROM %n.%n WHERE statement = %?", t.schema, t.name, statement)
		if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok && mysqlErr.Number == erNoSuchTable {
			return nil
		}
		return err
	}
	return dbconn.Exec(ctx, t.db, "DROP TABLE IF EXISTS %n.%n", t.schema, t.name)
}

// Write records a checkpoint row.
//
// Owned and Persistent keep a single row, overwriting it in place (REPLACE on
// the fixed primary key id=1): these tables have one logical owner, so there is
// no value in accumulating history, and REPLACE is one atomic statement — a
// crash mid-write leaves either the old row or the new one, never none. Shared
// appends instead, because one multi-table migration's row must not overwrite a
// concurrent migration's; those rows are bounded by the migration's runtime and
// ReadLatest picks the newest per statement.
//
// created_at is (re)assigned by the server on each write.
func (t *Table) Write(ctx context.Context, rec Record) error {
	if t.mode == Shared {
		return dbconn.Exec(ctx, t.db,
			"INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_position, statement, original_table_name) VALUES (%?, %?, %?, %?, %?)",
			t.schema, t.name,
			rec.CopierWatermark, rec.ChecksumWatermark, rec.Position, rec.Statement, rec.OriginalTableName,
		)
	}
	return dbconn.Exec(ctx, t.db,
		"REPLACE INTO %n.%n (id, copier_watermark, checksum_watermark, binlog_position, statement, original_table_name) VALUES (1, %?, %?, %?, %?, %?)",
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
	if t.mode == Shared {
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

// Exists reports whether the checkpoint table exists. datasync uses this as its
// resume signal: the table is created before any rows are copied, so its
// presence means a prior run already owns the target — even one that died
// before writing its first checkpoint row. Uses information_schema, so it works
// on a connection with no database selected (datasync's pre-open admin check).
func (t *Table) Exists(ctx context.Context) (bool, error) {
	var one int
	err := t.db.QueryRowContext(ctx,
		"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
		t.schema, t.name).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
