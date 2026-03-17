package table

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// TableSchema represents a table's name and its raw CREATE TABLE DDL statement.
// This is the common representation used across spirit, strata, and gap for
// passing schema information between components.
type TableSchema struct {
	Name   string // Table name
	Schema string // CREATE TABLE DDL statement
}

// FilterOptions controls which tables are excluded and which DDL
// transformations are applied when loading schema from a database.
// A zero-value FilterOptions performs no filtering (all tables returned as-is).
type FilterOptions struct {
	// TablesStartingWithUnderscore filters out tables whose name begins with "_".
	// This is commonly used to exclude Spirit's internal shadow/checkpoint tables
	// and other tool-generated temporary tables.
	TablesStartingWithUnderscore bool

	// ArchiveTables filters out tables matching the archive naming convention:
	// <name>_archive_YYYY, <name>_archive_YYYY_MM, or <name>_archive_YYYY_MM_DD.
	ArchiveTables bool

	// StripAutoIncrement removes the AUTO_INCREMENT=N table option from
	// CREATE TABLE statements. This is useful when comparing schemas to
	// avoid spurious diffs caused by differing auto-increment counters.
	StripAutoIncrement bool
}

// archiveTableRegexp matches table names following the archive convention:
// <name>_archive_YYYY, <name>_archive_YYYY_MM, or <name>_archive_YYYY_MM_DD.
var archiveTableRegexp = regexp.MustCompile(`^.*_archive_[0-9]{4}(_[0-9]{2}(_[0-9]{2})?)?$`)

// autoIncrementRegexp matches the AUTO_INCREMENT table option in CREATE TABLE output.
var autoIncrementRegexp = regexp.MustCompile(`(?i)\s*AUTO_INCREMENT\s*=?\s*[0-9]+`)

// isArchiveTable returns true if the table name matches the archive naming
// convention: <name>_archive_YYYY, <name>_archive_YYYY_MM, or
// <name>_archive_YYYY_MM_DD.
func isArchiveTable(name string) bool {
	return archiveTableRegexp.MatchString(name)
}

// stripAutoIncrement removes the AUTO_INCREMENT=N table option from a
// CREATE TABLE statement. This is useful when comparing schemas to avoid
// spurious diffs caused by differing auto-increment counters.
func stripAutoIncrement(stmt string) string {
	return autoIncrementRegexp.ReplaceAllString(stmt, "")
}

// LoadSchemaFromDB retrieves all table schemas from the database using the
// provided connection. When opts is provided (at most one), tables and DDL
// are filtered according to the specified options. With no opts or a
// zero-value FilterOptions, the raw DDL is returned unmodified.
func LoadSchemaFromDB(ctx context.Context, db *sql.DB, opts ...FilterOptions) ([]TableSchema, error) {
	if len(opts) > 1 {
		return nil, fmt.Errorf("at most one FilterOptions may be provided, got %d", len(opts))
	}
	var filter FilterOptions
	if len(opts) == 1 {
		filter = opts[0]
	}

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}
	var tables []TableSchema
	for _, name := range tableNames {
		if filter.TablesStartingWithUnderscore && strings.HasPrefix(name, "_") {
			continue
		}
		if filter.ArchiveTables && isArchiveTable(name) {
			continue
		}
		var tbl, createStmt string
		err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", name)).Scan(&tbl, &createStmt)
		if err != nil {
			return nil, fmt.Errorf("failed to get CREATE TABLE for %s: %w", name, err)
		}
		if filter.StripAutoIncrement {
			createStmt = stripAutoIncrement(createStmt)
		}
		tables = append(tables, TableSchema{Name: tbl, Schema: createStmt})
	}
	return tables, nil
}
