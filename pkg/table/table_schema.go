package table

import (
	"context"
	"database/sql"
	"fmt"
)

// TableSchema represents a table's name and its raw CREATE TABLE DDL statement.
// This is the common representation used across spirit, strata, and gap for
// passing schema information between components.
type TableSchema struct {
	Name   string // Table name
	Schema string // CREATE TABLE DDL statement
}

// LoadSchemaFromDB retrieves all table schemas from the database using the
// provided connection. It returns raw DDL strings without any filtering or
// transformation, making it suitable for use with any sql.DB driver
// (e.g. mysql, mysql-rds, strata).
func LoadSchemaFromDB(ctx context.Context, db *sql.DB) ([]TableSchema, error) {
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
		var tbl, createStmt string
		err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", name)).Scan(&tbl, &createStmt)
		if err != nil {
			return nil, fmt.Errorf("failed to get CREATE TABLE for %s: %w", name, err)
		}
		tables = append(tables, TableSchema{Name: tbl, Schema: createStmt})
	}
	return tables, nil
}
