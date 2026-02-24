package lint

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
)

// LoadSchemaFromDSN connects to a MySQL server and retrieves all CREATE TABLE
// statements from the connected database.
func LoadSchemaFromDSN(ctx context.Context, dsn string) ([]*statement.CreateTable, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer utils.CloseAndLog(rows)

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

	var tables []*statement.CreateTable
	for _, name := range tableNames {
		var tbl, createStmt string
		err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", name)).Scan(&tbl, &createStmt)
		if err != nil {
			return nil, fmt.Errorf("failed to get CREATE TABLE for %s: %w", name, err)
		}

		ct, err := statement.ParseCreateTable(createStmt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse CREATE TABLE for %s: %w", name, err)
		}
		tables = append(tables, ct)
	}

	return tables, nil
}

// LoadSchemaFromDir reads all .sql files from a directory and parses them as
// CREATE TABLE statements. Each file should contain exactly one CREATE TABLE statement.
func LoadSchemaFromDir(dir string) ([]*statement.CreateTable, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	var tables []*statement.CreateTable
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", path, err)
		}

		ct, err := statement.ParseCreateTable(string(content))
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", path, err)
		}
		tables = append(tables, ct)
	}

	return tables, nil
}
