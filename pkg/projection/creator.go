package projection

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/typeconv"
)

// TableCreator handles creating tables on target databases
type TableCreator struct {
	sourceDB   *sql.DB
	targetDB   *sql.DB
	targetType TargetType
	typeMapper typeconv.Mapper
}

// NewTableCreator creates a new TableCreator
func NewTableCreator(sourceDB, targetDB *sql.DB, targetType TargetType, typeMapper typeconv.Mapper) *TableCreator {
	return &TableCreator{
		sourceDB:   sourceDB,
		targetDB:   targetDB,
		targetType: targetType,
		typeMapper: typeMapper,
	}
}

// CreateTableFromProjection creates a table on the target database based on a projection
func (tc *TableCreator) CreateTableFromProjection(ctx context.Context, proj *Projection, sourceTable *table.TableInfo) error {
	switch tc.targetType {
	case TargetTypePostgreSQL:
		return tc.createPostgreSQLTable(ctx, proj, sourceTable)
	case TargetTypeMySQL:
		// For MySQL targets, we can use CREATE TABLE ... LIKE
		return tc.createMySQLTable(ctx, proj, sourceTable)
	default:
		return fmt.Errorf("unsupported target type: %s", tc.targetType)
	}
}

// createPostgreSQLTable creates a PostgreSQL table based on the projection
func (tc *TableCreator) createPostgreSQLTable(ctx context.Context, proj *Projection, sourceTable *table.TableInfo) error {
	// Check if table already exists
	var exists bool
	err := tc.targetDB.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
		proj.TargetTable).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		// Table already exists, skip creation
		return nil
	}

	// Build CREATE TABLE statement
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", proj.TargetTable))

	// Get target columns from projection
	targetColumns := proj.GetTargetColumns()
	sourceColumns := proj.GetSourceColumns()

	// Add columns
	columnDefs := make([]string, 0, len(targetColumns))
	for i, targetCol := range targetColumns {
		sourceCol := sourceColumns[i]

		// Get MySQL type for source column
		mysqlType, ok := sourceTable.GetColumnMySQLType(sourceCol)
		if !ok {
			return fmt.Errorf("failed to get type for source column %s", sourceCol)
		}

		// Check if there's a custom type mapping in the projection
		if customType, ok := proj.TypeMappings[targetCol]; ok {
			mysqlType = customType
		}

		// Map to PostgreSQL type
		pgType := tc.typeMapper.MapType(mysqlType)

		// Check if column is nullable
		nullable := true // Default to nullable
		// TODO: Get nullability from source table metadata if available

		// Build column definition
		colDef := fmt.Sprintf("  %s %s", targetCol, pgType)
		if !nullable && !slices.Contains(proj.PrimaryKey, targetCol) {
			// Don't add NOT NULL to PK columns (will be added in constraint)
			colDef += " NOT NULL"
		}

		columnDefs = append(columnDefs, colDef)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))

	// Add primary key constraint
	if len(proj.PrimaryKey) > 0 {
		ddl.WriteString(",\n")
		ddl.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(proj.PrimaryKey, ", ")))
	}

	ddl.WriteString("\n)")

	// Execute CREATE TABLE
	fmt.Printf("Creating table with DDL: %s\n", ddl.String())
	_, err = tc.targetDB.ExecContext(ctx, ddl.String())
	if err != nil {
		return fmt.Errorf("failed to create table: %w (DDL: %s)", err, ddl.String())
	}

	return nil
}

// createMySQLTable creates a MySQL table using CREATE TABLE ... LIKE
func (tc *TableCreator) createMySQLTable(ctx context.Context, proj *Projection, sourceTable *table.TableInfo) error {
	// For MySQL-to-MySQL, we can use CREATE TABLE ... LIKE
	// But only if the projection is a simple 1:1 mapping
	// For now, we'll use the same approach as PostgreSQL

	// Check if table already exists
	var exists int
	err := tc.targetDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		proj.TargetTable).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists > 0 {
		// Table already exists, skip creation
		return nil
	}

	// Get CREATE TABLE from source
	var tableName, createStmt string
	err = tc.sourceDB.QueryRowContext(ctx,
		fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", sourceTable.SchemaName, sourceTable.TableName)).
		Scan(&tableName, &createStmt)
	if err != nil {
		return fmt.Errorf("failed to get CREATE TABLE: %w", err)
	}

	// Replace table name with target name
	createStmt = strings.Replace(createStmt, fmt.Sprintf("CREATE TABLE `%s`", sourceTable.TableName),
		fmt.Sprintf("CREATE TABLE `%s`", proj.TargetTable), 1)

	// Execute CREATE TABLE
	_, err = tc.targetDB.ExecContext(ctx, createStmt)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}
