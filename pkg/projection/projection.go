package projection

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

// Projection represents a table projection that defines how data is synced
type Projection struct {
	Name         string
	SourceTable  string
	TargetTable  string
	Query        string
	PrimaryKey   []string
	TypeMappings map[string]string

	// Parsed information
	columns     []ProjectionColumn
	parsedQuery *ast.SelectStmt
}

// ProjectionColumn represents a column in the projection
type ProjectionColumn struct {
	SourceExpr   string // e.g., "user_id", "UPPER(name)"
	TargetColumn string // Target column name
	SourceColumn string // Source column name (if simple reference)
}

// Config is a simple configuration for creating a projection
type Config struct {
	Name         string
	SourceTable  string
	TargetTable  string
	Query        string
	PrimaryKey   []string
	TypeMappings map[string]string
}

// NewProjection creates a new Projection from configuration
func NewProjection(config Config) (*Projection, error) {
	proj := &Projection{
		Name:         config.Name,
		SourceTable:  config.SourceTable,
		TargetTable:  config.TargetTable,
		Query:        config.Query,
		PrimaryKey:   config.PrimaryKey,
		TypeMappings: config.TypeMappings,
	}

	if err := proj.Parse(); err != nil {
		return nil, fmt.Errorf("failed to parse projection: %w", err)
	}

	return proj, nil
}

// Parse parses the projection query and extracts column information
func (p *Projection) Parse() error {
	// Use TiDB parser to parse the SELECT statement
	parser := parser.New()
	stmt, err := parser.ParseOneStmt(p.Query, "", "")
	if err != nil {
		return fmt.Errorf("failed to parse projection query: %w", err)
	}

	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return fmt.Errorf("projection query must be a SELECT statement")
	}

	p.parsedQuery = selectStmt

	// Extract column information
	if selectStmt.Fields == nil {
		return fmt.Errorf("projection query must have a field list")
	}

	for _, field := range selectStmt.Fields.Fields {
		col := ProjectionColumn{}

		// Get the expression as string
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		if err := field.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("failed to restore expression: %w", err)
		}
		col.SourceExpr = buf.String()

		// Get the target column name (alias or expression)
		if field.AsName.String() != "" {
			col.TargetColumn = field.AsName.String()
		} else {
			// If no alias, try to extract column name
			if colName, ok := field.Expr.(*ast.ColumnNameExpr); ok {
				col.TargetColumn = colName.Name.Name.String()
				col.SourceColumn = colName.Name.Name.String()
			} else {
				return fmt.Errorf("expression %s must have an alias", col.SourceExpr)
			}
		}

		// If it's a simple column reference, set SourceColumn
		if colName, ok := field.Expr.(*ast.ColumnNameExpr); ok {
			col.SourceColumn = colName.Name.Name.String()
		}

		p.columns = append(p.columns, col)
	}

	return nil
}

// IsStillValid checks if the projection is still valid after a DDL change
func (p *Projection) IsStillValid(ctx context.Context, db *sql.DB) (bool, error) {
	// Try to execute the projection query with LIMIT 0
	// If it succeeds, projection is still valid
	testQuery := fmt.Sprintf("SELECT * FROM (%s) AS projection LIMIT 0", p.Query)
	_, err := db.QueryContext(ctx, testQuery)
	if err != nil {
		return false, fmt.Errorf("projection no longer valid: %w", err)
	}
	return true, nil
}

// GetReferencedColumns returns the list of source columns referenced in the projection
func (p *Projection) GetReferencedColumns() []string {
	var cols []string
	seen := make(map[string]bool)

	for _, col := range p.columns {
		if col.SourceColumn != "" && !seen[col.SourceColumn] {
			cols = append(cols, col.SourceColumn)
			seen[col.SourceColumn] = true
		}
	}

	return cols
}

// GetColumns returns the projection columns
func (p *Projection) GetColumns() []ProjectionColumn {
	return p.columns
}

// GetTargetColumns returns the list of target column names
func (p *Projection) GetTargetColumns() []string {
	cols := make([]string, len(p.columns))
	for i, col := range p.columns {
		cols[i] = col.TargetColumn
	}
	return cols
}

// GetSourceColumns returns the list of source column names (for simple projections)
func (p *Projection) GetSourceColumns() []string {
	cols := make([]string, len(p.columns))
	for i, col := range p.columns {
		if col.SourceColumn != "" {
			cols[i] = col.SourceColumn
		} else {
			// For expressions, use the target column name
			cols[i] = col.TargetColumn
		}
	}
	return cols
}
