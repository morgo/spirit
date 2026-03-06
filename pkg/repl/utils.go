package repl

import (
	"strings"

	"github.com/block/spirit/pkg/utils"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// pksToRowValueConstructor constructs a statement like this:
// DELETE FROM x WHERE (s_i_id,s_w_id) in ((7,10),(1,5));
func pksToRowValueConstructor(d []string) string {
	var pkValues []string
	for _, v := range d {
		pkValues = append(pkValues, utils.UnhashKeyToString(v))
	}
	return strings.Join(pkValues, ",")
}

type statement struct {
	numKeys int
	stmt    string
}

func extractStmt(stmts []statement) []string {
	var trimmed []string
	for _, stmt := range stmts {
		if stmt.stmt != "" {
			trimmed = append(trimmed, stmt.stmt)
		}
	}
	return trimmed
}

func encodeSchemaTable(schema, table string) string {
	return schema + "." + table
}

// getTableIdentity extracts the schema and table name from an AST node that has table information
func getTableIdentity(defaultSchema string, node ast.Node) (string, string) {
	var schema, table string
	switch t := node.(type) {
	case *ast.TableName:
		schema = t.Schema.String()
		table = t.Name.String()
	case *ast.TableSource:
		if tn, ok := t.Source.(*ast.TableName); ok {
			schema = tn.Schema.String()
			table = tn.Name.String()
		}
	}
	if schema == "" {
		schema = defaultSchema
	}
	return schema, table
}

// schemaTable is a parsed schema and table name pair extracted from a DDL statement.
type schemaTable struct {
	schema string
	table  string
}

// extractTablesFromDDLStmts extracts table names from DDL statements.
// The logic is based on canal: https://github.com/go-mysql-org/go-mysql/blob/34b6b0998dde44e51dff0bbcc1ac88339f57f830/canal/sync.go#L195-L245
func extractTablesFromDDLStmts(defaultSchema string, statements string) ([]schemaTable, error) {
	p := parser.New()
	stmts, _, err := p.Parse(statements, "", "")
	if err != nil {
		return nil, err
	}
	var tables []schemaTable
	for _, stmt := range stmts {
		switch t := stmt.(type) {
		case *ast.RenameTableStmt:
			for _, tableInfo := range t.TableToTables {
				schema, table := getTableIdentity(defaultSchema, tableInfo.OldTable)
				tables = append(tables, schemaTable{schema, table})
			}
		case *ast.DropTableStmt:
			for _, table := range t.Tables {
				schema, tableName := getTableIdentity(defaultSchema, table)
				tables = append(tables, schemaTable{schema, tableName})
			}
		case *ast.AlterTableStmt, *ast.CreateTableStmt, *ast.TruncateTableStmt,
			*ast.CreateIndexStmt, *ast.DropIndexStmt:
			var tableNode *ast.TableName
			switch n := t.(type) {
			case *ast.AlterTableStmt:
				tableNode = n.Table
			case *ast.CreateTableStmt:
				tableNode = n.Table
			case *ast.TruncateTableStmt:
				tableNode = n.Table
			case *ast.CreateIndexStmt:
				tableNode = n.Table
			case *ast.DropIndexStmt:
				tableNode = n.Table
			}
			schema, table := getTableIdentity(defaultSchema, tableNode)
			tables = append(tables, schemaTable{schema, table})
		}
	}
	return tables, nil
}
