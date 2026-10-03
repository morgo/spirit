package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	Register(&SpiritCompatibleLinter{})
}

// SpiritCompatibleLinter refuses a new table that Spirit could not alter later.
// Each rule mirrors a check in pkg/migration/check that refuses every ALTER
// against such a table, so the table can only be changed by blocking DDL or by
// first fixing it with a tool other than Spirit:
//
//   - no primary key (primarykeyexists): the copy is chunked on the key.
//   - a FLOAT or BIT primary key column (primarykeyfloat, primarykeybit): rows
//     cannot be located by key. See table.TableInfo.FloatPrimaryKeyError and
//     BitPrimaryKeyError.
//   - a foreign key, as either end of it (hasforeignkeys): a new table with a
//     FOREIGN KEY also makes the table it references unalterable. An inline
//     column REFERENCES counts: MySQL 9.0 and later create a foreign key for
//     it (addforeignkey).
//   - a '.' or a backtick in the table or schema name (tableidentifier). See
//     utils.UnsupportedIdentifierError.
//
// Only tables created by the changes are checked, and they are checked in
// their post-state, so a later ALTER in the same changes that fixes the table
// is taken into account. Existing tables are left to the runtime checks: a
// legacy table that Spirit cannot alter should not block unrelated changes.
//
// A server with sql_generate_invisible_primary_key=ON adds a primary key to a
// table created without one. The linter does not know the server's settings,
// so it still reports the table.
type SpiritCompatibleLinter struct{}

func (l *SpiritCompatibleLinter) String() string {
	return Stringer(l)
}

func (l *SpiritCompatibleLinter) Name() string {
	return "spirit_compatible"
}

func (l *SpiritCompatibleLinter) Description() string {
	return "Ensures new tables can be altered by Spirit later: a primary key with no FLOAT or BIT column, no foreign keys, and no '.' or backtick in the name"
}

func (l *SpiritCompatibleLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	created := newTablesInChanges(changes)
	for _, ct := range PostState(existingTables, changes) {
		if !created[strings.ToLower(ct.TableName)] || ct.Temporary {
			continue
		}
		violations = append(violations, l.checkTable(ct)...)
	}
	return violations
}

func (l *SpiritCompatibleLinter) checkTable(ct *statement.CreateTable) []Violation {
	var violations []Violation
	tableName := ct.GetTableName()
	add := func(v Violation) {
		v.Linter = l
		v.Severity = SeverityError
		if v.Location == nil {
			v.Location = &Location{Table: tableName}
		}
		violations = append(violations, v)
	}

	identifiers := []struct{ kind, name string }{{"table name", tableName}}
	if ct.Raw != nil && ct.Raw.Table != nil {
		identifiers = append(identifiers, struct{ kind, name string }{"schema name", ct.Raw.Table.Schema.O})
	}
	for _, id := range identifiers {
		if err := utils.UnsupportedIdentifierError(id.kind, id.name); err != nil {
			add(Violation{Message: fmt.Sprintf("Spirit cannot alter table %q: %s", tableName, err)})
		}
	}

	pkColumns := primaryKeyColumns(ct)
	if len(pkColumns) == 0 {
		add(Violation{
			Message:    fmt.Sprintf("Spirit cannot alter table %q: it has no primary key", tableName),
			Suggestion: new("Add a primary key, such as a BIGINT UNSIGNED AUTO_INCREMENT column"),
		})
	}
	for _, name := range pkColumns {
		col := columnByNameFold(ct.GetColumns(), name)
		if col == nil {
			continue
		}
		var typeName string
		switch columnMySQLType(col) {
		case mysql.TypeFloat:
			typeName = "FLOAT"
		case mysql.TypeBit:
			typeName = "BIT"
		default:
			continue
		}
		add(Violation{
			Message:    fmt.Sprintf("Spirit cannot alter table %q: primary key column %q is a %s", tableName, col.Name, typeName),
			Location:   &Location{Table: tableName, Column: &col.Name},
			Suggestion: new(fmt.Sprintf("Change primary key column %q to an integer, BINARY or VARBINARY type", col.Name)),
		})
	}

	for _, c := range ct.Constraints {
		if c.Type != "FOREIGN KEY" {
			continue
		}
		parent := ""
		if c.References != nil {
			parent = c.References.Table
		}
		v := Violation{
			Message:    fmt.Sprintf("Spirit cannot alter table %q%s: it has a FOREIGN KEY constraint", tableName, referencedTableClause(parent)),
			Suggestion: new("Remove the foreign key and enforce the relationship in the application"),
		}
		if c.Name != "" {
			name := c.Name
			v.Message = fmt.Sprintf("Spirit cannot alter table %q%s: it has FOREIGN KEY constraint %q", tableName, referencedTableClause(parent), name)
			v.Location = &Location{Table: tableName, Constraint: &name}
		}
		add(v)
	}
	for i := range ct.Columns {
		col := &ct.Columns[i]
		ref := inlineReference(col)
		if ref == nil {
			continue
		}
		parent := ""
		if ref.Table != nil {
			parent = ref.Table.Name.O
		}
		add(Violation{
			Message: fmt.Sprintf("Spirit cannot alter table %q%s: column %q is declared with an inline REFERENCES, which MySQL 9.0 and later create a foreign key for",
				tableName, referencedTableClause(parent), col.Name),
			Location:   &Location{Table: tableName, Column: &col.Name},
			Suggestion: new(fmt.Sprintf("Remove the REFERENCES clause from column %q", col.Name)),
		})
	}
	return violations
}

// referencedTableClause names the parent of a foreign key, which Spirit cannot
// alter either once the foreign key exists.
func referencedTableClause(parent string) string {
	if parent == "" {
		return ""
	}
	return fmt.Sprintf(" or the table it references (%q)", parent)
}

// primaryKeyColumns returns the columns of the table's primary key, declared
// either inline on a column or at table level.
func primaryKeyColumns(ct *statement.CreateTable) []string {
	for _, index := range ct.GetIndexes() {
		if index.Type == "PRIMARY KEY" {
			return index.Columns
		}
	}
	return nil
}

// columnMySQLType returns the column's MySQL type from its definition, or
// mysql.TypeUnspecified when the definition does not carry one.
func columnMySQLType(col *statement.Column) byte {
	if col.Raw == nil || col.Raw.Tp == nil {
		return mysql.TypeUnspecified
	}
	return col.Raw.Tp.GetType()
}

// inlineReference returns the column's inline REFERENCES clause, or nil.
func inlineReference(col *statement.Column) *ast.ReferenceDef {
	if col.Raw == nil {
		return nil
	}
	for _, opt := range col.Raw.Options {
		if opt.Tp == ast.ColumnOptionReference && opt.Refer != nil {
			return opt.Refer
		}
	}
	return nil
}
