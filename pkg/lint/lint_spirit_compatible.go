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
//     FOREIGN KEY also makes the table it references unalterable, and a new
//     table referenced by an existing table's foreign key is reported too. An
//     inline column REFERENCES counts: MySQL 9.0 and later create a foreign
//     key for it (addforeignkey).
//   - a '.' or a backtick in the table or schema name (tableidentifier). See
//     utils.UnsupportedIdentifierError.
//
// Only tables created by the changes are checked, and they are checked in
// their post-state, so a later ALTER in the same changes that fixes or renames
// the table is taken into account. A CREATE TABLE ... LIKE is checked as a
// copy of its source; one whose source is not in the schema is skipped, since
// nothing is known about it. Existing tables are left to the runtime checks: a
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
	created := createdTablesInChanges(changes)
	post := PostState(existingTables, changes)
	byName := make(map[string]*statement.CreateTable, len(post))
	for _, ct := range post {
		byName[strings.ToLower(ct.TableName)] = ct
	}
	for _, ct := range post {
		schema, isCreated := created[strings.ToLower(ct.TableName)]
		if !isCreated || ct.Temporary || isUnresolvedLike(ct) {
			continue
		}
		violations = append(violations, l.checkTable(ct, schema)...)
	}
	// A foreign key from a table the changes do not create makes a new table
	// its parent. One from a new table is reported on that table, which names
	// the parent.
	for _, child := range post {
		if _, isCreated := created[strings.ToLower(child.TableName)]; isCreated {
			continue
		}
		for _, fk := range foreignKeys(child) {
			key := strings.ToLower(fk.parent)
			parent, ok := byName[key]
			if _, isCreated := created[key]; !isCreated || !ok || parent.Temporary {
				continue
			}
			violations = append(violations, Violation{
				Linter:     l,
				Severity:   SeverityError,
				Location:   &Location{Table: parent.TableName},
				Message:    fmt.Sprintf("Spirit cannot alter table %q: table %q references it with %s", parent.TableName, child.TableName, fk.describe()),
				Suggestion: new(fmt.Sprintf("Remove the foreign key from table %q and enforce the relationship in the application", child.TableName)),
			})
		}
	}
	return violations
}

// checkTable checks a table the changes create. schema is the schema it is in
// after the changes, or "" when they do not name one.
func (l *SpiritCompatibleLinter) checkTable(ct *statement.CreateTable, schema string) []Violation {
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

	for _, id := range []struct{ kind, name string }{{"table name", tableName}, {"schema name", schema}} {
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

	for _, fk := range foreignKeys(ct) {
		v := Violation{
			Message:    fmt.Sprintf("Spirit cannot alter table %q%s: it has %s", tableName, referencedTableClause(fk.parent), fk.describe()),
			Suggestion: new("Remove the foreign key and enforce the relationship in the application"),
		}
		switch {
		case fk.column != "":
			column := fk.column
			v.Location = &Location{Table: tableName, Column: &column}
			v.Suggestion = new(fmt.Sprintf("Remove the REFERENCES clause from column %q", column))
		case fk.name != "":
			name := fk.name
			v.Location = &Location{Table: tableName, Constraint: &name}
		}
		add(v)
	}
	return violations
}

// foreignKey is a foreign key a table declares: a FOREIGN KEY constraint, or a
// column with an inline REFERENCES, which MySQL 9.0 and later create a foreign
// key for.
type foreignKey struct {
	name   string // the constraint name, "" when unnamed or inline
	column string // the column with an inline REFERENCES, "" for a constraint
	parent string // the referenced table, "" when unknown
}

func (fk foreignKey) describe() string {
	switch {
	case fk.column != "":
		return fmt.Sprintf("an inline REFERENCES on column %q, which MySQL 9.0 and later create a foreign key for", fk.column)
	case fk.name != "":
		return fmt.Sprintf("FOREIGN KEY constraint %q", fk.name)
	default:
		return "a FOREIGN KEY constraint"
	}
}

// foreignKeys returns the foreign keys ct declares.
func foreignKeys(ct *statement.CreateTable) []foreignKey {
	var fks []foreignKey
	for _, c := range ct.Constraints {
		if c.Type != "FOREIGN KEY" {
			continue
		}
		fk := foreignKey{name: c.Name}
		// PostState builds a constraint added by an ALTER from its AST alone,
		// without References.
		switch {
		case c.References != nil:
			fk.parent = c.References.Table
		case c.Raw != nil && c.Raw.Refer != nil && c.Raw.Refer.Table != nil:
			fk.parent = c.Raw.Refer.Table.Name.O
		}
		fks = append(fks, fk)
	}
	for _, col := range ct.Columns {
		if col.Raw == nil {
			continue
		}
		for _, opt := range col.Raw.Options {
			if opt.Tp != ast.ColumnOptionReference || opt.Refer == nil {
				continue
			}
			fk := foreignKey{column: col.Name}
			if opt.Refer.Table != nil {
				fk.parent = opt.Refer.Table.Name.O
			}
			fks = append(fks, fk)
		}
	}
	return fks
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
