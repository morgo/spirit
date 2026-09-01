package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
)

// rewritePlaceholderTable is the table name used while re-parsing an ALTER's
// clauses into a private AST. It is never sent to MySQL: only the clauses that
// follow the table name are read back out.
const rewritePlaceholderTable = "_spirit_rewrite"

// CheckConstraintsReferenced returns the check constraint names this ALTER
// refers to by name, i.e. the names in its DROP CHECK / DROP CONSTRAINT and
// ALTER CHECK clauses. It returns nil for a statement that is not an ALTER
// TABLE, or one that names no check constraints.
//
// Note that MySQL's DROP CONSTRAINT is not specific to check constraints - it
// also drops a foreign key or a unique constraint of that name - but the parser
// folds it into the same clause type as DROP CHECK, so a name here is only a
// candidate. Callers match it against the check constraints that the table
// actually has.
func (a *AbstractStatement) CheckConstraintsReferenced() []string {
	alterStmt, ok := a.AsAlterTable()
	if !ok {
		return nil
	}
	var names []string
	for _, spec := range alterStmt.Specs {
		switch spec.Tp { //nolint:exhaustive
		case ast.AlterTableDropCheck, ast.AlterTableAlterCheck:
			if spec.Constraint != nil && spec.Constraint.Name != "" {
				names = append(names, spec.Constraint.Name)
			}
		}
	}
	return names
}

// AlterWithRenamedCheckConstraints returns this ALTER's clauses with its check
// constraint symbols rewritten for a table other than the one the user named:
// the copy algorithm's _new table, which holds the same check constraints under
// different names because check constraint names are unique per schema rather
// than per table.
//
// renames maps a lower-cased check constraint name on the user's table to the
// name the same constraint has on the table the ALTER will be applied to. Names
// in DROP CHECK / DROP CONSTRAINT and ALTER CHECK clauses are translated
// through it; a name that is not in the map is left alone, so MySQL still
// reports it as missing rather than spirit guessing at what was meant.
//
// A named check constraint being added by this same ALTER under a name it also
// drops (the "widen this constraint" idiom: DROP CHECK c, ADD CONSTRAINT c
// CHECK (...)) has its symbol removed, because the user's table still owns that
// name for as long as it exists, and adding it to a second table in the schema
// is an error. MySQL generates a name instead - the same outcome the copy
// algorithm already produces for every check constraint it copies, and the
// resolution recommended in issue #418. Those names are returned so the caller
// can report them.
func (a *AbstractStatement) AlterWithRenamedCheckConstraints(renames map[string]string) (string, []string, error) {
	if !a.IsAlterTable() {
		return "", nil, ErrNotAlterTable
	}
	// Re-parse the clauses to get an AST this function can rewrite. Editing the
	// statement's own AST would also change what the direct-DDL attempts send to
	// the user's table, and what error messages quote back to them.
	copied, err := New(fmt.Sprintf("ALTER TABLE %s %s",
		sqlescape.EscapeIdentifier(rewritePlaceholderTable), a.TrimAlter()))
	if err != nil {
		return "", nil, fmt.Errorf("could not re-parse ALTER to rewrite check constraint names: %w", err)
	}
	alterStmt, ok := copied[0].AsAlterTable()
	if !ok {
		return "", nil, ErrNotAlterTable
	}

	dropped := make(map[string]struct{})
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableDropCheck && spec.Constraint != nil {
			dropped[strings.ToLower(spec.Constraint.Name)] = struct{}{}
		}
	}

	var unnamed []string
	for _, spec := range alterStmt.Specs {
		switch spec.Tp { //nolint:exhaustive
		case ast.AlterTableDropCheck, ast.AlterTableAlterCheck:
			if spec.Constraint == nil {
				continue
			}
			if renamed, ok := renames[strings.ToLower(spec.Constraint.Name)]; ok {
				spec.Constraint.Name = renamed
			}
		case ast.AlterTableAddConstraint:
			if spec.Constraint == nil || spec.Constraint.Tp != ast.ConstraintCheck || spec.Constraint.Name == "" {
				continue
			}
			if _, ok := dropped[strings.ToLower(spec.Constraint.Name)]; !ok {
				// The name is not being freed up by this ALTER. Leave it: if it
				// is in use elsewhere in the schema, MySQL rejecting it is the
				// same answer the user would get running the ALTER themselves.
				continue
			}
			unnamed = append(unnamed, spec.Constraint.Name)
			spec.Constraint.Name = ""
		}
	}

	alter, err := alterClauses(alterStmt, format.DefaultRestoreFlags)
	if err != nil {
		return "", nil, err
	}
	return alter, unnamed, nil
}
