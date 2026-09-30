package check

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
)

// requireCurrentColumnTypes reports whether Resources carries the table metadata
// the ENUM/SET checks need to compare a redeclared column against its current
// definition.
//
// A ScopeStatement caller may not have the table's DDL to build it from, so the
// check is skipped rather than guessing at the current type (see
// ScopeStatement). Every other scope runs inside a migration, which loads the
// table before it runs any check: metadata missing there means these three
// guards against a data-corrupting ENUM/SET change would not run at all, so the
// check fails rather than passing quietly.
func requireCurrentColumnTypes(r Resources, logger *slog.Logger, checkName string) (bool, error) {
	if r.Table != nil {
		return true, nil
	}
	if r.scope&ScopeStatement != 0 {
		logger.Debug("skipping check: no table metadata supplied, cannot compare against the current column types",
			"check", checkName)
		return false, nil
	}
	return false, fmt.Errorf("check %s cannot run: the table's current column types were not loaded", checkName)
}

// storedNewMembers returns the members MySQL stores for col's redeclared ENUM
// or SET definition, which is what the current members must be compared
// against. MySQL strips each member's trailing spaces unless the column's
// charset is binary, where 'b ' and 'b' are different members (see
// statement.StoredEnumSetMembers).
//
// Only a member that ends in a space needs the charset. When the column
// inherits the table default and the table metadata does not carry it — a
// TableInfo populated by SetInfo does not — the default is read from
// information_schema. A charset that still cannot be resolved fails the check
// with cannotClassify rather than guessing.
func storedNewMembers(ctx context.Context, r Resources, col modifiedColumn) ([]string, error) {
	tableDefault := statement.CharsetCollation{Charset: r.Table.DefaultCharset, Collation: r.Table.DefaultCollation}
	members, determined, err := r.Statement.StoredEnumSetMembers(col.ColDef, tableDefault)
	if err != nil {
		return nil, err
	}
	if !determined && tableDefault == (statement.CharsetCollation{}) && r.DB != nil {
		var collation string
		// Read the table SetInfo read, which it finds by DATABASE().
		if err := r.DB.QueryRowContext(ctx, "SELECT IFNULL(table_collation, '') FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?",
			r.Table.TableName).Scan(&collation); err != nil {
			return nil, cannotClassify("unable to read the default collation of table %q: %w", r.Table.TableName, err)
		}
		members, determined, err = r.Statement.StoredEnumSetMembers(col.ColDef, statement.CharsetCollation{Collation: collation})
		if err != nil {
			return nil, err
		}
	}
	if !determined {
		return nil, cannotClassify("unable to validate the members of column %q: whether MySQL keeps their trailing spaces depends on the table's default charset, which is not known", col.LookupName)
	}
	return members, nil
}

// isPrefix returns true if oldElems is a prefix of newElems.
// This is the safe case: all existing values remain at the same ordinal
// positions, and new values are only appended at the end.
func isPrefix(oldElems, newElems []string) bool {
	if len(newElems) < len(oldElems) {
		return false
	}
	for i, elem := range oldElems {
		if newElems[i] != elem {
			return false
		}
	}
	return true
}

// isCompatibleEnumChange returns true if newElems is a binlog-ordinal-safe
// modification of existingElems:
//
//   - Values present in BOTH lists must appear in the same RELATIVE ORDER
//     (existing values may be dropped from anywhere, but not moved).
//   - Values in newElems that are NOT in existingElems must appear only
//     AFTER all retained existing values (new values are appended at end,
//     never interleaved among or before existing values).
//
// The buffered replay path decodes binlog ENUM ordinals against the SOURCE
// table's element list and writes the resulting string into the target.
// Any retained value therefore decodes to the same string the target
// accepts. Rows whose value was dropped from the new enum land in the
// target as ” (the empty string — MySQL's invalid-ENUM sentinel, ordinal
// 0), which the post-cutover checksum catches as a mismatch, so the
// migration fails closed if such rows existed.
func isCompatibleEnumChange(existingElems, newElems []string) bool {
	existingSet := make(map[string]struct{}, len(existingElems))
	for _, e := range existingElems {
		existingSet[e] = struct{}{}
	}

	j := 0
	sawNew := false
	for _, e := range newElems {
		if _, isExisting := existingSet[e]; isExisting {
			if sawNew {
				return false // an existing value appears after a brand-new value
			}
			for j < len(existingElems) && existingElems[j] != e {
				j++
			}
			if j >= len(existingElems) {
				return false // existing value out of relative order, or repeated in newElems
			}
			j++
		} else {
			sawNew = true
		}
	}
	return true
}

// modifiedColumn describes a column in an ALTER TABLE that is being
// modified via MODIFY COLUMN or CHANGE COLUMN.
type modifiedColumn struct {
	LookupName string         // column name to look up in the existing table
	ColDef     *ast.ColumnDef // the new column definition from the ALTER
}

// findModifiedColumns extracts all MODIFY COLUMN and CHANGE COLUMN
// specs from an ALTER TABLE statement.
// These are the only two ALTER specs that can redefine a column's type.
func findModifiedColumns(stmtNode ast.StmtNode) []modifiedColumn {
	alterStmt, ok := stmtNode.(*ast.AlterTableStmt)
	if !ok {
		return nil
	}

	var result []modifiedColumn
	for _, spec := range alterStmt.Specs {
		var colDef *ast.ColumnDef
		var lookupName string

		switch spec.Tp { //nolint: exhaustive
		case ast.AlterTableModifyColumn:
			if len(spec.NewColumns) > 0 {
				colDef = spec.NewColumns[0]
				lookupName = colDef.Name.Name.O
			}
		case ast.AlterTableChangeColumn:
			if len(spec.NewColumns) > 0 {
				colDef = spec.NewColumns[0]
				if spec.OldColumnName != nil {
					lookupName = spec.OldColumnName.Name.O
				} else {
					lookupName = colDef.Name.Name.O
				}
			}
		}

		if colDef == nil || colDef.Tp == nil {
			continue
		}

		result = append(result, modifiedColumn{
			LookupName: lookupName,
			ColDef:     colDef,
		})
	}
	return result
}

// findModifiedEnumSetColumns returns only the modified columns whose new type
// is ENUM or SET.
func findModifiedEnumSetColumns(stmtNode ast.StmtNode) []modifiedColumn {
	var result []modifiedColumn
	for _, col := range findModifiedColumns(stmtNode) {
		tp := col.ColDef.Tp.GetType()
		if tp == mysql.TypeEnum || tp == mysql.TypeSet {
			result = append(result, col)
		}
	}
	return result
}
