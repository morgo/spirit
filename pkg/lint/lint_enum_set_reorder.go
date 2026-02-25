package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// EnumSetReorderLinter detects unsafe reordering of ENUM/SET values in MODIFY COLUMN
// and CHANGE COLUMN statements. MySQL stores enum/set values by their ordinal index
// internally, so reordering silently corrupts existing data. Adding new values at the
// end is fine; inserting them in the middle or rearranging is the problem.
type EnumSetReorderLinter struct{}

func init() {
	Register(&EnumSetReorderLinter{})
}

func (l *EnumSetReorderLinter) String() string {
	return Stringer(l)
}

func (l *EnumSetReorderLinter) Name() string {
	return "enum_set_reorder"
}

func (l *EnumSetReorderLinter) Description() string {
	return "Detects unsafe reordering of ENUM/SET values"
}

func (l *EnumSetReorderLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// Build a map of existing tables by name for quick lookup
	tableMap := make(map[string]*statement.CreateTable)
	for _, ct := range existingTables {
		tableMap[ct.TableName] = ct
	}

	for _, change := range changes {
		alter, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		existingTable := tableMap[change.Table]
		for _, spec := range alter.Specs {
			switch spec.Tp { //nolint: exhaustive
			case ast.AlterTableModifyColumn:
				// ALTER TABLE t1 MODIFY COLUMN col ENUM('a','b','c')
				if len(spec.NewColumns) > 0 {
					colDef := spec.NewColumns[0]
					colName := colDef.Name.Name.O
					if v := l.checkColumnReorder(change.Table, colName, colDef, existingTable); v != nil {
						violations = append(violations, *v)
					}
				}
			case ast.AlterTableChangeColumn:
				// ALTER TABLE t1 CHANGE COLUMN old_name new_name ENUM('a','b','c')
				if len(spec.NewColumns) > 0 {
					colDef := spec.NewColumns[0]
					// For CHANGE COLUMN, the old column name is used for lookup
					var lookupName string
					if spec.OldColumnName != nil {
						lookupName = spec.OldColumnName.Name.O
					} else {
						lookupName = colDef.Name.Name.O
					}
					if v := l.checkColumnReorder(change.Table, lookupName, colDef, existingTable); v != nil {
						violations = append(violations, *v)
					}
				}
			}
		}
	}
	return violations
}

// checkColumnReorder checks if a column modification reorders enum/set values.
// It compares the new column definition against the existing column in the table.
func (l *EnumSetReorderLinter) checkColumnReorder(tableName, colName string, colDef *ast.ColumnDef, existingTable *statement.CreateTable) *Violation {
	if colDef.Tp == nil {
		return nil
	}

	tp := colDef.Tp.GetType()
	if tp != mysql.TypeEnum && tp != mysql.TypeSet {
		return nil
	}

	newElems := colDef.Tp.GetElems()
	if len(newElems) == 0 {
		return nil
	}

	// If we don't have the existing table, we can't compare ordinals
	if existingTable == nil {
		return nil
	}

	// Look up the existing column
	existingCol := existingTable.Columns.ByName(colName)
	if existingCol == nil {
		return nil
	}

	var existingElems []string
	var typeName string
	if tp == mysql.TypeEnum {
		existingElems = existingCol.EnumValues
		typeName = "ENUM"
	} else {
		existingElems = existingCol.SetValues
		typeName = "SET"
	}

	if len(existingElems) == 0 {
		return nil
	}

	// Check if any existing value changed its ordinal position.
	// It's safe to append new values at the end, but inserting in the
	// middle or rearranging existing values corrupts stored data.
	reordered := detectReorder(existingElems, newElems)
	if len(reordered) == 0 {
		return nil
	}

	// Use the new column name for the violation location (may differ from lookupName for CHANGE COLUMN)
	newColName := colDef.Name.Name.O

	return &Violation{
		Linter: l,
		Location: &Location{
			Table:  tableName,
			Column: strPtr(newColName),
		},
		Message:    fmt.Sprintf("Unsafe %s value reorder detected in table %q column %q: %s. MySQL stores %s values by ordinal index, so reordering silently corrupts existing data", typeName, tableName, newColName, strings.Join(reordered, "; "), typeName),
		Severity:   SeverityError,
		Suggestion: strPtr(fmt.Sprintf("Only add new %s values at the end of the list. Do not remove, reorder, or insert values in the middle", typeName)),
	}
}

// detectReorder compares old and new enum/set element lists and returns descriptions
// of any values that changed ordinal position. Adding new values at the end is fine.
func detectReorder(oldElems, newElems []string) []string {
	var issues []string

	// Build a map of old element -> ordinal position (1-based, matching MySQL)
	oldPositions := make(map[string]int, len(oldElems))
	for i, elem := range oldElems {
		oldPositions[elem] = i + 1
	}

	// Check each existing value's position in the new list
	for newIdx, elem := range newElems {
		oldPos, existed := oldPositions[elem]
		if !existed {
			// New value — only a problem if it's not at the end (after all old values)
			// Check if there are any old values after this position
			for _, laterElem := range newElems[newIdx+1:] {
				if _, wasOld := oldPositions[laterElem]; wasOld {
					issues = append(issues, fmt.Sprintf("new value %q inserted before existing value %q", elem, laterElem))
					break
				}
			}
			continue
		}
		// Existing value — check if its ordinal position changed
		newPos := newIdx + 1
		if newPos != oldPos {
			issues = append(issues, fmt.Sprintf("value %q moved from position %d to %d", elem, oldPos, newPos))
		}
	}

	// Check for removed values (they also change effective ordinals of remaining values)
	for _, elem := range oldElems {
		found := false
		for _, newElem := range newElems {
			if newElem == elem {
				found = true
				break
			}
		}
		if !found {
			issues = append(issues, fmt.Sprintf("value %q was removed", elem))
		}
	}

	return issues
}
