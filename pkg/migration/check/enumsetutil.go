package check

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// parseEnumSetValues extracts the element values from a MySQL type string
// like "enum('a','b','c')" or "set('x','y','z')".
//
// It uses a small state machine to correctly handle values containing
// commas and escaped (doubled) single quotes within enum/set elements.
//
// It is fail-closed: any unexpected characters or malformed structure returns
// an error so that callers (safety checks) are never silently bypassed.
func parseEnumSetValues(mysqlType string) ([]string, error) {
	lower := strings.ToLower(mysqlType)
	if !strings.HasPrefix(lower, "enum(") && !strings.HasPrefix(lower, "set(") {
		return nil, fmt.Errorf("not an enum/set type: %q", mysqlType)
	}

	start := strings.IndexByte(mysqlType, '(')
	if start < 0 {
		return nil, fmt.Errorf("missing opening parenthesis in type: %q", mysqlType)
	}
	end := strings.LastIndexByte(mysqlType, ')')
	if end <= start {
		return nil, fmt.Errorf("missing closing parenthesis in type: %q", mysqlType)
	}

	inner := mysqlType[start+1 : end]
	if len(inner) == 0 {
		return nil, nil // e.g. enum() — valid but empty
	}

	return parseSQLQuotedList(inner)
}

// parseSQLQuotedList parses a comma-separated list of single-quoted SQL
// strings, correctly handling embedded commas and escaped (doubled) quotes.
//
// It is fail-closed: any unexpected character outside of quotes causes the
// parser to return an error rather than silently producing a partial result.
func parseSQLQuotedList(s string) ([]string, error) {
	var elems []string
	i := 0
	n := len(s)

	// expectValue tracks whether we are currently expecting the start of a
	// quoted value (true) or a delimiter/completion after a value (false).
	expectValue := true

	for {
		// Skip whitespace outside of values.
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}

		if expectValue {
			// We are expecting the start of a quoted value.
			if i >= n {
				// Empty input is handled by the caller; reaching EOF here
				// indicates a trailing delimiter or otherwise malformed input.
				if len(elems) == 0 {
					return nil, fmt.Errorf("empty quoted list %q", s)
				}
				return nil, fmt.Errorf("trailing delimiter in quoted list %q", s)
			}
			if s[i] != '\'' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", s[i], i, s)
			}

			// Opening quote found; collect the value.
			i++ // skip opening quote
			var buf strings.Builder
			closed := false
			for i < n {
				if s[i] == '\'' {
					// Check for doubled quote (escape sequence).
					if i+1 < n && s[i+1] == '\'' {
						buf.WriteByte('\'')
						i += 2
						continue
					}
					// Closing quote.
					i++
					closed = true
					break
				}
				buf.WriteByte(s[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted string in quoted list %q", s)
			}
			elems = append(elems, buf.String())
			// Next we expect either a comma delimiter or the end of the list.
			expectValue = false
		} else {
			// We have just parsed a value; now expect either a comma or EOF.
			if i >= n {
				// No more input; successfully parsed all elements.
				break
			}
			if s[i] != ',' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", s[i], i, s)
			}
			// Consume the comma and loop back to parse the next value.
			i++
			expectValue = true
		}
	}

	return elems, nil
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

// modifiedEnumSetColumn describes a column in an ALTER TABLE that is being
// modified and has an ENUM or SET type. It captures the information needed
// by the enum and set reorder checks.
type modifiedEnumSetColumn struct {
	LookupName string         // column name to look up in the existing table
	ColDef     *ast.ColumnDef // the new column definition from the ALTER
	IsSet      bool           // true for SET, false for ENUM
}

// findModifiedEnumSetColumns extracts all MODIFY COLUMN and CHANGE COLUMN
// specs from an ALTER TABLE statement that target ENUM or SET columns.
// These are the only two ALTER specs that can redefine a column's type.
func findModifiedEnumSetColumns(stmtNode ast.StmtNode) []modifiedEnumSetColumn {
	alterStmt, ok := stmtNode.(*ast.AlterTableStmt)
	if !ok {
		return nil
	}

	var result []modifiedEnumSetColumn
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

		tp := colDef.Tp.GetType()
		if tp != mysql.TypeEnum && tp != mysql.TypeSet {
			continue
		}

		result = append(result, modifiedEnumSetColumn{
			LookupName: lookupName,
			ColDef:     colDef,
			IsSet:      tp == mysql.TypeSet,
		})
	}
	return result
}
