package utils

import (
	"fmt"
	"strings"
)

// IsEnumType reports whether a MySQL column type string (as reported by
// information_schema.COLUMNS.column_type) is an ENUM, e.g. "enum('a','b')".
func IsEnumType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "enum(")
}

// IsSetType reports whether a MySQL column type string is a SET,
// e.g. "set('a','b')".
func IsSetType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "set(")
}

// IsEnumOrSetType reports whether a MySQL column type string is an ENUM or SET.
func IsEnumOrSetType(mysqlType string) bool {
	return IsEnumType(mysqlType) || IsSetType(mysqlType)
}

// ParseEnumSetElements extracts the element values from a MySQL type string
// like "enum('a','b','c')" or "set('x','y','z')".
//
// It uses a small state machine to correctly handle values containing
// commas and escaped (doubled) single quotes within enum/set elements.
//
// It is fail-closed: any unexpected characters or malformed structure returns
// an error so that callers (safety checks, the binlog ENUM/SET decoder) are
// never silently bypassed.
func ParseEnumSetElements(mysqlType string) ([]string, error) {
	if !IsEnumOrSetType(mysqlType) {
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
