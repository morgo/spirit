package table

import (
	"fmt"
	"strings"
)

// ENUM/SET binlog decoding.
//
// The go-mysql binlog reader returns ENUM values as int64 ordinals
// (1-indexed) and SET values as int64 bitmasks. To replay those values
// onto a target column that has been migrated to a string type (e.g.
// VARCHAR), we need the original string elements. They are not carried
// in the binlog stream, so we recover them by parsing the column's
// `column_type` text from information_schema, which TableInfo already
// caches in columnsMySQLTps.

// isEnumColumnType reports whether a column_type string from
// information_schema is an ENUM, e.g. "enum('a','b','c')".
func isEnumColumnType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "enum(")
}

// isSetColumnType reports whether a column_type string from
// information_schema is a SET, e.g. "set('a','b','c')".
func isSetColumnType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "set(")
}

// parseEnumSetElements extracts the element list from a column_type
// string like "enum('a','b','c')" or "set('x','y','z')". It is
// fail-closed: malformed input returns an error so callers never
// silently bypass the decoder. Duplicated intentionally from
// pkg/migration/check/enumsetutil.go to avoid an import cycle; the
// two parsers are covered by their respective tests.
func parseEnumSetElements(mysqlType string) ([]string, error) {
	if !isEnumColumnType(mysqlType) && !isSetColumnType(mysqlType) {
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
		return nil, nil
	}

	var elems []string
	i := 0
	n := len(inner)
	expectValue := true
	for {
		for i < n && (inner[i] == ' ' || inner[i] == '\t') {
			i++
		}
		if expectValue {
			if i >= n {
				if len(elems) == 0 {
					return nil, fmt.Errorf("empty quoted list %q", inner)
				}
				return nil, fmt.Errorf("trailing delimiter in quoted list %q", inner)
			}
			if inner[i] != '\'' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", inner[i], i, inner)
			}
			i++
			var buf strings.Builder
			closed := false
			for i < n {
				if inner[i] == '\'' {
					if i+1 < n && inner[i+1] == '\'' {
						buf.WriteByte('\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				buf.WriteByte(inner[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted string in quoted list %q", inner)
			}
			elems = append(elems, buf.String())
			expectValue = false
		} else {
			if i >= n {
				break
			}
			if inner[i] != ',' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", inner[i], i, inner)
			}
			i++
			expectValue = true
		}
	}
	return elems, nil
}

// decodeEnumOrdinal converts a 1-indexed ENUM ordinal into the matching
// element string. Ordinal 0 is MySQL's empty-string sentinel (”) for an
// invalid value and is preserved as "". Out-of-range ordinals are an
// error rather than a silent miscoding.
func decodeEnumOrdinal(ordinal int64, elements []string) (string, error) {
	if ordinal == 0 {
		return "", nil
	}
	if ordinal < 0 || int(ordinal) > len(elements) {
		return "", fmt.Errorf("ENUM ordinal %d out of range for %d elements", ordinal, len(elements))
	}
	return elements[ordinal-1], nil
}

// decodeSetBitmask converts a SET bitmask into a comma-joined string of
// the elements whose bits are set. Bit i (0-indexed) corresponds to
// elements[i]. Bits set above len(elements) are an error.
//
// The bitmask arrives from the go-mysql binlog reader as int64 because
// that is what its decodeValue() path returns, but MySQL SET supports
// up to 64 members — a value with bit 63 set (or all 64 bits set,
// which surfaces as -1) is valid. We reinterpret the int64 as uint64
// to walk the bits; out-of-range bits are caught by the
// i >= len(elements) guard rather than by a sign check.
func decodeSetBitmask(bitmask int64, elements []string) (string, error) {
	if bitmask == 0 {
		return "", nil
	}
	bits := uint64(bitmask)
	maxBits := len(elements)
	var parts []string
	for i := range 64 {
		if bits&(uint64(1)<<i) == 0 {
			continue
		}
		if i >= maxBits {
			return "", fmt.Errorf("SET bitmask bit %d set but only %d elements defined", i, maxBits)
		}
		parts = append(parts, elements[i])
	}
	return strings.Join(parts, ","), nil
}
