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
// caches in columnsMySQLTps (see utils.ParseEnumSetElements).

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
