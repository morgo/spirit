package table

import "strings"

// BINARY(N) binlog re-padding.
//
// MySQL stores BINARY(N) values right-padded with 0x00 to exactly N
// bytes, and a SELECT returns all N bytes. The binlog row image,
// however, is written via Field_string::pack(), which strips trailing
// pad bytes (0x00 for the binary charset) to save space — the reader
// is expected to re-pad to the column width. go-mysql returns the
// stripped value as-is, so without re-padding, spirit's buffered
// replay writes short values into any target column that does not
// itself re-pad (e.g. BINARY(N) -> VARBINARY(M), the gh-ost #909
// analog — see block/spirit#945). A stripped value in a binary
// PRIMARY KEY also breaks key matching: a DELETE built from the
// stripped key (0xAABBCCDD) never matches the padded row in the
// target, and watermark comparisons disagree with chunker boundaries
// read via SELECT.
//
// CHAR(N) is also stripped in the row image (trailing spaces), but
// needs no correction: a SELECT of a CHAR column strips trailing
// spaces too, so the copier and replay paths agree.

// isBinaryColumnType reports whether a column_type string from
// information_schema is a fixed-width BINARY, e.g. "binary(20)".
// VARBINARY/BLOB are variable-width and never pad, so they are
// excluded.
func isBinaryColumnType(mysqlType string) bool {
	t := strings.ToLower(mysqlType)
	return t == "binary" || strings.HasPrefix(t, "binary(")
}

// parseBinaryColumnWidth extracts N from a "binary(N)" column_type
// string. A bare "binary" is BINARY(1). Returns 0 (no padding) for
// anything malformed — fail-open is safe here because padding is an
// optimization for correctness on *short* values; an unpadded value is
// no worse than today's behaviour.
func parseBinaryColumnWidth(mysqlType string) int {
	t := strings.ToLower(mysqlType)
	if t == "binary" {
		return 1
	}
	inner, ok := strings.CutPrefix(t, "binary(")
	if !ok {
		return 0
	}
	inner, ok = strings.CutSuffix(inner, ")")
	if !ok {
		return 0
	}
	width := 0
	for _, c := range inner {
		if c < '0' || c > '9' {
			return 0
		}
		width = width*10 + int(c-'0')
	}
	return width
}

// padBinaryValue right-pads a BINARY(N) row-image value back to its
// declared width. Values already at (or somehow above) the width, and
// values of unexpected types (e.g. nil for NULL), are returned
// unchanged.
func padBinaryValue(value any, width int) any {
	switch v := value.(type) {
	case string:
		if len(v) < width {
			return v + strings.Repeat("\x00", width-len(v))
		}
	case []byte:
		if len(v) < width {
			padded := make([]byte, width)
			copy(padded, v)
			return padded
		}
	}
	return value
}
