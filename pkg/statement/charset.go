package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

// This file holds the exported charset/collation helpers used by callers
// outside the diff — notably pkg/lint, which compares columns across
// *different* tables rather than the two sides of one table's diff.

// DefaultCollationForCharset returns the charset and the collation MySQL
// applies to it when no COLLATE is written, and whether cs names a charset the
// parser knows. Both are spelled the way EffectiveCharsetCollation spells
// them, so values from the two can be compared directly — callers that need to
// supply a default for DDL which declares no charset at all should come
// through here rather than reading the parser's registry themselves.
func DefaultCollationForCharset(name string) (cs, collation string, ok bool) {
	def, ok := charset.MySQLDefaultCollation(name)
	if !ok {
		return "", "", false
	}
	return normalizeCharsetName(strings.ToLower(name)), normalizeCollationName(strings.ToLower(def)), true
}

// normalizeCharsetName folds the legacy "utf8" spelling of the 3-byte UTF-8
// charset onto MySQL 8.0's "utf8mb3".
func normalizeCharsetName(cs string) string {
	if cs == charset.CharsetUTF8 {
		return charset.CharsetUTF8MB3
	}
	return cs
}

// normalizeCollationName is normalizeCharsetName for collation names, which
// are their charset's name plus a suffix.
func normalizeCollationName(collation string) string {
	if rest, ok := strings.CutPrefix(collation, charset.CharsetUTF8+"_"); ok {
		return charset.CharsetUTF8MB3 + "_" + rest
	}
	return collation
}

// charsetDefaultCollationIsFixed reports whether a charset named without a
// collation takes the same collation on every server. utf8mb4's is the
// server's default_collation_for_utf8mb4, which a server can set to
// utf8mb4_general_ci, so naming utf8mb4 alone does not decide its collation.
// An empty charset is one the definition does not name.
func charsetDefaultCollationIsFixed(cs string) bool {
	return cs != "" && !strings.EqualFold(cs, charset.CharsetUTF8MB4)
}
