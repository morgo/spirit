// Package utils contains some common utilities used by all other packages.
package utils

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

const (
	PrimaryKeySeparator = "-#-" // used to hash a composite primary key

	// MaxTableNameLength is the maximum table name length in MySQL.
	MaxTableNameLength = 64
)

// HashKey is used to convert a composite key into a string
// so that it can be placed in a map.
// Each component is escaped before joining so that values containing
// the separator cannot collide with key boundaries: without escaping,
// ("a-#-b", "c") and ("a", "b-#-c") would hash to the same string,
// causing one row's buffered change to silently overwrite another's.
func HashKey(key []any) string {
	pk := make([]string, 0, len(key))
	for _, v := range key {
		pk = append(pk, escapeHashComponent(fmt.Sprintf("%v", v)))
	}
	return strings.Join(pk, PrimaryKeySeparator)
}

// escapeHashComponent escapes a single key component so the separator can
// never appear inside it: backslashes are doubled first, then every "#" is
// prefixed with a backslash. After escaping, every "#" originating from
// user data is preceded by a backslash, while the bare "#" in the
// separator never is — so splitting on the separator is unambiguous.
func escapeHashComponent(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `#`, `\#`)
}

// unescapeHashComponent reverses escapeHashComponent: each backslash
// escapes the byte that follows it.
func unescapeHashComponent(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// UnhashKeyToString converts a hashed key to a string that can be used in a query.
// Components are unescaped (reversing HashKey's escaping) before being
// SQL-escaped, so the resulting SQL contains the original values.
func UnhashKeyToString(key string) string {
	str := strings.Split(key, PrimaryKeySeparator)
	if len(str) == 1 {
		return "'" + sqlescape.EscapeString(unescapeHashComponent(str[0])) + "'"
	}
	for i, v := range str {
		str[i] = "'" + sqlescape.EscapeString(unescapeHashComponent(v)) + "'"
	}
	return "(" + strings.Join(str, ",") + ")"
}

// TruncateTableName truncates a table name to fit within MySQL's 64-character limit,
// reserving space for a suffix of the given length. If the table name already fits,
// it is returned unchanged.
func TruncateTableName(tableName string, suffixLength int) string {
	maxLen := MaxTableNameLength - suffixLength
	if maxLen <= 0 {
		return ""
	}
	if len(tableName) > maxLen {
		return tableName[:maxLen]
	}
	return tableName
}
