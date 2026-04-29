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
func HashKey(key []any) string {
	var pk []string
	for _, v := range key {
		pk = append(pk, fmt.Sprintf("%v", v))
	}
	return strings.Join(pk, PrimaryKeySeparator)
}

// UnhashKeyToString converts a hashed key to a string that can be used in a query.
func UnhashKeyToString(key string) string {
	str := strings.Split(key, PrimaryKeySeparator)
	if len(str) == 1 {
		return "'" + sqlescape.EscapeString(str[0]) + "'"
	}
	for i, v := range str {
		str[i] = "'" + sqlescape.EscapeString(v) + "'"
	}
	return "(" + strings.Join(str, ",") + ")"
}

// TruncateTableName truncates a table name to fit within MySQL's 64-character limit,
// reserving space for a suffix of the given length. If the table name already fits,
// it is returned unchanged.
func TruncateTableName(tableName string, suffixLength int) string {
	maxLen := MaxTableNameLength - suffixLength
	if len(tableName) > maxLen {
		return tableName[:maxLen]
	}
	return tableName
}
