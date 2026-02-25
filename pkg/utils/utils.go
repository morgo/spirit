// Package utils contains some common utilities used by all other packages.
package utils

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
)

const (
	PrimaryKeySeparator = "-#-" // used to hash a composite primary key
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

// IntersectNonGeneratedColumns returns a string of columns that are in both tables
// The column names are in backticks and comma separated.
func IntersectNonGeneratedColumns(t1, t2 *table.TableInfo) string {
	var intersection []string
	for _, col := range t1.NonGeneratedColumns {
		for _, col2 := range t2.NonGeneratedColumns {
			if col == col2 {
				intersection = append(intersection, "`"+col+"`")
			}
		}
	}
	return strings.Join(intersection, ", ")
}

// IntersectNonGeneratedColumnsAsSlice returns a slice of column names that are in both tables
func IntersectNonGeneratedColumnsAsSlice(t1, t2 *table.TableInfo) []string {
	var intersection []string
	for _, col := range t1.NonGeneratedColumns {
		for _, col2 := range t2.NonGeneratedColumns {
			if col == col2 {
				intersection = append(intersection, col)
			}
		}
	}
	return intersection
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
