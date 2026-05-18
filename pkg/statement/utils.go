package statement

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// quoteIdent wraps a MySQL identifier in backticks, doubling any embedded
// backtick to escape it per MySQL's identifier-quoting rules. The
// previous open-coded form (a Go format string with %s inside backticks)
// produced broken SQL when an identifier contained a backtick — caught
// only at re-parse time as a confusing parse error rather than at
// emission. See TestQuoteIdent for the doubled-backtick edge cases.
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// quoteIdentList applies quoteIdent to each element and joins them with
// the given separator. Helps the common "column list" rendering pattern.
func quoteIdentList(idents []string, sep string) string {
	quoted := make([]string, len(idents))
	for i, s := range idents {
		quoted[i] = quoteIdent(s)
	}
	return strings.Join(quoted, sep)
}

// formatPartitionValue renders a single partition value (e.g. the inner
// value of a VALUES LESS THAN (...) or VALUES IN (...) clause). The
// previous fmt.Sprintf("%v", v) form generated broken SQL when a string
// partition value contained a single quote.
//
// Caveat: parsePartitionClause currently restores every value to a Go
// string regardless of whether the source SQL had it as a numeric or
// string literal — so we can't simply quote on (v is string). We
// instead try to parse the string as a number; if it parses, render
// unquoted (matching RANGE/LIST partitions on integer expressions like
// YEAR(...) or hash columns). If it doesn't parse, treat as a string
// literal and quote+escape. A real type-aware fix requires preserving
// the AST literal kind through parsePartitionClause; this is a
// targeted patch on the SQL-emission side only.
func formatPartitionValue(v any) string {
	if s, ok := v.(string); ok {
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return s
		}
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return s
		}
		return "'" + sqlescape.EscapeString(s) + "'"
	}
	return fmt.Sprintf("%v", v)
}

// ptrEqual reports whether two value pointers are nil-or-equal. Both nil
// is equal; one nil and the other not is unequal; otherwise it
// dereferences and compares. Replaces the per-type stringPtrEqual /
// intPtrEqual / boolPtrEqual that this file used to carry.
func ptrEqual[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// getPreviousColumn returns the name of the column directly before
// `name` in the given slice, or "" if `name` is the first column or
// not found. Used by diffColumns to decide whether an ADD COLUMN needs
// an AFTER clause.
func getPreviousColumn(columns []Column, name string) string {
	for i, col := range columns {
		if col.Name == name {
			if i == 0 {
				return ""
			}
			return columns[i-1].Name
		}
	}
	return ""
}

// needsQuotes decides whether a column DEFAULT value needs to be wrapped
// in single quotes when emitted. SQL functions / boolean / NULL
// literals and parseable numerics are emitted bare; everything else is
// quoted as a string literal.
//
// Caveat: this is heuristic — there's no AST-level "literal kind" tag
// available at this point, so a bit literal like b'01' or a hex literal
// like 0x1A is misquoted as a string. The right fix is to thread a
// DefaultIsLiteral / kind tag through Column from the parser.
func needsQuotes(value string) bool {
	// Common SQL functions/expressions that don't need quotes
	upper := strings.ToUpper(value)
	if upper == "NULL" ||
		upper == "TRUE" ||
		upper == "FALSE" ||
		upper == "CURRENT_TIMESTAMP" ||
		upper == "NOW()" ||
		strings.HasPrefix(upper, "CURRENT_TIMESTAMP(") {
		return false
	}

	// If it parses as an integer, don't quote it
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return false
	}

	// If it parses as a float, don't quote it
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return false
	}

	// Default to quoting (for strings)
	return true
}
