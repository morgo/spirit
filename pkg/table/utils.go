package table

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// LazyFindP90 finds the second to last value in a slice.
// This is the same as a p90 if there are 10 values, but if
// there were 100 values it would technically be a p99 etc.
func LazyFindP90(a []time.Duration) time.Duration {
	slices.SortFunc(a, func(x, y time.Duration) int {
		return cmp.Compare(y, x) // descending
	})
	return a[len(a)/10]
}

// lazyFindP90Uint64 is the byte-signal twin of LazyFindP90, used by the
// memory-based dynamic chunker (see dynamicChunkSizer.TargetChunkBytes).
func lazyFindP90Uint64(a []uint64) uint64 {
	slices.SortFunc(a, func(x, y uint64) int {
		return cmp.Compare(y, x) // descending
	})
	return a[len(a)/10]
}

// castableTp returns an approximate type that tp can be casted to.
// This is because in the context of CAST()/CONVERT() MySQL will
// not allow all of the built-in types, but instead follows SQL standard
// types, which we need to map to.
func castableTp(tp string) string {
	newTp := removeWidth(tp)
	newTp = removeEnumSetOpts(newTp)
	newTp = removeZerofill(newTp)
	newTp = removeDecimalWidth(newTp)
	switch newTp {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return "signed"
	case "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "int unsigned", "bigint unsigned":
		return "unsigned"
	case "timestamp", "datetime":
		return "datetime"
	case "tinyblob", "blob", "mediumblob", "longblob", "varbinary":
		return "binary"
	case "binary":
		// Fixed-length binary needs special handling; blob etc is fine.
		// We must preserve the width (e.g. binary(16)) in the cast:
		//  - A plain CAST(col AS binary) does not pad, so a binary(50)->binary(100)
		//    widening migration would checksum-mismatch (the two sides zero-pad
		//    to different lengths). Casting both sides to the target's full type
		//    pads them to the same length, since the cast type always comes from
		//    the target table (see ColumnMapping.ChecksumExprs).
		//  - CAST(col AS binary(0)) must never be used: it truncates every value
		//    to zero bytes, which would make the checksum blind to the column's
		//    contents entirely.
		return tp
	case "float", "double": // required for MySQL 5.7
		return "char"
	case "json":
		// castExpr wraps json in a text round-trip rather than a plain
		// CAST; see the comment there.
		return "json"
	case "decimal":
		return tp
	default:
		// For cases like varchar, enum, set, text, mediumtext, longtext
		// We return char, but because the new table could also change charset we explicitly
		// convert to utf8mb4 which should be the superset, and can do all comparisons.
		return "char CHARACTER SET utf8mb4"
	}
}

// castExpr builds the CAST expression that the checksum uses for a single
// column (see ColumnMapping.ChecksumExprs). col is the column referenced in
// SQL (escaped here); tp is the MySQL column type the cast is derived from.
//
// JSON columns are checksummed through a text round-trip — render to text,
// re-parse as JSON — rather than a plain CAST(col AS json). A JSON document
// can hold scalar types that JSON text cannot express: DECIMAL (e.g. from
// JSON_OBJECT('a', CAST(169.09 AS DECIMAL(12,6)))) and temporal/binary
// opaques. Spirit's buffered copier and binlog applier write JSON values as
// text, so on the target those degrade to DOUBLE/strings. The values are
// still equal under JSON comparison, but their canonical text can differ —
// a DECIMAL renders at its declared scale ("169.090000") while the
// re-parsed DOUBLE renders shortest ("169.09") — so a strict checksum
// flags every applier-written row, and under concurrent DML re-flags rows
// faster than repairs fix them, failing all attempts. Round-tripping BOTH
// sides collapses each document to its text-expressible form: exactly the
// fidelity the text-based copy/replication pipeline can deliver, and no
// less — genuine value differences still hash differently, and int vs
// double (which text does preserve) stays distinct.
func castExpr(col, tp string) string {
	quotedCol := sqlescape.EscapeIdentifier(col)
	castTp := castableTp(tp)
	if castTp == "json" {
		return "CAST(CAST(" + quotedCol + " AS char CHARACTER SET utf8mb4) AS json)"
	}
	return "CAST(" + quotedCol + " AS " + castTp + ")"
}

func removeWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeDecimalWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+,[0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeEnumSetOpts(s string) string {
	if len(s) > 4 && strings.EqualFold(s[:4], "enum") {
		return "enum"
	}
	if len(s) > 3 && strings.EqualFold(s[:3], "set") {
		return "set"
	}
	return s
}

func removeZerofill(s string) string {
	return strings.ReplaceAll(s, " zerofill", "")
}

func QuoteColumns(cols []string) string {
	q := make([]string, len(cols))
	for i, col := range cols {
		q[i] = sqlescape.EscapeIdentifier(col)
	}
	return strings.Join(q, ", ")
}

// expandRowConstructorComparison is a workaround for MySQL
// not always optimizing conditions such as (a,b,c) > (1,2,3).
// This limitation is still current in 8.0, and was not fixed
// by the work in https://dev.mysql.com/worklog/task/?id=7019
//
// vals[i].String() is inlined directly into the SQL fragment because
// Datum.String() returns a pre-escaped self-contained SQL literal
// (see its doc comment for the contract). Don't change the format
// strings below to add quoting or escaping for the values — they already
// carry it. Column identifiers are escaped via sqlescape.EscapeIdentifier.
func expandRowConstructorComparison(cols []string, operator Operator, vals []Datum) string {
	if len(cols) != len(vals) {
		panic("cols should be same size as values")
	}
	if len(cols) == 1 {
		return fmt.Sprintf("%s %s %s", sqlescape.EscapeIdentifier(cols[0]), operator, vals[0].String())
	}
	// Unless we are in the "final" position
	// we need to use a different intermediate operator
	// for comparison. i.e. >= becomes >
	intermediateOperator := operator
	switch operator { //nolint: exhaustive
	case OpGreaterEqual:
		intermediateOperator = OpGreaterThan
	case OpLessEqual:
		intermediateOperator = OpLessThan
	}
	conds := []string{}
	buffer := []string{}
	for i, col := range cols {
		if i == 0 {
			conds = append(conds, fmt.Sprintf("(%s %s %s)", sqlescape.EscapeIdentifier(col), intermediateOperator, vals[i].String()))
			buffer = append(buffer, fmt.Sprintf("%s %s %s", sqlescape.EscapeIdentifier(col), "=", vals[i].String()))
			continue
		}
		// If we are in the final position we can
		// overwrite the intermediate operator with
		// the original operator.
		if i == len(cols)-1 {
			intermediateOperator = operator
		}
		conds = append(conds, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(buffer, " AND "), sqlescape.EscapeIdentifier(col), intermediateOperator, vals[i].String()))
		buffer = append(buffer, fmt.Sprintf("%s %s %s", sqlescape.EscapeIdentifier(col), "=", vals[i].String()))
	}
	return "(" + strings.Join(conds, "\n OR ") + ")"
}
