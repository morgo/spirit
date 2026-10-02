package statement

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
)

// This file holds low-level, stateless parsing helpers used by the CreateTable
// parse methods in parse_create_table.go — extracting literal values and
// lengths/precision from AST nodes and type strings. They are free functions;
// the CreateTable receivers that call them live in parse_create_table.go.

// stringLiteralValue returns the true, fully-unescaped value of a quoted
// string-literal AST node (for example the value behind a single-quoted
// DEFAULT, or a COMMENT, whose contents include escaped quote or backslash
// characters) along with true. For any other expression kind it returns an
// empty string and false.
//
// This is the load-bearing fix for the string round-trip bugs: the TiDB
// parser's Restore re-emits a string literal in its re-escaped, still-quoted
// form rather than as the raw value, so the previous approach of
// Restore-then-strip-outer-quotes left the inner escaping in place. Reading
// the literal's value directly off the AST yields the raw bytes, which we
// then escape exactly once at emission time via sqlescape (backslash
// escaping, which MySQL accepts in its default sql_mode). Note MySQL renders
// a literal quote in SHOW CREATE TABLE as a doubled quote, which the parser
// also accepts; the doubled and backslash forms are equivalent in the
// default sql_mode.
func stringLiteralValue(expr ast.ExprNode) (string, bool) {
	if v, ok := expr.(*ast.ValueExpr); ok && v.Kind() == ast.KindString {
		return v.GetString(), true
	}
	return "", false
}

// isExpressionDefault returns true when the default value expression should be
// wrapped in parentheses in the generated DDL. MySQL requires expression defaults
// (as opposed to literal defaults) to be enclosed in parens, e.g. DEFAULT (json_object()).
// This mirrors the logic in the TiDB parser's ColumnOption.Restore for ColumnOptionDefaultValue:
// non-CURRENT_TIMESTAMP function calls and column name expressions get outer parentheses.
func isExpressionDefault(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *ast.ParenthesesExpr:
		// The parser preserves the parentheses of DEFAULT ('{}') — MySQL's
		// expression-default form, required on BLOB/TEXT/JSON/GEOMETRY
		// columns — as a ParenthesesExpr wrapper.
		return true
	case *ast.FuncCallExpr:
		// CURRENT_TIMESTAMP (and aliases NOW, LOCALTIME, etc.) are literal-style defaults
		// that don't need parens. Everything else is an expression default.
		return !isTimestampFuncName(e.FnName.L)
	case *ast.ColumnNameExpr:
		return true
	default:
		return false
	}
}

// unwrapParenExpr removes any ParenthesesExpr wrappers, returning the
// innermost expression. Used where the parenthesized/bare distinction has
// already been captured (e.g. in DefaultIsExpr) and only the value inside
// the parentheses is needed.
func unwrapParenExpr(expr ast.ExprNode) ast.ExprNode {
	for {
		paren, ok := expr.(*ast.ParenthesesExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}

// restoreValueExprText converts a DEFAULT / ON UPDATE / partition expression
// to the string representation Spirit stores for it.
//
// bareTimestampKeyword selects between MySQL's two spellings of a zero-argument
// timestamp function. In the literal-style forms — DEFAULT CURRENT_TIMESTAMP,
// ON UPDATE CURRENT_TIMESTAMP — MySQL reports the bare keyword, while the
// parser's Restore always writes the call form, so the trailing "()" is
// stripped. In an *expression* default the call form is the canonical one:
// MySQL stores DEFAULT (CURRENT_TIMESTAMP) as DEFAULT (now()), and emitting
// the bare keyword inside the parentheses — DEFAULT (now) — would not even
// parse, since a bare `now` there is a column reference.
func restoreValueExprText(expr ast.ExprNode, bareTimestampKeyword bool) any {
	return restoreValueExprTextWith(expr, bareTimestampKeyword, 0)
}

// restoreValueExprTextWith is restoreValueExprText with extra restore flags
// added to whichever flag set the expression's shape selects, so that a
// normalization rule can render in the shape's own form plus, say,
// RestoreSkipRedundantParentheses.
func restoreValueExprTextWith(expr ast.ExprNode, bareTimestampKeyword bool, extra format.RestoreFlags) any {
	if expr == nil {
		return nil
	}

	// Handle different expression types
	switch e := expr.(type) {
	case *ast.FuncCallExpr:
		// Handle function calls like CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(3), UUID(), etc.
		// We use Restore to preserve function arguments (e.g. precision in CURRENT_TIMESTAMP(3)).
		// RestoreKeyWordLowercase renders the function name and any keywords
		// inside its arguments in lowercase — matching MySQL's canonical
		// SHOW CREATE TABLE form (e.g. DEFAULT (concat(...))) so that
		// function-name case never causes a spurious diff — while leaving
		// string-literal arguments byte-exact. The previous strings.ToLower
		// over the whole Restored text corrupted literal case:
		// DEFAULT (concat('A')) round-tripped to concat('a'), emitting a
		// different default value and making defaults that differ only in
		// literal case compare equal.
		restored, ok := restoreExprText(e, format.RestoreStringSingleQuotes|format.RestoreKeyWordLowercase|
			format.RestoreNameBackQuotes|extra)
		if !ok {
			return e.FnName.L // fallback to function name on error
		}
		// Normalize: MySQL's canonical SHOW CREATE TABLE uses "CURRENT_TIMESTAMP" (no parens)
		// when there is no fractional seconds precision, but the parser's Restore always adds "()".
		// We only strip parens for timestamp-family functions; other functions like json_object()
		// need to keep their parens as they represent actual function calls.
		if bareTimestampKeyword && isTimestampFuncName(e.FnName.L) &&
			len(e.Args) == 0 && strings.HasSuffix(restored, "()") {
			restored = strings.TrimSuffix(restored, "()")
		}
		return restored
	default:
		// For other types, fall back to text representation. A literal-style
		// default (or partition value) is a value, not an expression: MySQL
		// converts it to the column's charset and reports it with no
		// introducer, so every introducer is dropped from it. Inside an
		// expression default the introducer stays meaningful and is handled
		// by restoreExprText.
		flags := format.DefaultRestoreFlags | extra
		if bareTimestampKeyword {
			flags |= format.RestoreStringWithoutCharset
		}
		str, ok := restoreExprText(expr, flags)
		if !ok {
			return "<error>"
		}
		// if the string is quoted, remove quotes
		if strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'") {
			str = str[1 : len(str)-1]
		}
		return str
	}
}

// isTimestampFuncName reports whether name (lowercased) is one of the
// zero-argument timestamp functions MySQL accepts as a literal-style DEFAULT
// or ON UPDATE value.
func isTimestampFuncName(name string) bool {
	switch name {
	case "current_timestamp", "now", "localtime", "localtimestamp", "utc_timestamp":
		return true
	}
	return false
}

// parseExpressionText re-parses an expression that was previously restored to
// text, returning its AST node. Normalization rules use it to work on the tree
// rather than on the text: the structured form is where a MySQL canonical form
// can be reasoned about.
//
// The text was produced by restoring a successfully parsed expression, so a
// re-parse failure is not expected; callers treat a false result as "leave the
// text alone", whose worst outcome is a spurious diff on that expression,
// never a corrupted definition.
func parseExpressionText(p *parser.Parser, text string) (ast.ExprNode, bool) {
	stmt, err := p.ParseOneStmt("SELECT "+text, "", "")
	if err != nil {
		return nil, false
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok || sel.Fields == nil || len(sel.Fields.Fields) != 1 || sel.Fields.Fields[0].Expr == nil {
		return nil, false
	}
	return sel.Fields.Fields[0].Expr, true
}

// exprRewriter is an ast.Visitor that rewrites an expression tree in place and
// reports whether it changed anything, for rewriteExpressionText.
type exprRewriter interface {
	ast.Visitor
	Changed() bool
}

// rewriteExpressionText re-parses an expression text, runs rewriter over the
// tree and, if it changed anything, replaces the text with render's rendering
// of the result, in place. It reports whether the text changed. A nil or empty
// text, or one that does not re-parse, is left alone.
//
// render must be the same restore the text was originally produced with, so
// that a rewritten expression and one already in the rewritten form render
// identically — that identity is the point of every rule built on this. An
// expression the rewriter leaves alone is left byte-for-byte untouched rather
// than re-rendered, so a rule built on this cannot perturb a form another rule
// established (which is what keeps the rules order-independent).
func rewriteExpressionText(p *parser.Parser, text *string, render func(ast.ExprNode) (string, bool), rewriter exprRewriter) bool {
	if text == nil || *text == "" {
		return false
	}
	expr, ok := parseExpressionText(p, *text)
	if !ok {
		return false
	}
	node, ok := expr.Accept(rewriter)
	if !ok || !rewriter.Changed() {
		return false
	}
	// A rewriter edits nodes in place, so the node handed back is the
	// expression it was given. Check rather than assert anyway: normalization
	// runs on every parse, and leaving the text alone beats panicking if a
	// future visitor change breaks that.
	rewritten, ok := node.(ast.ExprNode)
	if !ok {
		return false
	}
	rendered, ok := render(rewritten)
	if !ok {
		return false
	}
	*text = rendered
	return true
}

// expressionColumnNames returns the names of the columns an expression text
// reads, e.g. dt for YEAR(`dt`). It returns false when the text does not
// parse.
func expressionColumnNames(p *parser.Parser, text string) ([]string, bool) {
	expr, ok := parseExpressionText(p, text)
	if !ok {
		return nil, false
	}
	var c columnNameCollector
	expr.Accept(&c)
	return c.names, true
}

// columnNameCollector is the ast.Visitor behind expressionColumnNames.
type columnNameCollector struct {
	names []string
}

func (c *columnNameCollector) Enter(n ast.Node) (ast.Node, bool) {
	if col, ok := n.(*ast.ColumnNameExpr); ok {
		c.names = append(c.names, col.Name.Name.O)
	}
	return n, false
}

func (c *columnNameCollector) Leave(n ast.Node) (ast.Node, bool) { return n, true }

// restoreExpressionText restores an expression AST node to its SQL text,
// stripping redundant outer parentheses. MySQL's SHOW CREATE TABLE wraps
// generated-column and CHECK expressions in an extra set of parentheses
// (e.g. GENERATED ALWAYS AS ((`a` + 1))); stripping them ensures a
// user-written `AS (a + 1)` compares equal to the canonical form.
// Unlike parseExpression, the result is NOT lowercased and string literals
// keep their quotes — these expressions may contain case-sensitive literals.
func restoreExpressionText(expr ast.ExprNode) (string, bool) {
	return restoreExprText(unwrapParenExpr(expr), format.DefaultRestoreFlags)
}

// restoreExprText renders an expression to the text Spirit stores for it: the
// given restore flags plus the charset-introducer policy every stored
// expression shares. It is the one place that policy lives; every restore of a
// generated-column, CHECK, functional-index, expression-default or partition
// expression goes through it, so the parse of a user's DDL and the parse of
// SHOW CREATE TABLE render a literal the same way.
//
// MySQL keeps a string literal's charset introducer in the expressions it
// stores, and the introducer can change what the expression means:
// CHAR_LENGTH(_binary'€') is 3 where CHAR_LENGTH('€') is 1, and
// _latin1'a' COLLATE latin1_bin is error 1253 once the introducer is gone,
// because latin1_bin is not a collation of the default charset. Dropping every
// introducer (the former format.RestoreStringWithoutCharset) therefore emitted
// a different expression from the one the user wrote. Introducers are kept
// instead, except those that spell the same value as the bare literal, which
// foldLiteralCharsets rewrites to the default charset first — MySQL adds the
// session's introducer to every bare literal it stores, so a user-written 'x'
// has to compare equal to the _utf8mb4'x' that SHOW CREATE TABLE reports for
// it. format.RestoreStringWithoutDefaultCharset then omits the default
// introducer, which is the form a bare literal parses to.
func restoreExprText(expr ast.ExprNode, flags format.RestoreFlags) (string, bool) {
	if expr == nil {
		return "", false
	}
	foldLiteralCharsets(expr)
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(flags|format.RestoreStringWithoutDefaultCharset, &sb)
	if err := expr.Restore(rCtx); err != nil {
		return "", false
	}
	return sb.String(), true
}

// foldLiteralCharsets rewrites, in place, the charset of every string literal
// in expr whose introducer is equivalent to none, so that the literal renders
// bare. An introducer is equivalent to none when it names:
//
//   - utf8mb4, the parser's default: a bare 'x' and _utf8mb4'x' are the same
//     parse, and MySQL reports the latter for a bare literal stored from a
//     utf8mb4 session;
//   - utf8mb3 (also spelled utf8, and the N'x' national form): every valid
//     utf8mb3 string is the same bytes in utf8mb4, so the value, its length
//     and its comparisons against a column do not change. MySQL reports
//     _utf8mb3 for bare literals stored from an older client;
//   - any other ASCII-compatible charset (latin1, ascii, …) when the literal
//     is pure ASCII, whose bytes and characters are the same in utf8mb4. MySQL
//     reports _latin1 for bare literals stored from a latin1 client, which the
//     mysql command-line client is by default.
//
// Kept are _binary (a binary literal measures and compares by bytes, so it is
// never the bare literal's expression), the UTF-16/32 family (not
// ASCII-compatible), a non-ASCII literal under any other charset (its bytes
// are not those of the utf8mb4 spelling), and any literal that is the direct
// operand of COLLATE, where the introducer is the charset the collation must
// belong to.
//
// The fold is not a complete equivalence: two literals that carry the same
// non-default introducer and are compared with each other use that charset's
// default collation, and folding both moves the comparison to utf8mb4's. That
// is accepted so that a table created from a non-utf8mb4 session converges
// rather than re-emitting the same expression on every diff.
func foldLiteralCharsets(expr ast.ExprNode) {
	if expr == nil {
		return
	}
	expr.Accept(literalCharsetFolder{})
}

// literalCharsetFolder is the ast.Visitor behind foldLiteralCharsets.
type literalCharsetFolder struct{}

func (literalCharsetFolder) Enter(n ast.Node) (ast.Node, bool) {
	switch e := n.(type) {
	case *ast.SetCollationExpr:
		// The operand's introducer is load-bearing under COLLATE; skip it.
		// Anything deeper (COLLATE over a function call) is still visited.
		if _, ok := unwrapParenExpr(e.Expr).(*ast.ValueExpr); ok {
			return n, true
		}
	case *ast.ValueExpr:
		if e.Kind() == ast.KindString && literalCharsetFoldsToDefault(e.GetType().GetCharset(), e.GetString()) {
			e.GetType().SetCharset(mysql.DefaultCharset)
			e.GetType().SetCollate(mysql.DefaultCollationName)
		}
	}
	return n, false
}

func (literalCharsetFolder) Leave(n ast.Node) (ast.Node, bool) { return n, true }

// literalCharsetFoldsToDefault reports whether a string literal under charset
// cs with the given value is the same literal under the default charset. See
// foldLiteralCharsets for the rule.
func literalCharsetFoldsToDefault(cs, value string) bool {
	switch cs {
	case "", mysql.DefaultCharset:
		return false // already bare
	case charset.CharsetUTF8, charset.CharsetUTF8MB3:
		return true
	case charset.CharsetBin, charset.CharsetUCS2, charset.CharsetUTF16, charset.CharsetUTF16LE, charset.CharsetUTF32:
		return false
	}
	for i := range len(value) {
		if value[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// extractLengthFromTypeString extracts length from type string like
// "varchar(100)". ok reports whether the type string carries a width at all, so
// that a zero width (varchar(0), char(0), binary(0)) is distinguished from none:
// MySQL stores and reports those columns with their zero width. A negative
// width is the parser's unspecified-length marker (year renders as year(-1))
// and is reported as no width.
func extractLengthFromTypeString(typeStr string) (length int, ok bool) {
	// Simple regex-like parsing for common cases
	if strings.Contains(typeStr, "(") && strings.Contains(typeStr, ")") {
		start := strings.Index(typeStr, "(")

		end := strings.Index(typeStr, ")")
		if start < end && start != -1 && end != -1 {
			lengthStr := typeStr[start+1 : end]
			// Handle cases like "decimal(10,2)" - take the first number
			if commaIdx := strings.Index(lengthStr, ","); commaIdx != -1 {
				lengthStr = lengthStr[:commaIdx]
			}

			if n, err := fmt.Sscanf(lengthStr, "%d", &length); n == 1 && err == nil && length >= 0 {
				return length, true
			}
		}
	}

	return 0, false
}

// extractPrecisionScaleFromTypeString extracts precision and scale from type string like "decimal(10,2)"
func extractPrecisionScaleFromTypeString(typeStr string) (int, int) {
	if strings.Contains(typeStr, "(") && strings.Contains(typeStr, ")") {
		start := strings.Index(typeStr, "(")

		end := strings.Index(typeStr, ")")
		if start < end && start != -1 && end != -1 {
			paramStr := typeStr[start+1 : end]
			if precisionStr, scaleStr, found := strings.Cut(paramStr, ","); found {
				precisionStr = strings.TrimSpace(precisionStr)
				scaleStr = strings.TrimSpace(scaleStr)

				var precision, scale int
				if n, err := fmt.Sscanf(precisionStr, "%d", &precision); n == 1 && err == nil {
					if n, err := fmt.Sscanf(scaleStr, "%d", &scale); n == 1 && err == nil {
						return precision, scale
					}

					return precision, 0
				}
			}
		}
	}

	return 0, 0
}
