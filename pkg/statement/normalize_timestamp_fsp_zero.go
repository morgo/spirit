package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
)

func init() { registerNormalizer(timestampFspZeroNormalizer{}) }

// fspFunctions are the builtins that take an optional fractional-seconds
// precision argument, by the name the parser records for each spelling.
var fspFunctions = map[string]bool{
	"current_time":      true,
	"current_timestamp": true,
	"curtime":           true,
	"localtime":         true,
	"localtimestamp":    true,
	"now":               true,
	"sysdate":           true,
	"utc_time":          true,
	"utc_timestamp":     true,
}

// timestampFspZeroNormalizer drops an explicit fractional-seconds precision
// of 0 from the timestamp functions, where MySQL drops it. An fsp of 0 is the
// same as none, and MySQL stores the call without it, so SHOW CREATE TABLE
// never reports one. Verified against MySQL 8.0.43:
//
//	datetime(0) DEFAULT CURRENT_TIMESTAMP(0)         -> datetime DEFAULT CURRENT_TIMESTAMP
//	timestamp(0) ... ON UPDATE CURRENT_TIMESTAMP(0)  -> ... ON UPDATE CURRENT_TIMESTAMP
//	datetime DEFAULT NOW(0) ON UPDATE LOCALTIMESTAMP(0)
//	  -> datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
//	datetime DEFAULT (CURRENT_TIMESTAMP(0))          -> DEFAULT (now())
//	datetime DEFAULT (NOW(0) + INTERVAL 1 DAY)       -> DEFAULT ((now() + interval 1 day))
//	time DEFAULT (CURTIME(0))                        -> DEFAULT (curtime())
//	datetime DEFAULT (UTC_TIMESTAMP(0))              -> DEFAULT (utc_timestamp())
//	datetime DEFAULT (SYSDATE(0))                    -> DEFAULT (sysdate())
//	bigint DEFAULT (UNIX_TIMESTAMP(NOW(0)))          -> DEFAULT (unix_timestamp(now()))
//	datetime(3) DEFAULT CURRENT_TIMESTAMP(3)         -> unchanged
//
// The type's own fsp of 0 is already dropped at parse time (datetime(0)
// arrives as datetime), but the argument of the function call was kept, so
// without this rule the diff emitted `MODIFY ... DEFAULT current_timestamp(0)`
// on every run.
//
// Covers the literal-style DEFAULT, ON UPDATE, and expression DEFAULTs, the
// only places MySQL accepts a nondeterministic function: generated columns,
// CHECK constraints and functional indexes reject them. The rule removes the
// argument only; renaming (current_timestamp -> now inside an expression) is
// functionAliasNormalizer's, and the two commute. An expression with no zero
// fsp in it is left byte-for-byte untouched.
type timestampFspZeroNormalizer struct{}

func (timestampFspZeroNormalizer) Name() string { return "timestamp-fsp-zero" }

func (timestampFspZeroNormalizer) Normalize(ct *CreateTable) *CreateTable {
	p := parser.New()
	for i := range ct.Columns {
		col := &ct.Columns[i]
		// A string-literal default holds a value, not an expression, even in
		// the parenthesized DEFAULT ('now(0)') form.
		if col.DefaultKind == DefaultKindUnknown {
			render := restoreLiteralStyleText
			if col.DefaultIsExpr {
				render = restoreExprDefaultText
			}
			dropZeroFsp(p, col.Default, render)
		}
		// ON UPDATE is restored through parseExpression, the literal style.
		dropZeroFsp(p, col.OnUpdate, restoreLiteralStyleText)
	}
	return ct
}

// zeroFspRemover is the ast.Visitor behind dropZeroFsp: it removes a zero
// fsp argument from each timestamp function call on the way back up the tree.
type zeroFspRemover struct{ removed bool }

func (r *zeroFspRemover) Enter(n ast.Node) (ast.Node, bool) { return n, false }

func (r *zeroFspRemover) Leave(n ast.Node) (ast.Node, bool) {
	call, ok := n.(*ast.FuncCallExpr)
	// A schema-qualified call is a stored function, not a builtin.
	if !ok || call.Schema.L != "" || !fspFunctions[call.FnName.L] || len(call.Args) != 1 {
		return n, true
	}
	if !isZeroIntLiteral(call.Args[0]) {
		return n, true
	}
	call.Args = nil
	r.removed = true
	return n, true
}

// isZeroIntLiteral reports whether expr is the integer literal 0.
func isZeroIntLiteral(expr ast.ExprNode) bool {
	v, ok := expr.(*ast.ValueExpr)
	if !ok {
		return false
	}
	switch v.Kind() {
	case ast.KindInt64:
		return v.GetInt64() == 0
	case ast.KindUint64:
		return v.GetUint64() == 0
	}
	return false
}

// dropZeroFsp removes every zero fsp argument from the timestamp function
// calls in an expression text, in place. render must be the restore the text
// was produced with, so the rewritten text matches the one MySQL's stored form
// parses to. A nil or empty text, and one with no zero fsp in it, is left
// alone.
func dropZeroFsp(p *parser.Parser, text *string, render func(ast.ExprNode) (string, bool)) {
	// The text is restored from the AST, which spells a zero fsp as "(0)", so
	// a text without it has nothing to remove and is not parsed again.
	if text == nil || !strings.Contains(*text, "(0)") {
		return
	}
	expr, ok := parseExpressionText(p, *text)
	if !ok {
		return
	}
	remover := &zeroFspRemover{}
	node, ok := expr.Accept(remover)
	if !ok || !remover.removed {
		return
	}
	rewritten, ok := node.(ast.ExprNode)
	if !ok {
		return
	}
	if rendered, ok := render(rewritten); ok {
		*text = rendered
	}
}
