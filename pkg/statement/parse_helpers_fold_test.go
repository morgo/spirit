package statement

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultExprOf parses a CREATE TABLE with one column and returns the AST of
// that column's DEFAULT expression.
func defaultExprOf(t *testing.T, column string) ast.ExprNode {
	t.Helper()
	stmts, _, err := parser.New().Parse("CREATE TABLE t ("+column+")", "", "")
	require.NoError(t, err)
	col := stmts[0].(*ast.CreateTableStmt).Cols[0]
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			return opt.Expr
		}
	}
	t.Fatalf("no DEFAULT on %s", column)
	return nil
}

// plainRestore renders expr with the parser's default flags only: every
// introducer the AST carries is written out.
func plainRestore(t *testing.T, expr ast.ExprNode) string {
	t.Helper()
	var sb strings.Builder
	require.NoError(t, expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)))
	return sb.String()
}

// TestRestoreExprTextFoldsWithoutMutatingTheAST checks that the introducer
// fold is confined to the rendered text: the AST the caller keeps (Column.Raw
// and the other retained nodes) still carries the introducer as parsed, so a
// later restore with other flags, or a later reading of the literal's charset,
// sees what was written.
func TestRestoreExprTextFoldsWithoutMutatingTheAST(t *testing.T) {
	expr := defaultExprOf(t, "c VARCHAR(10) DEFAULT (UPPER(_latin1'a'))")
	before := plainRestore(t, expr)
	require.Contains(t, before, "_LATIN1'a'")

	text, ok := restoreExpressionText(expr)
	require.True(t, ok)
	assert.Equal(t, "UPPER('a')", text, "the rendered text folds the ASCII latin1 literal")
	assert.Equal(t, before, plainRestore(t, expr), "the AST is left as parsed")

	again, ok := restoreExpressionText(expr)
	require.True(t, ok)
	assert.Equal(t, text, again, "and renders the same way a second time")
}

// TestRestoreExprTextKeepsCharsetObservingIntroducers pins where the fold
// stops: the operand of COLLATE in full, and the arguments of CHARSET(),
// COLLATION() and WEIGHT_STRING(), however deep the literal sits; and that
// it still folds beneath every other function.
func TestRestoreExprTextKeepsCharsetObservingIntroducers(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"_latin1'a' COLLATE latin1_bin", "_LATIN1'a' COLLATE latin1_bin"},
		{"CONCAT(_latin1'a') COLLATE latin1_bin", "CONCAT(_LATIN1'a') COLLATE latin1_bin"},
		{"CONCAT(_latin1'a', UPPER(_latin1'b')) COLLATE latin1_bin", "CONCAT(_LATIN1'a', UPPER(_LATIN1'b')) COLLATE latin1_bin"},
		{"CHARSET(_latin1'a')", "CHARSET(_LATIN1'a')"},
		{"CHARSET(IF(1, _latin1'a', _latin1'b'))", "CHARSET(IF(1, _LATIN1'a', _LATIN1'b'))"},
		{"COLLATION(_utf8mb3'a')", "COLLATION(_UTF8'a')"}, // the parser spells utf8mb3 as utf8
		{"HEX(WEIGHT_STRING(_latin1'a'))", "HEX(WEIGHT_STRING(_LATIN1'a'))"},
		{"CONCAT(CHARSET(_latin1'a'), _latin1'b')", "CONCAT(CHARSET(_LATIN1'a'), 'b')"},
		{"UPPER(_latin1'a')", "UPPER('a')"},
		{"LENGTH(_utf8mb3'a')", "LENGTH('a')"},
		{"CONCAT(_latin1'a', _latin1'é')", "CONCAT('a', _LATIN1'é')"},
		{"CHAR_LENGTH(_binary'a')", "CHAR_LENGTH(_BINARY'a')"},
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			expr := defaultExprOf(t, "c VARCHAR(64) DEFAULT ("+c.expr+")")
			text, ok := restoreExpressionText(expr)
			require.True(t, ok)
			assert.Equal(t, c.want, text)
		})
	}
}
