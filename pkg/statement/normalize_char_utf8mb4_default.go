package statement

import (
	"encoding/hex"
	"strings"
	"unicode/utf8"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(charUTF8MB4DefaultNormalizer{}) }

// charUTF8MB4DefaultNormalizer records a string DEFAULT on a utf8mb4 char or
// varchar column as the hex literal SHOW CREATE TABLE reports it as, when the
// string is not valid utf8mb3. MySQL converts the default to utf8mb3 to report
// it, and a default that does not convert is reported as its bytes instead, as
// it does on binary (see [binaryDefaultBytesNormalizer]). So
// `b char(4) DEFAULT '😀'` comes back as `b char(4) DEFAULT 0xF09F9880`.
// Without the rule the string diffs against the live hex literal and emits a
// MODIFY COLUMN that MySQL reports as hex again, so the next diff emits it
// again. Verified against MySQL 8.0.43, on a utf8mb4 table:
//
//	char(4) DEFAULT '😀'                     -> 0xF09F9880
//	varchar(4) DEFAULT 'a😀'                 -> 0x61F09F9880
//	char(4) DEFAULT _utf8mb4'😀'             -> 0xF09F9880
//	char(4) DEFAULT _binary'😀'              -> 0xF09F9880
//	char(8) DEFAULT _latin1'😀'              -> 'ðŸ˜€'        (the bytes read as latin1)
//	char(4) DEFAULT '😀 '                    -> 0xF09F9880    (char strips trailing spaces)
//	varchar(4) DEFAULT '😀 '                 -> 0xF09F988020
//	char(4) DEFAULT 'é'                      -> 'é'           (valid utf8mb3)
//	char(4) DEFAULT x'f09f9880'              -> 0xF09F9880    (already this form)
//	char(4) CHARACTER SET utf8mb3 DEFAULT '😀' -> error 1067
//	char(4) CHARACTER SET utf16 DEFAULT '😀'   -> 0xD83DDE00    (the utf16 bytes)
//	enum('😀','a') DEFAULT '😀'              -> enum('?','a') DEFAULT 0xF09F9880
//	char(4) DEFAULT ('😀')                   -> (_utf8mb4'????')
//
// MySQL before 8.0.33 reports each such character as '?' instead (Bug
// #104840), so a column with such a default cannot converge there.
//
// The hex literal also applies as intended: a MODIFY carrying
// `DEFAULT x'f09f9880'` on a utf8mb4 column stores the character '😀'.
//
// Left alone:
//
//   - a column whose charset is not utf8mb4. utf16, utf32 and gb18030 are
//     reported as hex too, but of the bytes in their own encoding, which this
//     rule does not reproduce, and utf8mb3 rejects the value.
//   - a column whose charset the table does not determine. Assuming utf8mb4
//     would be written back into the DDL: the MODIFY would carry the utf8mb4
//     bytes, which a table on utf16 would store as a different character. The
//     string stores the intended character on any charset that can hold it.
//   - enum and set. SHOW CREATE TABLE reports a member that is not valid
//     utf8mb3 as '?', so the column diffs whatever its default.
//   - a string with a charset introducer other than _utf8mb4 or _binary.
//     MySQL reads the bytes in the introducer's charset and converts them to
//     the column's, so _latin1'😀' is the four characters 'ðŸ˜€', which this
//     rule does not reproduce.
//   - a value longer than the column's width, which MySQL rejects.
//   - an expression default, which MySQL stores as an expression.
//
// A hex literal default is left to [charBinaryLiteralDefaultNormalizer]
// where it exists, which reads it as a string only when its bytes are valid
// utf8mb3. The two rules rewrite disjoint inputs to each other's fixed points,
// so they give the same result in either order.
type charUTF8MB4DefaultNormalizer struct{}

func (charUTF8MB4DefaultNormalizer) Name() string { return "char-utf8mb4-default" }

func (charUTF8MB4DefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.DefaultKind != DefaultKindString || c.Length == nil {
			continue
		}
		value := *c.Default
		switch storedColumnType(c, ct) {
		case "char":
			// MySQL strips a char default's trailing spaces when it stores it.
			value = strings.TrimRight(value, " ")
		case "varchar":
		default:
			continue
		}
		if !utf8.ValidString(value) || utils.ValidUTF8MB3(value) || utf8.RuneCountInString(value) > *c.Length {
			continue
		}
		if cs, _ := resolvedCharsetCollation(c, ct); NormalizeCharsetName(cs) != charset.CharsetUTF8MB4 {
			continue
		}
		switch defaultIntroducer(c) {
		case "", charset.CharsetUTF8MB4, charset.CharsetBin:
		default:
			continue
		}
		hexLiteral := "x'" + hex.EncodeToString([]byte(value)) + "'"
		c.Default, c.DefaultKind = &hexLiteral, DefaultKindHexLiteral
	}
	return ct
}

// defaultIntroducer returns the charset introducer written on a column's
// string DEFAULT (utf8mb4 for _utf8mb4'...'), or "" when it has none. The
// parsed default keeps only the string, so the introducer is read off the AST.
func defaultIntroducer(c *Column) string {
	if c.Raw == nil {
		return ""
	}
	var introducer string
	for _, opt := range c.Raw.Options {
		if opt.Tp != ast.ColumnOptionDefaultValue || opt.Expr == nil {
			continue
		}
		// The last DEFAULT wins, as it does when the column is parsed.
		introducer = ""
		if v, ok := opt.Expr.(*ast.ValueExpr); ok && v.Type.GetFlag()&mysql.UnderScoreCharsetFlag != 0 {
			introducer = strings.ToLower(v.Type.GetCharset())
		}
	}
	return introducer
}
