package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

func init() { registerNormalizer(textBlobLengthNormalizer{}) }

// textBlobLengthNormalizer resolves the length of a TEXT(M) or BLOB(M) column
// to the type MySQL stores it as. MySQL does not keep the length: it picks the
// smallest of the four sizes that holds M bytes (M characters for TEXT, at the
// charset's maximum bytes per character) and stores that type, so SHOW CREATE
// TABLE never reports a length. The limits are 255 (tiny), 65535 (plain),
// 16777215 (medium), and anything larger is long. Verified against MySQL
// 8.0.43:
//
//	blob(0), blob(100), blob(255)       -> tinyblob
//	blob(256), blob(65535)              -> blob
//	blob(65536)                         -> mediumblob
//	blob(16777216)                      -> longblob
//	text(0), text(63)                   -> tinytext   (utf8mb4: 4 bytes per char)
//	text(64), text(16383)               -> text
//	text(16384)                         -> mediumtext
//	text(4194304)                       -> longtext
//	text(85) CHARACTER SET utf8mb3      -> tinytext   (255 bytes)
//	text(86) CHARACTER SET utf8mb3      -> text
//	text(255) CHARACTER SET latin1      -> tinytext
//	text(127) CHARACTER SET ucs2        -> tinytext   (utf16, utf32, sjis, ujis, gb18030 likewise at their widths)
//	text(100) CHARACTER SET binary      -> tinyblob
//	text(4294967295) CHARACTER SET latin1 -> longtext
//
// The parser keeps the written type (`text`, `blob`) and does not record the
// length in Column.Length, so without this rule a column created exactly as
// declared diffs against the live table, and the emitted `MODIFY COLUMN ...
// text` is a real type change.
//
// The length is read from the parsed type (Column.Raw), which is the only
// place it survives; the parser sets it to -1 when no length was written.
//
// The charset is the one the column resolves to (see
// resolvedCharsetCollation). A table that names no charset takes the database
// default, which the statement does not determine; the column is then
// rewritten only when every charset gives the same size (1 to 4 bytes per
// character), which covers text(0) and any length whose size does not depend
// on it. Otherwise the column keeps its written length in Column.Length, so it
// is emitted as `text(M)` and MySQL resolves the size at the charset the column
// actually gets. Emitting it as a plain `text` would change the type: on a
// utf8mb4 schema text(20000) is created as mediumtext, and `MODIFY COLUMN a
// text` narrows it; on a latin1 schema text(64) is created as tinytext, and the
// same MODIFY widens it. The diff compares such a column against the other
// side's type at the other side's charset (see textLengthTypeEqual), since
// only the other side determines it.
//
// A column whose charset resolves to binary becomes a blob (see
// binaryCharsetNormalizer). This rule sizes it at 1 byte per character and
// keeps the family, so text(100) there resolves to tinytext and then to
// tinyblob, or to blob and then tinyblob, whichever rule runs first.
type textBlobLengthNormalizer struct{}

func (textBlobLengthNormalizer) Name() string { return "text-blob-length" }

func (textBlobLengthNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Raw == nil || c.Raw.Tp == nil || c.Raw.Tp.GetFlen() < 0 {
			continue // no length written
		}
		family := strings.ToLower(c.Type)
		if family != "text" && family != "blob" {
			continue // the only types that take a length
		}
		length := uint64(c.Raw.Tp.GetFlen())
		minBytes, maxBytes := length, length
		if family == "text" {
			minWidth, maxWidth := textCharWidths(c, ct)
			minBytes, maxBytes = length*minWidth, length*maxWidth
		}
		prefix := lobSizePrefix(minBytes)
		if prefix != lobSizePrefix(maxBytes) {
			// The size depends on a charset the statement does not decide:
			// keep the written length so the column is emitted as text(M).
			written := int(length)
			c.Length = &written
			continue
		}
		c.Type = prefix + family
	}
	return ct
}

// textLengthTypeEqual compares the types of two columns when either of them is
// a TEXT(M) that textBlobLengthNormalizer left unresolved (Type `text` with
// Column.Length set). It reports handled=false otherwise, and the caller
// compares Type and Length directly.
//
// The unresolved column's size depends on the charset it gets, which its own
// statement does not name. The other column's charset decides it: when the two
// columns' charsets differ, the charset comparison already reports a
// difference, and the emitted `MODIFY COLUMN ... text(M)` is resolved by MySQL
// at the charset the column ends up with. So the types are equal when the
// other column's type is the size text(M) takes at the other column's charset.
// This holds with IgnoreCharsetCollation too: that option stops charset
// differences from being diffed, but a MODIFY still stores the column at a
// real charset, so comparing at any other width would let a MODIFY emitted
// for another attribute resize the column. When the other column's charset is
// not determined either, the types are equal when the other column's type is a
// size text(M) takes at any charset (1 to 4 bytes per character), in keeping
// with charsetCollationEqual, which treats an unexpressed preference as a
// match rather than guessing at it.
//
// When both columns are unresolved, neither determines the charset, and the
// types are equal when the two lengths give the same size at every width:
// text(20000) and text(20001) are text at 1 to 3 bytes per character and
// mediumtext at 4.
func textLengthTypeEqual(a, b *Column, source, target *CreateTable) (equal, handled bool) {
	aLength, aUnresolved := unresolvedTextLength(a)
	bLength, bUnresolved := unresolvedTextLength(b)
	if !aUnresolved && !bUnresolved {
		return false, false
	}
	if aUnresolved && bUnresolved {
		for width := uint64(1); width <= 4; width++ {
			if lobSizePrefix(aLength*width) != lobSizePrefix(bLength*width) {
				return false, true
			}
		}
		return true, true
	}
	length, other, otherTable := aLength, b, target
	if bUnresolved {
		length, other, otherTable = bLength, a, source
	}
	if other.Length != nil {
		return false, true
	}
	minWidth, maxWidth := textCharWidths(other, otherTable)
	otherType := strings.ToLower(other.Type)
	for width := minWidth; width <= maxWidth; width++ {
		if otherType == lobSizePrefix(length*width)+"text" {
			return true, true
		}
	}
	return false, true
}

// unresolvedTextLength returns the written length of a TEXT(M) column that
// textBlobLengthNormalizer left unresolved.
func unresolvedTextLength(c *Column) (uint64, bool) {
	if c.Length == nil || *c.Length < 0 || !strings.EqualFold(c.Type, "text") {
		return 0, false
	}
	return uint64(*c.Length), true
}

// textCharWidths returns the range of bytes per character a text column can
// have: its charset's maximum when the statement determines the charset, and
// the full range of MySQL charsets (1 to 4) when it does not.
func textCharWidths(c *Column, ct *CreateTable) (minWidth, maxWidth uint64) {
	cs, _ := resolvedCharsetCollation(c, ct)
	if cs != "" {
		if info, err := charset.GetCharsetInfo(cs); err == nil && info.Maxlen > 0 {
			return uint64(info.Maxlen), uint64(info.Maxlen)
		}
	}
	return 1, 4
}

// lobSizePrefix returns the size prefix of the smallest TEXT/BLOB type that
// holds n bytes.
func lobSizePrefix(n uint64) string {
	switch {
	case n <= 255:
		return "tiny"
	case n <= 65535:
		return ""
	case n <= 16777215:
		return "medium"
	default:
		return "long"
	}
}
