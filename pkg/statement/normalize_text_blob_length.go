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
// on it. Otherwise it is left as written.
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
			continue // the size depends on a charset the statement does not decide
		}
		c.Type = prefix + family
	}
	return ct
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
