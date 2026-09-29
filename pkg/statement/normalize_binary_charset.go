package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

func init() { registerNormalizer(binaryCharsetNormalizer{}) }

// binaryCharsetNormalizer rewrites a character column whose charset resolves
// to binary into its binary type, the way MySQL does. The parser already does
// this for a column that writes `CHARACTER SET binary` itself (see
// parseColumn); this rule covers the two routes it does not see, because they
// need the rest of the definition:
//
//   - the column declares no charset or collation and inherits a binary table
//     default, written as either DEFAULT CHARSET=binary or DEFAULT
//     COLLATE=binary;
//   - the column writes `COLLATE binary`, which implies the binary charset.
//
// Verified against MySQL 8.0.43:
//
//	a varchar(3)                           (table charset binary)
//	  -> varbinary(3)
//	b char(3)                              -> binary(3)
//	c text                                 -> blob (tinytext, mediumtext, longtext likewise)
//	j varchar(3) BINARY                    -> varbinary(3)
//	a varchar(3) COLLATE binary            (table charset utf8mb4)
//	  -> varbinary(3)
//	g enum('x','y')                        -> enum('x','y')  (not rewritten)
//	i varchar(3) CHARACTER SET latin1      -> varchar(3) CHARACTER SET latin1 COLLATE latin1_swedish_ci
//	k varchar(3) COLLATE utf8mb4_bin       -> varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
//	n nvarchar(3)                          -> varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
//	DEFAULT COLLATE=binary                 -> DEFAULT CHARSET=binary
//	DEFAULT CHARSET=binary COLLATE=binary  -> DEFAULT CHARSET=binary
//
// Without this rule the written character type survives into the diff, which
// emits `MODIFY COLUMN a varchar(3)` against the live varbinary(3). MySQL
// applies it, stores varbinary(3) again, and the next diff emits the same
// statement.
//
// A rewritten column is given the binary charset and collation, as the parser
// gives an explicit `CHARACTER SET binary` column. Because the column then
// declares a charset and a collation, binaryAttributeNormalizer (which also
// skips a binary charset) and defaultCollationNormalizer (which skips a column
// that has a collation) leave it alone in either order.
//
// The table default is canonicalized to the form SHOW CREATE TABLE reports,
// DEFAULT CHARSET=binary with no COLLATE, so that DEFAULT COLLATE=binary and
// DEFAULT CHARSET=binary COLLATE=binary do not diff against it.
type binaryCharsetNormalizer struct{}

func (binaryCharsetNormalizer) Name() string { return "binary-charset" }

func (binaryCharsetNormalizer) Normalize(ct *CreateTable) *CreateTable {
	tableBinary := tableDefaultIsBinary(ct.TableOptions)
	for i := range ct.Columns {
		col := &ct.Columns[i]
		var colBinary bool
		switch {
		case col.Charset != nil:
			continue // the parser has already converted an explicit binary charset
		case col.Collation != nil:
			colBinary = strings.EqualFold(*col.Collation, charset.CollationBin)
		default:
			colBinary = tableBinary
		}
		if !colBinary {
			continue
		}
		binType, ok := binaryTypeOf(col.Type)
		if !ok {
			continue // enum and set keep their type
		}
		col.Type = binType
		cs, coll := charset.CharsetBin, charset.CollationBin
		col.Charset, col.Collation = &cs, &coll
	}
	if tableBinary {
		cs := charset.CharsetBin
		ct.TableOptions.Charset = &cs
		ct.TableOptions.Collation = nil
	}
	return ct
}

// tableDefaultIsBinary reports whether the table's default charset is binary.
// The binary collation belongs only to the binary charset, so either option
// decides it.
func tableDefaultIsBinary(opts *TableOptions) bool {
	if opts == nil {
		return false
	}
	return (opts.Charset != nil && strings.EqualFold(*opts.Charset, charset.CharsetBin)) ||
		(opts.Collation != nil && strings.EqualFold(*opts.Collation, charset.CollationBin))
}

// binaryTypeOf returns the binary type MySQL stores a character type as when
// its charset is binary. It reports false for a type that has no binary
// counterpart, including enum and set, which keep their type.
func binaryTypeOf(typ string) (string, bool) {
	switch typ {
	case "varchar":
		return "varbinary", true
	case "char":
		return "binary", true
	case "text":
		return "blob", true
	case "tinytext":
		return "tinyblob", true
	case "mediumtext":
		return "mediumblob", true
	case "longtext":
		return "longblob", true
	}
	return "", false
}
