package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

func init() { registerNormalizer(enumSetMemberSpacesNormalizer{}) }

// enumSetMemberSpacesNormalizer strips the trailing spaces from the members of
// an enum or set column whose charset is not binary, as MySQL does when it
// stores the column. Under the binary charset the spaces are data, and MySQL
// keeps them. The parser keeps every member as written, because the charset
// that decides it can come from a COLLATE clause or the table default, which
// the grammar rule for the type does not see.
//
// Verified against MySQL 8.0.28, 8.4 and 9.7, which agree:
//
//	enum('a','b ')                                  -> enum('a','b')
//	enum('a','b ') BINARY                           -> enum('a','b') ... COLLATE utf8mb4_bin
//	enum('a',x'6220')                               -> enum('a','b')
//	enum('a','b ','c\t') CHARACTER SET latin1       -> enum('a','b','c\t')  (only U+0020)
//	enum('a','b ') CHARACTER SET binary             -> enum('a','b ')
//	enum('a','b ') COLLATE binary                   -> enum('a','b ')
//	enum('a','b ')        (DEFAULT CHARSET=binary)  -> enum('a','b ')
//	enum('a','b ')        (DEFAULT COLLATE=binary)  -> enum('a','b ')
//	enum('a','b ') CHARACTER SET utf8mb4
//	                      (DEFAULT CHARSET=binary)  -> enum('a','b')
//	set('a','b ') CHARACTER SET binary              -> set('a','b ')
//
// Without this rule the two sides of a diff disagree on a non-binary member
// written with trailing spaces, and a MODIFY COLUMN emitted for a binary one
// would silently change the member.
//
// A column whose charset the definition does not determine (no charset of its
// own and no table default, which only hand-written DDL can reach) is taken to
// be non-binary, as the server default charset is.
//
// Every other rule that writes a column's charset or collation leaves whether
// it is binary unchanged: binaryCharsetNormalizer rewrites no enum or set
// column and only respells a binary table default, binaryAttributeNormalizer
// selects a non-binary _bin collation, and defaultCollationNormalizer fills in
// the collation of a charset already written. So this rule reads the same
// answer in any order.
type enumSetMemberSpacesNormalizer struct{}

func (enumSetMemberSpacesNormalizer) Name() string { return "enum-set-member-spaces" }

func (enumSetMemberSpacesNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if len(c.EnumValues) == 0 && len(c.SetValues) == 0 {
			continue
		}
		if columnCharsetIsBinary(c, ct) {
			continue // trailing spaces are data
		}
		c.EnumValues = stripMemberSpaces(c.EnumValues)
		c.SetValues = stripMemberSpaces(c.SetValues)
	}
	return ct
}

// columnCharsetIsBinary reports whether col's charset resolves to binary: its
// own CHARACTER SET or COLLATE, or the table default when it declares neither.
func columnCharsetIsBinary(col *Column, ct *CreateTable) bool {
	cs, collation := resolvedCharsetCollation(col, ct)
	return cs == charset.CharsetBin || collation == charset.CollationBin
}

// stripMemberSpaces returns members with the trailing spaces of each removed.
// It returns a new slice: the members are shared with the parsed AST, which a
// normalizer must not modify.
func stripMemberSpaces(members []string) []string {
	if members == nil {
		return nil
	}
	stripped := make([]string, len(members))
	for i, m := range members {
		stripped[i] = strings.TrimRight(m, " ")
	}
	return stripped
}
