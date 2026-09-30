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
// A column whose charset the definition does not determine (no charset or
// collation of its own and no table default, which a schema file that omits
// DEFAULT CHARSET reaches) is left as written. It inherits the database
// default, which may be binary. Diff decides its members against the default
// the emitted ALTER runs under instead (see [withMembersUnder]).
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
		c.EnumValues, _ = storedMembers(c.EnumValues, c, ct)
		c.SetValues, _ = storedMembers(c.SetValues, c, ct)
	}
	return ct
}

// storedMembers returns the members MySQL stores for col, written with the
// members given, in a table with table's defaults. It returns the members
// unchanged and false when neither col nor table determines the charset.
func storedMembers(written []string, col *Column, table *CreateTable) ([]string, bool) {
	cs, collation := resolvedCharsetCollation(col, table)
	switch {
	case cs == "" && collation == "":
		return written, false // the database default decides; it may be binary
	case cs == charset.CharsetBin || collation == charset.CollationBin:
		return written, true // trailing spaces are data
	}
	return stripMemberSpaces(written), true
}

// withMembersUnder returns col with the enum or set members MySQL stores when
// the column is written in an ALTER TABLE that runs under table's defaults.
// Normalization resolves the members under the column's own table, but a
// MODIFY or ADD COLUMN that Diff emits runs under whichever default the table
// has when the ALTER applies (see alterDefaults), and that can be the other
// table's: a schema file that declares no DEFAULT CHARSET, or one whose
// default IgnoreCharsetCollation keeps out of the ALTER. The members are
// re-derived from the ones written, which normalization may have stripped. A
// charset still undetermined leaves col unchanged.
func withMembersUnder(col Column, table *CreateTable) Column {
	if col.Raw == nil || col.Raw.Tp == nil || (col.EnumValues == nil && col.SetValues == nil) {
		return col
	}
	members, ok := storedMembers(col.Raw.Tp.GetElems(), &col, table)
	if !ok {
		return col
	}
	if col.EnumValues != nil {
		col.EnumValues = members
	} else {
		col.SetValues = members
	}
	return col
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
