package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

func init() { registerNormalizer(enumSetDefaultNormalizer{}) }

// enumSetDefaultNormalizer rewrites the string DEFAULT of an enum or set column
// to the member text SHOW CREATE TABLE reports. MySQL stores an enum or set
// default as the index (or bitmap) of the member(s) it names, not as the text
// written, so `b enum('a','b') DEFAULT 'b '` is reported as
// `b enum('a','b') DEFAULT 'b'`. Without the rewrite the written default diffs
// against the live column and emits a MODIFY COLUMN that MySQL stores as the
// member again, so the next diff emits it again.
//
// MySQL strips the trailing spaces (U+0020 only) from the default, then looks
// the rest up in the member list under the column's collation. A set default
// is split on commas after the strip, each element is looked up on its own,
// and the members are reported once each in definition order. The parser
// already strips trailing spaces from the members themselves, as MySQL does.
// Verified against MySQL 8.0.28, 8.0.43, 8.4 and 9.7, which agree on every
// case:
//
//	enum('a','b') DEFAULT 'b ' / 'b' ' ' / _latin1'b '  -> 'b'
//	enum('','b') DEFAULT ' '                            -> ''
//	enum('a','b') COLLATE utf8mb4_0900_bin DEFAULT 'b ' -> 'b'   (NO PAD too)
//	enum('a','b') CHARACTER SET utf16 DEFAULT 'b '      -> 'b'
//	enum('a','B') DEFAULT 'b' (any _ci collation)       -> 'B'
//	enum('a','B') COLLATE utf8mb4_0900_as_cs / _bin DEFAULT 'b' -> error 1067
//	enum('a','I') COLLATE utf8mb4_tr_0900_ai_ci DEFAULT 'i'     -> error 1067
//	enum('a','e') DEFAULT 'é' (utf8mb4_0900_ai_ci)      -> 'e'
//	enum('a','b') DEFAULT 'b\0' (utf8mb4_0900_ai_ci)    -> 'b'   (NUL is ignorable)
//	enum('a','b') DEFAULT ' b' / 'b\t' / 'b\n' / NBSP   -> error 1067
//	enum('a','b ') CHARACTER SET binary DEFAULT 'b '    -> 'b '  (spaces are data)
//	set('a','b') DEFAULT 'b ' / 'a,b ' / 'b,a' / 'B,A'  -> 'b' / 'a,b' / 'a,b' / 'a,b'
//	set('a','b') DEFAULT 'a,a'                          -> 'a'
//	set('a','b') DEFAULT 'a ,b ' / 'a, b' / 'a,'        -> error 1067
//	set('','a') DEFAULT ' '                             -> error 1067  (unlike enum)
//	enum('a','b') DEFAULT ('b ')                        -> (_utf8mb4'b ')  (an expression is kept)
//
// Modelling the collation in full is out of reach here, so the rule resolves
// a default only where the answer does not depend on it: a member equal to
// the stripped default byte for byte, or, on a case-insensitive (_ci)
// collation, a member that differs from it only in the case of ASCII letters.
// MySQL rejects an enum or set whose members are duplicates under the
// collation (error 1291), so at most one member can match and the one found
// is the one MySQL stores. The Turkish and Azerbaijani collations are not
// taken to fold case, since there I and i are different letters. A column
// whose collation the table does not determine is taken to use its charset's
// default collation, or utf8mb4's when the charset is not determined either,
// as the server default is; every such default is _ci except latin5's, which
// is Turkish.
//
// Left alone:
//
//   - a column whose charset or collation is binary, where trailing spaces
//     are data.
//   - a set default of only spaces, which MySQL rejects even though an enum
//     default of only spaces names the empty member.
//   - a default that no member matches by the test above: one MySQL rejects,
//     or one it matches through its collation alone (an accent, a non-ASCII
//     case pair, an ignorable character). A default written that way keeps
//     diffing, as it did before this rule.
//   - a numeric or TRUE/FALSE default, which MySQL reads as a member index
//     (see [booleanKeywordDefaultNormalizer] for how that differs by version).
//   - an expression default, which MySQL stores as written.
type enumSetDefaultNormalizer struct{}

func (enumSetDefaultNormalizer) Name() string { return "enum-set-default" }

func (enumSetDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.DefaultKind != DefaultKindString {
			continue
		}
		cs, collation := resolvedCharsetCollation(c, ct)
		if cs == charset.CharsetBin || collation == charset.CollationBin {
			continue // trailing spaces are data
		}
		foldCase := collationFoldsASCIICase(cs, collation)
		value := strings.TrimRight(*c.Default, " ")
		var resolved string
		var ok bool
		switch strings.ToLower(c.Type) {
		case "enum":
			resolved, ok = matchMember(value, c.EnumValues, foldCase)
		case "set":
			if value == "" && *c.Default != "" {
				continue // MySQL rejects a set default of only spaces
			}
			resolved, ok = resolveSetDefault(value, c.SetValues, foldCase)
		}
		if ok {
			c.Default = &resolved
		}
	}
	return ct
}

// matchMember returns the member a stripped enum or set default names, or
// false when no member matches it without modelling the collation.
func matchMember(value string, members []string, foldCase bool) (string, bool) {
	for _, m := range members {
		if m == value {
			return m, true
		}
	}
	if foldCase {
		for _, m := range members {
			if equalFoldASCII(m, value) {
				return m, true
			}
		}
	}
	return "", false
}

// resolveSetDefault returns a stripped set default as MySQL reports it: each
// member it names once, in definition order, joined by commas. It returns
// false when any element matches no member.
func resolveSetDefault(value string, members []string, foldCase bool) (string, bool) {
	if value == "" {
		return "", true
	}
	named := make(map[string]bool)
	for elem := range strings.SplitSeq(value, ",") {
		m, ok := matchMember(elem, members, foldCase)
		if !ok {
			return "", false
		}
		named[m] = true
	}
	var resolved []string
	for _, m := range members {
		if named[m] {
			resolved = append(resolved, m)
			delete(named, m) // a member listed twice is reported once
		}
	}
	return strings.Join(resolved, ","), true
}

// collationFoldsASCIICase reports whether a collation treats two strings that
// differ only in the case of ASCII letters as equal. A collation that is not
// determined is taken to be its charset's default, and a charset that is not
// determined to be utf8mb4, as the server default is.
func collationFoldsASCIICase(cs, collation string) bool {
	if collation == "" {
		if cs == "" {
			cs = charset.CharsetUTF8MB4
		}
		var ok bool
		if collation, ok = charset.MySQLDefaultCollation(cs); !ok {
			return false
		}
	}
	collation = strings.ToLower(collation)
	if !strings.HasSuffix(collation, "_ci") {
		return false
	}
	// I and i are different letters in Turkish and Azerbaijani.
	return !strings.Contains(collation, "turkish") &&
		!strings.Contains(collation, "_tr_") &&
		!strings.Contains(collation, "_az_")
}

// equalFoldASCII reports whether a and b are equal once ASCII letters are
// folded to one case. Every other byte must match exactly.
func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
