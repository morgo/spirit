package statement

import "testing"

// A hex or bit literal default on a char or varchar column is the string its
// bytes spell in the column's charset. Each live side is the SHOW CREATE TABLE
// reading of the declared side, taken from MySQL 8.0.43. The pairs run in both
// registration orders because binaryCharsetNormalizer rewrites a char column
// whose charset resolves to binary.
func TestCharBinaryLiteralDefaultConverge(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"char: a hex literal", "(b char(4) DEFAULT x'61')", "(b char(4) DEFAULT 'a')"},
		{"varchar: a 0x hex literal", "(b varchar(4) DEFAULT 0x61)", "(b varchar(4) DEFAULT 'a')"},
		{"a bit literal", "(b char(4) DEFAULT b'01100001')", "(b char(4) DEFAULT 'a')"},
		{"an empty hex literal", "(b varchar(4) DEFAULT x'')", "(b varchar(4) DEFAULT '')"},
		{"NOT NULL", "(b varchar(4) NOT NULL DEFAULT x'61')", "(b varchar(4) NOT NULL DEFAULT 'a')"},
		{"a quote", "(b char(4) DEFAULT x'27')", "(b char(4) DEFAULT '''')"},
		{"a backslash", "(b char(4) DEFAULT x'5c')", "(b char(4) DEFAULT '\\\\')"},
		{"a NUL", "(b char(4) DEFAULT x'00')", "(b char(4) DEFAULT '\\0')"},
		{"a value that fills the width", "(b char(2) DEFAULT x'6162')", "(b char(2) DEFAULT 'ab')"},
		{"a multi-byte character counts once against the width", "(b char(1) DEFAULT x'c3a9') DEFAULT CHARSET=utf8mb4", "(b char(1) DEFAULT 'é') DEFAULT CHARSET=utf8mb4"},
		{"a multi-byte character", "(b char(4) DEFAULT x'c3a9') DEFAULT CHARSET=utf8mb4", "(b char(4) DEFAULT 'é') DEFAULT CHARSET=utf8mb4"},
		{"a utf8mb3 column", "(b char(4) CHARACTER SET utf8mb3 DEFAULT x'c3a9')", "(b char(4) CHARACTER SET utf8mb3 DEFAULT 'é')"},
		{"a utf8mb4 collation", "(b varchar(4) COLLATE utf8mb4_bin DEFAULT x'61')", "(b varchar(4) COLLATE utf8mb4_bin DEFAULT 'a')"},
		{"an ascii column", "(b char(4) CHARACTER SET ascii DEFAULT x'61')", "(b char(4) CHARACTER SET ascii DEFAULT 'a')"},
		{"a latin1 table", "(b char(4) DEFAULT x'61') DEFAULT CHARSET=latin1", "(b char(4) DEFAULT 'a') DEFAULT CHARSET=latin1"},
		{"a 4-byte character is reported as hex, as written", "(b char(4) DEFAULT x'f09f9880')", "(b char(4) DEFAULT 0xF09F9880)"},
		{"char inheriting a binary table default is binary", "(b char(3) DEFAULT x'61') DEFAULT CHARSET=binary", "(b binary(3) DEFAULT 'a\\0\\0') DEFAULT CHARSET=binary"},
		{"varchar COLLATE binary is varbinary", "(b varchar(4) COLLATE binary DEFAULT x'ff')", "(b varbinary(4) DEFAULT 0xFF)"},
	})
}

// Where the rule cannot reproduce the string MySQL reports, or MySQL rejects
// the default, it is left exactly as written.
func TestCharBinaryLiteralDefaultLeavesOtherDefaultsAlone(t *testing.T) {
	for _, tt := range []struct {
		name   string
		column string
		want   string
		kind   DefaultKind
	}{
		{"bytes that are not UTF-8, which MySQL rejects", "`b` char(4) DEFAULT x'ff'", "x'ff'", DefaultKindHexLiteral},
		{"a 4-byte character on utf8mb3, which MySQL rejects", "`b` char(4) CHARACTER SET utf8mb3 DEFAULT x'f09f9880'", "x'f09f9880'", DefaultKindHexLiteral},
		{"a value longer than the width, which MySQL rejects", "`b` char(1) DEFAULT x'6162'", "x'6162'", DefaultKindHexLiteral},
		{"a non-ASCII byte on ascii, which MySQL rejects", "`b` char(4) CHARACTER SET ascii DEFAULT x'80'", "x'80'", DefaultKindHexLiteral},
		{"a non-ASCII byte on latin1, which MySQL transcodes", "`b` char(4) CHARACTER SET latin1 DEFAULT x'e9'", "x'e9'", DefaultKindHexLiteral},
		{"a wide charset, which pads the bytes", "`b` char(4) CHARACTER SET utf16 DEFAULT x'61'", "x'61'", DefaultKindHexLiteral},
		{"multi-byte UTF-8 under an undetermined charset", "`b` char(4) DEFAULT x'c3a9'", "x'c3a9'", DefaultKindHexLiteral},
		{"an expression default", "`b` varchar(4) DEFAULT (0x61)", "x'61'", DefaultKindHexLiteral},
		{"enum", "`b` enum('a','b') DEFAULT x'61'", "x'61'", DefaultKindHexLiteral},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requireDefaultLeftAlone(t, tt.column, tt.want, tt.kind)
		})
	}
}

// A different default must still diff, and the MODIFY must carry the string
// the bytes spell.
func TestCharBinaryLiteralDefaultStillDiffsRealChanges(t *testing.T) {
	requireDefaultStillDiffs(t, "`b` varchar(4) DEFAULT x'62'", "`b` varchar(4) DEFAULT 'a'", "MODIFY COLUMN `b` varchar(4) NULL DEFAULT 'b'")
	requireDefaultStillDiffs(t, "`b` char(4) DEFAULT x'27'", "`b` char(4)", "MODIFY COLUMN `b` char(4) NULL DEFAULT '\\''")
}
