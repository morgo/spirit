package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A literal default on a binary(N) column is the same default MySQL stores
// NUL-padded to the column width, so the declared and live forms must reach Diff
// already padded together. Each live side is the SHOW CREATE TABLE reading of
// the declared side, taken from MySQL 8.0.43. The pairs run in both registration
// orders because binaryCharsetNormalizer (char -> binary) and
// booleanKeywordDefaultNormalizer (TRUE -> '1' on char) rewrite the same
// columns, and normalizers must not depend on registration order.
func TestBinaryDefaultPaddingConverges(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{"a string shorter than the width", "(b binary(3) DEFAULT 'a')", "(b binary(3) DEFAULT 'a\\0\\0')"},
		{"NOT NULL", "(b binary(3) NOT NULL DEFAULT 'a')", "(b binary(3) NOT NULL DEFAULT 'a\\0\\0')"},
		{"an empty string", "(b binary(3) DEFAULT '')", "(b binary(3) DEFAULT '\\0\\0\\0')"},
		{"a column written without a width is binary(1)", "(b binary DEFAULT '')", "(b binary(1) DEFAULT '\\0')"},
		{"TRUE on a column written without a width", "(b binary DEFAULT TRUE)", "(b binary(1) DEFAULT '1')"},
		{"a string that is already the full width", "(b binary(3) DEFAULT 'abc')", "(b binary(3) DEFAULT 'abc')"},
		{"a string with some of its NULs written", "(b binary(3) DEFAULT 'a\\0')", "(b binary(3) DEFAULT 'a\\0\\0')"},
		{"trailing spaces are data, not padding", "(b binary(3) DEFAULT 'a ')", "(b binary(3) DEFAULT 'a \\0')"},
		{"a multi-byte character pads by bytes", "(b binary(3) DEFAULT 'é')", "(b binary(3) DEFAULT 'é\\0')"},
		{"TRUE", "(b binary(4) DEFAULT TRUE)", "(b binary(4) DEFAULT '1\\0\\0\\0')"},
		{"FALSE", "(b binary(4) NOT NULL DEFAULT FALSE)", "(b binary(4) NOT NULL DEFAULT '0\\0\\0\\0')"},
		{"an integer", "(b binary(3) DEFAULT 1)", "(b binary(3) DEFAULT '1\\0\\0')"},
		{"a negative integer", "(b binary(3) DEFAULT -1)", "(b binary(3) DEFAULT '-1\\0')"},
		{"a signed positive integer", "(b binary(3) DEFAULT +1)", "(b binary(3) DEFAULT '1\\0\\0')"},
		{"a hex literal", "(b binary(3) DEFAULT x'61')", "(b binary(3) DEFAULT 'a\\0\\0')"},
		{"a 0x hex literal", "(b binary(3) DEFAULT 0x61)", "(b binary(3) DEFAULT 'a\\0\\0')"},
		{"an empty hex literal", "(b binary(3) DEFAULT x'')", "(b binary(3) DEFAULT '\\0\\0\\0')"},
		{"a hex literal with a leading zero byte", "(b binary(3) DEFAULT x'0061')", "(b binary(3) DEFAULT '\\0a\\0')"},
		{"a bit literal", "(b binary(3) DEFAULT b'01100001')", "(b binary(3) DEFAULT 'a\\0\\0')"},
		{"a bit literal keeps its leading zero byte", "(b binary(3) DEFAULT b'0000000001100001')", "(b binary(3) DEFAULT '\\0a\\0')"},
		{"bytes that are not UTF-8 are reported as hex", "(b binary(3) DEFAULT x'ff')", "(b binary(3) DEFAULT 0xFF0000)"},
		{"a 4-byte character is not utf8mb3, so is reported as hex", "(b binary(4) DEFAULT x'f09f9880')", "(b binary(4) DEFAULT 0xF09F9880)"},
		{"a string whose bytes are not utf8mb3", "(b binary(5) DEFAULT '😀')", "(b binary(5) DEFAULT 0xF09F988000)"},
		{"char inheriting a binary table default", "(b char(3) DEFAULT 'a') DEFAULT CHARSET=binary", "(b binary(3) DEFAULT 'a\\0\\0') DEFAULT CHARSET=binary"},
		{"TRUE on char inheriting a binary table default", "(b char(3) DEFAULT TRUE) DEFAULT CHARSET=binary", "(b binary(3) DEFAULT '1\\0\\0') DEFAULT CHARSET=binary"},
		{"char COLLATE binary", "(b char(3) COLLATE binary DEFAULT 'a')", "(b binary(3) DEFAULT 'a\\0\\0')"},
		{"char CHARACTER SET binary", "(b char(3) CHARACTER SET binary DEFAULT 'a')", "(b binary(3) DEFAULT 'a\\0\\0')"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				target, err := ParseCreateTable("CREATE TABLE `t` " + tt.declared)
				require.NoError(t, err)
				source, err := ParseCreateTable("CREATE TABLE `t` " + tt.live)
				require.NoError(t, err)

				stmts, err := source.Diff(target, nil)
				require.NoError(t, err)
				assert.Nil(t, stmts, "the declared and live forms are the same default")
				stmts, err = target.Diff(source, nil)
				require.NoError(t, err)
				assert.Nil(t, stmts)
			}
		})
	}
}

// Where MySQL does not NUL-pad the default, or the width to pad to is unknown,
// the default is left exactly as written.
func TestBinaryDefaultPaddingLeavesOtherDefaultsAlone(t *testing.T) {
	tests := []struct {
		name   string
		column string
		want   string
		kind   DefaultKind
	}{
		{"binary(0), whose only default has nothing to pad", "`b` binary(0) DEFAULT ''", "", DefaultKindString},
		{"a default longer than the width, which MySQL rejects", "`b` binary(3) DEFAULT 'abcd'", "abcd", DefaultKindString},
		{"a decimal, which MySQL formats itself", "`b` binary(3) DEFAULT 1.5", "1.5", DefaultKindNumber},
		{"an expression default, which MySQL stores unpadded", "`b` binary(3) DEFAULT ('a')", "a", DefaultKindString},
		{"varbinary, which has no width to pad to", "`b` varbinary(3) DEFAULT 'a'", "a", DefaultKindString},
		{"char on a character charset", "`b` char(3) DEFAULT 'a'", "a", DefaultKindString},
		{"NULL", "`b` binary(3) DEFAULT NULL", "NULL", DefaultKindUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE `t` (" + tt.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, tt.want, *ct.Columns[0].Default)
			assert.Equal(t, tt.kind, ct.Columns[0].DefaultKind)
		})
	}
}

// A default that is genuinely different must still diff, and the emitted MODIFY
// must carry the padded value in a form MySQL stores unchanged: NULs escaped
// inside a string, or a bare hex literal.
func TestBinaryDefaultPaddingStillDiffsRealChanges(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		live     string
		want     string
	}{
		{
			name:     "a different string",
			declared: "`b` binary(3) DEFAULT 'b'",
			live:     "`b` binary(3) DEFAULT 'a\\0\\0'",
			want:     "MODIFY COLUMN `b` binary(3) NULL DEFAULT 'b\\0\\0'",
		},
		{
			name:     "a default added to a column that had none",
			declared: "`b` binary(3) DEFAULT 'a'",
			live:     "`b` binary(3)",
			want:     "MODIFY COLUMN `b` binary(3) NULL DEFAULT 'a\\0\\0'",
		},
		{
			name:     "a default that is not UTF-8",
			declared: "`b` binary(3) DEFAULT x'ff'",
			live:     "`b` binary(3)",
			want:     "MODIFY COLUMN `b` binary(3) NULL DEFAULT x'ff0000'",
		},
		{
			name:     "a width change pads to the new width",
			declared: "`b` binary(4) DEFAULT 'a'",
			live:     "`b` binary(3) DEFAULT 'a\\0\\0'",
			want:     "MODIFY COLUMN `b` binary(4) NULL DEFAULT 'a\\0\\0\\0'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := ParseCreateTable("CREATE TABLE `t` (" + tt.declared + ")")
			require.NoError(t, err)
			source, err := ParseCreateTable("CREATE TABLE `t` (" + tt.live + ")")
			require.NoError(t, err)

			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Contains(t, stmts[0].Statement, tt.want)
		})
	}
}
