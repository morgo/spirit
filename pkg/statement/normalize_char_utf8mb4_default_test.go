package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A string default on a utf8mb4 char or varchar column that is not valid
// utf8mb3 is the same default MySQL reports as a hex literal, so the declared
// and live forms must reach Diff already converted together. Each live side is
// the SHOW CREATE TABLE reading of the declared side, taken from MySQL 8.0.43.
// The pairs run in both registration orders because other rules rewrite the
// defaults and charsets of the same columns.
func TestCharUTF8MB4DefaultConverge(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	const utf8mb4 = " DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{"char", "(b char(4) DEFAULT '😀')" + utf8mb4, "(b char(4) DEFAULT 0xF09F9880)" + utf8mb4},
		{"varchar", "(b varchar(4) DEFAULT '😀')" + utf8mb4, "(b varchar(4) DEFAULT 0xF09F9880)" + utf8mb4},
		{"NOT NULL", "(b char(4) NOT NULL DEFAULT '😀')" + utf8mb4, "(b char(4) NOT NULL DEFAULT 0xF09F9880)" + utf8mb4},
		{"mixed with utf8mb3 characters", "(b varchar(4) DEFAULT 'a😀é')" + utf8mb4, "(b varchar(4) DEFAULT 0x61F09F9880C3A9)" + utf8mb4},
		{"a quote and a backslash", "(b varchar(4) DEFAULT '''\\\\😀')" + utf8mb4, "(b varchar(4) DEFAULT 0x275CF09F9880)" + utf8mb4},
		{"a NUL", "(b varchar(4) DEFAULT 'a\\0😀')" + utf8mb4, "(b varchar(4) DEFAULT 0x6100F09F9880)" + utf8mb4},
		{"exactly the width in characters", "(b char(2) DEFAULT '😀😀')" + utf8mb4, "(b char(2) DEFAULT 0xF09F9880F09F9880)" + utf8mb4},
		{"char strips trailing spaces", "(b char(4) DEFAULT '😀  ')" + utf8mb4, "(b char(4) DEFAULT 0xF09F9880)" + utf8mb4},
		{"varchar keeps trailing spaces", "(b varchar(4) DEFAULT '😀 ')" + utf8mb4, "(b varchar(4) DEFAULT 0xF09F988020)" + utf8mb4},
		{"a charset introducer", "(b char(4) DEFAULT _utf8mb4'😀')" + utf8mb4, "(b char(4) DEFAULT 0xF09F9880)" + utf8mb4},
		{"a hex literal, which is already the reported form", "(b char(4) DEFAULT x'f09f9880')" + utf8mb4, "(b char(4) DEFAULT 0xF09F9880)" + utf8mb4},
		{"a utf8mb4 column in a latin1 table", "(b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT '😀') DEFAULT CHARSET=latin1", "(b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 0xF09F9880) DEFAULT CHARSET=latin1"},
		{"a utf8mb4 collation", "(b varchar(4) COLLATE utf8mb4_bin DEFAULT '😀')" + utf8mb4, "(b varchar(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 0xF09F9880)" + utf8mb4},
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

// Where MySQL reports the string as written, or the rule cannot reproduce the
// bytes MySQL reports, the default is left exactly as written.
func TestCharUTF8MB4DefaultLeavesOtherDefaultsAlone(t *testing.T) {
	const utf8mb4 = " DEFAULT CHARSET=utf8mb4"
	tests := []struct {
		name  string
		table string
		want  string
		kind  DefaultKind
	}{
		{"a utf8mb3 string", "(`b` char(4) DEFAULT 'é')" + utf8mb4, "é", DefaultKindString},
		{"a utf8mb3 string with trailing spaces", "(`b` char(4) DEFAULT 'a ')" + utf8mb4, "a ", DefaultKindString},
		{"longer than the width, which MySQL rejects", "(`b` char(1) DEFAULT '😀😀')" + utf8mb4, "😀😀", DefaultKindString},
		{"an expression default", "(`b` char(4) DEFAULT ('😀'))" + utf8mb4, "😀", DefaultKindString},
		{"an undetermined charset", "(`b` char(4) DEFAULT '😀')", "😀", DefaultKindString},
		{"utf8mb3, which rejects the value", "(`b` char(4) CHARACTER SET utf8mb3 DEFAULT '😀')", "😀", DefaultKindString},
		{"utf16, which reports its own bytes", "(`b` char(4) CHARACTER SET utf16 DEFAULT '😀')" + utf8mb4, "😀", DefaultKindString},
		{"a utf16 column in a utf8mb4 table", "(`b` varchar(4) COLLATE utf16_bin DEFAULT '😀')" + utf8mb4, "😀", DefaultKindString},
		{"enum, whose member is reported as '?'", "(`b` enum('😀','a') DEFAULT '😀')" + utf8mb4, "😀", DefaultKindString},
		{"set, whose member is reported as '?'", "(`b` set('😀','a') DEFAULT '😀')" + utf8mb4, "😀", DefaultKindString},
		{"a binary charset, left to the binary rule", "(`b` varchar(4) DEFAULT '😀') DEFAULT CHARSET=binary", "x'f09f9880'", DefaultKindHexLiteral},
		{"NULL", "(`b` char(4) DEFAULT NULL)" + utf8mb4, "NULL", DefaultKindUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE `t` " + tt.table)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, tt.want, *ct.Columns[0].Default)
			assert.Equal(t, tt.kind, ct.Columns[0].DefaultKind)
		})
	}
}

// A default that is genuinely different must still diff, and the emitted MODIFY
// must carry a non-utf8mb3 value as the bare hex literal MySQL reports.
func TestCharUTF8MB4DefaultStillDiffsRealChanges(t *testing.T) {
	const utf8mb4 = " DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tests := []struct {
		name     string
		declared string
		live     string
		want     string
	}{
		{
			name:     "a different 4-byte character",
			declared: "(b char(4) DEFAULT '😁')" + utf8mb4,
			live:     "(b char(4) DEFAULT 0xF09F9880)" + utf8mb4,
			want:     "MODIFY COLUMN `b` char(4) NULL DEFAULT x'f09f9881'",
		},
		{
			name:     "a new default",
			declared: "(b varchar(4) DEFAULT '😀')" + utf8mb4,
			live:     "(b varchar(4))" + utf8mb4,
			want:     "MODIFY COLUMN `b` varchar(4) NULL DEFAULT x'f09f9880'",
		},
		{
			name:     "from a utf8mb3 string",
			declared: "(b varchar(4) DEFAULT '😀')" + utf8mb4,
			live:     "(b varchar(4) DEFAULT 'a')" + utf8mb4,
			want:     "MODIFY COLUMN `b` varchar(4) NULL DEFAULT x'f09f9880'",
		},
		{
			name:     "to a utf8mb3 string",
			declared: "(b varchar(4) DEFAULT 'a')" + utf8mb4,
			live:     "(b varchar(4) DEFAULT 0xF09F9880)" + utf8mb4,
			want:     "MODIFY COLUMN `b` varchar(4) NULL DEFAULT 'a'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := ParseCreateTable("CREATE TABLE `t` " + tt.declared)
			require.NoError(t, err)
			source, err := ParseCreateTable("CREATE TABLE `t` " + tt.live)
			require.NoError(t, err)
			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Contains(t, stmts[0].Statement, tt.want)
		})
	}
}
