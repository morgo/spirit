package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A string default on an enum or set column is stored as the member(s) it
// names, so the declared and live forms must reach Diff already resolved to
// the member text. Each live side is the SHOW CREATE TABLE reading of the
// declared side, taken from MySQL 8.0.43 (8.0.28, 8.4 and 9.7 agree). The
// pairs run in both registration orders because defaultCollationNormalizer and
// binaryCharsetNormalizer rewrite the charset and collation this rule reads,
// and normalizers must not depend on registration order.
func TestEnumSetDefaultConverge(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{"enum: trailing spaces", "(b enum('a','b') DEFAULT 'b ')", "(b enum('a','b') DEFAULT 'b')"},
		{"enum: NOT NULL", "(b enum('a','b') NOT NULL DEFAULT 'b   ')", "(b enum('a','b') NOT NULL DEFAULT 'b')"},
		{"enum: a member written with trailing spaces", "(b enum('a','b ') DEFAULT 'b   ')", "(b enum('a','b') DEFAULT 'b')"},
		{"enum: only spaces name the empty member", "(b enum('','b') DEFAULT ' ')", "(b enum('','b') DEFAULT '')"},
		{"enum: adjacent string literals", "(b enum('a','b') DEFAULT 'b' ' ')", "(b enum('a','b') DEFAULT 'b')"},
		{"enum: a charset introducer", "(b enum('a','b') DEFAULT _latin1'b ')", "(b enum('a','b') DEFAULT 'b')"},
		{"enum: a NO PAD collation", "(b enum('a','b') COLLATE utf8mb4_0900_bin DEFAULT 'b ')", "(b enum('a','b') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin DEFAULT 'b')"},
		{"enum: utf16", "(b enum('a','b') CHARACTER SET utf16 DEFAULT 'b ')", "(b enum('a','b') CHARACTER SET utf16 COLLATE utf16_general_ci DEFAULT 'b')"},
		{"enum: case under the server default collation", "(b enum('a','B') DEFAULT 'b')", "(b enum('a','B') DEFAULT 'B')"},
		{"enum: case and trailing spaces", "(b enum('a','B') DEFAULT 'b  ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "(b enum('a','B') DEFAULT 'B') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
		{"enum: case under utf8mb4_general_ci", "(b enum('a','B') COLLATE utf8mb4_general_ci DEFAULT 'b')", "(b enum('a','B') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT 'B')"},
		{"enum: case under latin1's default collation", "(b enum('a','B') CHARACTER SET latin1 DEFAULT 'b')", "(b enum('a','B') CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'B')"},
		{"enum: case beside a non-ASCII character", "(b enum('a','Bé') DEFAULT 'bé ')", "(b enum('a','Bé') DEFAULT 'Bé')"},
		{"set: trailing spaces", "(b set('a','b') DEFAULT 'b ')", "(b set('a','b') DEFAULT 'b')"},
		{"set: several members", "(b set('a','b') DEFAULT 'a,b ')", "(b set('a','b') DEFAULT 'a,b')"},
		{"set: members out of order", "(b set('a','b') DEFAULT 'b,a ')", "(b set('a','b') DEFAULT 'a,b')"},
		{"set: members in another case", "(b set('a','b','c') DEFAULT 'c,A,b,a')", "(b set('a','b','c') DEFAULT 'a,b,c')"},
		{"set: a member listed twice", "(b set('a','b') DEFAULT 'a,a')", "(b set('a','b') DEFAULT 'a')"},
		{"set: the empty set", "(b set('a','b') DEFAULT '')", "(b set('a','b') DEFAULT '')"},
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

// Where MySQL rejects the default, keeps its spaces, or matches it only
// through collation rules the rule does not model, the default is left
// exactly as written.
func TestEnumSetDefaultLeavesOtherDefaultsAlone(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  string
		kind  DefaultKind
	}{
		{"enum: a leading space, which MySQL rejects", "(`b` enum('a','b') DEFAULT ' b')", " b", DefaultKindString},
		{"enum: a trailing tab, which MySQL rejects", "(`b` enum('a','b') DEFAULT 'b\\t')", "b\t", DefaultKindString},
		{"enum: a trailing NUL, ignorable only under some collations", "(`b` enum('a','b') DEFAULT 'b\\0')", "b\x00", DefaultKindString},
		{"enum: a no-break space, which MySQL rejects", "(`b` enum('a','b') DEFAULT 'b ')", "b ", DefaultKindString},
		{"enum: no member, which MySQL rejects", "(`b` enum('a','b') DEFAULT 'c ')", "c ", DefaultKindString},
		{"enum: case under a _cs collation, which MySQL rejects", "(`b` enum('a','B') COLLATE utf8mb4_0900_as_cs DEFAULT 'b')", "b", DefaultKindString},
		{"enum: case under a _bin collation, which MySQL rejects", "(`b` enum('a','B') COLLATE utf8mb4_bin DEFAULT 'b ')", "b ", DefaultKindString},
		{"enum: case under a Turkish collation", "(`b` enum('a','I') COLLATE utf8mb4_tr_0900_ai_ci DEFAULT 'i')", "i", DefaultKindString},
		{"enum: case under latin5, whose default collation is Turkish", "(`b` enum('a','B') CHARACTER SET latin5 DEFAULT 'b')", "b", DefaultKindString},
		{"enum: an accent, matched by the collation alone", "(`b` enum('a','e') DEFAULT 'é')", "é", DefaultKindString},
		{"enum: a non-ASCII case pair", "(`b` enum('a','é') DEFAULT 'É')", "É", DefaultKindString},
		{"enum: CHARACTER SET binary keeps its spaces", "(`b` enum('a','b') CHARACTER SET binary DEFAULT 'b ')", "b ", DefaultKindString},
		{"enum: a binary table default keeps its spaces", "(`b` enum('a','b') DEFAULT 'b ') DEFAULT CHARSET=binary", "b ", DefaultKindString},
		{"enum: an expression default", "(`b` enum('a','b') DEFAULT ('b '))", "b ", DefaultKindString},
		{"enum: a member index", "(`b` enum('a','b') DEFAULT 2)", "2", DefaultKindNumber},
		{"enum: TRUE", "(`b` enum('0','1') DEFAULT TRUE)", "TRUE", DefaultKindKeywordBool},
		{"enum: NULL", "(`b` enum('a','b') DEFAULT NULL)", "NULL", DefaultKindUnknown},
		{"set: a space inside the list, which MySQL rejects", "(`b` set('a','b') DEFAULT 'a ,b ')", "a ,b ", DefaultKindString},
		{"set: a space after a comma, which MySQL rejects", "(`b` set('a','b') DEFAULT 'a, b')", "a, b", DefaultKindString},
		{"set: an empty element, which MySQL rejects", "(`b` set('a','b') DEFAULT 'a,')", "a,", DefaultKindString},
		{"set: only spaces, which MySQL rejects", "(`b` set('a','b') DEFAULT ' ')", " ", DefaultKindString},
		{"set: only spaces with an empty member, which MySQL rejects", "(`b` set('','a') DEFAULT '  ')", "  ", DefaultKindString},
		{"varchar keeps its trailing spaces", "(`b` varchar(4) DEFAULT 'b ')", "b ", DefaultKindString},
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

// A default that names a different member must still diff, and the emitted
// MODIFY carries the member text MySQL reports back.
func TestEnumSetDefaultStillDiffsRealChanges(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		live     string
		want     string
	}{
		{
			name:     "a different member",
			declared: "`b` enum('a','b') DEFAULT 'a '",
			live:     "`b` enum('a','b') DEFAULT 'b'",
			want:     "MODIFY COLUMN `b` enum('a','b') NULL DEFAULT 'a'",
		},
		{
			name:     "a default added to a column that had none",
			declared: "`b` enum('a','B') DEFAULT 'b '",
			live:     "`b` enum('a','B')",
			want:     "MODIFY COLUMN `b` enum('a','B') NULL DEFAULT 'B'",
		},
		{
			name:     "a set default with another member",
			declared: "`b` set('a','b','c') DEFAULT 'c,a '",
			live:     "`b` set('a','b','c') DEFAULT 'a'",
			want:     "MODIFY COLUMN `b` set('a','b','c') NULL DEFAULT 'a,c'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := ParseCreateTable("CREATE TABLE `t` (" + tt.declared + ") DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
			require.NoError(t, err)
			source, err := ParseCreateTable("CREATE TABLE `t` (" + tt.live + ") DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
			require.NoError(t, err)
			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Contains(t, stmts[0].Statement, tt.want)
		})
	}
}
