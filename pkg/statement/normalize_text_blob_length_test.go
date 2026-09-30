package statement

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTextBlobLength(t *testing.T) {
	tests := []struct {
		column     string
		options    string // table options
		wantType   string
		wantLength *int // the written length, kept only when the size is not resolved
	}{
		// BLOB(M): M is bytes.
		{"a blob(0)", "", "tinyblob", nil},
		{"a blob(100)", "", "tinyblob", nil},
		{"a blob(255)", "", "tinyblob", nil},
		{"a blob(256)", "", "blob", nil},
		{"a blob(65535)", "", "blob", nil},
		{"a blob(65536)", "", "mediumblob", nil},
		{"a blob(16777215)", "", "mediumblob", nil},
		{"a blob(16777216)", "", "longblob", nil},
		// TEXT(M): M is characters, at the charset's maximum bytes each.
		{"a text(0)", "", "tinytext", nil},
		{"a text(63)", "DEFAULT CHARSET=utf8mb4", "tinytext", nil},
		{"a text(64)", "DEFAULT CHARSET=utf8mb4", "text", nil},
		{"a text(16383)", "DEFAULT CHARSET=utf8mb4", "text", nil},
		{"a text(16384)", "DEFAULT CHARSET=utf8mb4", "mediumtext", nil},
		{"a text(4194303)", "DEFAULT CHARSET=utf8mb4", "mediumtext", nil},
		{"a text(4194304)", "DEFAULT CHARSET=utf8mb4", "longtext", nil},
		{"a text(64)", "DEFAULT COLLATE=utf8mb4_bin", "text", nil},
		{"a text(85) CHARACTER SET utf8mb3", "", "tinytext", nil},
		{"a text(86) CHARACTER SET utf8mb3", "", "text", nil},
		{"a text(85) CHARACTER SET utf8", "", "tinytext", nil},
		{"a text(255) CHARACTER SET latin1", "DEFAULT CHARSET=utf8mb4", "tinytext", nil},
		{"a text(256) CHARACTER SET latin1", "DEFAULT CHARSET=utf8mb4", "text", nil},
		{"a text(300) COLLATE latin1_bin", "DEFAULT CHARSET=utf8mb4", "text", nil},
		{"a text(100) COLLATE latin1_bin", "DEFAULT CHARSET=utf8mb4", "tinytext", nil},
		{"a text(4294967295) CHARACTER SET latin1", "", "longtext", nil},
		{"a text(127) CHARACTER SET ucs2", "", "tinytext", nil},
		{"a text(128) CHARACTER SET ucs2", "", "text", nil},
		{"a text(63) CHARACTER SET utf16", "", "tinytext", nil},
		{"a text(64) CHARACTER SET utf16", "", "text", nil},
		{"a text(100)", "DEFAULT CHARSET=latin1", "tinytext", nil},
		// The BINARY attribute selects a collation, not a charset.
		{"a text(100) BINARY", "DEFAULT CHARSET=utf8mb4", "text", nil},
		// The binary charset makes a blob, sized at 1 byte per character.
		{"a text(100) CHARACTER SET binary", "DEFAULT CHARSET=utf8mb4", "tinyblob", nil},
		{"a text(100)", "DEFAULT CHARSET=binary", "tinyblob", nil},
		{"a text(300)", "DEFAULT CHARSET=binary", "blob", nil},
		// No charset in the statement: rewritten only when the size is the
		// same for every charset (1 to 4 bytes per character). Otherwise the
		// written length is kept, so the column is emitted as text(M).
		{"a text(63)", "", "tinytext", nil},
		{"a text(64)", "", "text", new(64)},       // 64..256 bytes
		{"a text(20000)", "", "text", new(20000)}, // 20000..80000 bytes
		{"a text(70000)", "", "mediumtext", nil},
		{"a text(5000000)", "", "text", new(5000000)}, // 5000000..20000000 bytes
		{"a text(16777216)", "", "longtext", nil},
		// A charset the parser does not know is treated as undetermined.
		{"a text(64) CHARACTER SET nosuchcharset", "", "text", new(64)},
		// No length written: left alone.
		{"a text", "", "text", nil},
		{"a blob", "", "blob", nil},
		{"a tinytext", "", "tinytext", nil},
		{"a mediumblob", "", "mediumblob", nil},
		{"a longtext", "", "longtext", nil},
		// Types that are not text/blob keep their length.
		{"a varchar(100)", "", "varchar", new(100)},
	}
	for _, tc := range tests {
		t.Run(tc.column+" "+tc.options, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + tc.column + ") " + tc.options)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantType, ct.Columns[0].Type)
			assert.Equal(t, tc.wantLength, ct.Columns[0].Length)
		})
	}
}

// TestTextBlobLengthIsIdempotent runs the rule a second time over a table it
// has already normalized and checks nothing changes.
func TestTextBlobLengthIsIdempotent(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (a text(0), b text(64), c blob(70000), d text(100)) DEFAULT CHARSET=binary")
	require.NoError(t, err)
	want := []string{"tinyblob", "tinyblob", "mediumblob", "tinyblob"}
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], c.Type, c.Name)
	}
	ct = textBlobLengthNormalizer{}.Normalize(ct)
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], c.Type, c.Name)
	}
}

// TestTextBlobLengthConverges checks that each declared form diffs clean
// against the definition MySQL reports for it, in both directions.
func TestTextBlobLengthConverges(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (" +
		"a text(0), b blob(0), c blob(100), d text(64), e text(100) CHARACTER SET latin1, f blob(70000)" +
		") DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE `t` (" +
		"`a` tinytext, `b` tinyblob, `c` tinyblob, `d` text, " +
		"`e` tinytext CHARACTER SET latin1 COLLATE latin1_swedish_ci, `f` mediumblob" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)

	stmts, err = declared.Diff(live, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)
}

// TestTextBlobLengthUndeterminedIsNotNarrowed checks that a TEXT(M) whose size
// depends on a charset the statement does not name is not emitted as a plain
// `text`: on a utf8mb4 schema MySQL creates text(20000) as mediumtext, and
// `MODIFY COLUMN a text` would narrow it.
func TestTextBlobLengthUndeterminedIsNotNarrowed(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, a text(20000), PRIMARY KEY (id))")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE `t` (`id` int NOT NULL, `a` mediumtext, PRIMARY KEY (`id`)) " +
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	for _, s := range stmts {
		assert.NotContains(t, s.Statement, "`a` text NULL")
	}
}

// TestTextBlobLengthUndeterminedComparesAtOtherCharset checks that an
// unresolved TEXT(M) matches the other side's type exactly when text(M) takes
// that size at the other side's charset, in both diff directions, and that a
// mismatch emits text(M) so MySQL resolves the size at the column's charset.
func TestTextBlobLengthUndeterminedComparesAtOtherCharset(t *testing.T) {
	const utf8mb4 = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	const latin1 = " ENGINE=InnoDB DEFAULT CHARSET=latin1"
	tests := []struct {
		name     string
		declared string // column definition; the table names no charset
		live     string // the live CREATE TABLE
		want     string // the live-to-declared ALTER, "" for none
	}{
		{"utf8mb4 medium", "a text(20000)", "CREATE TABLE `t` (`a` mediumtext)" + utf8mb4, ""},
		{"utf8mb4 plain", "a text(100)", "CREATE TABLE `t` (`a` text)" + utf8mb4, ""},
		{"utf8mb4 narrower", "a text(20000)", "CREATE TABLE `t` (`a` text)" + utf8mb4,
			"ALTER TABLE `t` MODIFY COLUMN `a` text(20000) NULL"},
		{"utf8mb4 wider", "a text(100)", "CREATE TABLE `t` (`a` mediumtext)" + utf8mb4,
			"ALTER TABLE `t` MODIFY COLUMN `a` text(100) NULL"},
		{"latin1 tiny", "a text(64)", "CREATE TABLE `t` (`a` tinytext)" + latin1, ""},
		{"latin1 wider", "a text(64)", "CREATE TABLE `t` (`a` text)" + latin1,
			"ALTER TABLE `t` MODIFY COLUMN `a` text(64) NULL"},
		{"latin1 varchar", "a text(64)", "CREATE TABLE `t` (`a` varchar(10) DEFAULT NULL)" + latin1,
			"ALTER TABLE `t` MODIFY COLUMN `a` text(64) NULL"},
		{"latin1 blob", "a text(64)", "CREATE TABLE `t` (`a` tinyblob)" + latin1,
			"ALTER TABLE `t` MODIFY COLUMN `a` text(64) NULL"},
		// A difference in the column charset is still a difference.
		{"latin1 column in utf8mb4 table", "a text(100)",
			"CREATE TABLE `t` (`a` tinytext CHARACTER SET latin1 COLLATE latin1_swedish_ci)" + utf8mb4,
			"ALTER TABLE `t` MODIFY COLUMN `a` text(100) NULL"},
		// Neither side determines the charset: any size text(M) takes at
		// 1 to 4 bytes per character matches.
		{"undetermined medium", "a text(20000)", "CREATE TABLE `t` (`a` mediumtext)", ""},
		{"undetermined plain", "a text(20000)", "CREATE TABLE `t` (`a` text)", ""},
		{"undetermined long", "a text(20000)", "CREATE TABLE `t` (`a` longtext)",
			"ALTER TABLE `t` MODIFY COLUMN `a` text(20000) NULL"},
		{"both unresolved, same length", "a text(20000)", "CREATE TABLE `t` (`a` text(20000))", ""},
		{"both unresolved, other length", "a text(20000)", "CREATE TABLE `t` (`a` text(30000))",
			"ALTER TABLE `t` MODIFY COLUMN `a` text(20000) NULL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			declared, err := ParseCreateTable("CREATE TABLE t (" + tc.declared + ")")
			require.NoError(t, err)
			live, err := ParseCreateTable(tc.live)
			require.NoError(t, err)

			stmts, err := live.Diff(declared, nil)
			require.NoError(t, err)
			if tc.want == "" {
				assert.Nil(t, stmts)
			} else {
				require.Len(t, stmts, 1)
				assert.Equal(t, tc.want, stmts[0].Statement)
			}

			// The reverse direction agrees on whether the column differs. It
			// may also set the live table's DEFAULT CHARSET, which the
			// declared table does not name, so only the column is checked.
			stmts, err = declared.Diff(live, nil)
			require.NoError(t, err)
			var modifiesA bool
			for _, s := range stmts {
				modifiesA = modifiesA || strings.Contains(s.Statement, "MODIFY COLUMN `a`")
			}
			assert.Equal(t, tc.want != "", modifiesA)
		})
	}
}
