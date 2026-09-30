package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTextBlobLength(t *testing.T) {
	tests := []struct {
		column   string
		options  string // table options
		wantType string
	}{
		// BLOB(M): M is bytes.
		{"a blob(0)", "", "tinyblob"},
		{"a blob(100)", "", "tinyblob"},
		{"a blob(255)", "", "tinyblob"},
		{"a blob(256)", "", "blob"},
		{"a blob(65535)", "", "blob"},
		{"a blob(65536)", "", "mediumblob"},
		{"a blob(16777215)", "", "mediumblob"},
		{"a blob(16777216)", "", "longblob"},
		// TEXT(M): M is characters, at the charset's maximum bytes each.
		{"a text(0)", "", "tinytext"},
		{"a text(63)", "DEFAULT CHARSET=utf8mb4", "tinytext"},
		{"a text(64)", "DEFAULT CHARSET=utf8mb4", "text"},
		{"a text(16383)", "DEFAULT CHARSET=utf8mb4", "text"},
		{"a text(16384)", "DEFAULT CHARSET=utf8mb4", "mediumtext"},
		{"a text(4194303)", "DEFAULT CHARSET=utf8mb4", "mediumtext"},
		{"a text(4194304)", "DEFAULT CHARSET=utf8mb4", "longtext"},
		{"a text(64)", "DEFAULT COLLATE=utf8mb4_bin", "text"},
		{"a text(85) CHARACTER SET utf8mb3", "", "tinytext"},
		{"a text(86) CHARACTER SET utf8mb3", "", "text"},
		{"a text(85) CHARACTER SET utf8", "", "tinytext"},
		{"a text(255) CHARACTER SET latin1", "DEFAULT CHARSET=utf8mb4", "tinytext"},
		{"a text(256) CHARACTER SET latin1", "DEFAULT CHARSET=utf8mb4", "text"},
		{"a text(300) COLLATE latin1_bin", "DEFAULT CHARSET=utf8mb4", "text"},
		{"a text(100) COLLATE latin1_bin", "DEFAULT CHARSET=utf8mb4", "tinytext"},
		{"a text(4294967295) CHARACTER SET latin1", "", "longtext"},
		{"a text(127) CHARACTER SET ucs2", "", "tinytext"},
		{"a text(128) CHARACTER SET ucs2", "", "text"},
		{"a text(63) CHARACTER SET utf16", "", "tinytext"},
		{"a text(64) CHARACTER SET utf16", "", "text"},
		{"a text(100)", "DEFAULT CHARSET=latin1", "tinytext"},
		// The BINARY attribute selects a collation, not a charset.
		{"a text(100) BINARY", "DEFAULT CHARSET=utf8mb4", "text"},
		// The binary charset makes a blob, sized at 1 byte per character.
		{"a text(100) CHARACTER SET binary", "DEFAULT CHARSET=utf8mb4", "tinyblob"},
		{"a text(100)", "DEFAULT CHARSET=binary", "tinyblob"},
		{"a text(300)", "DEFAULT CHARSET=binary", "blob"},
		// No charset in the statement: rewritten only when the size is the
		// same for every charset (1 to 4 bytes per character).
		{"a text(63)", "", "tinytext"},
		{"a text(64)", "", "text"},    // 64..256 bytes: left as written
		{"a text(20000)", "", "text"}, // 20000..80000 bytes: left as written
		{"a text(70000)", "", "mediumtext"},
		{"a text(5000000)", "", "text"}, // 5000000..20000000 bytes: left as written
		{"a text(16777216)", "", "longtext"},
		// No length written: left alone.
		{"a text", "", "text"},
		{"a blob", "", "blob"},
		{"a tinytext", "", "tinytext"},
		{"a mediumblob", "", "mediumblob"},
		{"a longtext", "", "longtext"},
		// Types that are not text/blob keep their length.
		{"a varchar(100)", "", "varchar"},
	}
	for _, tc := range tests {
		t.Run(tc.column+" "+tc.options, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + tc.column + ") " + tc.options)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantType, ct.Columns[0].Type)
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
