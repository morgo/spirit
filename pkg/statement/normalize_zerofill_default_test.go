package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZerofillDefault(t *testing.T) {
	tests := []struct {
		column      string
		wantDefault *string
		wantKind    DefaultKind
	}{
		{"a int(10) zerofill DEFAULT 5", new("0000000005"), DefaultKindString},
		{"a int(10) unsigned zerofill DEFAULT '0000000005'", new("0000000005"), DefaultKindString},
		{"a int(3) zerofill DEFAULT 5", new("005"), DefaultKindString},
		{"a int(3) zerofill DEFAULT 12345", new("12345"), DefaultKindString}, // never truncated
		{"a tinyint(2) zerofill DEFAULT '7'", new("07"), DefaultKindString},
		{"a bigint(20) zerofill DEFAULT 0", new("00000000000000000000"), DefaultKindString},
		{"a int(4) zerofill NOT NULL DEFAULT 42", new("0042"), DefaultKindString},
		{"a tinyint(1) zerofill DEFAULT 1", new("1"), DefaultKindString},
		// A width that is unwritten or 0 is the unsigned default width.
		{"a int(0) zerofill DEFAULT 5", new("0000000005"), DefaultKindString},
		{"a int zerofill DEFAULT 5", new("0000000005"), DefaultKindString},
		{"a tinyint zerofill DEFAULT 5", new("005"), DefaultKindString},
		{"a smallint zerofill DEFAULT 5", new("00005"), DefaultKindString},
		{"a mediumint zerofill DEFAULT 5", new("00000005"), DefaultKindString},
		{"a bigint zerofill DEFAULT 5", new("00000000000000000005"), DefaultKindString},
		// Hex, bit and keyword literals convert to their integer.
		{"a int(4) zerofill DEFAULT 0x10", new("0016"), DefaultKindString},
		{"a int(4) zerofill DEFAULT x'10'", new("0016"), DefaultKindString},
		{"a int(4) zerofill DEFAULT b'101'", new("0005"), DefaultKindString},
		{"a int(4) zerofill DEFAULT TRUE", new("0001"), DefaultKindString},
		{"a int(4) zerofill DEFAULT FALSE", new("0000"), DefaultKindString},
		// A decimal and a string round half up; a float rounds half to even.
		{"a int(4) zerofill DEFAULT 2.5", new("0003"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 5.4", new("0005"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 1.4999", new("0001"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 2.50000000000000000001", new("0003"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 0.4", new("0000"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 2.5e0", new("0002"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 3.5e0", new("0004"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 0.5e0", new("0000"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 25e-1", new("0002"), DefaultKindString},
		{"a int(4) zerofill DEFAULT 1e1", new("0010"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '2.5'", new("0003"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '5.6'", new("0006"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '2.5e0'", new("0003"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '25e-1'", new("0003"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '1.5e1'", new("0015"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '2.49999999999999999999'", new("0002"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '.5'", new("0001"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '5.'", new("0005"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '+5'", new("0005"), DefaultKindString},
		{"a int(4) zerofill DEFAULT ' 5 '", new("0005"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '007'", new("0007"), DefaultKindString},
		{"a int(4) zerofill DEFAULT +7", new("0007"), DefaultKindString},
		// Left alone.
		{"a int(4) zerofill DEFAULT (5)", new("5"), DefaultKindNumber},
		{"a int(4) zerofill DEFAULT NULL", new("NULL"), DefaultKindUnknown},
		{"a int(4) zerofill", nil, DefaultKindUnknown},
		{"a int(4) zerofill DEFAULT 'abc'", new("abc"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '-0.4'", new("-0.4"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '0x10'", new("0x10"), DefaultKindString},
		{"a int(4) zerofill DEFAULT '1e100'", new("1e100"), DefaultKindString},
		{"a decimal(6,2) zerofill DEFAULT 1.5", new("1.5"), DefaultKindNumber},
		{"a double zerofill DEFAULT 1.5", new("1.5"), DefaultKindNumber},
		// Not ZEROFILL: no padding.
		{"a int(4) unsigned DEFAULT 5", new("5"), DefaultKindNumber},
	}
	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + tc.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantDefault, ct.Columns[0].Default)
			assert.Equal(t, tc.wantKind, ct.Columns[0].DefaultKind)
		})
	}
}

// TestZerofillDefaultIsIdempotent runs the rule a second time over a table it
// has already normalized and checks nothing changes.
func TestZerofillDefaultIsIdempotent(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (a int(10) zerofill DEFAULT 5, b int(4) zerofill DEFAULT 0x10, c int(3) zerofill DEFAULT 12345)")
	require.NoError(t, err)
	want := []string{"0000000005", "0016", "12345"}
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], *c.Default, c.Name)
	}
	ct = zerofillDefaultNormalizer{}.Normalize(ct)
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], *c.Default, c.Name)
	}
}

// TestZerofillDefaultConverges checks that each declared form diffs clean
// against the definition MySQL reports for it, in both directions.
func TestZerofillDefaultConverges(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (" +
		"a int(10) zerofill DEFAULT 5, b int(3) zerofill DEFAULT 12345, c tinyint(2) zerofill DEFAULT '7', " +
		"d int(4) zerofill DEFAULT 0x10, e int(4) zerofill DEFAULT TRUE, f int(4) zerofill NOT NULL DEFAULT 2.5)")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE `t` (" +
		"`a` int(10) unsigned zerofill DEFAULT '0000000005', `b` int(3) unsigned zerofill DEFAULT '12345', " +
		"`c` tinyint(2) unsigned zerofill DEFAULT '07', `d` int(4) unsigned zerofill DEFAULT '0016', " +
		"`e` int(4) unsigned zerofill DEFAULT '0001', `f` int(4) unsigned zerofill NOT NULL DEFAULT '0003')")
	require.NoError(t, err)

	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)

	stmts, err = declared.Diff(live, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)
}

// A changed default is still a change after padding.
func TestZerofillDefaultChangeIsDetected(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (a int(4) zerofill DEFAULT 6)")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE `t` (`a` int(4) unsigned zerofill DEFAULT '0005')")
	require.NoError(t, err)

	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE `t` MODIFY COLUMN `a` int(4) unsigned zerofill NULL DEFAULT '0006'", stmts[0].Statement)
}
