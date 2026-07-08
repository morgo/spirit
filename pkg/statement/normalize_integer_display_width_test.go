package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripIntegerDisplayWidth(t *testing.T) {
	tests := []struct {
		sql     string
		wantLen *int // nil = width stripped
	}{
		{"CREATE TABLE t (a int(11))", nil},
		{"CREATE TABLE t (a int)", nil}, // parser injects (11); still stripped
		{"CREATE TABLE t (a bigint(20))", nil},
		{"CREATE TABLE t (a smallint(6))", nil},
		{"CREATE TABLE t (a mediumint(9))", nil},
		{"CREATE TABLE t (a tinyint(4))", nil},
		{"CREATE TABLE t (a tinyint(1))", new(1)},                 // BOOLEAN form, preserved
		{"CREATE TABLE t (a boolean)", new(1)},                    // folds to tinyint(1)
		{"CREATE TABLE t (a int(10) unsigned zerofill)", new(10)}, // width kept under zerofill
		{"CREATE TABLE t (a varchar(11))", new(11)},               // not an integer type
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantLen, ct.Columns[0].Length)
		})
	}
}

// TestStripIntegerDisplayWidthConverges is the payoff: an int(11) authored in a
// schema file no longer diffs against a live `int`.
func TestStripIntegerDisplayWidthConverges(t *testing.T) {
	authored, err := ParseCreateTable("CREATE TABLE t (id int(11) NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)

	stmts, err := authored.Diff(live, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts, "int(11) should normalize to int and produce no diff")
}
