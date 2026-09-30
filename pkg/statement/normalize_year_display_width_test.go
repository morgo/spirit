package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestYearDisplayWidth(t *testing.T) {
	tests := []struct {
		sql     string
		wantLen *int // nil = no width
	}{
		{"CREATE TABLE t (a year(4))", nil},
		{"CREATE TABLE t (a YEAR(4))", nil},
		{"CREATE TABLE t (a year)", nil},
		{"CREATE TABLE t (a year(4) NOT NULL DEFAULT 2024)", nil},
		{"CREATE TABLE t (a varchar(4))", new(4)}, // not a year column
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

// TestYearDisplayWidthConverges checks that a year(4) written in a schema file
// diffs clean against the `year` MySQL reports, in both directions.
func TestYearDisplayWidthConverges(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (a year(4), b year(4) NOT NULL DEFAULT 2024)")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE `t` (`a` year DEFAULT NULL, `b` year NOT NULL DEFAULT '2024')")
	require.NoError(t, err)

	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)

	stmts, err = declared.Diff(live, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)
}
