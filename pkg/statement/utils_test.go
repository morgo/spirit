package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQuoteIdent verifies that quoteIdent doubles embedded backticks so
// an identifier containing one renders as valid MySQL SQL. Without this,
// a column or index name with a backtick produced broken SQL that only
// surfaced at re-parse time as a confusing parse error.
func TestQuoteIdent(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"foo", "`foo`"},
		{"", "``"},
		{"foo`bar", "`foo``bar`"},
		{"`", "````"}, // open + doubled-backtick + close = 4
		{"a`b`c", "`a``b``c`"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, quoteIdent(tc.in), "input=%q", tc.in)
	}
}

// TestFormatPartitionValue verifies that string partition values are
// quoted and escaped, while integer/float literals (which the parser
// flattens into Go strings during Restore) are rendered unquoted.
func TestFormatPartitionValue(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"int_literal_string", "2020", "2020"},
		{"negative_int", "-1", "-1"},
		{"float_literal_string", "3.14", "3.14"},
		{"string_literal", "asia", "'asia'"},
		{"string_with_single_quote", "o'brien", "'o\\'brien'"},
		{"raw_int", 42, "42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, formatPartitionValue(tc.in))
		})
	}
}
