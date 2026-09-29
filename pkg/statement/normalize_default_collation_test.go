package statement

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDefaultCollationOrderIndependent: this rule and
// binaryAttributeNormalizer both write a column's collation, and normalizers
// must not depend on registration order. Each case is parsed with the
// registry in its normal order and reversed, and must resolve the same way.
// In particular a BINARY column with a declared charset must get that
// charset's _bin collation either way: if this rule filled in the default
// (utf8mb3_general_ci, latin1_swedish_ci) first, the binary rule would read it
// as a written COLLATE and keep it.
func TestDefaultCollationOrderIndependent(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	for _, tc := range []struct {
		sql, collation string
	}{
		{"CREATE TABLE t (c NCHAR(3) BINARY) DEFAULT CHARSET=utf8mb4", "utf8_bin"},
		{"CREATE TABLE t (c NVARCHAR(3)) DEFAULT CHARSET=utf8mb4", "utf8_general_ci"},
		{"CREATE TABLE t (c NCHAR(3) BINARY COLLATE utf8mb3_unicode_ci) DEFAULT CHARSET=utf8mb4", "utf8_unicode_ci"},
		{"CREATE TABLE t (c varchar(3) CHARACTER SET utf8mb3 BINARY) DEFAULT CHARSET=latin1", "utf8_bin"},
		{"CREATE TABLE t (c varchar(3) BINARY) DEFAULT CHARSET=utf8mb3", "utf8_bin"},
		{"CREATE TABLE t (c varchar(3) CHARACTER SET latin1 BINARY) DEFAULT CHARSET=utf8mb4", "latin1_bin"},
		{"CREATE TABLE t (c varchar(3) CHARACTER SET latin1) DEFAULT CHARSET=utf8mb4", "latin1_swedish_ci"},
		{"CREATE TABLE t (c varchar(3) CHARACTER SET latin1 BINARY COLLATE latin1_general_ci) DEFAULT CHARSET=utf8mb4", "latin1_general_ci"},
		{"CREATE TABLE t (c varchar(3) BINARY) DEFAULT CHARSET=ascii", "ascii_bin"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				ct, err := ParseCreateTable(tc.sql)
				require.NoError(t, err)
				require.Len(t, ct.Columns, 1)
				require.NotNil(t, ct.Columns[0].Collation)
				require.Equal(t, tc.collation, *ct.Columns[0].Collation)
			}
		})
	}
}

func reversed(n []Normalizer) []Normalizer {
	r := slices.Clone(n)
	slices.Reverse(r)
	return r
}
