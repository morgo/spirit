package copier

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDatumByteSize covers the per-value size estimate for each concrete type
// database/sql yields when scanning into `any`, plus the nil and fallback cases.
func TestDatumByteSize(t *testing.T) {
	require.Equal(t, uint64(1), datumByteSize(nil), "nil is counted as 1 byte")
	require.Equal(t, uint64(5), datumByteSize([]byte("hello")), "[]byte sized by length")
	require.Equal(t, uint64(0), datumByteSize([]byte{}), "empty []byte is zero")
	require.Equal(t, uint64(3), datumByteSize("abc"), "string sized by length")
	require.Equal(t, uint64(16), datumByteSize(time.Now()), "time.Time is a fixed nominal width")
	require.Equal(t, uint64(8), datumByteSize(int64(42)), "scalars fall back to 8 bytes")
	require.Equal(t, uint64(8), datumByteSize(float64(1.5)), "scalars fall back to 8 bytes")
	require.Equal(t, uint64(8), datumByteSize(true), "scalars fall back to 8 bytes")
}

// TestRowsByteSize covers the whole-chunk sum, including the empty-chunk case
// (which the buffered copier reports as zero bytes for a gap chunk).
func TestRowsByteSize(t *testing.T) {
	require.Equal(t, uint64(0), rowsByteSize(nil), "no rows is zero bytes")
	require.Equal(t, uint64(0), rowsByteSize([][]any{}), "empty rows slice is zero bytes")

	rows := [][]any{
		{int64(1), []byte("alice"), nil},   // 8 + 5 + 1 = 14
		{int64(2), []byte("bob"), "extra"}, // 8 + 3 + 5 = 16
	}
	require.Equal(t, uint64(30), rowsByteSize(rows), "sum across all values of all rows")
}
