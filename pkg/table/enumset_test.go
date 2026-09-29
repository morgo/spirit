package table

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeEnumOrdinal(t *testing.T) {
	elements := []string{"active", "inactive", "pending"}

	got, err := decodeEnumOrdinal(1, elements)
	require.NoError(t, err)
	require.Equal(t, "active", got)

	got, err = decodeEnumOrdinal(2, elements)
	require.NoError(t, err)
	require.Equal(t, "inactive", got)

	got, err = decodeEnumOrdinal(3, elements)
	require.NoError(t, err)
	require.Equal(t, "pending", got)

	// 0 is MySQL's "invalid value" sentinel; preserved as empty string.
	got, err = decodeEnumOrdinal(0, elements)
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = decodeEnumOrdinal(4, elements)
	require.Error(t, err)
	_, err = decodeEnumOrdinal(-1, elements)
	require.Error(t, err)
}

func TestDecodeSetBitmask(t *testing.T) {
	elements := []string{"read", "write", "execute"}

	got, err := decodeSetBitmask(0, elements)
	require.NoError(t, err)
	require.Empty(t, got)

	got, err = decodeSetBitmask(1, elements) // read
	require.NoError(t, err)
	require.Equal(t, "read", got)

	got, err = decodeSetBitmask(3, elements) // read | write
	require.NoError(t, err)
	require.Equal(t, "read,write", got)

	got, err = decodeSetBitmask(5, elements) // read | execute
	require.NoError(t, err)
	require.Equal(t, "read,execute", got)

	got, err = decodeSetBitmask(7, elements) // all
	require.NoError(t, err)
	require.Equal(t, "read,write,execute", got)

	_, err = decodeSetBitmask(8, elements) // bit 3, no element
	require.Error(t, err)
	// -1 (all 64 bits) is rejected here because elements only defines 3.
	// The 64-element case is covered separately below.
	_, err = decodeSetBitmask(-1, elements)
	require.Error(t, err)
}

// TestDecodeSetBitmask64Elements covers the upper edge of MySQL SET:
// 64 members means valid bitmasks can use bit 63, which surfaces as a
// negative int64 from the go-mysql binlog reader. Regression test for
// the earlier check that rejected any negative input outright.
func TestDecodeSetBitmask64Elements(t *testing.T) {
	elements := make([]string, 64)
	for i := range elements {
		elements[i] = fmt.Sprintf("e%d", i)
	}

	// Bit 63 alone — int64 representation is math.MinInt64 (negative).
	// Build via math.MinInt64 to avoid constant-overflow rules around
	// 1 << 63 in either signed or unsigned form.
	bit63 := int64(math.MinInt64)
	got, err := decodeSetBitmask(bit63, elements)
	require.NoError(t, err)
	require.Equal(t, "e63", got)

	// All 64 bits set — int64 representation is -1.
	got, err = decodeSetBitmask(-1, elements)
	require.NoError(t, err)
	expectedParts := make([]string, 64)
	for i := range expectedParts {
		expectedParts[i] = fmt.Sprintf("e%d", i)
	}
	require.Equal(t, strings.Join(expectedParts, ","), got)

	// Mixed: bit 0 + bit 63.
	got, err = decodeSetBitmask(int64(1)|bit63, elements)
	require.NoError(t, err)
	require.Equal(t, "e0,e63", got)
}
