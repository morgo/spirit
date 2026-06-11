package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsBinaryColumnType(t *testing.T) {
	require.True(t, isBinaryColumnType("binary(20)"))
	require.True(t, isBinaryColumnType("BINARY(1)"))
	require.True(t, isBinaryColumnType("binary"))
	require.False(t, isBinaryColumnType("varbinary(20)"))
	require.False(t, isBinaryColumnType("blob"))
	require.False(t, isBinaryColumnType("tinyblob"))
	require.False(t, isBinaryColumnType("char(20)"))
	require.False(t, isBinaryColumnType("bit(8)"))
}

func TestParseBinaryColumnWidth(t *testing.T) {
	require.Equal(t, 20, parseBinaryColumnWidth("binary(20)"))
	require.Equal(t, 1, parseBinaryColumnWidth("binary(1)"))
	require.Equal(t, 255, parseBinaryColumnWidth("BINARY(255)"))
	require.Equal(t, 1, parseBinaryColumnWidth("binary")) // bare BINARY is BINARY(1)
	// Fail-open on anything malformed: 0 means "no padding".
	require.Equal(t, 0, parseBinaryColumnWidth("binary("))
	require.Equal(t, 0, parseBinaryColumnWidth("binary(x)"))
	require.Equal(t, 0, parseBinaryColumnWidth("varbinary(20)"))
}

func TestPadBinaryValue(t *testing.T) {
	// Stripped string values are re-padded to the declared width.
	require.Equal(t, "\xaa\xbb\x00\x00", padBinaryValue("\xaa\xbb", 4))
	// All-zero values are stripped to the empty string by MySQL.
	require.Equal(t, "\x00\x00\x00\x00", padBinaryValue("", 4))
	// Full-width values are unchanged.
	require.Equal(t, "\xaa\xbb\xcc\xdd", padBinaryValue("\xaa\xbb\xcc\xdd", 4))
	// []byte values pad without mutating the input.
	in := []byte{0xaa}
	out := padBinaryValue(in, 3)
	require.Equal(t, []byte{0xaa, 0x00, 0x00}, out)
	require.Equal(t, []byte{0xaa}, in)
	// NULL (nil) and unexpected types pass through.
	require.Nil(t, padBinaryValue(nil, 4))
	require.Equal(t, int64(7), padBinaryValue(int64(7), 4))
	// Over-width values (shouldn't happen) are left alone.
	require.Equal(t, "\x01\x02\x03\x04\x05", padBinaryValue("\x01\x02\x03\x04\x05", 4))
}
