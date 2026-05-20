package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsEnumColumnType(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"enum('a','b','c')", true},
		{"ENUM('a')", true},
		{"set('a','b')", false},
		{"varchar(255)", false},
		{"", false},
	}
	for _, c := range cases {
		require.Equalf(t, c.want, isEnumColumnType(c.in), "input=%q", c.in)
	}
}

func TestIsSetColumnType(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"set('a','b','c')", true},
		{"SET('a')", true},
		{"enum('a','b')", false},
		{"varchar(255)", false},
		{"", false},
	}
	for _, c := range cases {
		require.Equalf(t, c.want, isSetColumnType(c.in), "input=%q", c.in)
	}
}

func TestParseEnumSetElements(t *testing.T) {
	cases := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{"enum('active','inactive','pending')", []string{"active", "inactive", "pending"}, false},
		{"set('read','write','execute')", []string{"read", "write", "execute"}, false},
		{"enum('a','b,c','d')", []string{"a", "b,c", "d"}, false},
		{"enum('it''s','fine')", []string{"it's", "fine"}, false},
		{"enum()", nil, false},
		{"varchar(255)", nil, true},
		{"enum('unterminated", nil, true},
		{"enum('a',,'b')", nil, true},
	}
	for _, c := range cases {
		got, err := parseEnumSetElements(c.in)
		if c.wantErr {
			require.Errorf(t, err, "input=%q", c.in)
			continue
		}
		require.NoErrorf(t, err, "input=%q", c.in)
		require.Equalf(t, c.want, got, "input=%q", c.in)
	}
}

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
	require.Equal(t, "", got)

	_, err = decodeEnumOrdinal(4, elements)
	require.Error(t, err)
	_, err = decodeEnumOrdinal(-1, elements)
	require.Error(t, err)
}

func TestDecodeSetBitmask(t *testing.T) {
	elements := []string{"read", "write", "execute"}

	got, err := decodeSetBitmask(0, elements)
	require.NoError(t, err)
	require.Equal(t, "", got)

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
	_, err = decodeSetBitmask(-1, elements)
	require.Error(t, err)
}
