package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseEnumSetElements(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
		wantErr  bool
	}{
		// Basic cases.
		{"enum('a','b','c')", []string{"a", "b", "c"}, false},
		{"set('read','write','execute')", []string{"read", "write", "execute"}, false},
		{"enum('active','inactive','pending')", []string{"active", "inactive", "pending"}, false},

		// Non-enum/set types return an error (fail-closed).
		{"int", nil, true},
		{"varchar(255)", nil, true},

		// Empty element list: valid syntax, no elements, no error.
		{"enum()", nil, false},

		// Values containing commas.
		{"enum('a,b','c')", []string{"a,b", "c"}, false},
		{"enum('one','two,three','four')", []string{"one", "two,three", "four"}, false},

		// Values containing escaped (doubled) single quotes.
		{"enum('it''s','ok')", []string{"it's", "ok"}, false},
		{"enum('he said ''hi''','bye')", []string{"he said 'hi'", "bye"}, false},

		// Commas and escaped quotes combined.
		{"enum('a,b''c','d')", []string{"a,b'c", "d"}, false},

		// Single element.
		{"enum('only')", []string{"only"}, false},

		// Spaces between elements (MySQL SHOW CREATE TABLE may include them).
		{"enum('a', 'b', 'c')", []string{"a", "b", "c"}, false},

		// Case-insensitive prefix.
		{"ENUM('X','Y')", []string{"X", "Y"}, false},
		{"SET('r','w')", []string{"r", "w"}, false},

		// Empty string as a valid ENUM value.
		{"enum('','a','b')", []string{"", "a", "b"}, false},

		// Malformed inputs: fail-closed (return error, not partial results).
		{"enum(a,'b','c')", nil, true},      // unquoted value
		{"enum('a','b',3)", nil, true},      // numeric literal without quotes
		{"enum('a'  x  'b')", nil, true},    // junk between elements
		{"enum('a','b", nil, true},          // unterminated quote (missing closing paren clips it)
		{"enum('unterminated", nil, true},   // unterminated quote, no closing paren
		{"enum('a',,'b')", nil, true},       // empty element between delimiters
		{"enum('a', 'b', 'c',)", nil, true}, // trailing comma is malformed
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseEnumSetElements(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIsEnumOrSetType(t *testing.T) {
	// ENUM types
	require.True(t, IsEnumOrSetType("enum('a','b','c')"))
	require.True(t, IsEnumOrSetType("ENUM('A','B')"))
	require.True(t, IsEnumOrSetType("Enum('x')"))

	// SET types
	require.True(t, IsEnumOrSetType("set('read','write')"))
	require.True(t, IsEnumOrSetType("SET('r','w')"))
	require.True(t, IsEnumOrSetType("Set('a')"))

	// Non-ENUM/SET types
	require.False(t, IsEnumOrSetType("varchar(191)"))
	require.False(t, IsEnumOrSetType("int"))
	require.False(t, IsEnumOrSetType("bigint"))
	require.False(t, IsEnumOrSetType("text"))
	require.False(t, IsEnumOrSetType("decimal(10,2)"))
	require.False(t, IsEnumOrSetType(""))
}

func TestIsEnumType(t *testing.T) {
	require.True(t, IsEnumType("enum('a','b','c')"))
	require.True(t, IsEnumType("ENUM('A','B')"))
	require.False(t, IsEnumType("set('read','write')"))
	require.False(t, IsEnumType("varchar(191)"))
	require.False(t, IsEnumType(""))
}

func TestIsSetType(t *testing.T) {
	require.True(t, IsSetType("set('read','write')"))
	require.True(t, IsSetType("SET('r','w')"))
	require.False(t, IsSetType("enum('a','b')"))
	require.False(t, IsSetType("varchar(191)"))
	require.False(t, IsSetType(""))
}

func TestParseSQLQuotedListUnterminated(t *testing.T) {
	// A quoted string that is never closed should return an error.
	result, err := parseSQLQuotedList("'abc")
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "unterminated")
}
