package check

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEnumSetValues(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		// Basic cases.
		{"enum('a','b','c')", []string{"a", "b", "c"}},
		{"set('read','write','execute')", []string{"read", "write", "execute"}},
		{"enum('active','inactive','pending')", []string{"active", "inactive", "pending"}},

		// Non-enum/set types.
		{"int", nil},
		{"varchar(255)", nil},

		// Empty element list.
		{"enum()", nil},

		// Values containing commas.
		{"enum('a,b','c')", []string{"a,b", "c"}},
		{"enum('one','two,three','four')", []string{"one", "two,three", "four"}},

		// Values containing escaped (doubled) single quotes.
		{"enum('it''s','ok')", []string{"it's", "ok"}},
		{"enum('he said ''hi''','bye')", []string{"he said 'hi'", "bye"}},

		// Commas and escaped quotes combined.
		{"enum('a,b''c','d')", []string{"a,b'c", "d"}},

		// Single element.
		{"enum('only')", []string{"only"}},

		// Spaces between elements (MySQL SHOW CREATE TABLE may include them).
		{"enum('a', 'b', 'c')", []string{"a", "b", "c"}},

		// Case-insensitive prefix.
		{"ENUM('X','Y')", []string{"X", "Y"}},
		{"SET('r','w')", []string{"r", "w"}},

		// Empty string as a valid ENUM value.
		{"enum('','a','b')", []string{"", "a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseEnumSetValues(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPrefix(t *testing.T) {
	// Identical
	assert.True(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "b", "c"}))

	// Append at end (safe)
	assert.True(t, isPrefix([]string{"a", "b"}, []string{"a", "b", "c"}))

	// Empty old (always a prefix)
	assert.True(t, isPrefix([]string{}, []string{"a", "b"}))

	// Reorder
	assert.False(t, isPrefix([]string{"a", "b", "c"}, []string{"c", "a", "b"}))

	// Insert in middle
	assert.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "x", "b", "c"}))

	// Remove value
	assert.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "b"}))

	// Remove from middle
	assert.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "c"}))

	// Completely different
	assert.False(t, isPrefix([]string{"a", "b"}, []string{"x", "y", "z"}))
}
