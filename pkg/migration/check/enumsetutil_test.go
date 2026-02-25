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
		{"enum('a','b','c')", []string{"a", "b", "c"}},
		{"set('read','write','execute')", []string{"read", "write", "execute"}},
		{"enum('active','inactive','pending')", []string{"active", "inactive", "pending"}},
		{"int", nil},
		{"varchar(255)", nil},
		{"enum()", nil},
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
