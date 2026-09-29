package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsPrefix(t *testing.T) {
	// Identical
	require.True(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "b", "c"}))

	// Append at end (safe)
	require.True(t, isPrefix([]string{"a", "b"}, []string{"a", "b", "c"}))

	// Empty old (always a prefix)
	require.True(t, isPrefix([]string{}, []string{"a", "b"}))

	// Reorder
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"c", "a", "b"}))

	// Insert in middle
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "x", "b", "c"}))

	// Remove value
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "b"}))

	// Remove from middle
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "c"}))

	// Completely different
	require.False(t, isPrefix([]string{"a", "b"}, []string{"x", "y", "z"}))
}
