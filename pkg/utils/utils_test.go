package utils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHashKey(t *testing.T) {
	// This func helps put composite keys in a map.
	require.Equal(t, "1234-#-ACDC-#-12", HashKey([]any{"1234", "ACDC", "12"}))
	// This also works on single keys.
	require.Equal(t, "1234", HashKey([]any{"1234"}))
}

// TestHashKeySeparatorCollision pins the fix for hash-key collisions:
// without escaping, ("a-#-b", "c") and ("a", "b-#-c") hash to the same
// string, so one row's buffered change silently overwrites another's.
func TestHashKeySeparatorCollision(t *testing.T) {
	require.NotEqual(t,
		HashKey([]any{"a-#-b", "c"}),
		HashKey([]any{"a", "b-#-c"}))
	require.NotEqual(t,
		HashKey([]any{"a-#-b-#-c"}),
		HashKey([]any{"a", "b", "c"}))
	require.NotEqual(t,
		HashKey([]any{"a#", "#b"}),
		HashKey([]any{"a", "#", "b"}))
	// Components containing escape characters must not collide either.
	require.NotEqual(t,
		HashKey([]any{`a\`, "b"}),
		HashKey([]any{"a", `\b`}))
}

// TestHashKeyInjective verifies that HashKey produces a distinct map key
// for components containing the separator, escape characters, binary
// bytes, and edge-case values. Injectivity is the only property the
// buffered map relies on — two different key tuples must never hash to
// the same string (which would let one row's change silently overwrite
// another's). The hash is never reversed back into SQL, so round-tripping
// no longer matters; the applier builds DELETE literals from the original
// typed values instead (see block/spirit#948).
func TestHashKeyInjective(t *testing.T) {
	keys := [][]any{
		{"hello"},
		{"a", "b"},
		{"a-#-b"},
		{"a-#-b", "c"},
		{"a", "b-#-c"},
		{"a#b", "c#"},
		{`a\b`, `c\`},
		{`a\#b`},
		{"-#-"},
		{""},
		{"", ""},
		{"", "a"},
		{"a-", "-b"},
		{int64(42), "x"},
		// Binary / non-UTF8 key components must hash distinctly too.
		{"\xbb\x00\x00\x00\x00\x00\x00\x00"},
		{"\xbb\x00", "abc"},
	}
	seen := make(map[string][]any, len(keys))
	for _, k := range keys {
		h := HashKey(k)
		if prev, ok := seen[h]; ok {
			t.Errorf("hash collision: %#v and %#v both hash to %q", prev, k, h)
		}
		seen[h] = k
	}
}

func TestTruncateTableName(t *testing.T) {
	// Short name that doesn't need truncation
	require.Equal(t, "mytable", TruncateTableName("mytable", 21))

	// Name that exactly fits (64 - 21 = 43)
	name43 := strings.Repeat("a", 43)
	require.Equal(t, name43, TruncateTableName(name43, 21))

	// Name that exceeds the limit and needs truncation
	name56 := strings.Repeat("b", 56)
	require.Equal(t, strings.Repeat("b", 43), TruncateTableName(name56, 21))

	// Zero suffix length means full 64 chars available
	name64 := strings.Repeat("d", 64)
	require.Equal(t, name64, TruncateTableName(name64, 0))

	// Name longer than 64 with zero suffix gets truncated to 64
	name70 := strings.Repeat("e", 70)
	require.Equal(t, strings.Repeat("e", 64), TruncateTableName(name70, 0))
}
