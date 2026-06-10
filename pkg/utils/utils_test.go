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

func TestHashAndUnhashKey(t *testing.T) {
	// This func helps put composite keys in a map.
	key := []any{"1234", "ACDC", "12"}
	hashed := HashKey(key)
	require.Equal(t, "1234-#-ACDC-#-12", hashed)
	unhashed := UnhashKeyToString(hashed)
	// unhashed returns as a string, not the original any
	require.Equal(t, "('1234','ACDC','12')", unhashed)

	// This also works on single keys.
	key = []any{"1234"}
	hashed = HashKey(key)
	require.Equal(t, "1234", hashed)
	unhashed = UnhashKeyToString(hashed)
	require.Equal(t, "'1234'", unhashed)
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

// TestHashKeyRoundTrip verifies that UnhashKeyToString recovers the exact
// original values (correctly SQL-quoted) for components containing the
// separator, escape characters, and edge-case values.
func TestHashKeyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		key  []any
		want string
	}{
		{"plain single", []any{"hello"}, "'hello'"},
		{"plain composite", []any{"a", "b"}, "('a','b')"},
		{"separator in single key", []any{"a-#-b"}, "'a-#-b'"},
		{"separator in composite components", []any{"a-#-b", "c"}, "('a-#-b','c')"},
		{"separator in second component", []any{"a", "b-#-c"}, "('a','b-#-c')"},
		{"bare hash", []any{"a#b", "c#"}, "('a#b','c#')"},
		{"backslash", []any{`a\b`, `c\`}, `('a\\b','c\\')`}, // sqlescape doubles backslashes for SQL
		{"backslash before hash", []any{`a\#b`}, `'a\\#b'`},
		{"only separator", []any{"-#-"}, "'-#-'"},
		{"empty single", []any{""}, "''"},
		{"empty components", []any{"", ""}, "('','')"},
		{"empty first component", []any{"", "a"}, "('','a')"},
		{"trailing dash boundary", []any{"a-", "-b"}, "('a-','-b')"},
		{"non-string types", []any{int64(42), "x"}, "('42','x')"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, UnhashKeyToString(HashKey(tc.key)))
		})
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
