package utils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuxTableName(t *testing.T) {
	// Short names: no truncation, exact format.
	require.Equal(t, "_t_chkpnt", AuxTableName("t", "_chkpnt"))
	require.Equal(t, "_t_new", AuxTableName("t", "_new"))
	require.Equal(t, "_t_old", AuxTableName("t", "_old"))
	require.Equal(t, "_t_old_20260101_000000", AuxTableName("t", "_old_20260101_000000"))

	// At the boundary that would just exceed 64 chars: truncated.
	// _<table>_chkpnt: leading "_" + 7-char suffix = 8 extra chars, so 56 fits.
	name57 := strings.Repeat("a", 57)
	got := AuxTableName(name57, "_chkpnt")
	require.LessOrEqual(t, len(got), MaxTableNameLength)
	require.True(t, strings.HasPrefix(got, "_"))
	require.True(t, strings.HasSuffix(got, "_chkpnt"))

	// 64-char input with each suffix still fits within 64.
	name64 := strings.Repeat("b", MaxTableNameLength)
	for _, suffix := range []string{"_chkpnt", "_new", "_old", "_old_20260101_000000"} {
		out := AuxTableName(name64, suffix)
		require.LessOrEqual(t, len(out), MaxTableNameLength,
			"AuxTableName(%d-char name, %q) = %q (len=%d) exceeds MySQL limit",
			len(name64), suffix, out, len(out))
		require.True(t, strings.HasPrefix(out, "_"))
		require.True(t, strings.HasSuffix(out, suffix))
	}

	// Determinism: same input → same output.
	require.Equal(t, AuxTableName(name64, "_chkpnt"), AuxTableName(name64, "_chkpnt"))
}

func TestAuxTableNameTypedHelpers(t *testing.T) {
	require.Equal(t, "_t_chkpnt", CheckpointTableName("t"))
	require.Equal(t, "_t_new", NewTableName("t"))
	require.Equal(t, "_t_old", OldTableName("t"))
	require.Equal(t, "_t_old_20260101_000000", OldTableNameWithTimestamp("t", "20260101_000000"))
}
