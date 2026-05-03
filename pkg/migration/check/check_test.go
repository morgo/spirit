package check

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckAPI(t *testing.T) {
	testVal := "test"
	myfunc := func(
		_ context.Context,
		_ Resources,
		_ *slog.Logger,
	) error {
		testVal = "newval"
		return nil
	}
	checkLen := len(checks)
	registerCheck("mycheck", myfunc, ScopeTesting)
	require.Len(t, checks, checkLen+1)

	// Can't be duplicate registered because of a map
	registerCheck("mycheck", myfunc, ScopeTesting)
	require.Len(t, checks, checkLen+1)

	require.Equal(t, "test", testVal)
	err := RunChecks(t.Context(), Resources{}, slog.Default(), ScopeTesting)
	require.NoError(t, err)
	require.Equal(t, "newval", testVal)
}
