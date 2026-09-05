package dbconn

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestRequireDifferentDatabase(t *testing.T) {
	sourceName, source := testutils.CreateUniqueTestDatabase(t)
	_, different := testutils.CreateUniqueTestDatabase(t)
	require.NoError(t, RequireDifferentDatabase(t.Context(), source, different))
	require.ErrorContains(t, RequireDifferentDatabase(t.Context(), source, source), "same database")
	alias, err := New(testutils.DSNForDatabase(sourceName), NewDBConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, alias.Close()) }()
	require.ErrorContains(t, RequireDifferentDatabase(t.Context(), source, alias), "same database")
}
