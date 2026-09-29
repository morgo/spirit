package dbconn

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestActivateAllRolesOnLogin(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)

	var value string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.activate_all_roles_on_login").Scan(&value))
	want := value == "1" || strings.EqualFold(value, "ON")
	require.Equal(t, want, ActivateAllRolesOnLogin(t.Context(), db, slog.Default()))

	// A failed read (here: a closed pool) reports false rather than an error.
	require.NoError(t, db.Close())
	require.False(t, ActivateAllRolesOnLogin(t.Context(), db, slog.Default()))
}
