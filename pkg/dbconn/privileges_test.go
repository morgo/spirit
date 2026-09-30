package dbconn

import (
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/block/mysql"
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

// TestCheckPartialRevokesOff reads the real server's setting. It does not
// SET GLOBAL partial_revokes: other test binaries run in parallel against
// the same server, and the variable cannot be turned OFF again while a
// partial revoke exists. partialRevokesError covers the refusal.
func TestCheckPartialRevokesOff(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)

	var value string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.partial_revokes").Scan(&value))
	err = CheckPartialRevokesOff(t.Context(), db)
	if value == "1" || strings.EqualFold(value, "ON") {
		require.ErrorContains(t, err, "partial_revokes must be OFF")
	} else {
		require.NoError(t, err)
	}

	require.NoError(t, db.Close())
	require.ErrorContains(t, CheckPartialRevokesOff(t.Context(), db), "could not read partial_revokes")
}

func TestPartialRevokesError(t *testing.T) {
	unknown := &mysql.MySQLError{Number: 1193, Message: "Unknown system variable 'partial_revokes'"}
	other := &mysql.MySQLError{Number: 1227, Message: "Access denied"}
	for _, tc := range []struct {
		name    string
		value   string
		err     error
		wantErr string
	}{
		{name: "off", value: "0"},
		{name: "off by name", value: "OFF"},
		{name: "on", value: "1", wantErr: "partial_revokes must be OFF"},
		{name: "on by name", value: "ON", wantErr: "partial_revokes must be OFF"},
		{name: "server predates the variable", err: unknown},
		{name: "other read error", err: other, wantErr: "could not read partial_revokes"},
		{name: "non-MySQL error", err: errors.New("connection reset"), wantErr: "could not read partial_revokes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := partialRevokesError(tc.value, tc.err)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
