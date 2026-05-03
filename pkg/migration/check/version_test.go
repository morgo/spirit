package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	r := Resources{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
	}
	err = versionCheck(t.Context(), r, slog.Default())
	if isMySQLSupported(t.Context(), db) {
		require.NoError(t, err) // all looks good of course.
	} else {
		require.Error(t, err)
	}
}
