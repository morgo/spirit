package check

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsuser")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsuser")
	assert.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testprivsuser"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)

	// ForceKill is now enabled by default, so we test with it enabled.
	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true, // default behavior
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // privileges fail, since user has nothing granted.

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs replication client

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsuser")
	assert.NoError(t, err)

	// With ForceKill enabled (the default), basic replication privileges are not enough.
	// We also need the force-kill privileges.
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs force-kill privileges

	_, err = db.ExecContext(t.Context(), "GRANT SELECT on `performance_schema`.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs connection_admin

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs PROCESS
	t.Log(err)

	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testprivsuser")
	assert.NoError(t, err)

	// Reconnect before checking again.
	// There seems to be a race in MySQL where privileges don't show up immediately
	// That this can work around.
	assert.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)
	r.DB = lowPrivDB

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // all force-kill privileges granted, should pass now

	// Test the root user
	r = Resources{
		DB:        db,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true,
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // privileges work fine
}

// TestPrivilegesWithSkipForceKill tests that when ForceKill is disabled
// (i.e. --skip-force-kill is set), the additional force-kill privileges
// are not required.
func TestPrivilegesWithSkipForceKill(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsskipfk")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsskipfk")
	assert.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testprivsskipfk"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	// With ForceKill disabled (--skip-force-kill), the force-kill privileges
	// should not be required.
	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: false, // --skip-force-kill
	}

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsskipfk")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsskipfk")
	assert.NoError(t, err)

	// Without force-kill, basic privileges should be sufficient.
	// No CONNECTION_ADMIN, PROCESS, or performance_schema access needed.
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)
}
