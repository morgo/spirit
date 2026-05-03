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
	"github.com/stretchr/testify/require"
)

func TestPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsuser")
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsuser")
	require.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testprivsuser"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)

	// ForceKill is now enabled by default, so we test with it enabled.
	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true, // default behavior
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // privileges fail, since user has nothing granted.

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs replication client

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsuser")
	require.NoError(t, err)

	// With ForceKill enabled (the default), basic replication privileges are not enough.
	// We also need the force-kill privileges.
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs force-kill privileges

	_, err = db.ExecContext(t.Context(), "GRANT SELECT on `performance_schema`.* TO testprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs connection_admin

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO testprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs PROCESS
	t.Log(err)

	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testprivsuser")
	require.NoError(t, err)

	// Reconnect before checking again.
	// There seems to be a race in MySQL where privileges don't show up immediately
	// That this can work around.
	require.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)
	r.DB = lowPrivDB

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all force-kill privileges granted, should pass now

	// Test the root user
	r = Resources{
		DB:        db,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true,
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // privileges work fine
}

// TestPrivilegesWithRDSSuperuserRole tests that rds_superuser_role is tolerated
// only when activate_all_roles_on_login=ON.
func TestPrivilegesWithRDSSuperuserRole(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Clean up any previous test artifacts
	_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testrdsroleuser")
	_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")

	// Create an opaque role that simulates rds_superuser_role on RDS.
	// We intentionally do NOT grant CONNECTION_ADMIN to it, because on real
	// RDS the role is opaque and cannot be inspected. The privilege check
	// tolerates it by name only.
	_, err = db.ExecContext(t.Context(), "CREATE ROLE rds_superuser_role")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")
	})

	_, err = db.ExecContext(t.Context(), "CREATE USER testrdsroleuser")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testrdsroleuser")
	})

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testrdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testrdsroleuser")
	require.NoError(t, err)
	// Grant performance_schema and PROCESS directly so the probe queries succeed.
	// On real RDS, rds_superuser_role grants these, but our test role is opaque
	// (no actual privileges) so we simulate by granting them directly.
	// The test specifically validates that CONNECTION_ADMIN tolerance works via
	// the rds_superuser_role name check + activate_all_roles_on_login guard.
	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO testrdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testrdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT rds_superuser_role TO testrdsroleuser")
	require.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testrdsroleuser"
	config.Passwd = ""

	// Save original value and restore after test
	var origValue string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.activate_all_roles_on_login").Scan(&origValue))
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), fmt.Sprintf("SET GLOBAL activate_all_roles_on_login = %s", origValue))
	})

	// With activate_all_roles_on_login=OFF, rds_superuser_role should NOT be tolerated
	_, err = db.ExecContext(t.Context(), "SET GLOBAL activate_all_roles_on_login = OFF")
	require.NoError(t, err)

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true,
	}

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err, "should fail when activate_all_roles_on_login=OFF")
	require.Contains(t, err.Error(), "CONNECTION_ADMIN")

	// With activate_all_roles_on_login=ON, rds_superuser_role should be tolerated
	_, err = db.ExecContext(t.Context(), "SET GLOBAL activate_all_roles_on_login = ON")
	require.NoError(t, err)

	// Must reconnect after changing the global variable so the new connection
	// gets roles activated on login.
	require.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	r.DB = lowPrivDB

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err, "should pass when activate_all_roles_on_login=ON and rds_superuser_role is granted")
}

// TestPrivilegesWithSkipForceKill tests that when ForceKill is disabled
// (i.e. --skip-force-kill is set), the additional force-kill privileges
// are not required.
func TestPrivilegesWithSkipForceKill(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsskipfk")
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsskipfk")
	require.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testprivsskipfk"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	// With ForceKill disabled (--skip-force-kill), the force-kill privileges
	// should not be required.
	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: false, // --skip-force-kill
	}

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsskipfk")
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsskipfk")
	require.NoError(t, err)

	// Without force-kill, basic privileges should be sufficient.
	// No CONNECTION_ADMIN, PROCESS, or performance_schema access needed.
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}
