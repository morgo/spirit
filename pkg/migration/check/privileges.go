package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("privileges", privilegesCheck, ScopePreflight)
}

// grantedRolesRegexp matches role grants in SHOW GRANTS output.
// MySQL outputs role grants as: GRANT `role_name`@`%` TO `user`@`%`
// There may be multiple roles in a single line, comma-separated.
var grantedRolesRegexp = regexp.MustCompile("`([^`]+)`@`[^`]+`")

// Check the privileges of the user running the migration.
// Ensure there is LOCK TABLES etc so we don't find out and get errors
// at cutover time.
func privilegesCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	// This is a re-implementation of the gh-ost check
	// validateGrants() in gh-ost/go/logic/inspect.go
	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool
	var grantedRoles []string
	rows, err := r.DB.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return err
		}
		foundAll, foundSuper, foundReplicationClient, foundReplicationSlave,
			foundDBAll, foundReload, foundConnectionAdmin, foundProcess = scanGrantLine(
			grant, r.Table.SchemaName,
			foundAll, foundSuper, foundReplicationClient, foundReplicationSlave,
			foundDBAll, foundReload, foundConnectionAdmin, foundProcess,
		)
		// Collect role names from grant lines like:
		// GRANT `rds_superuser_role`@`%` TO `user`@`%`
		if strings.HasPrefix(grant, "GRANT `") && strings.Contains(grant, " TO ") {
			roles := parseRoleNames(grant)
			grantedRoles = append(grantedRoles, roles...)
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if foundAll {
		return nil
	}

	if r.ForceKill {
		var errs []error
		// Parsing performance_schema grants seems really hard, so we just try to execute the queries and see if they succeed.
		if _, err := dbconn.GetTableLocks(ctx, r.DB, []*table.TableInfo{r.Table}, logger, nil); err != nil {
			errs = append(errs, err)
		}
		if _, err := dbconn.GetLockingTransactions(ctx, r.DB, []*table.TableInfo{r.Table}, nil, logger, nil); err != nil {
			errs = append(errs, err)
		}
		// Check CONNECTION_ADMIN/SUPER and PROCESS. If not found in direct grants,
		// try expanding role privileges using SHOW GRANTS FOR CURRENT_USER() USING.
		if (!foundConnectionAdmin && !foundSuper) || !foundProcess {
			roleAll, roleSuper, _, _, _, _, roleConnectionAdmin, roleProcess := scanGrantsWithRoles(ctx, r.DB, r.Table.SchemaName, grantedRoles, logger)
			if !foundConnectionAdmin && !foundSuper {
				if !roleConnectionAdmin && !roleSuper && !roleAll {
					// On RDS, rds_superuser_role is opaque and SHOW GRANTS USING
					// cannot expand it. Since rds_superuser_role includes
					// CONNECTION_ADMIN, we tolerate its presence specifically,
					// but only when activate_all_roles_on_login=ON ensures the
					// role is actually active at runtime.
					if !hasRole(grantedRoles, "rds_superuser_role") || !activateAllRolesOnLogin(ctx, r.DB, logger) {
						errs = append(errs, errors.New("missing CONNECTION_ADMIN or SUPER privilege"))
					}
				}
			}
			if !foundProcess {
				if !roleProcess && !roleAll {
					if !hasRole(grantedRoles, "rds_superuser_role") || !activateAllRolesOnLogin(ctx, r.DB, logger) {
						errs = append(errs, errors.New("missing PROCESS privilege"))
					}
				}
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("insufficient privileges to run a migration with force-kill enabled (disable with --skip-force-kill). Needed: CONNECTION_ADMIN/SUPER, PROCESS, and SELECT on performance_schema.*: %w", errors.Join(errs...))
		}
	}

	if foundSuper && foundReplicationSlave && foundDBAll {
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload {
		return nil
	}

	return fmt.Errorf("insufficient privileges to run a migration. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", r.Table.SchemaName)
}

// scanGrantLine scans a single SHOW GRANTS line and updates the found flags.
func scanGrantLine(grant, schemaName string, foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool) (bool, bool, bool, bool, bool, bool, bool, bool) {
	if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
		foundAll = true
	}
	if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
		foundSuper = true
	}
	if strings.Contains(grant, `REPLICATION CLIENT`) && strings.Contains(grant, ` ON *.*`) {
		foundReplicationClient = true
	}
	if strings.Contains(grant, `REPLICATION SLAVE`) && strings.Contains(grant, ` ON *.*`) {
		foundReplicationSlave = true
	}
	if strings.Contains(grant, `RELOAD`) && strings.Contains(grant, ` ON *.*`) {
		foundReload = true
	}
	if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", schemaName)) {
		foundDBAll = true
	}
	if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", strings.ReplaceAll(schemaName, "_", "\\_"))) {
		foundDBAll = true
	}
	if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
		foundDBAll = true
	}
	if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, fmt.Sprintf(" ON `%s`.*", schemaName)) {
		foundDBAll = true
	}
	if strings.Contains(grant, `CONNECTION_ADMIN`) && strings.Contains(grant, ` ON *.*`) {
		foundConnectionAdmin = true
	}
	if strings.Contains(grant, `PROCESS`) && strings.Contains(grant, ` ON *.*`) {
		foundProcess = true
	}
	return foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess
}

// parseRoleNames extracts role names from a SHOW GRANTS line that grants roles.
// e.g. "GRANT `rds_superuser_role`@`%`,`other_role`@`%` TO `user`@`%`"
// returns ["rds_superuser_role", "other_role"]
func parseRoleNames(grant string) []string {
	// Split on " TO " to get only the roles part (before the target user)
	parts := strings.SplitN(grant, " TO ", 2)
	if len(parts) < 2 {
		return nil
	}
	rolesPart := parts[0] // "GRANT `role1`@`%`,`role2`@`%`"
	matches := grantedRolesRegexp.FindAllStringSubmatch(rolesPart, -1)
	var roles []string
	for _, match := range matches {
		if len(match) >= 2 {
			roles = append(roles, match[1])
		}
	}
	return roles
}

// scanGrantsWithRoles uses SHOW GRANTS FOR CURRENT_USER() USING 'role1','role2',...
// to expand role privileges into individual grant lines, then scans them.
// This is needed because plain SHOW GRANTS only shows the role name
// (e.g. GRANT `rds_superuser_role`@`%` TO `user`@`%`) without expanding
// the individual privileges the role contains.
// Note: on RDS, rds_superuser_role is opaque and USING will not expand it.
// Standard MySQL roles will be expanded correctly.
func scanGrantsWithRoles(ctx context.Context, db *sql.DB, schemaName string, roles []string, logger *slog.Logger) (foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool) {
	if len(roles) == 0 {
		return
	}

	// Build the USING clause: SHOW GRANTS FOR CURRENT_USER() USING `role1`,`role2`
	quotedRoles := make([]string, len(roles))
	for i, role := range roles {
		quotedRoles[i] = "`" + role + "`"
	}
	query := fmt.Sprintf("SHOW GRANTS FOR CURRENT_USER() USING %s", strings.Join(quotedRoles, ","))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		logger.Debug("SHOW GRANTS FOR CURRENT_USER() USING failed", "error", err)
		return
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return
		}
		foundAll, foundSuper, foundReplicationClient, foundReplicationSlave,
			foundDBAll, foundReload, foundConnectionAdmin, foundProcess = scanGrantLine(
			grant, schemaName,
			foundAll, foundSuper, foundReplicationClient, foundReplicationSlave,
			foundDBAll, foundReload, foundConnectionAdmin, foundProcess,
		)
	}
	if err := rows.Err(); err != nil {
		logger.Debug("error iterating SHOW GRANTS USING rows", "error", err)
	}
	return
}

// activateAllRolesOnLogin returns true if the server has activate_all_roles_on_login=ON.
// When this is enabled, all granted roles are automatically activated on login,
// so role-granted privileges are available without explicit SET ROLE ALL.
func activateAllRolesOnLogin(ctx context.Context, db *sql.DB, logger *slog.Logger) bool {
	var value string
	err := db.QueryRowContext(ctx, "SELECT @@global.activate_all_roles_on_login").Scan(&value)
	if err != nil {
		logger.Debug("failed to check activate_all_roles_on_login", "error", err)
		return false
	}
	return value == "1" || strings.EqualFold(value, "ON")
}

// hasRole returns true if the given role name is present in the list of granted roles.
func hasRole(grantedRoles []string, roleName string) bool {
	for _, r := range grantedRoles {
		if r == roleName {
			return true
		}
	}
	return false
}

// stringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func stringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}
