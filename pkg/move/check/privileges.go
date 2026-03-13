package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("privileges", privilegesCheck, ScopePreflight)
}

// privilegesCheck checks the privileges of the user running the move operation.
// Move operations require:
// - REPLICATION CLIENT and REPLICATION SLAVE (or SUPER) for binlog reading
// - RELOAD for FLUSH TABLES
// - Table-level privileges (SELECT, INSERT, etc.) on the source database
// - LOCK TABLES for cutover
// - CONNECTION_ADMIN + PROCESS + performance_schema access for force-kill (enabled by default)
//
// In RDS environments, some of these privileges may be granted via roles
// (e.g. rds_superuser_role). The check accounts for this by using SET ROLE ALL
// when testing role-based privileges.
func privilegesCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.SourceDB == nil {
		return nil // skip if no source DB connection yet (pre-run phase)
	}

	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool

	schemaName := ""
	if r.SourceConfig != nil {
		schemaName = r.SourceConfig.DBName
	}

	rows, err := r.SourceDB.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return err
		}
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
		if schemaName != "" {
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", schemaName)) {
				foundDBAll = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", strings.ReplaceAll(schemaName, "_", "\\_"))) {
				foundDBAll = true
			}
			if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, fmt.Sprintf(" ON `%s`.*", schemaName)) {
				foundDBAll = true
			}
		}
		if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
			foundDBAll = true
		}
		if strings.Contains(grant, `CONNECTION_ADMIN`) && strings.Contains(grant, ` ON *.*`) {
			foundConnectionAdmin = true
		}
		if strings.Contains(grant, `PROCESS`) && strings.Contains(grant, ` ON *.*`) {
			foundProcess = true
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if foundAll {
		return nil
	}

	// Move operations always use force-kill (it's enabled by default in DBConfig).
	// Check the force-kill related privileges.
	var errs []error
	// Verify SELECT access on performance_schema.*, which is required for the
	// queries used by force-kill during cutover.
	if !hasSelectOnPerformanceSchema(ctx, r.SourceDB, logger) {
		errs = append(errs, errors.New("missing SELECT privilege on performance_schema.*"))
	}
	// If CONNECTION_ADMIN or PROCESS are not found in direct grants,
	// check if they are available via roles (e.g. rds_superuser_role in RDS).
	if !foundConnectionAdmin && !foundSuper && !foundAll {
		if !checkPrivilegeWithRoles(ctx, r.SourceDB, logger, "CONNECTION_ADMIN") {
			errs = append(errs, errors.New("missing CONNECTION_ADMIN privilege"))
		}
	}
	if !foundProcess && !foundAll {
		if !checkPrivilegeWithRoles(ctx, r.SourceDB, logger, "PROCESS") {
			errs = append(errs, errors.New("missing PROCESS privilege"))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("insufficient privileges to run a move with force-kill enabled. Needed: CONNECTION_ADMIN/SUPER, PROCESS, and SELECT on performance_schema.*: %w", errors.Join(errs...))
	}

	if foundSuper && foundReplicationSlave && foundDBAll {
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload {
		return nil
	}

	return fmt.Errorf("insufficient privileges to run a move. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", schemaName)
}

// scanGrantsForPerformanceSchemaSelect checks SHOW GRANTS output for sufficient
// privileges to SELECT from performance_schema.* (either directly or via global grants).
func scanGrantsForPerformanceSchemaSelect(rows *sql.Rows) bool {
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return false
		}
		// Global ALL or SELECT on *.* implies SELECT on performance_schema.*
		if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
			return true
		}
		if strings.Contains(grant, `SELECT`) && strings.Contains(grant, ` ON *.*`) {
			return true
		}
		// Explicit privileges on performance_schema.* (backtick-quoted in SHOW GRANTS output).
		// We check for SELECT independently of the grant prefix because the grant line
		// may contain multiple privileges, e.g. "GRANT SELECT, EXECUTE ON `performance_schema`.*"
		if strings.Contains(grant, "ON `performance_schema`.*") {
			if strings.Contains(grant, `ALL PRIVILEGES`) || strings.Contains(grant, `SELECT`) {
				return true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false
	}
	return false
}

// hasSelectOnPerformanceSchema checks whether the current user effectively has
// SELECT on performance_schema.*, considering both direct grants and grants
// obtained via roles (activated using SET ROLE ALL in a transaction).
func hasSelectOnPerformanceSchema(ctx context.Context, db *sql.DB, logger *slog.Logger) bool {
	// First, check direct grants.
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err == nil {
		defer utils.CloseAndLog(rows)
		if scanGrantsForPerformanceSchemaSelect(rows) {
			return true
		}
	}
	// If direct grants are not sufficient, try again with roles activated.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer tx.Rollback() //nolint:errcheck
	cleanup, err := dbconn.SetRoleAllOnTxn(ctx, tx, logger)
	if err != nil {
		// If setting roles fails, the user has no roles or roles cannot be activated.
		return false
	}
	defer cleanup()
	rows, err = tx.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return false
	}
	defer utils.CloseAndLog(rows)
	return scanGrantsForPerformanceSchemaSelect(rows)
}

// checkPrivilegeWithRoles checks if a specific privilege is available via roles
// by executing SET ROLE ALL in a transaction and then checking SHOW GRANTS.
// This is needed in RDS environments where privileges like CONNECTION_ADMIN or PROCESS
// may be granted via a role (e.g. rds_superuser_role) that is not enabled by default.
func checkPrivilegeWithRoles(ctx context.Context, db *sql.DB, logger *slog.Logger, privilege string) bool {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer tx.Rollback() //nolint:errcheck

	// Activate all granted roles and ensure they are reset before the transaction ends.
	cleanup, err := dbconn.SetRoleAllOnTxn(ctx, tx, logger)
	if err != nil {
		// If setting roles fails, the user has no roles or roles cannot be activated
		return false
	}
	defer cleanup()

	// Now check SHOW GRANTS which will include privileges from active roles
	rows, err := tx.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return false
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return false
		}
		if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
			return true
		}
		if strings.Contains(grant, privilege) && strings.Contains(grant, ` ON *.*`) {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		logger.Error("error iterating SHOW GRANTS rows", "err", err)
		return false
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
