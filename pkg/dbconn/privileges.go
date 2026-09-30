package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
)

// ActivateAllRolesOnLogin returns true if the server has activate_all_roles_on_login=ON.
// When this is enabled, all granted roles are automatically activated on login,
// so role-granted privileges are available without explicit SET ROLE ALL.
// A failed read is logged at debug level and reported as false.
func ActivateAllRolesOnLogin(ctx context.Context, db *sql.DB, logger *slog.Logger) bool {
	var value string
	err := db.QueryRowContext(ctx, "SELECT @@global.activate_all_roles_on_login").Scan(&value)
	if err != nil {
		logger.Debug("failed to check activate_all_roles_on_login", "error", err)
		return false
	}
	return value == "1" || strings.EqualFold(value, "ON")
}

// CheckPartialRevokesOff returns an error if the server has
// partial_revokes=ON. Spirit does not support it: with it ON, SHOW GRANTS
// can print a REVOKE line that removes a global grant for one schema, and
// MySQL takes the database name in a grant literally rather than as a
// pattern. The privilege checks read SHOW GRANTS as additive GRANT lines,
// so they would pass for a user that cannot act on the schema. The variable
// is OFF by default and only exists on MySQL 8.0.16+; an older server
// cannot have partial revokes, so unknown-variable passes.
func CheckPartialRevokesOff(ctx context.Context, db *sql.DB) error {
	var value string
	err := db.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&value)
	return partialRevokesError(value, err)
}

// partialRevokesError classifies the result of reading partial_revokes.
func partialRevokesError(value string, err error) error {
	if err != nil {
		if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok && myErr.Number == parsermysql.ErrUnknownSystemVariable {
			return nil
		}
		return fmt.Errorf("could not read partial_revokes: %w", err)
	}
	if value == "1" || strings.EqualFold(value, "ON") {
		return errors.New("partial_revokes must be OFF: spirit does not support partial revokes (this is the MySQL default)")
	}
	return nil
}
