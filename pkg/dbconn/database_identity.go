package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// RequireDifferentDatabase refuses a copy whose target aliases its source.
// Connection strings are insufficient: different users or hostnames can reach
// the same database. Check the selected schemas and, when they overlap, the
// server UUIDs before any target writes or destructive recovery.
func RequireDifferentDatabase(ctx context.Context, source, target *sql.DB) error {
	sourceSchema, err := selectedDatabase(ctx, source, "source")
	if err != nil {
		return err
	}
	targetSchema, err := selectedDatabase(ctx, target, "target")
	if err != nil {
		return err
	}
	// Be conservative about case: servers may fold database names. Distinct
	// schema names remain valid even on the same server, including test setups.
	if !strings.EqualFold(sourceSchema, targetSchema) {
		return nil
	}
	var sourceUUID, targetUUID string
	if err := source.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&sourceUUID); err != nil {
		return fmt.Errorf("verify source and target databases are different: read source server UUID: %w", err)
	}
	if err := target.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&targetUUID); err != nil {
		return fmt.Errorf("verify source and target databases are different: read target server UUID: %w", err)
	}
	if sourceUUID == "" || targetUUID == "" {
		return fmt.Errorf("cannot verify source and target databases are different: empty server UUID")
	}
	if strings.EqualFold(sourceUUID, targetUUID) {
		return fmt.Errorf("source and target refer to the same database %q on server %s; refusing to modify the source", sourceSchema, sourceUUID)
	}
	return nil
}

func selectedDatabase(ctx context.Context, db *sql.DB, role string) (string, error) {
	var schema sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return "", fmt.Errorf("read %s database identity: %w", role, err)
	}
	if !schema.Valid || schema.String == "" {
		return "", fmt.Errorf("%s connection has no selected database; specify one in its DSN", role)
	}
	return schema.String, nil
}
