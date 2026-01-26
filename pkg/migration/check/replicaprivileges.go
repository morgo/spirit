package check

import (
	"context"

	"github.com/block/spirit/pkg/utils"
	"log/slog"

	"github.com/block/spirit/pkg/throttler"
)

func init() {
	registerCheck("replica", replicaPrivilegeCheck, ScopePreflight)
}

// Check that there is permission to run perfschema queries for replication (8.0)
func replicaPrivilegeCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Replica == nil {
		return nil // The user is not using the replica DSN feature.
	}
	var version string
	if err := r.Replica.QueryRowContext(ctx, "select substr(version(), 1, 1)").Scan(&version); err != nil {
		return err //  can not get version
	}
	rows, err := r.Replica.QueryContext(ctx, throttler.MySQL8LagQuery)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	_, err = scanToMap(rows)
	return err
}
