package check

import (
	"context"
	"log/slog"

	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("replica", replicaPrivilegeCheck, ScopePreflight)
}

// Check that there is permission to run perfschema queries for replication (8.0)
// on all configured replicas.
func replicaPrivilegeCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.Replicas) == 0 {
		return nil // The user is not using the replica DSN feature.
	}
	for _, replica := range r.Replicas {
		var version string
		if err := replica.QueryRowContext(ctx, "select substr(version(), 1, 1)").Scan(&version); err != nil {
			return err //  can not get version
		}
		rows, err := replica.QueryContext(ctx, throttler.MySQL8LagQuery)
		if err != nil {
			return err
		}
		_, err = scanToMap(rows)
		utils.CloseAndLog(rows)
		if err != nil {
			return err
		}
	}
	return nil
}
