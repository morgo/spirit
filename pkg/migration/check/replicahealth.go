package check

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

// ErrReplicaNotHealthy is returned when a replica's IO or SQL thread is not running.
var ErrReplicaNotHealthy = fmt.Errorf("replica is not healthy")

func init() {
	registerCheck("replicahealth", replicaHealth, ScopePostSetup|ScopeCutover)
}

// replicaHealth checks SHOW REPLICA STATUS for Yes and Yes on all replicas.
// It should be run at various stages of the migration if replicas are present.
func replicaHealth(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.Replicas) == 0 {
		return nil // The user is not using the replica DSN feature.
	}
	for _, replica := range r.Replicas {
		rows, err := replica.QueryContext(ctx, "SHOW REPLICA STATUS")
		if err != nil {
			return err
		}
		status, err := scanToMap(rows)
		utils.CloseAndLog(rows)
		if err != nil {
			return err
		}

		ioRunning := status["Replica_IO_Running"].String
		sqlRunning := status["Replica_SQL_Running"].String

		if ioRunning != "Yes" {
			ioState := status["Replica_IO_State"].String
			host := replicaHost(ctx, replica)
			return fmt.Errorf("%w: IO thread not running on %s (Replica_IO_State: %s)", ErrReplicaNotHealthy, host, ioState)
		}
		if sqlRunning != "Yes" {
			sqlState := status["Replica_SQL_Running_State"].String
			host := replicaHost(ctx, replica)
			return fmt.Errorf("%w: SQL thread not running on %s (Replica_SQL_Running_State: %s)", ErrReplicaNotHealthy, host, sqlState)
		}
	}
	return nil
}

// replicaHost attempts to identify the replica by querying @@hostname and @@port.
// Falls back to "unknown" if the query fails.
func replicaHost(ctx context.Context, db *sql.DB) string {
	var hostname string
	var port int
	if err := db.QueryRowContext(ctx, "SELECT @@hostname, @@port").Scan(&hostname, &port); err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", hostname, port)
}

func scanToMap(rows *sql.Rows) (map[string]sql.NullString, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			return nil, err
		} else {
			return nil, nil
		}
	}
	values := make([]any, len(columns))
	for index := range values {
		values[index] = new(sql.NullString)
	}
	err = rows.Scan(values...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]sql.NullString)
	for index, columnName := range columns {
		result[columnName] = *values[index].(*sql.NullString)
	}
	return result, nil
}
