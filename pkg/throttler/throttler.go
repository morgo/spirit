// Package throttler contains code to throttle the rate of writes to a table.
package throttler

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

type Throttler interface {
	Open(ctx context.Context) error
	Close() error
	IsThrottled() bool
	BlockWait(ctx context.Context)
	UpdateLag(ctx context.Context) error
}

// NewReplicationThrottler returns a Throttler for MySQL 8.0+ replicas.
// It uses performance_schema to monitor replication lag.
func NewReplicationThrottler(replica *sql.DB, lagTolerance time.Duration, logger *slog.Logger) (Throttler, error) {
	return &Replica{
		replica:      replica,
		lagTolerance: lagTolerance,
		logger:       logger,
	}, nil
}
