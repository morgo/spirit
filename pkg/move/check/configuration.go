package check

import (
	"context"
	"errors"
	"log/slog"
)

func init() {
	registerCheck("configuration", configurationCheck, ScopePreflight)
}

// configurationCheck verifies the MySQL configuration on the source database
// is suitable for move operations. Move operations require:
// - ROW binlog format for reading changes
// - Binary logging enabled
// - log_slave_updates enabled (unless we verify no replication)
// - FULL binlog_row_image (for buffered copy which is always used in move)
func configurationCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	var binlogFormat, binlogRowImage, logBin, logSlaveUpdates, binlogRowValueOptions, performanceSchema string
	err := r.SourceDB.QueryRowContext(ctx,
		`SELECT @@global.binlog_format,
		@@global.binlog_row_image,
		@@global.log_bin,
		@@global.log_slave_updates,
		@@global.binlog_row_value_options,
		@@global.performance_schema`).Scan(
		&binlogFormat,
		&binlogRowImage,
		&logBin,
		&logSlaveUpdates,
		&binlogRowValueOptions,
		&performanceSchema,
	)
	if err != nil {
		return err
	}

	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be ROW")
	}

	// Move always uses buffered copy, which requires FULL binlog_row_image
	if binlogRowImage != "FULL" {
		return errors.New("binlog_row_image must be FULL for move operations (buffered copy requires reading all columns from binlog)")
	}

	if binlogRowValueOptions != "" {
		return errors.New("binlog_row_value_options must be empty for move operations (buffered copy requires reading all columns from binlog)")
	}

	if logBin != "1" {
		return errors.New("log_bin must be enabled")
	}

	if logSlaveUpdates != "1" {
		// This is a hard requirement unless we enhance this to confirm
		// it's not receiving any updates via the replication stream.
		return errors.New("log_slave_updates must be enabled")
	}

	if performanceSchema != "1" {
		// Move operations use force-kill by default, which requires performance_schema
		// to find and kill locking transactions via metadata_locks and threads tables.
		return errors.New("performance_schema must be enabled for move operations")
	}

	return nil
}
