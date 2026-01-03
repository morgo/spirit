package sync

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Checkpoint represents the saved state of a sync operation
type Checkpoint struct {
	SyncName             string
	ProjectionName       string
	CopierWatermark      string
	ChecksumWatermark    string
	BinlogName           string
	BinlogPos            int
	LastChecksumEnd      *time.Time
	LastChecksumStatus   string
	TotalRowsSynced      int64
	TotalChunksChecked   int64
	ChunksWithDifferences int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// ChecksumState tracks the state of checksumming
type ChecksumState struct {
	LastFullChecksumStart    *time.Time
	LastFullChecksumEnd      *time.Time
	LastFullChecksumStatus   string
	CurrentChecksumWatermark string
	CurrentChecksumStart     *time.Time
	TotalChunksChecked       int64
	ChunksWithDifferences    int64
	LastDifferenceFound      *time.Time
}

// CreateCheckpointTable creates the checkpoint table on the source database
func CreateCheckpointTable(ctx context.Context, db *sql.DB, schemaName, tableName string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id INT AUTO_INCREMENT PRIMARY KEY,
			sync_name VARCHAR(255) NOT NULL,
			projection_name VARCHAR(255) NOT NULL,
			copier_watermark TEXT NULL,
			checksum_watermark TEXT NULL,
			binlog_name VARCHAR(255) NOT NULL,
			binlog_pos INT NOT NULL,
			last_full_checksum_start TIMESTAMP NULL,
			last_full_checksum_end TIMESTAMP NULL,
			last_full_checksum_status ENUM('passed', 'failed', 'in_progress') NULL,
			total_rows_synced BIGINT DEFAULT 0,
			total_chunks_checked BIGINT DEFAULT 0,
			chunks_with_differences BIGINT DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY (sync_name, projection_name)
		)
	`, schemaName, tableName)

	_, err := db.ExecContext(ctx, query)
	return err
}

// SaveCheckpoint saves the current checkpoint state
func SaveCheckpoint(ctx context.Context, db *sql.DB, schemaName, tableName string, checkpoint *Checkpoint) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s (
			sync_name,
			projection_name,
			copier_watermark,
			checksum_watermark,
			binlog_name,
			binlog_pos,
			last_full_checksum_end,
			last_full_checksum_status,
			total_rows_synced,
			total_chunks_checked,
			chunks_with_differences
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			copier_watermark = VALUES(copier_watermark),
			checksum_watermark = VALUES(checksum_watermark),
			binlog_name = VALUES(binlog_name),
			binlog_pos = VALUES(binlog_pos),
			last_full_checksum_end = VALUES(last_full_checksum_end),
			last_full_checksum_status = VALUES(last_full_checksum_status),
			total_rows_synced = VALUES(total_rows_synced),
			total_chunks_checked = VALUES(total_chunks_checked),
			chunks_with_differences = VALUES(chunks_with_differences),
			updated_at = NOW()
	`, schemaName, tableName)

	_, err := db.ExecContext(ctx, query,
		checkpoint.SyncName,
		checkpoint.ProjectionName,
		checkpoint.CopierWatermark,
		checkpoint.ChecksumWatermark,
		checkpoint.BinlogName,
		checkpoint.BinlogPos,
		checkpoint.LastChecksumEnd,
		checkpoint.LastChecksumStatus,
		checkpoint.TotalRowsSynced,
		checkpoint.TotalChunksChecked,
		checkpoint.ChunksWithDifferences,
	)

	return err
}

// LoadCheckpoint loads the checkpoint for a given sync name
func LoadCheckpoint(ctx context.Context, db *sql.DB, schemaName, tableName, syncName string) (*Checkpoint, error) {
	query := fmt.Sprintf(`
		SELECT
			sync_name,
			projection_name,
			copier_watermark,
			checksum_watermark,
			binlog_name,
			binlog_pos,
			last_full_checksum_end,
			last_full_checksum_status,
			total_rows_synced,
			total_chunks_checked,
			chunks_with_differences,
			created_at,
			updated_at
		FROM %s.%s
		WHERE sync_name = ?
		ORDER BY updated_at DESC
		LIMIT 1
	`, schemaName, tableName)

	var checkpoint Checkpoint
	var copierWatermark, checksumWatermark sql.NullString
	var lastChecksumEnd sql.NullTime
	var lastChecksumStatus sql.NullString
	var createdAt, updatedAt interface{} // Can be []byte or time.Time

	err := db.QueryRowContext(ctx, query, syncName).Scan(
		&checkpoint.SyncName,
		&checkpoint.ProjectionName,
		&copierWatermark,
		&checksumWatermark,
		&checkpoint.BinlogName,
		&checkpoint.BinlogPos,
		&lastChecksumEnd,
		&lastChecksumStatus,
		&checkpoint.TotalRowsSynced,
		&checkpoint.TotalChunksChecked,
		&checkpoint.ChunksWithDifferences,
		&createdAt,
		&updatedAt,
	)

	if err != nil {
		return nil, err
	}

	if copierWatermark.Valid {
		checkpoint.CopierWatermark = copierWatermark.String
	}
	if checksumWatermark.Valid {
		checkpoint.ChecksumWatermark = checksumWatermark.String
	}
	if lastChecksumEnd.Valid {
		checkpoint.LastChecksumEnd = &lastChecksumEnd.Time
	}
	if lastChecksumStatus.Valid {
		checkpoint.LastChecksumStatus = lastChecksumStatus.String
	}
	
	// Parse created_at (can be []byte or time.Time)
	if createdAt != nil {
		if t, ok := createdAt.(time.Time); ok {
			checkpoint.CreatedAt = t
		} else if b, ok := createdAt.([]byte); ok && len(b) > 0 {
			// Parse MySQL timestamp format: "2006-01-02 15:04:05"
			if parsed, err := time.Parse("2006-01-02 15:04:05", string(b)); err == nil {
				checkpoint.CreatedAt = parsed
			}
		}
	}
	
	// Parse updated_at (can be []byte or time.Time)
	if updatedAt != nil {
		if t, ok := updatedAt.(time.Time); ok {
			checkpoint.UpdatedAt = t
		} else if b, ok := updatedAt.([]byte); ok && len(b) > 0 {
			// Parse MySQL timestamp format: "2006-01-02 15:04:05"
			if parsed, err := time.Parse("2006-01-02 15:04:05", string(b)); err == nil {
				checkpoint.UpdatedAt = parsed
			}
		}
	}

	return &checkpoint, nil
}

// CheckpointExists checks if a checkpoint exists for the given sync name
func CheckpointExists(ctx context.Context, db *sql.DB, schemaName, tableName, syncName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s.%s
		WHERE sync_name = ?
	`, schemaName, tableName)

	var count int
	err := db.QueryRowContext(ctx, query, syncName).Scan(&count)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// DeleteCheckpoint deletes the checkpoint for a given sync name
func DeleteCheckpoint(ctx context.Context, db *sql.DB, schemaName, tableName, syncName string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s.%s
		WHERE sync_name = ?
	`, schemaName, tableName)

	_, err := db.ExecContext(ctx, query, syncName)
	return err
}
