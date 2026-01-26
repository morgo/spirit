package migration

import (
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

// TestE2EBinlogSubscribingCompositeKeyDateTime tests binlog subscription with composite key
// where the first column is DATETIME. This validates that KeyAboveHighWatermark
// and KeyBelowLowWatermark optimizations work with temporal keys.
//
// Since watermark optimizations are disabled before checksum (see runner.go), there's no risk
// of checksum corruption even if temporal comparison differs slightly between Go and MySQL.
//
// Expected behavior:
// - KeyAboveHighWatermark compares DATETIME keys and may discard events above the watermark
// - KeyBelowLowWatermark compares DATETIME keys and buffers events below the watermark
// - Migration completes successfully with optimization enabled
func TestE2EBinlogSubscribingCompositeKeyDateTime(t *testing.T) {
	t.Parallel()
	tbl := `CREATE TABLE e2et4 (
		created_at DATETIME NOT NULL,
		event_id int NOT NULL,
		data varchar(255) NOT NULL default '',
		PRIMARY KEY (created_at, event_id))`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS e2et4, _e2et4_new`)
	testutils.RunSQL(t, tbl)

	// Helper function to insert test data for a given event_id
	insertRows := func(eventID int) {
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO e2et4 (created_at, event_id) 
			SELECT DATE_ADD('2024-01-01 00:00:00', INTERVAL n*%d HOUR), %d
			FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t`, eventID, eventID))
	}

	// Insert initial row
	testutils.RunSQL(t, `INSERT INTO e2et4 (created_at, event_id) SELECT '2024-01-01 00:00:00', 1 FROM dual`)

	// Add more rows across multiple timestamps to get ~60 rows total
	for i := 2; i <= 12; i++ {
		insertRows(i)
	}

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		Table:           "e2et4",
		Alter:           "ENGINE=InnoDB",
		ReplicaMaxLag:   0,
		TargetChunkTime: 50,
	})
	assert.NoError(t, err)

	// Setup but don't call Run() - step through manually
	m.startTime = time.Now()
	m.dbConfig = dbconn.NewDBConfig()
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	assert.NoError(t, err)
	defer m.db.Close()

	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.setup(t.Context()))

	// Start copying
	m.status.Set(status.CopyRows)
	ccopier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// First chunk
	chunk, err := m.copyChunker.Next()
	assert.NoError(t, err)
	assert.NotNil(t, chunk)
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// Insert data with a timestamp far in the future (should be above watermark)
	testutils.RunSQL(t, `INSERT INTO e2et4 (created_at, event_id, data) VALUES ('2025-12-31 23:59:59', 999, 'future event')`)

	// For DATETIME first column, KeyAboveHighWatermark should work
	// This event might be discarded if it's above the watermark
	assert.NoError(t, m.replClient.BlockWait(t.Context()))

	// Verify KeyAboveHighWatermark returns true for future timestamps
	assert.True(t, m.copyChunker.KeyAboveHighWatermark("2025-12-31 23:59:59"))

	// Insert data with an early timestamp (should be below watermark after chunk completes)
	testutils.RunSQL(t, `INSERT INTO e2et4 (created_at, event_id, data) VALUES ('2024-01-01 01:00:00', 1000, 'early event')`)
	assert.NoError(t, m.replClient.BlockWait(t.Context()))

	// The early timestamp should be kept (not discarded)
	assert.False(t, m.copyChunker.KeyAboveHighWatermark("2024-01-01 01:00:00"))

	// Continue copying remaining chunks
	for {
		chunk, err = m.copyChunker.Next()
		assert.NoError(t, err)
		if chunk == nil {
			break
		}
		assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	// Insert another event after copying is done
	testutils.RunSQL(t, `INSERT INTO e2et4 (created_at, event_id, data) VALUES ('2024-06-15 12:00:00', 2000, 'mid-year event')`)
	assert.NoError(t, m.replClient.BlockWait(t.Context()))

	// Verify we have deltas
	assert.Greater(t, m.replClient.GetDeltaLen(), 0, "Should have buffered events")

	// Flush and complete migration
	assert.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	m.status.Set(status.Checksum)
	assert.NoError(t, m.checksum(t.Context()))
	assert.Equal(t, "postChecksum", m.status.Get().String())

	// Verify final row count (original rows + inserts)
	var count int
	err = m.db.QueryRow("SELECT COUNT(*) FROM e2et4").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 63, count, "Should have 60 original rows + 3 inserts")
}
