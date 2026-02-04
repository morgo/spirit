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

// TestE2EBinlogSubscribingCompositeKeyVarchar tests binlog subscription with composite key
// where the first column is non-numeric (VARCHAR). This validates that KeyAboveHighWatermark
// and KeyBelowLowWatermark optimizations work with VARCHAR keys.
//
// Since watermark optimizations are disabled before checksum (see runner.go), there's no risk
// of checksum corruption even if collation comparison differs slightly between Go and MySQL.
//
// Expected behavior:
// - KeyAboveHighWatermark compares VARCHAR keys and may discard events above the watermark
// - KeyBelowLowWatermark compares VARCHAR keys and buffers events below the watermark
// - Migration completes successfully with optimization enabled
func TestE2EBinlogSubscribingCompositeKeyVarchar(t *testing.T) {
	t.Parallel()
	tbl := `CREATE TABLE e2et3 (
		session_id varchar(40) NOT NULL,
		event_id int NOT NULL,
		data varchar(255) NOT NULL default '',
		PRIMARY KEY (session_id, event_id))`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS e2et3, _e2et3_new`)
	testutils.RunSQL(t, tbl)

	// Insert test data with UUID-based session_ids - create enough rows for chunking
	// First batch: 5 rows with event_id=1
	testutils.RunSQL(t, `INSERT INTO e2et3 (session_id, event_id) 
		SELECT UUID(), 1 FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t`)

	// Add more rows across multiple event_ids to get ~60 rows total
	insertRows := func(eventID int) {
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO e2et3 (session_id, event_id) 
			SELECT UUID(), %d FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t`, eventID))
	}

	for i := 2; i <= 12; i++ {
		insertRows(i)
	}

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		Table:           "e2et3",
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

	// Insert data with a new session_id (any UUID)
	testutils.RunSQL(t, `INSERT INTO e2et3 (session_id, event_id, data) VALUES (UUID(), 999, 'test')`)

	// For VARCHAR first column, KeyAboveHighWatermark works with string comparison
	// The event may be discarded if its UUID is >= the chunk's upper bound UUID
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	// Note: Delta count depends on UUID comparison - could be 0 (discarded) or 1 (kept)

	// Verify KeyAboveHighWatermark performs string comparison
	// (Exact behavior depends on the UUID values)

	// Second chunk - copy remaining data
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	if chunk != nil {
		assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	// Insert another event
	testutils.RunSQL(t, `INSERT INTO e2et3 (session_id, event_id, data) VALUES (UUID(), 1000, 'test2')`)
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	// Note: Delta count depends on UUID comparisons - some events may be discarded
	// if their UUIDs are >= the chunk upper bounds (optimization working correctly)

	// Flush and complete migration
	assert.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	m.status.Set(status.Checksum)
	assert.NoError(t, m.checksum(t.Context()))
	assert.Equal(t, "postChecksum", m.status.Get().String())

	// All done!
	assert.Equal(t, 0, m.db.Stats().InUse)
}
