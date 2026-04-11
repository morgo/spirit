package move

import (
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNtoMShardedMove tests an N:M resharding scenario:
// 2 source shards → 2 target shards, with data redistributed by even/odd hash.
func TestNtoMShardedMove(t *testing.T) {
	// Create 2 source databases and 2 target databases.
	src0Name, _ := testutils.CreateUniqueTestDatabase(t)
	src1Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt0Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt1Name, _ := testutils.CreateUniqueTestDatabase(t)

	// Create identical tables on both sources.
	for _, dbName := range []string{src0Name, src1Name} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		)`)
	}

	// Insert rows 1-50 on source 0, rows 51-100 on source 1.
	for i := 1; i <= 50; i++ {
		testutils.RunSQLInDatabase(t, src0Name, fmt.Sprintf(
			"INSERT INTO users (id, name) VALUES (%d, 'user_%d')", i, i))
	}
	for i := 51; i <= 100; i++ {
		testutils.RunSQLInDatabase(t, src1Name, fmt.Sprintf(
			"INSERT INTO users (id, name) VALUES (%d, 'user_%d')", i, i))
	}

	// Build source DSNs.
	src0DSN := testutils.DSNForDatabase(src0Name)
	src1DSN := testutils.DSNForDatabase(src1Name)

	// Build target connections.
	dbConfig := dbconn.NewDBConfig()
	tgt0DB, err := dbconn.New(testutils.DSNForDatabase(tgt0Name), dbConfig)
	require.NoError(t, err)
	tgt0Config, err := mysql.ParseDSN(testutils.DSNForDatabase(tgt0Name))
	require.NoError(t, err)

	tgt1DB, err := dbconn.New(testutils.DSNForDatabase(tgt1Name), dbConfig)
	require.NoError(t, err)
	tgt1Config, err := mysql.ParseDSN(testutils.DSNForDatabase(tgt1Name))
	require.NoError(t, err)

	targets := []applier.Target{
		{KeyRange: "-80", DB: tgt0DB, Config: tgt0Config},
		{KeyRange: "80-", DB: tgt1DB, Config: tgt1Config},
	}

	move := &Move{
		SourceDSNs:      []string{src0DSN, src1DSN},
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		Targets:         targets,
		SourceTables:    []string{"users"},
		ShardingProvider: &testShardingProvider{
			shardingColumn: "id",
			hashFunc:       testutils.EvenOddHasher,
		},
	}

	err = move.Run()
	assert.NoError(t, err)

	// Verify: all 100 rows should be across the 2 targets.
	var tgt0Count, tgt1Count int
	err = tgt0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&tgt0Count)
	assert.NoError(t, err)
	err = tgt1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&tgt1Count)
	assert.NoError(t, err)

	assert.Equal(t, 100, tgt0Count+tgt1Count, "total rows across targets should be 100")
	assert.Equal(t, 50, tgt0Count, "even ids should go to target 0 (-80)")
	assert.Equal(t, 50, tgt1Count, "odd ids should go to target 1 (80-)")

	// Verify even IDs are on target 0, odd IDs on target 1.
	var minID, maxID int
	err = tgt0DB.QueryRowContext(t.Context(), "SELECT MIN(id), MAX(id) FROM users").Scan(&minID, &maxID)
	assert.NoError(t, err)
	// All even IDs should be on target 0.
	var oddCount int
	err = tgt0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users WHERE id % 2 = 1").Scan(&oddCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, oddCount, "target 0 should have no odd IDs")

	var evenCount int
	err = tgt1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users WHERE id % 2 = 0").Scan(&evenCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, evenCount, "target 1 should have no even IDs")
}

// testShardingProvider is defined in move_sharded_test.go
