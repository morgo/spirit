package move

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

// TestShardedMove tests a move operation with multiple target shards.
// This simulates a Vitess-style resharding operation where data is distributed
// across multiple shards based on a hash of a sharding column.
//
// It includes a generated column to ensure that generated columns are handled correctly
// and a reserved name column: `values` which stores an email address.
// The generated column appears before the sharding column to test column order handling.
func TestShardedMove(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Setup source database
	src := cfg.Clone()
	src.DBName = "source_sharded"
	sourceDSN := src.FormatDSN()

	// Setup two target shards with key ranges that split the hash space in half
	// Shard 0: -80 (0x0000000000000000 to 0x8000000000000000)
	// Shard 1: 80- (0x8000000000000000 to 0xffffffffffffffff)
	shard0 := cfg.Clone()
	shard0.DBName = "dest_sharded_0"
	shard1 := cfg.Clone()
	shard1.DBName = "dest_sharded_1"

	// Create source database with test data
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_sharded`)
	testutils.RunSQL(t, `CREATE DATABASE source_sharded`)
	testutils.RunSQL(t, "CREATE TABLE source_sharded.users (id INT PRIMARY KEY, name_reversed VARCHAR(255) AS (REVERSE(name)) STORED, user_id INT NOT NULL,name VARCHAR(255) NOT NULL, `values` VARCHAR(255) NOT NULL, KEY idx_user_id (user_id))")

	// Insert test data - we'll use user_id as the sharding column
	// Insert enough rows to ensure both shards get data
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	for i := 1; i <= 100; i++ {
		_, err := db.ExecContext(t.Context(), "INSERT INTO source_sharded.users (id, user_id, name, `values`) VALUES (?, ?, ?, ?)",
			i, i, fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		assert.NoError(t, err)
	}

	// Create target databases
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_sharded_0`)
	testutils.RunSQL(t, `CREATE DATABASE dest_sharded_0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_sharded_1`)
	testutils.RunSQL(t, `CREATE DATABASE dest_sharded_1`)

	// Create database connections for each shard
	dbConfig := dbconn.NewDBConfig()
	shard0DB, err := dbconn.New(shard0.FormatDSN(), dbConfig)
	assert.NoError(t, err)
	defer utils.CloseAndLog(shard0DB)

	shard1DB, err := dbconn.New(shard1.FormatDSN(), dbConfig)
	assert.NoError(t, err)
	defer utils.CloseAndLog(shard1DB)

	// Create a sharding provider that uses user_id as the sharding column
	shardingProvider := &testShardingProvider{
		shardingColumn: "user_id",
		hashFunc:       testutils.EvenOddHasher,
	}

	// Configure the move with multiple targets
	move := &Move{
		SourceDSN:        sourceDSN,
		TargetChunkTime:  5 * time.Second,
		Threads:          2,
		WriteThreads:     2,
		ShardingProvider: shardingProvider,
		Targets: []applier.Target{
			{
				KeyRange: "-80",
				DB:       shard0DB,
				Config:   shard0,
			},
			{
				KeyRange: "80-",
				DB:       shard1DB,
				Config:   shard1,
			},
		},
	}

	// Track if cutover was called
	cutoverCalled := false
	cutoverFunc := func(ctx context.Context) error {
		cutoverCalled = true
		return nil
	}
	runner, err := NewRunner(move)
	assert.NoError(t, err)
	runner.SetCutover(cutoverFunc)

	// Check initial data before move
	var initialCount int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM source_sharded.users").Scan(&initialCount)
	assert.NoError(t, err)
	t.Logf("Initial source count: %d", initialCount)

	// Run the sharded move
	err = runner.Run(t.Context())
	assert.NoError(t, err, "Sharded move should succeed")

	// Verify cutover was called
	assert.True(t, cutoverCalled, "Cutover function should have been called")

	// Check if users_old exists in source
	var oldTableCount int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM source_sharded.users_old").Scan(&oldTableCount)
	assert.NoError(t, err)

	// Verify data distribution across shards
	var shard0Count, shard1Count int
	err = shard0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard0Count)
	assert.NoError(t, err)
	err = shard1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard1Count)
	assert.NoError(t, err)

	assert.Equal(t, initialCount, oldTableCount, "Source old table should have all rows")
	assert.Equal(t, initialCount/2, shard0Count, "half the rows in shard 0")
	assert.Equal(t, initialCount/2, shard1Count, "half the rows in shard 1")

	assert.NoError(t, runner.Close()) // Clean up
}

// testShardingProvider is a simple implementation of ShardingMetadataProvider for testing.
// It returns the same sharding configuration for all tables.
type testShardingProvider struct {
	shardingColumn string
	hashFunc       table.HashFunc
}

func (p *testShardingProvider) GetShardingMetadata(schemaName, tableName string) (string, table.HashFunc, error) {
	return p.shardingColumn, p.hashFunc, nil
}
