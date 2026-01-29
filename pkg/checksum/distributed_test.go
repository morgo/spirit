package checksum

import (
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFixCorruptWithApplier(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	newDBName := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS corruptt1")
	testutils.RunSQL(t, "CREATE TABLE corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (2, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (3, 2, 3)")

	testutils.RunSQL(t, "CREATE TABLE "+newDBName+".corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (1, 2, 3)")
	// row 2 is missing
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (3, 9, 9)")

	destDB := cfg.Clone()
	destDB.DBName = newDBName

	src, err := dbconn.New(cfg.FormatDSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(src)
	dest, err := dbconn.New(destDB.FormatDSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(dest)

	t1 := table.NewTableInfo(src, "test", "corruptt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(dest, newDBName, "corruptt1")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()
	target := applier.Target{
		DB:       dest,
		KeyRange: "0",
		Config:   destDB,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	// Start the applier so its workers can process async Apply calls
	feed := repl.NewClient(src, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:                     logger,
		Concurrency:                4,
		TargetBatchTime:            time.Second,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		Applier:                    applier,
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.Applier = applier

	checker, err := NewChecker(src, chunker, feed, config)
	assert.Equal(t, "0/3 0.00%", checker.GetProgress())
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(t.Context())) // should be fixed!
	assert.Equal(t, "3/3 100.00%", checker.GetProgress())
}

func TestDistributedChecksum(t *testing.T) {
	// Create source database with test data
	// use a reserved word in the column names.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS checksum_distributed`)
	testutils.RunSQL(t, `CREATE DATABASE checksum_distributed`)
	testutils.RunSQL(t, "CREATE TABLE checksum_distributed.t1 (id INT PRIMARY KEY AUTO_INCREMENT, pad1 VARBINARY(255) NOT NULL,`values` VARBINARY(255) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM dual")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS checksum_distributed_0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS checksum_distributed_1`)
	testutils.RunSQL(t, `CREATE DATABASE checksum_distributed_0`)
	testutils.RunSQL(t, `CREATE DATABASE checksum_distributed_1`)
	testutils.RunSQL(t, `CREATE TABLE checksum_distributed_0.t1 LIKE checksum_distributed.t1`)
	testutils.RunSQL(t, `CREATE TABLE checksum_distributed_1.t1 LIKE checksum_distributed.t1`)
	testutils.RunSQL(t, `INSERT INTO checksum_distributed_0.t1 SELECT * FROM checksum_distributed.t1 WHERE MOD(id, 2) = 0`)
	testutils.RunSQL(t, `INSERT INTO checksum_distributed_1.t1 SELECT * FROM checksum_distributed.t1 WHERE MOD(id, 2) = 1`)

	// Parse DSN and create database connections
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Source database
	sourceCfg := cfg.Clone()
	sourceCfg.DBName = "checksum_distributed"
	sourceDB, err := dbconn.New(sourceCfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	// Shard 0 database (even IDs)
	shard0Cfg := cfg.Clone()
	shard0Cfg.DBName = "checksum_distributed_0"
	shard0DB, err := dbconn.New(shard0Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(shard0DB)

	// Shard 1 database (odd IDs)
	shard1Cfg := cfg.Clone()
	shard1Cfg.DBName = "checksum_distributed_1"
	shard1DB, err := dbconn.New(shard1Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(shard1DB)

	// Create table info for source
	sourceTable := table.NewTableInfo(sourceDB, "checksum_distributed", "t1")
	require.NoError(t, sourceTable.SetInfo(t.Context()))

	// Configure sharding on the source table
	// Use the id column for sharding with EvenOddHasher
	sourceTable.ShardingColumn = "id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	// Create table info for shard 0
	shard0Table := table.NewTableInfo(shard0DB, "checksum_distributed_0", "t1")
	require.NoError(t, shard0Table.SetInfo(t.Context()))

	// Create table info for shard 1
	shard1Table := table.NewTableInfo(shard1DB, "checksum_distributed_1", "t1")
	require.NoError(t, shard1Table.SetInfo(t.Context()))

	// Create ShardedApplier with key ranges
	// Shard 0: "-80" (even IDs)
	// Shard 1: "80-" (odd IDs)
	targets := []applier.Target{
		{DB: shard0DB, KeyRange: "-80", Config: shard0Cfg},
		{DB: shard1DB, KeyRange: "80-", Config: shard1Cfg},
	}
	shardedApplier, err := applier.NewShardedApplier(targets, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	// Create replication feed
	// We're not going to do anything, but it's required by the distributed checker
	logger := slog.Default()
	feed := repl.NewClient(sourceDB, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
		Applier:         shardedApplier,
	})
	defer feed.Close()

	// For distributed checksum, we only add one subscription for the source table
	// The applier handles distribution to multiple shards
	require.NoError(t, feed.AddSubscription(sourceTable, sourceTable, nil))
	require.NoError(t, feed.Run(t.Context()))

	// Create chunker - for distributed checksum, we pass nil for the new table
	// because the chunker will use the source table for both sides
	chunker, err := table.NewChunker(sourceTable, nil, 0, logger)
	require.NoError(t, err)
	require.NoError(t, chunker.Open())

	// Create distributed checker config
	config := NewCheckerDefaultConfig()
	config.Applier = shardedApplier
	config.FixDifferences = false // Should pass without needing fixes

	// Create and run the distributed checker
	checker, err := NewChecker(sourceDB, chunker, feed, config)
	require.NoError(t, err)

	// Run the checksum - should pass since data is correctly distributed
	require.NoError(t, checker.Run(t.Context()))

	// Verify data distribution is correct
	var shard0Count, shard1Count, sourceCount int
	err = shard0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&shard0Count)
	require.NoError(t, err)
	err = shard1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&shard1Count)
	require.NoError(t, err)
	err = sourceDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&sourceCount)
	require.NoError(t, err)

	assert.Equal(t, sourceCount, shard0Count+shard1Count, "Total rows in shards should equal source")
}
