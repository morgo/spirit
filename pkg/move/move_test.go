package move

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	status.CheckpointDumpInterval = 100 * time.Millisecond
	sentinelCheckInterval = 100 * time.Millisecond
	sentinelWaitLimit = 10 * time.Second
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestBasicMove(t *testing.T) {
	settingsCheck(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source"
	dest := cfg.Clone()
	dest.DBName = "dest"

	// Convert src and dest back to DSNs.
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// create some data to copy.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source`)
	testutils.RunSQL(t, `CREATE DATABASE source`)
	testutils.RunSQL(t, `CREATE TABLE source.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `CREATE TABLE source.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO source.t1 (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	testutils.RunSQL(t, `INSERT INTO source.t2 (id, val) VALUES (4, 'four'), (5, 'five'), (6, 'six')`)

	// reset the target database.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS dest")
	assert.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE DATABASE dest")
	assert.NoError(t, err)
	defer db.Close()
	// test
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         4,
		CreateSentinel:  false,
	}
	assert.NoError(t, move.Run())
}

func TestResumeFromCheckpointE2E(t *testing.T) {
	settingsCheck(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_resume"
	dest := cfg.Clone()
	dest.DBName = "dest_resume"

	// Convert src and dest back to DSNs.
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// create some data to copy.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_resume`)
	testutils.RunSQL(t, `CREATE DATABASE source_resume`)
	testutils.RunSQL(t, `CREATE TABLE source_resume.t1 (id INT NOT NULL PRIMARY KEY auto_increment, val VARBINARY(255))`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM dual`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)

	// reset the target database.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS dest_resume")
	assert.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE DATABASE dest_resume")
	assert.NoError(t, err)
	defer db.Close()
	// test
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
	}
	r, err := NewRunner(move)
	assert.NoError(t, err)

	// Do all the setup stuff from runnner.Run()
	// Just don't run copier.Run() or cutover etc.
	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	r.source, err = dbconn.New(r.move.SourceDSN, r.dbConfig)
	assert.NoError(t, err)

	// Set up target (single target for this test)
	targetDB, err := dbconn.New(r.move.TargetDSN, r.dbConfig)
	assert.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	assert.NoError(t, err)
	r.targets = []applier.Target{
		{
			DB:       targetDB,
			Config:   targetConfig,
			KeyRange: "0", // Empty for single target
		},
	}

	r.sourceConfig, err = mysql.ParseDSN(r.move.SourceDSN)
	assert.NoError(t, err)
	assert.NoError(t, r.setup(ctx))
	r.startBackgroundRoutines(ctx)

	// Run the copier in a goroutine.
	// We are going to cancel it.
	go func() {
		err := r.copier.Run(ctx)
		assert.Error(t, err)
	}()

	// Wait for a checkpoint to be created in the target
	// In tests this should happen in 100ms, so we wait
	// 10x that.
	time.Sleep(time.Second)

	// Now close the context, we are canceling this run.
	r.cancelFunc()
	assert.NoError(t, r.source.Close())
	// Close all target databases
	for _, target := range r.targets {
		assert.NoError(t, target.DB.Close())
	}
	r.Close()

	// Alter the definition of target.t1 just to be difficult.
	// This will prevent resume.
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 ADD COLUMN extra_col INT DEFAULT 0`)
	r, err = NewRunner(move)
	assert.NoError(t, err)
	assert.Error(t, r.Run(t.Context()))
	assert.NoError(t, r.Close())

	// Drop the additional column, we should be able to resume now.
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 DROP COLUMN extra_col`)
	r, err = NewRunner(move)
	assert.NoError(t, err)
	defer r.Close()
	assert.NoError(t, r.Run(t.Context()))
}

// settingsCheck checks that the database settings are appropriate for running moves.
// Move is not supported unless there is full binlog images, etc. but in CI
// we have some tests and features that do not require this.
func settingsCheck(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	defer db.Close()

	var binlogRowImage, binlogRowValueOptions string
	err = db.QueryRowContext(t.Context(),
		`SELECT 
		@@global.binlog_row_image,
		@@global.binlog_row_value_options`).Scan(
		&binlogRowImage,
		&binlogRowValueOptions,
	)
	assert.NoError(t, err)
	if binlogRowImage != "FULL" || binlogRowValueOptions != "" {
		t.Skip("Skipping test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
	}
}

// TestBasicReshard tests resharding from one source to two sharded targets (-80 and 80-).
func TestBasicReshard(t *testing.T) {
	settingsCheck(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_reshard"
	shard1 := cfg.Clone()
	shard1.DBName = "dest_shard1"
	shard2 := cfg.Clone()
	shard2.DBName = "dest_shard2"

	// Convert to DSNs
	sourceDSN := src.FormatDSN()
	shard1DSN := shard1.FormatDSN()
	shard2DSN := shard2.FormatDSN()

	// Create source database with data
	// Note: Adding user_id column which will be the vindex column
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_reshard`)
	testutils.RunSQL(t, `CREATE DATABASE source_reshard`)
	testutils.RunSQL(t, `CREATE TABLE source_reshard.t1 (id INT PRIMARY KEY, user_id BIGINT NOT NULL, val VARCHAR(255))`)
	testutils.RunSQL(t, `CREATE TABLE source_reshard.t2 (id INT PRIMARY KEY, user_id BIGINT NOT NULL, val VARCHAR(255))`)

	// Insert data with even and odd user_ids
	// Even user_ids (2, 4, 6) should go to shard -80
	// Odd user_ids (1, 3, 5) should go to shard 80-
	testutils.RunSQL(t, `INSERT INTO source_reshard.t1 (id, user_id, val) VALUES (1, 1, 'one'), (2, 2, 'two'), (3, 3, 'three'), (4, 4, 'four'), (5, 5, 'five'), (6, 6, 'six')`)
	testutils.RunSQL(t, `INSERT INTO source_reshard.t2 (id, user_id, val) VALUES (7, 1, 'seven'), (8, 2, 'eight'), (9, 3, 'nine'), (10, 4, 'ten'), (11, 5, 'eleven'), (12, 6, 'twelve')`)

	// Create target shard databases
	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS dest_shard1")
	assert.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE DATABASE dest_shard1")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS dest_shard2")
	assert.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE DATABASE dest_shard2")
	assert.NoError(t, err)

	// Set up target connections
	dbConfig := dbconn.NewDBConfig()

	shard1DB, err := dbconn.New(shard1DSN, dbConfig)
	assert.NoError(t, err)
	defer shard1DB.Close()

	shard2DB, err := dbconn.New(shard2DSN, dbConfig)
	assert.NoError(t, err)
	defer shard2DB.Close()

	// Create vindex provider with evenOddHasher
	vindexProvider := &testVindexProvider{
		vindexColumn: "user_id",
		vindexFunc:   evenOddHasher,
	}

	// Configure the move with multiple targets
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         4,
		CreateSentinel:  false,
		VindexProvider:  vindexProvider,
		Targets: []applier.Target{
			{
				DB:       shard1DB,
				Config:   shard1,
				KeyRange: "-80", // Lower half of keyspace
			},
			{
				DB:       shard2DB,
				Config:   shard2,
				KeyRange: "80-", // Upper half of keyspace
			},
		},
	}

	// Run the reshard
	assert.NoError(t, move.Run())

	// Verify data distribution
	// Shard 1 (-80) should have even user_ids: 2, 4, 6
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dest_shard1.t1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count, "Shard 1 should have 3 rows in t1 (even user_ids)")

	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dest_shard1.t2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count, "Shard 1 should have 3 rows in t2 (even user_ids)")

	// Shard 2 (80-) should have odd user_ids: 1, 3, 5
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dest_shard2.t1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count, "Shard 2 should have 3 rows in t1 (odd user_ids)")

	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dest_shard2.t2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count, "Shard 2 should have 3 rows in t2 (odd user_ids)")

	// Verify specific rows went to correct shards
	// Check shard 1 has even user_ids
	var userID int64
	err = db.QueryRowContext(t.Context(), "SELECT user_id FROM dest_shard1.t1 WHERE id = 2").Scan(&userID)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), userID, "Shard 1 should have user_id 2")

	err = db.QueryRowContext(t.Context(), "SELECT user_id FROM dest_shard1.t1 WHERE id = 4").Scan(&userID)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), userID, "Shard 1 should have user_id 4")

	// Check shard 2 has odd user_ids
	err = db.QueryRowContext(t.Context(), "SELECT user_id FROM dest_shard2.t1 WHERE id = 1").Scan(&userID)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), userID, "Shard 2 should have user_id 1")

	err = db.QueryRowContext(t.Context(), "SELECT user_id FROM dest_shard2.t1 WHERE id = 3").Scan(&userID)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), userID, "Shard 2 should have user_id 3")
}

// testVindexProvider is a simple implementation of VindexMetadataProvider for testing
type testVindexProvider struct {
	vindexColumn string
	vindexFunc   table.VindexFunc
}

func (t *testVindexProvider) GetVindexMetadata(schemaName, tableName string) (string, table.VindexFunc, error) {
	return t.vindexColumn, t.vindexFunc, nil
}

// evenOddHasher is a test hash function that shards assuming -80 and 80- shards.
// even goes to -80, odd goes to 80-
func evenOddHasher(colAny any) (uint64, error) {
	col, ok := colAny.(int64)
	if !ok {
		return 0, fmt.Errorf("expected int64 for sharding column, got %T", colAny)
	}
	// Simple hash: map even user_ids to lower half, odd to upper half
	// This simulates a hash function that distributes across the full uint64 space
	var hash uint64
	if col%2 == 0 {
		// Even user_ids map to 0x0000000000000000 - + the int
		// Use a simple formula that keeps us in the lower half
		hash = uint64(col)
	} else {
		// Odd user_ids map to 0x8000000000000000 + the int.
		// Start from the midpoint and add a small offset
		hash = 0x8000000000000000 + uint64(col)
	}
	return hash, nil
}
