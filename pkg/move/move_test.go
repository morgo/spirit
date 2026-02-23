package move

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}
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
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest`)
	testutils.RunSQL(t, `CREATE DATABASE dest`)

	// test
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         2,
		WriteThreads:    2,
		CreateSentinel:  false,
	}
	assert.NoError(t, move.Run())
}
func TestResumeFromCheckpointE2E(t *testing.T) {
	testResumeFromCheckpointE2E(t, false)
	testResumeFromCheckpointE2E(t, true)
}

func testResumeFromCheckpointE2E(t *testing.T, deferSecondaryIndexes bool) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}
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
	defer utils.CloseAndLog(db)
	// test
	move := &Move{
		SourceDSN:             sourceDSN,
		TargetDSN:             targetDSN,
		TargetChunkTime:       100 * time.Millisecond,
		Threads:               1,
		DeferSecondaryIndexes: deferSecondaryIndexes,
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
	db, err = dbconn.New(r.move.TargetDSN, r.dbConfig)
	assert.NoError(t, err)
	r.sourceConfig, err = mysql.ParseDSN(r.move.SourceDSN)
	assert.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	assert.NoError(t, err)
	r.targets = []applier.Target{{
		KeyRange: "0",
		DB:       db,
		Config:   targetConfig,
	}}
	assert.NoError(t, r.setup(ctx))
	r.startBackgroundRoutines(ctx)

	// Run the copier in a goroutine.
	// We are going to cancel it.
	go func() {
		err := r.copier.Run(ctx)
		assert.Error(t, err)
	}()

	// Wait until the copier has made enough progress for a checkpoint
	// to be written. Previously this was time.Sleep(time.Second) which
	// was racy — if the copier hadn't completed enough chunks, the
	// checkpoint watermark wasn't ready and no checkpoint was saved,
	// causing checksum failures on resume.
	for {
		if err := r.DumpCheckpoint(t.Context()); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now close the context, we are canceling this run.
	r.cancelFunc()
	assert.NoError(t, r.source.Close())
	assert.NoError(t, r.targets[0].DB.Close())
	assert.NoError(t, r.Close())

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
	defer utils.CloseAndLog(r)
	assert.NoError(t, r.Run(t.Context()))
}

// TestEmptyDatabaseMove tests that a move operation succeeds when the source database has no tables.
// This is a valid scenario for shard splits where an empty shard needs to be split.
func TestEmptyDatabaseMove(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_empty"
	dest := cfg.Clone()
	dest.DBName = "dest_empty"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// Create empty source database (no tables)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_empty`)
	testutils.RunSQL(t, `CREATE DATABASE source_empty`)

	// Create empty target database
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_empty`)
	testutils.RunSQL(t, `CREATE DATABASE dest_empty`)

	// Track if cutover was called
	cutoverCalled := false
	cutoverFunc := func(ctx context.Context) error {
		cutoverCalled = true
		return nil
	}

	// Run move with empty source
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         4,
		CreateSentinel:  false,
	}

	runner, err := NewRunner(move)
	assert.NoError(t, err)
	runner.SetCutover(cutoverFunc)

	// The move should succeed even with no tables
	err = runner.Run(t.Context())
	assert.NoError(t, err, "Move should succeed with empty source database")

	// Verify cutover was called
	assert.True(t, cutoverCalled, "Cutover function should have been called")

	// Clean up
	assert.NoError(t, runner.Close())
}
