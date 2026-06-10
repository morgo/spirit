package move

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	status.CheckpointDumpInterval = 100 * time.Millisecond
	sentinelCheckInterval = 100 * time.Millisecond
	sentinelWaitLimit = 10 * time.Second
	goleak.VerifyTestMain(m)
}

func TestBasicMove(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

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
	require.NoError(t, move.Run())
}
func TestResumeFromCheckpointE2E(t *testing.T) {
	t.Run("deferFalse", func(t *testing.T) { // known to race.
		testResumeFromCheckpointE2E(t, false)
	})
	t.Run("deferTrue", func(t *testing.T) {
		testResumeFromCheckpointE2E(t, true)
	})
}

func testResumeFromCheckpointE2E(t *testing.T, deferSecondaryIndexes bool) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

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

	// reset the target database.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS dest_resume")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE DATABASE dest_resume")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	// test
	move := &Move{
		SourceDSN:             sourceDSN,
		TargetDSN:             targetDSN,
		TargetChunkTime:       100 * time.Millisecond,
		Threads:               1,
		WriteThreads:          1,
		DeferSecondaryIndexes: deferSecondaryIndexes,
	}
	r, err := NewRunner(move)
	require.NoError(t, err)

	// Do all the setup stuff from runnner.Run()
	// Just don't run copier.Run() or cutover etc.
	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	srcDB, err := dbconn.New(r.move.SourceDSN, r.dbConfig)
	require.NoError(t, err)
	srcConfig, err := mysql.ParseDSN(r.move.SourceDSN)
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcDB, config: srcConfig, dsn: r.move.SourceDSN}}
	db, err = dbconn.New(r.move.TargetDSN, r.dbConfig)
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{
		KeyRange: "0",
		DB:       db,
		Config:   targetConfig,
	}}
	require.NoError(t, r.setup(ctx))

	// copy what there is to be copied. we don't need to cancel it,
	// we are just running the copier part.
	require.NoError(t, r.copier.Run(ctx))
	require.NoError(t, r.DumpCheckpoint(ctx))

	// Close everything manually.
	r.cancelFunc()
	require.NoError(t, r.sources[0].db.Close())
	require.NoError(t, r.targets[0].DB.Close())
	require.NoError(t, r.Close())

	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)

	// Alter the definition of target.t1 just to be difficult.
	// This will prevent resume.
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 ADD COLUMN extra_col INT DEFAULT 0`)
	r, err = NewRunner(move)
	require.NoError(t, err)
	require.Error(t, r.Run(t.Context()))
	require.NoError(t, r.Close())

	// Drop the additional column, we should be able to resume now.
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 DROP COLUMN extra_col`)
	r, err = NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.NoError(t, r.Close())
}

// TestEmptyDatabaseMove tests that a move operation succeeds when the source database has no tables.
// This is a valid scenario for shard splits where an empty shard needs to be split.
func TestEmptyDatabaseMove(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

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
		WriteThreads:    4,
		CreateSentinel:  false,
	}

	runner, err := NewRunner(move)
	require.NoError(t, err)
	runner.SetCutover(cutoverFunc)

	// The move should succeed even with no tables
	err = runner.Run(t.Context())
	require.NoError(t, err, "Move should succeed with empty source database")

	// Verify cutover was called
	require.True(t, cutoverCalled, "Cutover function should have been called")

	// Clean up
	require.NoError(t, runner.Close())
}

// TestMoveReservedWordPK is a regression test for issue #828. Moving a
// table whose primary key contains columns named with MySQL reserved
// words used to fail because the chunker_composite prefetch query joined
// chunkKeys without backticks. The move path drives the same chunker, so
// it failed on the first chunk fetch.
func TestMoveReservedWordPK(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_reserved_word"
	dest := cfg.Clone()
	dest.DBName = "dest_reserved_word"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_reserved_word`)
	testutils.RunSQL(t, `CREATE DATABASE source_reserved_word`)
	testutils.RunSQL(t, "CREATE TABLE source_reserved_word.osm_points_of_interest ("+
		"osm_id BIGINT NOT NULL, "+
		"`key` VARCHAR(64) NOT NULL, "+
		"`value` TEXT, "+
		"PRIMARY KEY (osm_id, `key`)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO source_reserved_word.osm_points_of_interest (osm_id, `key`, `value`) "+
		"VALUES (1,'amenity','restaurant'),(2,'amenity','cafe'),(3,'shop','grocery'),"+
		"(4,'tourism','hotel'),(5,'amenity','pub'),(6,'shop','bakery')")

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_reserved_word`)
	testutils.RunSQL(t, `CREATE DATABASE dest_reserved_word`)
	testutils.RunSQL(t, "CREATE TABLE dest_reserved_word.osm_points_of_interest ("+
		"osm_id BIGINT NOT NULL, "+
		"`key` VARCHAR(64) NOT NULL, "+
		"`value` TEXT, "+
		"PRIMARY KEY (osm_id, `key`)"+
		") ENGINE=InnoDB")

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         2,
		WriteThreads:    2,
		CreateSentinel:  false,
	}
	require.NoError(t, move.Run())
}

// TestMoveReservedWordTableName covers issue #828 — moving a table whose
// name is itself a MySQL reserved word (e.g. `order`). The migration paths
// rely on QuotedTableName being backtick-quoted; this test guards against
// any SQL builder regressing to the unquoted TableName.
func TestMoveReservedWordTableName(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_reserved_table_name"
	dest := cfg.Clone()
	dest.DBName = "dest_reserved_table_name"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_reserved_table_name`)
	testutils.RunSQL(t, `CREATE DATABASE source_reserved_table_name`)
	testutils.RunSQL(t, "CREATE TABLE source_reserved_table_name.`order` ("+
		"id BIGINT NOT NULL AUTO_INCREMENT, "+
		"v VARCHAR(64) NOT NULL, "+
		"PRIMARY KEY (id)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO source_reserved_table_name.`order` (v) "+
		"VALUES ('a'),('b'),('c'),('d'),('e'),('f')")

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_reserved_table_name`)
	testutils.RunSQL(t, `CREATE DATABASE dest_reserved_table_name`)
	testutils.RunSQL(t, "CREATE TABLE dest_reserved_table_name.`order` ("+
		"id BIGINT NOT NULL AUTO_INCREMENT, "+
		"v VARCHAR(64) NOT NULL, "+
		"PRIMARY KEY (id)"+
		") ENGINE=InnoDB")

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         2,
		WriteThreads:    2,
		CreateSentinel:  false,
	}
	require.NoError(t, move.Run())
}

// TestPostCopyAnalyzeTargetSchema is a regression test for the bug where
// postCopyPhase ran ANALYZE TABLE against the *source* schema name instead of
// the target's. The move is schema-name-agnostic everywhere else (the
// documented default is /src -> /dest, with differently-named schemas), so the
// ANALYZE silently no-op'd on a real cross-cluster target — the target entered
// cutover with stale InnoDB statistics. We use the canonical /src -> /dest
// pattern (here w3c_src -> w3c_dest), delete the target table's row from
// mysql.innodb_table_stats, drive the runner through postCopyPhase, and assert
// the row was repopulated for the *target* schema. On the unfixed code the
// ANALYZE targets the source schema, so the target's stats row never reappears
// and this test fails.
func TestPostCopyAnalyzeTargetSchema(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "w3c_src"
	dest := cfg.Clone()
	dest.DBName = "w3c_dest"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// Source and target schemas have *different* names.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_src`)
	testutils.RunSQL(t, `CREATE DATABASE w3c_src`)
	testutils.RunSQL(t, `CREATE TABLE w3c_src.t1 (id INT NOT NULL PRIMARY KEY auto_increment, val VARBINARY(255))`)
	testutils.RunSQL(t, `INSERT INTO w3c_src.t1 (val) SELECT RANDOM_BYTES(255) FROM dual`)
	testutils.RunSQL(t, `INSERT INTO w3c_src.t1 (val) SELECT RANDOM_BYTES(255) FROM w3c_src.t1 a JOIN w3c_src.t1 b LIMIT 1000`)

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_dest`)
	testutils.RunSQL(t, `CREATE DATABASE w3c_dest`)
	t.Cleanup(func() {
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_src`)
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_dest`)
	})

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         1,
		WriteThreads:    1,
	}
	r, err := NewRunner(move)
	require.NoError(t, err)

	// Drive the same setup the real Run() does, then run the copier so the
	// target table exists and is populated, then call postCopyPhase directly.
	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	srcDB, err := dbconn.New(r.move.SourceDSN, r.dbConfig)
	require.NoError(t, err)
	srcConfig, err := mysql.ParseDSN(r.move.SourceDSN)
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcDB, config: srcConfig, dsn: r.move.SourceDSN}}
	targetDB, err := dbconn.New(r.move.TargetDSN, r.dbConfig)
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{
		KeyRange: "0",
		DB:       targetDB,
		Config:   targetConfig,
	}}
	require.NoError(t, r.setup(ctx))
	t.Cleanup(func() {
		r.cancelFunc()
		// Runner.Close() closes the target DB and repl clients but not the raw
		// source DB connection, so close it explicitly to avoid a goroutine leak
		// (goleak runs in TestMain).
		require.NoError(t, srcDB.Close())
		require.NoError(t, r.Close())
	})

	// Copy the rows so the target table is created and populated.
	require.NoError(t, r.copier.Run(ctx))

	// Delete the *target* schema's stats row. A correct ANALYZE (run unqualified
	// on the target's own connection) repopulates it; the buggy ANALYZE (qualified
	// with the source schema) touched w3c_src.t1 instead and left this missing.
	testutils.RunSQL(t, `DELETE FROM mysql.innodb_table_stats WHERE database_name = 'w3c_dest' AND table_name = 't1'`)
	var beforeCount int
	require.NoError(t, targetDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mysql.innodb_table_stats WHERE database_name = 'w3c_dest' AND table_name = 't1'`).Scan(&beforeCount))
	require.Equal(t, 0, beforeCount, "precondition: target stats row should be deleted")

	require.NoError(t, r.postCopyPhase(ctx))

	var afterCount int
	require.NoError(t, targetDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mysql.innodb_table_stats WHERE database_name = 'w3c_dest' AND table_name = 't1'`).Scan(&afterCount))
	require.Equal(t, 1, afterCount,
		"postCopyPhase must ANALYZE the TARGET schema (w3c_dest.t1); stats row was not repopulated, so ANALYZE hit the wrong schema")
}

// TestAnalyzeTableMissingTargetIsError verifies that analyzeTable inspects the
// ANALYZE TABLE result set and surfaces a missing table as an error, rather
// than silently returning nil (the failure mode that hid the cross-schema bug
// on real cross-cluster targets).
func TestAnalyzeTableMissingTargetIsError(t *testing.T) {
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_analyze`)
	testutils.RunSQL(t, `CREATE DATABASE w3c_analyze`)
	testutils.RunSQL(t, `CREATE TABLE w3c_analyze.present (id INT PRIMARY KEY)`)
	t.Cleanup(func() { testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_analyze`) })

	// analyzeTable addresses the table UNQUALIFIED, so the connection must
	// default to the target schema (as the real target.DB does) — connect with
	// w3c_analyze as the default database rather than the usual `test`.
	dbConfig := dbconn.NewDBConfig()
	db, err := dbconn.New(testutils.DSNForDatabase("w3c_analyze"), dbConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// analyzeTable only reads r.logger (for non-error rows), so give it a real
	// logger so the warning path does not dereference a nil pointer.
	r := &Runner{logger: slog.Default()}

	// A freshly-created table analyzes cleanly (Msg_type "status").
	require.NoError(t, r.analyzeTable(t.Context(), db, "present"))

	// Re-analyzing an already-analyzed table must also succeed. Depending on the
	// MySQL version/engine this may report a non-"OK" status row (e.g. "Table is
	// already up to date"); analyzeTable must treat any non-"Error" Msg_type as
	// success and never abort on a non-OK Msg_text.
	require.NoError(t, r.analyzeTable(t.Context(), db, "present"))

	// A missing table reports an Error row in the result set; analyzeTable must
	// turn that into a returned error instead of a silent no-op. (MySQL emits an
	// "Error" row followed by a trailing "status: Operation failed" row; the
	// error must be detected via the "Error" Msg_type, not via the non-OK text.)
	err = r.analyzeTable(t.Context(), db, "does_not_exist")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ANALYZE TABLE")
}
