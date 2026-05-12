package migration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// serverHasGTIDMode reports whether the server backing MYSQL_DSN currently
// has gtid_mode=ON. Used to gate GTID-specific tests on test infrastructure
// that may run against either a GTID-enabled (replication-ci.yml) or a plain
// (compose.yml) MySQL.
func serverHasGTIDMode(t *testing.T) bool {
	t.Helper()
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	var mode string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.gtid_mode").Scan(&mode))
	return strings.EqualFold(mode, "ON")
}

// TestResumeFromGTIDCheckpoint exercises the GTID-mode write/read path
// without performing an actual source failover. It runs against a single
// GTID-enabled server, dumps a checkpoint, verifies the binlog_gtid column
// is populated, then resumes a second migration and asserts that the GTID
// path was used (the replClient ends up in gtid_mode after restore).
func TestResumeFromGTIDCheckpoint(t *testing.T) {
	t.Parallel()
	if !serverHasGTIDMode(t) {
		t.Skip("skipping GTID resume test because @@global.gtid_mode != ON; run with replication-ci.yml compose")
	}

	tt := testutils.NewTestTable(t, "gtidresume", `CREATE TABLE gtidresume (
		pk int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO gtidresume (name) SELECT 'a'", 50000)

	alterSQL := "ENGINE=InnoDB"

	m := NewTestRunner(t, "gtidresume", alterSQL,
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(ctx) // expected to be cancelled mid-run
	}()
	waitForCheckpoint(t, m)
	cancel()
	<-done
	require.NoError(t, m.Close())

	// Confirm that the GTID column was populated in the checkpoint row.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	var binlogGTID sql.NullString
	cpName := m.checkpointTableName()
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT binlog_gtid FROM `%s` ORDER BY id DESC LIMIT 1", cpName),
	).Scan(&binlogGTID))
	require.True(t, binlogGTID.Valid && binlogGTID.String != "",
		"checkpoint should have a non-empty binlog_gtid when gtid_mode=ON")

	// A few more writes so that resume has work to replay through the
	// repl client (and would notice if it had picked the wrong position).
	testutils.RunSQL(t, "INSERT INTO gtidresume (name) VALUES ('t')")

	m2 := NewTestRunner(t, "gtidresume", alterSQL, WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint, "second run should resume from checkpoint")
	require.True(t, m2.replClient.GTIDModeEnabled(),
		"second run's repl client should be in gtid_mode after resume")
	require.NoError(t, m2.Close())
}

// TestResumeFromSourceFailover validates the headline scenario for issue #270:
// a migration that has checkpointed against the original source can be
// resumed by pointing it at the promoted replica (now the new source) as
// long as both servers have gtid_mode=ON. After this test the original
// replica is no longer a replica, so we restore the replication topology
// in t.Cleanup so the rest of the test suite is unaffected.
//
// Gated on REPLICA_DSN being set; the compose/replication-tls/replication-ci.yml
// stack supplies it.
func TestResumeFromSourceFailover(t *testing.T) {
	// Not Parallel: this test reconfigures replication on the test
	// MySQL instances, which is shared global state.
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping source failover test because REPLICA_DSN not set")
	}
	if !serverHasGTIDMode(t) {
		t.Skip("skipping source failover test because @@global.gtid_mode != ON")
	}
	testutils.WaitForReplicaHealthy(t, replicaDSN, 30*time.Second)

	// Build a DSN to the replica that has the same database as the test DSN.
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	replicaCfg, err := mysql.ParseDSN(replicaDSN)
	require.NoError(t, err)
	replicaCfg.DBName = cfg.DBName
	replicaCfg.User = cfg.User // use the test-suite user so the migration has DDL rights
	replicaCfg.Passwd = cfg.Passwd

	tt := testutils.NewTestTable(t, "gtidfailover", `CREATE TABLE gtidfailover (
		pk int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO gtidfailover (name) SELECT 'a'", 50000)

	alterSQL := "ENGINE=InnoDB"

	m := NewTestRunner(t, "gtidfailover", alterSQL,
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(ctx) // expected to be cancelled mid-run
	}()
	waitForCheckpoint(t, m)
	cancel()
	<-done
	require.NoError(t, m.Close())

	// Wait for replication to catch up so the checkpoint row is visible
	// on the replica before we promote it.
	waitForReplicaCaughtUp(t, replicaDSN, 30*time.Second)

	// Promote the replica: stop replication, clear its replication state,
	// and we now treat replicaDSN as the new primary.
	replicaRoot, err := sql.Open("mysql", replicaDSN)
	require.NoError(t, err)
	defer func() { _ = replicaRoot.Close() }()
	_, err = replicaRoot.ExecContext(t.Context(), "STOP REPLICA")
	require.NoError(t, err)
	_, err = replicaRoot.ExecContext(t.Context(), "RESET REPLICA ALL")
	require.NoError(t, err)
	t.Cleanup(func() {
		// Restore the original topology so subsequent tests are not affected.
		// We can use the original replication-ci init script's CHANGE
		// REPLICATION SOURCE TO statement here.
		// Cleanup uses Background context, the test context may already be done.
		bg := context.Background()
		if _, err := replicaRoot.ExecContext(bg, "STOP REPLICA"); err != nil {
			t.Logf("cleanup: STOP REPLICA: %v", err)
		}
		// CHANGE REPLICATION SOURCE TO requires the original source host
		// to be reachable from the replica container; in compose that's "mysql".
		if _, err := replicaRoot.ExecContext(bg,
			"CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql', SOURCE_USER='rsandbox', "+
				"SOURCE_PASSWORD='rsandbox', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1"); err != nil {
			t.Logf("cleanup: CHANGE REPLICATION SOURCE: %v", err)
		}
		if _, err := replicaRoot.ExecContext(bg, "START REPLICA"); err != nil {
			t.Logf("cleanup: START REPLICA: %v", err)
		}
	})

	// Insert additional rows on the (promoted) new primary so resume has
	// real binlog work to replay through.
	_, err = replicaRoot.ExecContext(t.Context(),
		fmt.Sprintf("INSERT INTO `%s`.gtidfailover (name) VALUES ('post-failover')", cfg.DBName))
	require.NoError(t, err)

	// Spin up a second runner pointed at the promoted replica and verify
	// it resumes from the checkpoint using the GTID.
	m2Migration := newTestMigration(t,
		WithThreads(2),
		WithTable("gtidfailover"),
		WithAlter(alterSQL),
	)
	m2Migration.Host = replicaCfg.Addr
	m2, err := NewRunner(m2Migration)
	require.NoError(t, err)
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint,
		"resume against the promoted replica should have succeeded via the GTID checkpoint")
	require.NoError(t, m2.Close())
}

// waitForReplicaCaughtUp polls SHOW REPLICA STATUS until Seconds_Behind_Source
// is 0, indicating the replica has caught up to the primary.
func waitForReplicaCaughtUp(t *testing.T, dsn string, timeout time.Duration) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(t.Context(), "SHOW REPLICA STATUS")
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		cols, _ := rows.Columns()
		if rows.Next() {
			values := make([]any, len(cols))
			for i := range values {
				values[i] = new(sql.NullString)
			}
			if err := rows.Scan(values...); err == nil {
				for i, name := range cols {
					if name == "Seconds_Behind_Source" {
						v := values[i].(*sql.NullString)
						if v.Valid && v.String == "0" {
							_ = rows.Close()
							return
						}
					}
				}
			}
		}
		_ = rows.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("replica did not catch up within %s", timeout)
}
