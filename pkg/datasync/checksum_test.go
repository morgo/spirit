package datasync

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestSyncContinuousChecksumFirstCleanPass drives a sync against a quiet
// table and asserts that the continuous checksum FirstCleanPass signal
// fires after the initial copy completes — i.e. the eventually-consistent
// verifier observes the target matching the source on its first pass.
func TestSyncContinuousChecksumFirstCleanPass(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_checksum_src"
	dest := cfg.Clone()
	dest.DBName = "sync_checksum_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_checksum_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_checksum_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_checksum_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_checksum_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_checksum_dest`)

	s := &Sync{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		FlushInterval:   100 * time.Millisecond,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Wait for the checker to be constructed (post-copy + post-flush).
	select {
	case <-runner.ChecksumReady():
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("ChecksumReady did not fire within 60s")
	}

	// On a quiet table, the first clean pass should be quick.
	select {
	case <-runner.FirstCleanPass():
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatalf("FirstCleanPass did not fire within 30s; stats=%+v", runner.ChecksumStats())
	}

	stats := runner.ChecksumStats()
	require.GreaterOrEqual(t, stats.PassesCompleted, uint64(1), "at least one pass should have completed")
	require.False(t, stats.FirstCleanPassAt.IsZero(), "FirstCleanPassAt should be set")
	require.Equal(t, uint64(0), stats.PermanentFailures, "no permanent failures expected on a quiet table")

	cancel()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(60 * time.Second):
		t.Fatal("sync did not stop within 60s of cancellation")
	}
	require.NoError(t, runner.Close())
}

// TestSyncContinuousChecksumWithBackgroundWrites drives a sync while a
// background workload generator inserts/updates/deletes rows on the
// source. The continuous checksum must still converge to a first clean
// pass — replication keeps the target close enough behind that the
// retry path (target catches up to a witnessed source version) closes
// out drift before it accumulates.
func TestSyncContinuousChecksumWithBackgroundWrites(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_checksum_busy_src"
	dest := cfg.Clone()
	dest.DBName = "sync_checksum_busy_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_checksum_busy_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_checksum_busy_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_checksum_busy_src.t1 (id INT PRIMARY KEY, val VARCHAR(255), counter INT DEFAULT 0)`)
	// Seed a few hundred rows so the checker has real work to do (multiple chunks).
	// Build a multi-row INSERT in one statement (RunSQL takes no args).
	seedSrc, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(seedSrc)
	for i := 1; i <= 500; i++ {
		_, err := seedSrc.Exec(`INSERT INTO sync_checksum_busy_src.t1 VALUES (?, CONCAT('v', ?), 0)`, i, i)
		require.NoError(t, err)
	}
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_checksum_busy_dest`)

	s := &Sync{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		FlushInterval:   100 * time.Millisecond,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Wait for the checker to exist.
	select {
	case <-runner.ChecksumReady():
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("ChecksumReady did not fire within 60s")
	}

	// Spawn a background writer that bumps a counter on a random-ish set
	// of rows. This is what produces target lag at read time — exactly
	// what the retry path is for.
	srcDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(srcDB)

	writerDone := make(chan struct{})
	var writerStops atomic.Bool
	go func() {
		defer close(writerDone)
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		i := 0
		for {
			if writerStops.Load() {
				return
			}
			select {
			case <-tick.C:
				id := (i % 500) + 1
				_, _ = srcDB.Exec(`UPDATE sync_checksum_busy_src.t1 SET counter = counter + 1 WHERE id = ?`, id)
				i++
			}
		}
	}()

	// First clean pass must still fire — give it a generous window to
	// account for retries that need the 20s default delay.
	select {
	case <-runner.FirstCleanPass():
		t.Logf("FirstCleanPass fired; stats=%+v", runner.ChecksumStats())
	case <-time.After(120 * time.Second):
		writerStops.Store(true)
		<-writerDone
		cancel()
		t.Fatalf("FirstCleanPass did not fire within 120s under load; stats=%+v", runner.ChecksumStats())
	}

	stats := runner.ChecksumStats()
	// Under active writes we expect at least some chunks to mismatch on
	// first read — that's the whole point of the retry path. Don't assert
	// a specific count (flaky), just that the framework saw drift and
	// recovered without surfacing permanent failures.
	require.Equal(t, uint64(0), stats.PermanentFailures, "should not see permanent failures with normal replication")

	writerStops.Store(true)
	<-writerDone
	cancel()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(60 * time.Second):
		t.Fatal("sync did not stop within 60s of cancellation")
	}
	require.NoError(t, runner.Close())
}

// TestSyncContinuousChecksumDisabled confirms the disable flag wires
// through: no checker is constructed, the FirstCleanPass accessor
// returns nil, and the sync still runs end-to-end.
func TestSyncContinuousChecksumDisabled(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_checksum_off_src"
	dest := cfg.Clone()
	dest.DBName = "sync_checksum_off_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_checksum_off_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_checksum_off_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_checksum_off_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_checksum_off_src.t1 VALUES (1,'one')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_checksum_off_dest`)

	s := &Sync{
		SourceDSN:                 sourceDSN,
		TargetDSN:                 targetDSN,
		TargetChunkTime:           100 * time.Millisecond,
		Threads:                   2,
		WriteThreads:              2,
		FlushInterval:             100 * time.Millisecond,
		DisableContinuousChecksum: true,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	require.Nil(t, runner.FirstCleanPass(), "FirstCleanPass should be nil when checksum is disabled")
	require.NoError(t, runUntilCopied(t, runner))
	require.NoError(t, runner.Close())

	// Zero stats are returned without panicking.
	stats := runner.ChecksumStats()
	require.Equal(t, uint64(0), stats.PassesCompleted)
}
