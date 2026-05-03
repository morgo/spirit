// Package replstress contains stand-alone stress tests that probe MySQL
// behaviour at the binlog/visibility boundary without going through any of
// spirit's pkg/repl machinery.
//
// The tests here use database/sql + go-mysql-org/go-mysql/replication
// directly, so any failure attributes to MySQL itself (or to the test
// producer) rather than to spirit's wrappers — which is the explicit
// reason this package is separate from pkg/repl.
package replstress

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestBinlogVisibilityStress reproduces the contract that, with
// binlog_order_commits=ON, every row event the binlog streamer observes
// for a committed transaction must already be visible to a fresh
// autocommit SELECT on a separate connection.
//
// Producers run BEGIN/INSERT/UPDATE/COMMIT in a tight loop. A single
// streamer goroutine reads WriteRowsEvents from the binlog and, for each
// new PK, immediately runs `SELECT id FROM <table> WHERE id = ?` on a
// pool of fresh connections (distinct from the producers). If the SELECT
// can't see the row, the test logs the discrepancy and fails.
//
// The hypothesis is that under load MySQL sometimes lets a binlog write
// be observable before the InnoDB commit is visible on other
// connections. The test design intentionally omits every spirit-side
// abstraction (delta map, watermark, chunker, applier, buffered map) so
// a failure here strongly attributes to MySQL.
//
// Tunables (env vars, all optional):
//
//	SPIRIT_VISIBILITY_STRESS_DURATION   Go duration string (default 30s)
//	SPIRIT_VISIBILITY_STRESS_PRODUCERS  int (default 8)
//	SPIRIT_VISIBILITY_STRESS_VERBOSE    "1" enables per-event trace logs
func TestBinlogVisibilityStress(t *testing.T) {
	duration := envDuration(t, "SPIRIT_VISIBILITY_STRESS_DURATION", 30*time.Second)
	numProducers := envInt(t, "SPIRIT_VISIBILITY_STRESS_PRODUCERS", 8)
	verbose := os.Getenv("SPIRIT_VISIBILITY_STRESS_VERBOSE") == "1"

	cfg, err := mysqldriver.ParseDSN(testutils.DSN())
	require.NoError(t, err, "parse MYSQL_DSN")

	rootDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rootDB.Close() })

	checkServerSettings(t, rootDB)

	dbName, scopedDB := testutils.CreateUniqueTestDatabase(t)
	const tableName = "stress_repro"
	_, err = scopedDB.ExecContext(t.Context(), `
		CREATE TABLE `+tableName+` (
			id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			x_token VARCHAR(64) NOT NULL,
			payload VARCHAR(255) NOT NULL,
			created_at DATETIME(6) NOT NULL,
			updated_at DATETIME(6) NOT NULL,
			UNIQUE KEY idx_x_token (x_token)
		) ENGINE=InnoDB`)
	require.NoError(t, err)

	verifierDB, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifierDB.Close() })
	verifierDB.SetMaxOpenConns(8)
	verifierDB.SetMaxIdleConns(8)

	host, portStr, err := net.SplitHostPort(cfg.Addr)
	require.NoError(t, err, "split host:port %q", cfg.Addr)
	port, err := strconv.ParseUint(portStr, 10, 16)
	require.NoError(t, err)

	startPos, err := getCurrentBinlogPosition(t.Context(), rootDB)
	require.NoError(t, err)

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: newServerID(),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     cfg.User,
		Password: cfg.Passwd,
	}
	syncer := replication.NewBinlogSyncer(syncerCfg)
	t.Cleanup(syncer.Close)
	streamer, err := syncer.StartSync(startPos)
	require.NoError(t, err)

	stress := &stressRun{
		t:          t,
		table:      tableName,
		schema:     dbName,
		verifier:   verifierDB,
		verbose:    verbose,
		writes:     make(map[int64]struct{}),
		writesLock: &sync.Mutex{},
	}

	streamCtx, cancelStream := context.WithCancel(t.Context())
	defer cancelStream()

	var streamWG sync.WaitGroup
	streamWG.Add(1)
	go func() {
		defer streamWG.Done()
		stress.runStreamer(streamCtx, streamer)
	}()

	prodCtx, cancelProducers := context.WithTimeout(t.Context(), duration)
	defer cancelProducers()

	var prodWG sync.WaitGroup
	for i := 0; i < numProducers; i++ {
		prodWG.Add(1)
		go func(workerID int) {
			defer prodWG.Done()
			stress.runProducer(prodCtx, scopedDB, workerID)
		}(i)
	}
	prodWG.Wait()

	// Producers are done. Give the streamer a grace window to deliver any
	// trailing events so the verification covers the entire run.
	time.Sleep(500 * time.Millisecond)
	cancelStream()
	streamWG.Wait()

	t.Logf("producers=%d duration=%s commits=%d events_observed=%d "+
		"verifier_checks=%d misses_initial=%d misses_permanent=%d "+
		"producer_errors=%d",
		numProducers, duration,
		stress.commits.Load(), stress.eventsObserved.Load(),
		stress.checks.Load(), stress.missesInitial.Load(),
		stress.missesPermanent.Load(), stress.producerErrors.Load())

	if stress.missesInitial.Load() > 0 {
		t.Errorf(
			"binlog visibility violated: %d row events were observed in "+
				"the binlog but the corresponding row was not visible to a "+
				"fresh SELECT (%d remained invisible after a 50ms retry). "+
				"With binlog_order_commits=ON this should not happen and "+
				"likely indicates an upstream MySQL bug.",
			stress.missesInitial.Load(),
			stress.missesPermanent.Load(),
		)
	}
	if stress.commits.Load() == 0 {
		t.Fatalf("producers did not complete any commits — test config or "+
			"MySQL connectivity is broken (errors=%d)",
			stress.producerErrors.Load())
	}
	if stress.eventsObserved.Load() == 0 {
		t.Fatalf("streamer observed no row events — log_bin disabled, "+
			"streamer credentials wrong, or binlog position skipped past "+
			"all writes (commits=%d)", stress.commits.Load())
	}
}

type stressRun struct {
	t        *testing.T
	schema   string
	table    string
	verifier *sql.DB
	verbose  bool

	commits         atomic.Int64
	producerErrors  atomic.Int64
	eventsObserved  atomic.Int64
	checks          atomic.Int64
	missesInitial   atomic.Int64
	missesPermanent atomic.Int64

	writesLock *sync.Mutex
	writes     map[int64]struct{} // ids that producers have committed
}

func (s *stressRun) recordCommit(id int64) {
	s.commits.Add(1)
	s.writesLock.Lock()
	s.writes[id] = struct{}{}
	s.writesLock.Unlock()
}

func (s *stressRun) wasCommitted(id int64) bool {
	s.writesLock.Lock()
	_, ok := s.writes[id]
	s.writesLock.Unlock()
	return ok
}

// runProducer loops BEGIN; INSERT; UPDATE; COMMIT. The shape mirrors
// spirit's TestCutoverAtomicityWithConcurrentWrites writers (see #746).
// We use a UUIDv4 for x_token so concurrent goroutines can't collide on
// the UNIQUE index — collisions cause rollbacks, which are fine for the
// stress test but pollute the signal.
func (s *stressRun) runProducer(ctx context.Context, db *sql.DB, workerID int) {
	for ctx.Err() == nil {
		if err := s.oneCommit(ctx, db, workerID); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.producerErrors.Add(1)
			// Brief backoff so a hard error (network, server gone) doesn't
			// spin the CPU.
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func (s *stressRun) oneCommit(ctx context.Context, db *sql.DB, workerID int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	xtoken := uuid.NewString()
	payload := fmt.Sprintf("worker=%d-%s", workerID, xtoken)
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table+
		" (x_token, payload, created_at, updated_at) VALUES (?, ?, NOW(6), NOW(6))",
		xtoken, payload)
	if err != nil {
		return err
	}
	var id int64
	if err = tx.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+s.table+
		" SET updated_at = NOW(6), payload = ? WHERE id = ?",
		payload+"-v2", id)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.recordCommit(id)
	return nil
}

func (s *stressRun) runStreamer(ctx context.Context, streamer *replication.BinlogStreamer) {
	for ctx.Err() == nil {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			s.t.Logf("streamer error (continuing if recoverable): %v", err)
			return
		}
		rowsEv, ok := ev.Event.(*replication.RowsEvent)
		if !ok {
			continue
		}
		// Only check INSERTs. With binlog_row_image=MINIMAL, INSERT events
		// still carry the full row image (MINIMAL only trims UPDATE/DELETE
		// images) so e.Rows[i][0] is reliably the PK regardless of variant.
		if string(rowsEv.Table.Schema) != s.schema || string(rowsEv.Table.Table) != s.table {
			continue
		}
		et := ev.Header.EventType
		if et != replication.WRITE_ROWS_EVENTv0 &&
			et != replication.WRITE_ROWS_EVENTv1 &&
			et != replication.WRITE_ROWS_EVENTv2 {
			continue
		}
		for _, row := range rowsEv.Rows {
			if len(row) == 0 {
				continue
			}
			id, ok := toInt64(row[0])
			if !ok {
				s.t.Logf("unexpected PK type %T in row event", row[0])
				continue
			}
			s.eventsObserved.Add(1)
			s.verifyVisible(ctx, id, ev.Header.LogPos)
		}
	}
}

// verifyVisible runs `SELECT id FROM <table> WHERE id = ?` on the verifier
// DB pool — guaranteed to be a different connection from the producer
// that committed this row. With binlog_order_commits=ON the row MUST be
// visible at this point per the documented MySQL contract.
//
// On miss, retry once after 50ms to distinguish a permanent miss
// (still gone — strong evidence of the visibility violation) from a
// delayed-visibility miss (visible after a brief wait — softer signal,
// but still a violation of the documented contract).
func (s *stressRun) verifyVisible(ctx context.Context, id int64, binlogPos uint32) {
	s.checks.Add(1)

	visible, err := s.selectByID(ctx, id)
	if err != nil {
		if ctx.Err() == nil {
			s.t.Logf("verifier query error for id=%d: %v", id, err)
		}
		return
	}
	if visible {
		if s.verbose {
			s.t.Logf("OK id=%d binlog_pos=%d", id, binlogPos)
		}
		return
	}

	s.missesInitial.Add(1)
	// Sanity check: did our own producer think this commit succeeded? If
	// not, the streamer somehow saw a row event for an id our producers
	// don't recall committing — that would point at a streamer bookkeeping
	// bug in the test rather than the visibility race.
	committedFromProducer := s.wasCommitted(id)

	time.Sleep(50 * time.Millisecond)
	visibleAfterRetry, retryErr := s.selectByID(ctx, id)
	switch {
	case retryErr != nil:
		s.t.Logf("MISSING id=%d binlog_pos=%d producer_recorded=%v "+
			"retry_error=%v",
			id, binlogPos, committedFromProducer, retryErr)
	case !visibleAfterRetry:
		s.missesPermanent.Add(1)
		s.t.Logf("MISSING (permanent) id=%d binlog_pos=%d "+
			"producer_recorded=%v — row event delivered but row not "+
			"visible to fresh SELECT even after 50ms",
			id, binlogPos, committedFromProducer)
	default:
		s.t.Logf("MISSING (delayed) id=%d binlog_pos=%d "+
			"producer_recorded=%v — row event delivered but row only "+
			"became visible to fresh SELECT after 50ms (still a "+
			"binlog_order_commits=ON violation)",
			id, binlogPos, committedFromProducer)
	}
}

func (s *stressRun) selectByID(ctx context.Context, id int64) (bool, error) {
	var got int64
	err := s.verifier.QueryRowContext(ctx,
		"SELECT id FROM "+s.table+" WHERE id = ?", id).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == id, nil
}

// checkServerSettings logs (and where strictly required, enforces)
// MySQL settings that affect interpretation of the test outcome.
//
// We INTENTIONALLY do not enforce binlog_order_commits=ON here. The
// premise of the stress test is exactly to probe whether MySQL honours
// that setting, and a value of OFF is a useful sanity-check baseline
// (the test should fail reliably with OFF, confirming the harness can
// detect the race). We do log the value so failures are interpretable.
func checkServerSettings(t *testing.T, db *sql.DB) {
	t.Helper()
	var binlogFormat, binlogRowImage, logBin, gtidMode, binlogOrderCommits string
	err := db.QueryRowContext(t.Context(), `SELECT
		@@global.binlog_format,
		@@global.binlog_row_image,
		@@global.log_bin,
		@@global.gtid_mode,
		@@global.binlog_order_commits`).Scan(
		&binlogFormat, &binlogRowImage, &logBin, &gtidMode, &binlogOrderCommits)
	require.NoError(t, err, "read server settings")

	t.Logf("server settings: binlog_format=%s binlog_row_image=%s "+
		"log_bin=%s gtid_mode=%s binlog_order_commits=%s",
		binlogFormat, binlogRowImage, logBin, gtidMode, binlogOrderCommits)

	// log_bin must be ON, otherwise the streamer has nothing to read.
	if !strings.EqualFold(logBin, "ON") && logBin != "1" {
		t.Skipf("log_bin is %q; binlog visibility stress test requires "+
			"log_bin=ON", logBin)
	}
	if !strings.EqualFold(binlogFormat, "ROW") {
		t.Skipf("binlog_format is %q; this test requires ROW", binlogFormat)
	}
}

func getCurrentBinlogPosition(ctx context.Context, db *sql.DB) (gomysql.Position, error) {
	// Prefer SHOW BINARY LOG STATUS (MySQL 8.4+), falling back to SHOW
	// MASTER STATUS (8.0). Both return a row with File/Position as the
	// first two columns — but the row also carries Binlog_Do_DB,
	// Binlog_Ignore_DB, and (8.0+) Executed_Gtid_Set. We only need the
	// first two, so scan into a generic []sql.NullString.
	for _, stmt := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		rows, err := db.QueryContext(ctx, stmt)
		if err != nil {
			continue
		}
		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			continue
		}
		if !rows.Next() {
			_ = rows.Close()
			continue
		}
		dest := make([]any, len(cols))
		for i := range dest {
			dest[i] = new(sql.NullString)
		}
		if err := rows.Scan(dest...); err != nil {
			_ = rows.Close()
			return gomysql.Position{}, err
		}
		_ = rows.Close()
		var file, pos string
		for i, c := range cols {
			v := dest[i].(*sql.NullString).String
			switch strings.ToLower(c) {
			case "file":
				file = v
			case "position":
				pos = v
			}
		}
		if file == "" || pos == "" {
			continue
		}
		p, err := strconv.ParseUint(pos, 10, 32)
		if err != nil {
			return gomysql.Position{}, err
		}
		return gomysql.Position{Name: file, Pos: uint32(p)}, nil
	}
	return gomysql.Position{}, errors.New("could not read current binlog position")
}

// newServerID generates a unique 32-bit server ID for the binlog
// connection, modelled on pkg/repl.NewServerID. We don't import that
// helper because this package deliberately depends on no spirit
// machinery beyond test plumbing.
func newServerID() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint32(time.Now().UnixNano()&0xFFFFFFFF) | 1001
	}
	v := binary.BigEndian.Uint32(b[:])
	return (v % (^uint32(0) - 1000)) + 1001
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	case int:
		return int64(n), true
	}
	return 0, false
}

func envDuration(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	require.NoError(t, err, "parse %s=%q", key, v)
	return d
}

func envInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	require.NoError(t, err, "parse %s=%q", key, v)
	return n
}
