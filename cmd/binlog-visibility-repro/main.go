// binlog-visibility-repro is a minimal stand-alone reproducer for an
// apparent MySQL contract violation: with binlog_order_commits=ON, a row
// event observable to a binlog replication client is sometimes NOT yet
// visible to a fresh autocommit SELECT on a separate connection.
//
// MySQL contract (per the docs for binlog_order_commits, default ON):
//
//	"transactions commit to InnoDB in the same order as their writes
//	 to the binary log"
//
// In particular, by the time a binlog row event is deliverable to a
// replication client, the corresponding InnoDB commit has finished and
// the row is visible to all subsequent read views. A fresh autocommit
// SELECT on any other connection MUST see the row.
//
// This program tests that contract under concurrent commit load. It:
//
//  1. Connects to MySQL, creates a small InnoDB table.
//  2. Spawns N producer goroutines that each loop
//     BEGIN; INSERT; UPDATE; COMMIT  on the test table.
//  3. Opens a binlog replication connection (go-mysql-org/go-mysql) and
//     for every WriteRowsEvent on the test table, immediately runs
//     "SELECT id FROM <table> WHERE id = ?" on a separate connection.
//  4. Stops on the first miss, sleeps -recheck-delay (default 100 ms),
//     re-runs the SELECT and classifies the miss as *permanent* (still
//     not visible) or *delayed* (now visible). Both are violations of
//     the contract; "delayed" is the soft form, "permanent" the hard
//     form.
//  5. Prints a summary and exits 1 (miss seen) / 0 (no miss in
//     -max-duration) / 2 (test infrastructure problem).
//
// Producer shape matters. On Docker MySQL 8.0.45 (no -race), short
// runs (5s × ~16k commits) consistently surface the race only with the
// INSERT+UPDATE-in-same-transaction shape:
//
//   - autocommit `INSERT … VALUES (…)`:    no reproduction observed
//   - `BEGIN; INSERT; COMMIT;`:            no reproduction observed
//   - `BEGIN; INSERT; UPDATE; COMMIT;`:    reproduces (probabilistic)
//
// Single-statement transactions, with or without explicit BEGIN/COMMIT,
// do not reproduce in the same time budget. The minimum producer shape
// that triggers the race is therefore a multi-statement transaction
// that INSERTs and then UPDATEs the same row before COMMIT. The repro
// is still probabilistic — expect "no misses" runs even with the
// INSERT+UPDATE shape.
//
// Build:    go build -o /tmp/repro ./cmd/binlog-visibility-repro
// Run:      /tmp/repro -dsn 'user:pass@tcp(host:port)/' -max-duration 5m
//
// The default DSN matches spirit's docker-compose dev image so the
// binary can be run directly inside spirit's tree for development.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func main() {
	os.Exit(realMain())
}

// realMain owns all setup and cleanup. It returns the exit code so that
// main can call os.Exit *after* all of realMain's deferred cleanup runs
// (otherwise os.Exit short-circuits the defers — gocritic/exitAfterDefer).
func realMain() int {
	dsn := flag.String("dsn", "tsandbox:msandbox@tcp(127.0.0.1:8033)/", "MySQL DSN (no database; the program creates and drops its own)")
	maxDuration := flag.Duration("max-duration", 5*time.Minute, "give up if no miss is seen within this many seconds")
	recheckDelay := flag.Duration("recheck-delay", 100*time.Millisecond, "after the first miss, wait this long before re-checking the row's visibility")
	producers := flag.Int("producers", 8, "concurrent producer goroutines")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg, err := mysqldriver.ParseDSN(*dsn)
	must(err, "parse dsn")
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	must(err, "split addr %q", cfg.Addr)
	port, err := strconv.ParseUint(portStr, 10, 16)
	must(err, "parse port")

	rootDB, err := sql.Open("mysql", *dsn)
	must(err, "open root db")
	defer closeAndLog("rootDB", rootDB)

	logServerSettings(ctx, rootDB)

	dbName := fmt.Sprintf("binlog_visibility_repro_%d", time.Now().UnixNano())
	_, err = rootDB.ExecContext(ctx, "CREATE DATABASE "+dbName)
	must(err, "create database")
	defer func() {
		// Use a fresh background context for cleanup so we still drop the
		// database if the main context was cancelled (e.g. ^C, timeout).
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := rootDB.ExecContext(dropCtx, "DROP DATABASE IF EXISTS "+dbName); err != nil {
			log.Printf("cleanup: drop database: %v", err)
		}
	}()

	scopedDSN := scopeDSN(cfg, dbName)
	scopedDB, err := sql.Open("mysql", scopedDSN)
	must(err, "open scoped db")
	defer closeAndLog("scopedDB", scopedDB)

	const table = "stress_repro"
	_, err = scopedDB.ExecContext(ctx, `
		CREATE TABLE `+table+` (
			id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL
		) ENGINE=InnoDB`)
	must(err, "create table")

	verifierDB, err := sql.Open("mysql", scopedDSN)
	must(err, "open verifier db")
	defer closeAndLog("verifierDB", verifierDB)
	verifierDB.SetMaxOpenConns(8)
	verifierDB.SetMaxIdleConns(8)

	startPos, err := currentBinlogPosition(ctx, rootDB)
	must(err, "read binlog position")

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: randServerID(),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     cfg.User,
		Password: cfg.Passwd,
	})
	defer syncer.Close()
	streamer, err := syncer.StartSync(startPos)
	must(err, "start binlog sync")

	// runCtx is cancelled either by the max-duration timer or by the
	// verifier when it detects the first miss. Producers and the streamer
	// derive from it, so a miss shuts everything down quickly.
	runCtx, cancelRun := context.WithTimeout(ctx, *maxDuration)
	defer cancelRun()

	r := &run{
		schema:       dbName,
		table:        table,
		verifier:     verifierDB,
		recheckDelay: *recheckDelay,
		stop:         cancelRun,
	}

	var streamWG sync.WaitGroup
	streamWG.Add(1)
	go func() {
		defer streamWG.Done()
		r.runStreamer(runCtx, streamer)
	}()

	var prodWG sync.WaitGroup
	for i := 0; i < *producers; i++ {
		prodWG.Add(1)
		go func() {
			defer prodWG.Done()
			r.runProducer(runCtx, scopedDB)
		}()
	}
	prodWG.Wait()
	streamWG.Wait()

	fmt.Printf("\nSUMMARY: producers=%d commits=%d events_observed=%d "+
		"verifier_checks=%d producer_errors=%d\n",
		*producers,
		r.commits.Load(), r.eventsObserved.Load(),
		r.checks.Load(), r.producerErrors.Load())

	switch {
	case r.firstMissPermanent.Load():
		fmt.Println("\nFAIL: binlog_order_commits=ON contract was violated.")
		fmt.Println("Row event was observed in the binlog stream but the corresponding row")
		fmt.Printf("was NOT visible to a fresh SELECT even after %s.\n", *recheckDelay)
		return 1
	case r.firstMissDelayed.Load():
		fmt.Println("\nFAIL: binlog_order_commits=ON contract was violated.")
		fmt.Println("Row event was observed in the binlog stream but the corresponding row")
		fmt.Printf("only became visible to a fresh SELECT after %s. The visibility lag is\n", *recheckDelay)
		fmt.Println("transient but it is still a contract violation.")
		return 1
	case r.commits.Load() == 0 || r.eventsObserved.Load() == 0:
		fmt.Println("\nINCONCLUSIVE: no commits or no events observed — check connectivity / log_bin")
		return 2
	default:
		fmt.Printf("\nPASS: no visibility violations observed within %s.\n", *maxDuration)
		return 0
	}
}

type run struct {
	schema       string
	table        string
	verifier     *sql.DB
	recheckDelay time.Duration

	// stop is invoked once, by the verifier, after the first miss is
	// classified. Cancelling it tears down all goroutines so realMain can
	// print the summary and exit.
	stop context.CancelFunc
	// firstMissSeen guarantees the "first miss" handling runs exactly
	// once even if multiple verifier goroutines race to a miss.
	firstMissSeen atomic.Bool
	// firstMissPermanent / firstMissDelayed describe the outcome of the
	// post-miss recheck. Exactly one is true when a miss has been seen.
	firstMissPermanent atomic.Bool
	firstMissDelayed   atomic.Bool

	commits        atomic.Int64
	producerErrors atomic.Int64
	eventsObserved atomic.Int64
	checks         atomic.Int64
}

func (r *run) runProducer(ctx context.Context, db *sql.DB) {
	for ctx.Err() == nil {
		if err := r.oneCommit(ctx, db); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.producerErrors.Add(1)
			time.Sleep(2 * time.Millisecond)
		}
	}
}

// oneCommit runs the minimum producer shape that surfaces the race —
// see the package-level doc-comment for the bisect.
func (r *run) oneCommit(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO "+r.table+" (payload) VALUES ('x')"); err != nil {
		return err
	}
	var id int64
	if err := tx.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE "+r.table+" SET payload = 'x2' WHERE id = ?", id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.commits.Add(1)
	return nil
}

func (r *run) runStreamer(ctx context.Context, streamer *replication.BinlogStreamer) {
	for ctx.Err() == nil {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			log.Printf("streamer error: %v", err)
			return
		}
		rowsEv, ok := ev.Event.(*replication.RowsEvent)
		if !ok {
			continue
		}
		if string(rowsEv.Table.Schema) != r.schema || string(rowsEv.Table.Table) != r.table {
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
				continue
			}
			r.eventsObserved.Add(1)
			r.verifyVisible(ctx, id, ev.Header.LogPos)
		}
	}
}

// verifyVisible is called by the streamer for every WriteRowsEvent on
// the test table. On the very first miss it classifies it (permanent vs
// delayed) and then signals the rest of the program to shut down.
// Subsequent verifier calls become no-ops once a miss has been seen.
func (r *run) verifyVisible(ctx context.Context, id int64, binlogPos uint32) {
	if r.firstMissSeen.Load() {
		return
	}
	r.checks.Add(1)
	visible, err := r.selectByID(ctx, id)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("verifier query error id=%d: %v", id, err)
		}
		return
	}
	if visible {
		return
	}
	// First miss wins. Classify it, then shut everything down via r.stop.
	if !r.firstMissSeen.CompareAndSwap(false, true) {
		return
	}
	defer r.stop()

	log.Printf("first miss observed: id=%d binlog_pos=%d — row event was "+
		"delivered by the binlog streamer but a fresh SELECT does not see "+
		"the row. Re-checking after %s …",
		id, binlogPos, r.recheckDelay)

	// Use a fresh background context for the re-check so the recheck
	// itself doesn't get cancelled by r.stop().
	recheckCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	time.Sleep(r.recheckDelay)
	visibleAfter, err := r.selectByID(recheckCtx, id)
	switch {
	case err != nil:
		log.Printf("recheck error id=%d: %v", id, err)
		// Treat as permanent so the test still surfaces a problem.
		r.firstMissPermanent.Store(true)
	case !visibleAfter:
		r.firstMissPermanent.Store(true)
		log.Printf("MISSING (permanent) id=%d binlog_pos=%d — row event "+
			"observed but row still not visible to a fresh SELECT after %s",
			id, binlogPos, r.recheckDelay)
	default:
		r.firstMissDelayed.Store(true)
		log.Printf("MISSING (delayed) id=%d binlog_pos=%d — row event "+
			"observed but row only became visible after %s",
			id, binlogPos, r.recheckDelay)
	}
}

func (r *run) selectByID(ctx context.Context, id int64) (bool, error) {
	var got int64
	err := r.verifier.QueryRowContext(ctx,
		"SELECT id FROM "+r.table+" WHERE id = ?", id).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == id, nil
}

func currentBinlogPosition(ctx context.Context, db *sql.DB) (gomysql.Position, error) {
	// MySQL 8.4 deprecated SHOW MASTER STATUS in favour of SHOW BINARY LOG
	// STATUS. Try the new name first, fall back to the old name.
	for _, stmt := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		pos, err := readBinlogPositionRow(ctx, db, stmt)
		if err == nil {
			return pos, nil
		}
	}
	return gomysql.Position{}, errors.New("could not read current binlog position")
}

func readBinlogPositionRow(ctx context.Context, db *sql.DB, stmt string) (gomysql.Position, error) {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return gomysql.Position{}, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return gomysql.Position{}, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return gomysql.Position{}, err
		}
		return gomysql.Position{}, errors.New("no rows from " + stmt)
	}
	dest := make([]any, len(cols))
	for i := range dest {
		dest[i] = new(sql.NullString)
	}
	if err := rows.Scan(dest...); err != nil {
		return gomysql.Position{}, err
	}
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
		return gomysql.Position{}, fmt.Errorf("%s: missing File/Position columns", stmt)
	}
	p, err := strconv.ParseUint(pos, 10, 32)
	if err != nil {
		return gomysql.Position{}, err
	}
	return gomysql.Position{Name: file, Pos: uint32(p)}, nil
}

func logServerSettings(ctx context.Context, db *sql.DB) {
	q := func(name string) string {
		var v string
		if err := db.QueryRowContext(ctx, "SELECT @@global."+name).Scan(&v); err != nil {
			return "?"
		}
		return v
	}
	fmt.Printf("server: version=%s binlog_format=%s binlog_row_image=%s log_bin=%s "+
		"gtid_mode=%s binlog_order_commits=%s sync_binlog=%s "+
		"innodb_flush_log_at_trx_commit=%s\n",
		q("version"), q("binlog_format"), q("binlog_row_image"), q("log_bin"),
		q("gtid_mode"), q("binlog_order_commits"), q("sync_binlog"),
		q("innodb_flush_log_at_trx_commit"))
}

// closeAndLog closes c and logs (but does not propagate) any error. Used
// for *sql.DB cleanup defers where the only failure mode is a connection
// that's already gone.
func closeAndLog(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("close %s: %v", name, err)
	}
}

func scopeDSN(cfg *mysqldriver.Config, dbName string) string {
	c := *cfg
	c.DBName = dbName
	return c.FormatDSN()
}

func randServerID() uint32 {
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

func must(err error, format string, args ...any) {
	if err != nil {
		log.Fatalf("%s: %v", fmt.Sprintf(format, args...), err)
	}
}
