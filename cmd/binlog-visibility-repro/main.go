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
//  4. Retries any miss once after 50 ms (to distinguish *permanent*
//     misses from *delayed-visibility* misses; per the contract, both
//     are violations).
//  5. Prints a summary; exits 1 if any miss was observed.
//
// Bisect summary (the multi-statement transaction matters):
//   - autocommit INSERT only:        does NOT reproduce in 5s × 5 runs
//   - BEGIN; INSERT; COMMIT:         does NOT reproduce in 5s × 5 runs
//   - BEGIN; INSERT; UPDATE; COMMIT: reproduces ~2/3 of 5s runs
//
// We therefore keep the INSERT+UPDATE-in-same-transaction shape as the
// minimum that surfaces the race.
//
// Build:    go build -o /tmp/repro ./cmd/binlog-visibility-repro
// Run:      /tmp/repro -dsn 'user:pass@tcp(host:port)/' -duration 30s
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
	"log"
	"net"
	"os"
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
	dsn := flag.String("dsn", "tsandbox:msandbox@tcp(127.0.0.1:8033)/", "MySQL DSN (no database; the program creates and drops its own)")
	durationFlag := flag.Duration("duration", 30*time.Second, "how long to run the producers")
	producers := flag.Int("producers", 8, "concurrent producer goroutines")
	flag.Parse()

	cfg, err := mysqldriver.ParseDSN(*dsn)
	must(err, "parse dsn")
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	must(err, "split addr %q", cfg.Addr)
	port, err := strconv.ParseUint(portStr, 10, 16)
	must(err, "parse port")

	rootDB, err := sql.Open("mysql", *dsn)
	must(err, "open root db")
	defer rootDB.Close()

	logServerSettings(rootDB)

	dbName := fmt.Sprintf("binlog_visibility_repro_%d", time.Now().UnixNano())
	_, err = rootDB.Exec("CREATE DATABASE " + dbName)
	must(err, "create database")
	defer func() {
		if _, err := rootDB.Exec("DROP DATABASE IF EXISTS " + dbName); err != nil {
			log.Printf("cleanup: drop database: %v", err)
		}
	}()

	scopedDSN := scopeDSN(cfg, dbName)
	scopedDB, err := sql.Open("mysql", scopedDSN)
	must(err, "open scoped db")
	defer scopedDB.Close()

	const table = "stress_repro"
	_, err = scopedDB.Exec(`
		CREATE TABLE ` + table + ` (
			id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL
		) ENGINE=InnoDB`)
	must(err, "create table")

	verifierDB, err := sql.Open("mysql", scopedDSN)
	must(err, "open verifier db")
	defer verifierDB.Close()
	verifierDB.SetMaxOpenConns(8)
	verifierDB.SetMaxIdleConns(8)

	startPos, err := currentBinlogPosition(rootDB)
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

	r := &run{
		schema:   dbName,
		table:    table,
		verifier: verifierDB,
	}

	streamCtx, cancelStream := context.WithCancel(context.Background())
	var streamWG sync.WaitGroup
	streamWG.Add(1)
	go func() {
		defer streamWG.Done()
		r.runStreamer(streamCtx, streamer)
	}()

	prodCtx, cancelProd := context.WithTimeout(context.Background(), *durationFlag)
	defer cancelProd()
	var prodWG sync.WaitGroup
	for i := 0; i < *producers; i++ {
		prodWG.Add(1)
		go func() {
			defer prodWG.Done()
			r.runProducer(prodCtx, scopedDB)
		}()
	}
	prodWG.Wait()

	// Drain trailing events.
	time.Sleep(500 * time.Millisecond)
	cancelStream()
	streamWG.Wait()

	fmt.Printf("\nSUMMARY: producers=%d duration=%s commits=%d events_observed=%d "+
		"verifier_checks=%d misses_initial=%d misses_permanent=%d producer_errors=%d\n",
		*producers, *durationFlag,
		r.commits.Load(), r.eventsObserved.Load(),
		r.checks.Load(), r.missesInitial.Load(),
		r.missesPermanent.Load(), r.producerErrors.Load())

	if r.missesInitial.Load() > 0 {
		fmt.Println("\nFAIL: binlog_order_commits=ON contract was violated.")
		fmt.Println("A row event was observed in the binlog stream but the corresponding")
		fmt.Println("row was NOT visible to a fresh autocommit SELECT on a separate connection.")
		os.Exit(1)
	}
	if r.commits.Load() == 0 || r.eventsObserved.Load() == 0 {
		fmt.Println("\nFAIL: no commits or no events observed — check connectivity / log_bin")
		os.Exit(2)
	}
	fmt.Println("\nPASS: no visibility violations observed.")
}

type run struct {
	schema   string
	table    string
	verifier *sql.DB

	commits         atomic.Int64
	producerErrors  atomic.Int64
	eventsObserved  atomic.Int64
	checks          atomic.Int64
	missesInitial   atomic.Int64
	missesPermanent atomic.Int64
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

func (r *run) oneCommit(ctx context.Context, db *sql.DB) error {
	// Bisect summary (Docker MySQL 8.0.45, no -race, ~16k commits in 5s):
	//   - autocommit INSERT only:               5/5 PASS
	//   - BEGIN; INSERT; COMMIT:                5/5 PASS
	//   - BEGIN; INSERT; UPDATE; COMMIT:        2/3 FAIL  <-- minimal repro
	//
	// The repro requires a multi-statement transaction that both INSERTs
	// AND UPDATEs the same row before COMMIT. Single-statement transactions
	// (with or without explicit BEGIN/COMMIT) do not reproduce the
	// visibility race in the same time budget.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
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

func (r *run) verifyVisible(ctx context.Context, id int64, binlogPos uint32) {
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
	r.missesInitial.Add(1)
	time.Sleep(50 * time.Millisecond)
	visibleAfter, err := r.selectByID(ctx, id)
	switch {
	case err != nil:
		log.Printf("MISSING id=%d binlog_pos=%d retry_error=%v", id, binlogPos, err)
	case !visibleAfter:
		r.missesPermanent.Add(1)
		log.Printf("MISSING (permanent) id=%d binlog_pos=%d — row event observed "+
			"but row not visible to fresh SELECT even after 50ms",
			id, binlogPos)
	default:
		log.Printf("MISSING (delayed) id=%d binlog_pos=%d — row event observed "+
			"but row only became visible after 50ms",
			id, binlogPos)
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

func currentBinlogPosition(db *sql.DB) (gomysql.Position, error) {
	for _, stmt := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		rows, err := db.Query(stmt)
		if err != nil {
			continue
		}
		cols, _ := rows.Columns()
		if !rows.Next() {
			rows.Close()
			continue
		}
		dest := make([]any, len(cols))
		for i := range dest {
			dest[i] = new(sql.NullString)
		}
		if err := rows.Scan(dest...); err != nil {
			rows.Close()
			return gomysql.Position{}, err
		}
		rows.Close()
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

func logServerSettings(db *sql.DB) {
	var v string
	q := func(name string) string {
		_ = db.QueryRow("SELECT @@global." + name).Scan(&v)
		return v
	}
	fmt.Printf("server: version=%s binlog_format=%s binlog_row_image=%s log_bin=%s "+
		"gtid_mode=%s binlog_order_commits=%s sync_binlog=%s "+
		"innodb_flush_log_at_trx_commit=%s\n",
		q("version"), q("binlog_format"), q("binlog_row_image"), q("log_bin"),
		q("gtid_mode"), q("binlog_order_commits"), q("sync_binlog"),
		q("innodb_flush_log_at_trx_commit"))
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
