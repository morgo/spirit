package change

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestSyncerConfigDecodeOptions verifies that both change sources pin the
// row-decode options that keep replayed values byte-identical to what the
// server stored: TIMESTAMPs must be rendered in UTC (the applier writes
// over time_zone='+00:00' connections, so a process-local-time string
// would shift stored values on any non-UTC host) and JSON must be
// rendered as MySQL text (see the rationale in binlog.go). The GTID
// client previously omitted TimestampStringLocation, silently corrupting
// TIMESTAMP columns under --gtid on non-UTC hosts.
func TestSyncerConfigDecodeOptions(t *testing.T) {
	configs := map[string]replication.BinlogSyncerConfig{
		"binlog": (&binlogClient{serverID: 123}).buildSyncerConfig("127.0.0.1", 3306),
		"gtid":   (&gtidClient{serverID: 123}).buildSyncerConfig("127.0.0.1", 3306),
	}
	for name, cfg := range configs {
		require.Equal(t, time.UTC, cfg.TimestampStringLocation, "%s client must decode TIMESTAMP values in UTC", name)
		require.True(t, cfg.RenderJSONAsMySQLText, "%s client must render JSON as MySQL text", name)
	}
}

// TestGTIDClient mirrors TestReplClient but uses the GTID-backed change
// source. Verifies the basic INSERT → buffer → flush loop end-to-end.
func TestGTIDClient(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidreplt1, gtidreplt2")
	testutils.RunSQL(t, "CREATE TABLE gtidreplt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidreplt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidreplt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidreplt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	require.NotEmpty(t, client.Position(), "fresh Start should record a non-empty GTID position")

	testutils.RunSQL(t, "INSERT INTO gtidreplt1 (a, b, c) VALUES (1, 2, 3)")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidreplt2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestGTIDClientTransactionCompression verifies that transactions written
// with binlog_transaction_compression=ON are decoded and buffered rather
// than silently dropped. This matters twice over for the GTID client: the
// row events carry the deltas, and the XIDEvent that promotes the pending
// GTID into the resume set arrives *inside* the compressed payload (only
// the GTID event stays outside). A client that skipped payload events would
// lose the deltas AND leave bufferedGTID permanently behind gtid_executed,
// so the BlockWait calls below would time out.
func TestGTIDClientTransactionCompression(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidcompresst1, gtidcompresst2")
	testutils.RunSQL(t, "CREATE TABLE gtidcompresst1 (a INT NOT NULL, b INT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidcompresst2 (a INT NOT NULL, b INT, c VARCHAR(255), PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidcompresst1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidcompresst2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	conn := compressedConn(t, db)

	// Even a single-row autocommit INSERT is wrapped in a payload event.
	_, err = conn.ExecContext(t.Context(), "INSERT INTO gtidcompresst1 (a, b, c) VALUES (1, 2, 'compressed')")
	require.NoError(t, err)
	require.NoError(t, client.BlockWait(t.Context()), "bufferedGTID must catch up to gtid_executed: the promoting XIDEvent is inside the payload")
	require.Equal(t, 1, client.GetDeltaLen(), "changes from a compressed transaction must be buffered, not dropped")

	// A multi-statement transaction produces one payload containing several
	// TableMap/RowsEvents plus the XID terminator.
	tx, err := conn.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "INSERT INTO gtidcompresst1 (a, b, c) VALUES (2, 2, REPEAT('x', 200)), (3, 3, REPEAT('y', 200))")
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "UPDATE gtidcompresst1 SET b = 20 WHERE a = 1")
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "DELETE FROM gtidcompresst1 WHERE a = 2")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Interleave an uncompressed change from another connection to show both
	// formats coexist on the same stream.
	testutils.RunSQL(t, "INSERT INTO gtidcompresst1 (a, b, c) VALUES (4, 4, 'plain')")

	require.NoError(t, client.BlockWait(t.Context()))
	// Keys pending: 1 (insert+update), 2 (insert+delete), 3 (insert), 4 (insert).
	require.Equal(t, 4, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))
	require.True(t, client.AllChangesFlushed())

	// The new table must match: a=1 (with the in-transaction update applied),
	// a=3, a=4; a=2 was deleted in the same compressed transaction.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidcompresst2").Scan(&count))
	require.Equal(t, 3, count)
	var b int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT b FROM gtidcompresst2 WHERE a = 1").Scan(&b))
	require.Equal(t, 20, b)
}

// TestGTIDStartFromMalformedPosition verifies that StartFromPosition
// rejects an input string that is not a valid GTID set (including the
// common operator mistake of resuming a legacy file:offset checkpoint
// against the GTID client). The parse failure surfaces as
// ErrPositionNotFound so migration strict-mode treats it as a real
// "cannot resume from this checkpoint" rather than silently restarting.
//
// The gtid_purged path (server has dropped binlogs we'd need) is
// validated inside Start() against @@GLOBAL.gtid_purged and is not
// exercised here — flipping global gtid_purged on a shared test
// container would race with every other concurrent test.
func TestGTIDStartFromMalformedPosition(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidresumet1, gtidresumet2")
	testutils.RunSQL(t, "CREATE TABLE gtidresumet1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidresumet2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidresumet1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidresumet2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))

	// A non-GTID input (e.g. a legacy file:offset checkpoint) is the
	// most likely real-world trigger for this branch.
	err = client.StartFromPosition(t.Context(), "binlog.000123:4567")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPositionNotFound)
}

// TestGTIDClientUnparseableDDL is a regression test: a QueryEvent the
// TiDB parser cannot parse (CREATE TRIGGER, stored procedure bodies,
// certain ALTER USER variants, ...) must still promote the
// transaction's pending GTID into bufferedGTID. (XA statements are also
// unparseable but never reach the parser — they get explicit handling
// in readStream because promoting mid-XA-group would be incorrect; see
// TestGTIDClientXAPromotionOrdering.) Every QueryEvent on
// the entire server flows through the parser — the schema filter only
// applies after parsing — so before the fix a single unparseable
// statement in a *completely unrelated schema* left bufferedGTID
// permanently behind gtid_executed: BlockWait timed out forever, Flush
// looped indefinitely, and a GTID-mode migration was wedged until
// cancelled.
func TestGTIDClientUnparseableDDL(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidunparset1, gtidunparset2")
	testutils.RunSQL(t, "CREATE TABLE gtidunparset1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidunparset2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	// A separate schema, to prove the unparseable statement does not even
	// need to be near the migrated table: the parse happens before any
	// schema filtering.
	otherSchema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, otherSchema, "CREATE TABLE unrelated (a INT NOT NULL PRIMARY KEY)")

	t1 := table.NewTableInfo(db, "test", "gtidunparset1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidunparset2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// CREATE TRIGGER is binlogged as a QueryEvent that the TiDB parser
	// rejects (it is also recorded with a DEFINER clause, which fails the
	// parse on its own). The statement is its own server transaction with
	// its own GTID, terminated without an XIDEvent.
	testutils.RunSQLInDatabase(t, otherSchema,
		"CREATE TRIGGER gtidunparse_trg BEFORE INSERT ON unrelated FOR EACH ROW SET @gtid_unparse_test = 1")

	// A normal tracked-table write after the unparseable event.
	testutils.RunSQL(t, "INSERT INTO gtidunparset1 (a, b, c) VALUES (1, 2, 3)")

	// Before the fix this times out after DefaultTimeout (30s): the
	// trigger's GTID is in the server's gtid_executed but never enters
	// bufferedGTID, so the buffered set can never contain the target.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidunparset2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestGTIDClientNonXIDCommit is a regression test for the other dropped
// promotion path: a transaction on a non-transactional engine (MyISAM)
// is terminated by a `COMMIT` QueryEvent instead of an XIDEvent. The
// COMMIT/ROLLBACK early-out used to `continue` without promoting the
// pending GTID, leaving bufferedGTID permanently behind gtid_executed.
func TestGTIDClientNonXIDCommit(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var support string
	err = db.QueryRowContext(t.Context(), "SELECT SUPPORT FROM information_schema.ENGINES WHERE ENGINE='MyISAM'").Scan(&support)
	if err != nil || (support != "YES" && support != "DEFAULT") {
		t.Skipf("MyISAM engine not available (support=%q, err=%v); skipping the end-to-end COMMIT/ROLLBACK QueryEvent regression coverage. The promotePendingGTID helper's semantics are still unit-tested separately by TestGTIDPromotePendingGTID, but that test does not exercise the readStream wiring this test covers", support, err)
	}

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidmyisamt1, gtidmyisamt2, gtidmyisamt3")
	testutils.RunSQL(t, "CREATE TABLE gtidmyisamt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidmyisamt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	// An untracked non-transactional table: writes to it produce
	// GTIDEvent, BEGIN, rows, then a `COMMIT` QueryEvent — no XIDEvent.
	testutils.RunSQL(t, "CREATE TABLE gtidmyisamt3 (a INT NOT NULL PRIMARY KEY) ENGINE=MyISAM")
	t.Cleanup(func() {
		testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidmyisamt3")
	})

	t1 := table.NewTableInfo(db, "test", "gtidmyisamt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidmyisamt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	testutils.RunSQL(t, "INSERT INTO gtidmyisamt3 (a) VALUES (1)")
	testutils.RunSQL(t, "INSERT INTO gtidmyisamt1 (a, b, c) VALUES (1, 2, 3)")

	// Before the fix this times out: the MyISAM transaction's GTID is in
	// gtid_executed but its `COMMIT` QueryEvent never promoted it.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidmyisamt2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// xaTestXIDPrefix namespaces the XA xids used by tests in this file.
// Each run generates unique xids under it (a hard-coded xid would fail
// XA START with XAER_DUPID against a prepared transaction left dangling
// by an interrupted prior run on a shared server), and the prefix keeps
// any stragglers attributable and sweepable by prefix.
const xaTestXIDPrefix = "spirit_gtid_xa"

// rollbackDanglingXATestTxns rolls back prepared XA transactions left
// dangling by an interrupted run. A run that dies between XA PREPARE and
// XA COMMIT leaves a prepared transaction that survives disconnect and
// keeps its row and metadata locks, wedging any later statement that
// touches the same tables.
//
// knownXIDs (the calling run's own xids) are rolled back by exact name —
// that needs no special privilege and is the path that matters when a
// test fails gracefully. Stragglers from prior hard-killed runs are
// additionally swept by prefix via XA RECOVER, but XA RECOVER requires
// XA_RECOVER_ADMIN, which the tsandbox user provisioned by
// compose/bootstrap.sql does not hold — so the sweep is typically a
// logged no-op, and the per-run unique table names in
// TestGTIDClientXATransaction are what keep such stragglers from
// blocking future runs. Errors are deliberately ignored beyond logging:
// this is best-effort hygiene on a shared server (XAER_NOTA for an
// already-terminated xid is the happy path).
func rollbackDanglingXATestTxns(t *testing.T, db *sql.DB, knownXIDs ...string) {
	t.Helper()
	xids := slices.Clone(knownXIDs)
	rows, err := db.QueryContext(context.Background(), "XA RECOVER")
	if err != nil {
		t.Logf("XA RECOVER unavailable (likely missing XA_RECOVER_ADMIN); sweeping only this run's xids: %v", err)
	} else {
		func() {
			defer utils.CloseAndLog(rows)
			for rows.Next() {
				var formatID, gtridLen, bqualLen int64
				var data []byte
				if err := rows.Scan(&formatID, &gtridLen, &bqualLen, &data); err != nil {
					continue
				}
				if gtridLen <= 0 || gtridLen > int64(len(data)) {
					continue
				}
				gtrid := string(data[:gtridLen])
				if bqualLen == 0 && strings.HasPrefix(gtrid, xaTestXIDPrefix) && !slices.Contains(xids, gtrid) {
					xids = append(xids, gtrid)
				}
			}
			_ = rows.Err()
		}()
	}
	for _, xid := range xids {
		// The xids here are exclusively ones these tests generated
		// (prefix + UUID): plain ASCII, safe to single-quote back.
		if _, err := db.ExecContext(context.Background(), fmt.Sprintf("XA ROLLBACK '%s'", xid)); err == nil {
			t.Logf("rolled back dangling prepared XA transaction %q", xid)
		}
	}
}

// TestGTIDClientXATransaction drives a real two-phase XA transaction
// (plus a one-phase variant) through the feed end-to-end. The binlog
// shape, verified against MySQL 8.0: nothing is written until XA
// PREPARE, at which point the entire first group — GTIDEvent(g1),
// Query("XA START ..."), the row events, Query("XA END ...") and the
// terminating XA_PREPARE_LOG_EVENT — is flushed at once, and g1 enters
// the server's gtid_executed. The terminal XA COMMIT (or XA ROLLBACK)
// arrives later as its own single-statement transaction under its own
// GTID (g2), with no row events.
//
// The BlockWait calls double as promotion-liveness assertions: BlockWait
// only returns once bufferedGTID covers the server's gtid_executed, so a
// handler that failed to promote at the XA prepare (or at the terminal
// XA COMMIT) would time out here.
//
// Both the xids and the table names are unique per run. Unique xids
// because XA START against a hard-coded xid fails with XAER_DUPID if an
// interrupted prior run left a prepared transaction under that name.
// Unique table names because such a straggler also keeps metadata locks
// on the tables it touched until it is terminated — and it cannot even
// be discovered without XA_RECOVER_ADMIN (see
// rollbackDanglingXATestTxns) — so reusing fixed table names would let
// it block the next run's DROP TABLE indefinitely. With fresh names,
// stragglers from hard-killed runs leak in isolation instead of wedging
// future runs; graceful failures are reclaimed by the cleanup below.
func TestGTIDClientXATransaction(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Unique per run: see the doc comment. The gtrid limit is 64 bytes;
	// prefix + suffix + UUID stays well under it.
	xid := xaTestXIDPrefix + "_" + uuid.NewString()
	xid1p := xaTestXIDPrefix + "_1p_" + uuid.NewString()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	srcTable := "gtidxat1_" + suffix
	dstTable := "gtidxat2_" + suffix

	// Best-effort sweep of stragglers from prior interrupted runs (a
	// logged no-op without XA_RECOVER_ADMIN).
	rollbackDanglingXATestTxns(t, db)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", srcTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", dstTable))
	t.Cleanup(func() {
		// The test's defers (including closing db) have already run by
		// cleanup time, so use a fresh connection. Terminate this run's
		// XA transactions first — a failure between XA PREPARE and XA
		// COMMIT would otherwise leave the DROP below blocked on the
		// prepared transaction's metadata locks. The lock_wait_timeout
		// bounds the worst case so a leftover can fail the cleanup but
		// not hang the suite.
		cleanupDB, err := sql.Open("mysql", testutils.DSN())
		if err != nil {
			return
		}
		defer utils.CloseAndLog(cleanupDB)
		rollbackDanglingXATestTxns(t, cleanupDB, xid, xid1p)
		_, _ = cleanupDB.ExecContext(context.Background(), "SET SESSION lock_wait_timeout=5")
		_, _ = cleanupDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	})

	t1 := table.NewTableInfo(db, "test", srcTable)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", dstTable)
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// XA transactions are session-scoped between XA START and XA
	// PREPARE: pin a single connection for the whole two-phase dance.
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer utils.CloseAndLog(conn)
	xaExec := func(stmt string) {
		t.Helper()
		_, err := conn.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}

	xaExec(fmt.Sprintf("XA START '%s'", xid))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (1, 2, 3)", srcTable))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (2, 3, 4)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid))

	// Before XA PREPARE nothing of the transaction exists in the binary
	// log — no GTID has been assigned yet, so it cannot be in the
	// buffered set, and no row events can have streamed.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen(), "row events must not stream before XA PREPARE")

	xaExec(fmt.Sprintf("XA PREPARE '%s'", xid))

	// The prepare flushes the whole group and terminates it.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 2, client.GetDeltaLen(), "both row events must be buffered once XA PREPARE flushes the group")

	// The buffered images can flush before the XA COMMIT ever happens.
	// (Known pre-existing property, shared with the binlog client: row
	// images are applied from the prepare-time group, so a later XA
	// ROLLBACK of the prepared transaction would not be compensated.)
	require.NoError(t, client.Flush(t.Context()))
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	xaExec(fmt.Sprintf("XA COMMIT '%s'", xid))

	// Capture gtid_executed right after the commit: it necessarily
	// includes both the prepare-group GTID (g1) and the commit GTID (g2).
	// Captured before BlockWait/Flush so the containment assertion below
	// is deterministic even with unrelated concurrent load advancing
	// gtid_executed on a shared server.
	var executed string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_executed").Scan(&executed))
	executedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(executed))
	require.NoError(t, err)

	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	// The resume coordinate must cover both XA GTIDs: resuming from it
	// must not re-request (or worse, skip) any part of the XA transaction.
	flushedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(client.Position()))
	require.NoError(t, err)
	require.True(t, flushedSet.Contain(executedSet),
		"flushed position %s must cover the executed set %s (both XA GTIDs)", flushedSet.String(), executedSet.String())

	// One-phase variant: `XA COMMIT ... ONE PHASE` is a single group
	// terminated by an XA_PREPARE_LOG_EVENT rather than a QueryEvent.
	xaExec(fmt.Sprintf("XA START '%s'", xid1p))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (3, 4, 5)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid1p))
	xaExec(fmt.Sprintf("XA COMMIT '%s' ONE PHASE", xid1p))

	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

// TestGTIDClientXATransactionCompression re-runs the XA two-phase dance
// with binlog_transaction_compression=ON on the XA session. The entire XA
// first group — Query("XA START"), row events, Query("XA END") and the
// terminating XA_PREPARE_LOG_EVENT — arrives inside one compressed
// Transaction_payload event (verified against MySQL 8.0.43); only the
// terminal XA COMMIT QueryEvent stays outside, under its own GTID. The
// BlockWait after the prepare is the promotion-liveness assertion: it only
// returns if processTransactionPayload promoted the pending GTID at the
// inner XA_PREPARE_LOG_EVENT, and the delta count proves the inner row
// events were buffered rather than dropped.
//
// Unique xids and per-run table names for the same reasons documented on
// TestGTIDClientXATransaction.
func TestGTIDClientXATransactionCompression(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	xid := xaTestXIDPrefix + "_comp_" + uuid.NewString()
	xid1p := xaTestXIDPrefix + "_comp1p_" + uuid.NewString()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	srcTable := "gtidxacompt1_" + suffix
	dstTable := "gtidxacompt2_" + suffix

	// Best-effort sweep of stragglers from prior interrupted runs (a
	// logged no-op without XA_RECOVER_ADMIN).
	rollbackDanglingXATestTxns(t, db)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", srcTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", dstTable))
	t.Cleanup(func() {
		// See TestGTIDClientXATransaction: terminate this run's XA
		// transactions before dropping the tables, from a fresh connection.
		cleanupDB, err := sql.Open("mysql", testutils.DSN())
		if err != nil {
			return
		}
		defer utils.CloseAndLog(cleanupDB)
		rollbackDanglingXATestTxns(t, cleanupDB, xid, xid1p)
		_, _ = cleanupDB.ExecContext(context.Background(), "SET SESSION lock_wait_timeout=5")
		_, _ = cleanupDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	})

	t1 := table.NewTableInfo(db, "test", srcTable)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", dstTable)
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// XA transactions are session-scoped between XA START and XA PREPARE;
	// the compression-enabled conn pins one session for the whole dance.
	conn := compressedConn(t, db)
	xaExec := func(stmt string) {
		t.Helper()
		_, err := conn.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}

	xaExec(fmt.Sprintf("XA START '%s'", xid))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (1, 2, 3)", srcTable))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (2, 3, 4)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid))
	xaExec(fmt.Sprintf("XA PREPARE '%s'", xid))

	// The prepare flushes the whole group as one compressed payload. A
	// handler that dropped the payload (or failed to promote at the inner
	// XA_PREPARE_LOG_EVENT) would time out here.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 2, client.GetDeltaLen(), "row events inside the compressed XA group must be buffered")

	xaExec(fmt.Sprintf("XA COMMIT '%s'", xid))
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count))
	require.Equal(t, 2, count)

	// One-phase variant: a single compressed group, likewise terminated by
	// an XA_PREPARE_LOG_EVENT.
	xaExec(fmt.Sprintf("XA START '%s'", xid1p))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (3, 4, 5)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid1p))
	xaExec(fmt.Sprintf("XA COMMIT '%s' ONE PHASE", xid1p))

	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count))
	require.Equal(t, 3, count)
}

// TestGTIDClientXAPromotionOrdering is the deterministic regression test
// for the premature-promotion bug: the "XA START" QueryEvent used to
// fall through to the parser path (the TiDB parser cannot parse XA
// syntax) and promote the pending GTID before the transaction's row
// events had been buffered. A flush in that window published a resume
// coordinate that already covered the transaction, so a crash before
// the next flush resumed past it and silently lost its rows.
//
// Events are injected through a synthetic go-mysql BinlogStreamer
// rather than a real server because the server writes an XA
// transaction's entire first group to the binlog in one burst at XA
// PREPARE time (see TestGTIDClientXATransaction): wall-clock timing
// cannot reliably observe the stream state between the "XA START"
// QueryEvent and the XA_PREPARE_LOG_EVENT of the same burst. Row events
// for a subscribed table are used as ordering barriers: events are
// consumed strictly in order, so once GetDeltaLen reflects a row event,
// every event injected before it has been processed.
func TestGTIDClientXAPromotionOrdering(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Real tables so TableInfo has genuine column/PK metadata; the event
	// stream itself is synthetic and never touches the server.
	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidxasyn1, gtidxasyn2")
	testutils.RunSQL(t, "CREATE TABLE gtidxasyn1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidxasyn2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidxasyn1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidxasyn2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))

	// Wire readStream to a synthetic streamer instead of client.Start().
	empty, err := mysql.ParseMysqlGTIDSet("")
	require.NoError(t, err)
	streamer := replication.NewBinlogStreamer()
	ctx, cancel := context.WithCancel(t.Context())
	client.streamer = streamer
	client.bufferedGTID = empty
	client.flushedGTID = empty.Clone()
	client.cancelFunc = cancel
	client.streamWG.Add(1)
	go client.readStream(ctx)
	defer client.Close()

	const sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	sidUUID := uuid.MustParse(sid)
	gtidEvent := func(gno int64) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{SID: sidUUID[:], GNO: gno},
		}
	}
	queryEvent := func(q string) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
			Event:  &replication.QueryEvent{Schema: []byte("test"), Query: []byte(q)},
		}
	}
	rowEvent := func(pk int32) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2},
			Event: &replication.RowsEvent{
				Table: &replication.TableMapEvent{Schema: []byte("test"), Table: []byte("gtidxasyn1")},
				Rows:  [][]any{{pk, int32(0), int32(0)}},
			},
		}
	}
	inject := func(evs ...*replication.BinlogEvent) {
		t.Helper()
		for _, ev := range evs {
			require.NoError(t, streamer.AddEventToStreamer(ev))
		}
	}
	buffered := func(gno int64) bool {
		target, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", sid, gno))
		require.NoError(t, err)
		return client.getBufferedGTID().Contain(target)
	}

	// The XA transaction's first group, exactly as the server writes it
	// at XA PREPARE time. The row event doubles as the ordering barrier
	// for the "XA START" QueryEvent before it.
	inject(gtidEvent(100), queryEvent("XA START X'78',X'',1"), rowEvent(1))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 1 },
		5*time.Second, 5*time.Millisecond, "row event after XA START was not processed")
	require.False(t, buffered(100),
		"the GTID must not be promoted at XA START: the transaction's row events are not buffered yet")

	// XA END does not terminate the group either. The second row event
	// is the ordering barrier — the real group has none in this spot,
	// but row events are inert to the promotion logic.
	inject(queryEvent("XA END X'78',X'',1"), rowEvent(2))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 2 },
		5*time.Second, 5*time.Millisecond, "row event after XA END was not processed")
	require.False(t, buffered(100),
		"the GTID must not be promoted at XA END: the group ends at the XA prepare event")
	client.mu.Lock()
	pendingGNO := client.pendingGNO
	client.mu.Unlock()
	require.EqualValues(t, 100, pendingGNO, "the XA transaction's GTID must still be pending after XA END")

	// The XA_PREPARE_LOG_EVENT terminates the group. go-mysql has no
	// dedicated decoder for it, so it surfaces as a GenericEvent
	// identified only by the header type — as in readStream itself.
	inject(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XA_PREPARE_LOG_EVENT},
		Event:  &replication.GenericEvent{},
	})
	require.Eventually(t, func() bool { return buffered(100) },
		5*time.Second, 5*time.Millisecond, "the XA prepare event must promote the pending GTID")

	// The terminal XA COMMIT arrives later under its own GTID, with no
	// row events. Same for an XA ROLLBACK outcome.
	inject(gtidEvent(101), queryEvent("XA COMMIT X'78',X'',1"))
	require.Eventually(t, func() bool { return buffered(101) },
		5*time.Second, 5*time.Millisecond, "XA COMMIT must promote its own GTID")

	inject(gtidEvent(102), queryEvent("XA ROLLBACK X'79',X'',1"))
	require.Eventually(t, func() bool { return buffered(102) },
		5*time.Second, 5*time.Millisecond, "XA ROLLBACK must promote its own GTID")
}

// TestGTIDPromotePendingGTID unit-tests the promotion helper directly
// (no MySQL required beyond package setup). This documents the ROLLBACK
// design decision: any GTID the server streams — even for a transaction
// terminated by ROLLBACK (the mixed-engine case, where the
// non-transactional writes survive) — is part of the server's
// gtid_executed, so it must be promoted into bufferedGTID once the
// transaction's event stream ends. Not promoting can never be correct:
// the only effect is that bufferedGTID falls permanently behind
// gtid_executed and BlockWait stalls.
func TestGTIDPromotePendingGTID(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	gset, err := mysql.ParseMysqlGTIDSet(sid + ":1-5")
	require.NoError(t, err)
	c := &gtidClient{
		logger:       slog.Default(),
		bufferedGTID: gset,
	}

	// No pending GTID: promotion is a no-op (e.g. right after a stream
	// reconnect, where recreateStreamer cleared the pending state).
	c.promotePendingGTID()
	require.Equal(t, sid+":1-5", c.bufferedGTID.String())

	u := uuid.MustParse(sid)
	c.pendingSID = u[:]
	c.pendingGNO = 6
	c.promotePendingGTID()

	target, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:1-6", sid))
	require.NoError(t, err)
	require.True(t, c.bufferedGTID.Contain(target), "promoted GTID must enter the buffered set, got %s", c.bufferedGTID.String())
	require.Nil(t, c.pendingSID, "pending SID must be cleared after promotion")
	require.Zero(t, c.pendingGNO, "pending GNO must be cleared after promotion")

	// Promoting again is idempotent-by-vacancy: the pending state was
	// consumed, so nothing changes.
	c.promotePendingGTID()
	require.Equal(t, sid+":1-6", c.bufferedGTID.String())
}

// TestFlushedGTIDIsMonotonicAcrossOverlappingFlushes is the GTID twin of
// TestFlushedPosIsMonotonicAcrossOverlappingFlushes: flush() snapshots
// bufferedGTID before flushing the subscriptions and publishes the snapshot
// into flushedGTID afterwards, so if two flushes overlap, the
// later-finishing one can hold the older (smaller) snapshot. Without the
// containment guard that stale snapshot would overwrite a newer flushedGTID,
// silently regressing the checkpoint resume coordinate.
func TestFlushedGTIDIsMonotonicAcrossOverlappingFlushes(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	older, err := mysql.ParseMysqlGTIDSet(sid + ":1-100")
	require.NoError(t, err)
	newer, err := mysql.ParseMysqlGTIDSet(sid + ":1-200")
	require.NoError(t, err)

	client := &gtidClient{
		logger:       slog.Default(),
		subs:         newSubscriptionRegistry(),
		bufferedGTID: older,
	}
	sub := &gatedSubscription{gates: make(chan chan struct{})}
	require.True(t, client.subs.Add("test", sub))

	// Flush A snapshots bufferedGTID=1-100, then parks inside the
	// subscription flush (the unbuffered gates send synchronizes with
	// Flush entry, so the snapshot is guaranteed taken once it returns).
	doneA := make(chan error, 1)
	go func() { doneA <- client.flush(t.Context(), false, nil) }()
	gateA := make(chan struct{})
	sub.gates <- gateA

	// More transactions arrive, then flush B snapshots bufferedGTID=1-200
	// and parks too. (Flush A is already past <-s.gates, parked on gateA,
	// so this send can only be received by flush B.)
	client.mu.Lock()
	client.bufferedGTID = newer
	client.mu.Unlock()
	doneB := make(chan error, 1)
	go func() { doneB <- client.flush(t.Context(), false, nil) }()
	gateB := make(chan struct{})
	sub.gates <- gateB

	// Flush B (the newer snapshot) finishes first and publishes 1-200.
	close(gateB)
	require.NoError(t, <-doneB)
	client.mu.Lock()
	require.Equal(t, sid+":1-200", client.flushedGTID.String())
	client.mu.Unlock()

	// Flush A (the stale snapshot) finishes last. Without the guard it
	// would store 1-100 over 1-200, regressing the resume coordinate.
	close(gateA)
	require.NoError(t, <-doneA)
	client.mu.Lock()
	require.Equal(t, sid+":1-200", client.flushedGTID.String(),
		"an overlapping flush holding a stale bufferedGTID snapshot must not regress flushedGTID")
	client.mu.Unlock()
}

// TestGTIDRoundtripPosition verifies that the opaque Position string
// emitted after Start parses back into a GTIDSet (i.e. format round-trips).
func TestGTIDRoundtripPosition(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidroundtript1, gtidroundtript2")
	testutils.RunSQL(t, "CREATE TABLE gtidroundtript1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidroundtript2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidroundtript1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidroundtript2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	pos := client.Position()
	require.NotEmpty(t, pos)

	// Spin up a second client and have it resume from the first's
	// position. The validate path checks the position against
	// @@GLOBAL.gtid_purged — on a healthy server this should succeed.
	client2 := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker2, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client2.AddSubscription(t1, t2, chunker2))
	require.NoError(t, client2.StartFromPosition(t.Context(), pos))
	client2.Close()
}

// TestGTIDProcessDDLNotificationMoveStyle mirrors the "move-style
// subscription (nil newTable)" subtest of TestProcessDDLNotification for
// the GTID client. Regression: with no ddlFilterSchema configured, the
// subscription-match loop used to walk past the current table into the
// nil Tables() entry of a move-style subscription and panic the
// readStream goroutine on any DDL notification for a non-subscribed
// table.
func TestGTIDProcessDDLNotificationMoveStyle(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY)")

	tbl := table.NewTableInfo(db, dbName, "orders")
	require.NoError(t, tbl.SetInfo(t.Context()))

	cancelled := false
	c := &gtidClient{
		logger:           slog.Default(),
		callerCancelFunc: func(FatalReason) bool { cancelled = true; return true },
		subs:             newSubscriptionRegistry(),
	}
	chunker, err := table.NewChunker(tbl, table.ChunkerConfig{})
	require.NoError(t, err)
	sub, err := NewBufferedSubscription(BufferedSubscriptionConfig{
		CurrentTable: tbl,
		Applier:      applier.NewSingleTargetForTest(t, db),
		Chunker:      chunker,
	})
	require.NoError(t, err)
	require.True(t, c.subs.Add(dbName+".orders", sub))

	// DDL on an unrelated table must be ignored — not panic.
	c.processDDLNotification(dbName, "unrelated_table")
	require.False(t, cancelled, "should not cancel on DDL for an unrelated table")

	// DDL on the subscribed table still cancels.
	c.processDDLNotification(dbName, "orders")
	require.True(t, cancelled, "should cancel on DDL matching the subscribed table")
}
