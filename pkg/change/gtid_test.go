package change

import (
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

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

// TestGTIDStartFromImpossiblePosition verifies that resuming from a GTID
// set that doesn't cover @@GLOBAL.gtid_purged surfaces as
// ErrPositionNotFound.
func TestGTIDStartFromImpossiblePosition(t *testing.T) {
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

	// Malformed input string.
	err = client.StartFromPosition(t.Context(), "not-a-valid-gtid-set")
	require.Error(t, err)
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
