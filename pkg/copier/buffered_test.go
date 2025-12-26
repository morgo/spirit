package copier

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferedCopier(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS bufferedt1, bufferedt2")
	testutils.RunSQL(t, "CREATE TABLE bufferedt1 (a INT NOT NULL, b INT, c VARCHAR(255), d VARBINARY(255), e JSON, f DATETIME, g TIMESTAMP, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE bufferedt2 (a INT NOT NULL, b INT, c VARCHAR(255), d VARBINARY(255), e JSON, f DATETIME, g TIMESTAMP, PRIMARY KEY (a))")

	// Insert all sorts of evil data.
	testutils.RunSQL(t, "INSERT INTO bufferedt1 VALUES (1, NULL, 'normal'' string', RANDOM_BYTES(10), JSON_ARRAY(1,2,3), NOW(), NOW())")
	testutils.RunSQL(t, `INSERT INTO bufferedt1 VALUES (2, 42, 'string with \\ backslash', RANDOM_BYTES(10), JSON_OBJECT('key', 'value\\ \''), NOW(), NOW())`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "bufferedt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "bufferedt2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	cfg := NewCopierDefaultConfig()
	cfg.UseExperimentalBufferedCopier = true
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
	}
	cfg.Applier = applier.NewSingleTargetApplier(target, dbconn.NewDBConfig(), slog.Default())
	chunker, err := table.NewChunker(t1, t2, cfg.TargetChunkTime, cfg.Logger)
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())

	copier, err := NewCopier(db, chunker, cfg)
	assert.NoError(t, err)
	assert.NoError(t, copier.Run(t.Context())) // works.

	// We should expect to have the same number of rows
	// and a basic checksum confirms a match.
	var checksumSrc, checksumDst string
	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(CONCAT(a, IFNULL(b, ''), c, d, e, f, g))) as checksum FROM bufferedt1").Scan(&checksumSrc)
	assert.NoError(t, err)

	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(CONCAT(a, IFNULL(b, ''), c, d, e, f, g))) as checksum FROM bufferedt2").Scan(&checksumDst)
	assert.NoError(t, err)
	assert.Equal(t, checksumSrc, checksumDst, "Checksums do not match between source and destination tables")

	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

// TestBufferedCopierCharsetConversion tests that the buffered copier
// handles charset conversions correctly.
//
// In the unbuffered copier, we don't really have to worry about this because
// MySQL can infer source and dest charset from the INSERT.. SELECT
// and do any conversion that is required.
//
// In the buffered copier, we need to set the connection charset to utf8mb4.
// For this test, what this means is that on *read* of charsetsrc, the characters
// will be converted from latin1 to utf8mb4 by the MySQL server. We then insert
// into charsetdst as utf8mb4 characters.
//
// In the reverse direction (utf8mb4 -> latin1) the server knows that the client
// is in utf8mb4 and is able to convert on insert to match the tables requirements.
func TestBufferedCopierCharsetConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS charsetsrc, charsetdst")
	testutils.RunSQL(t, "CREATE TABLE charsetsrc (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, b VARCHAR(100) NOT NULL) CHARSET=latin1")
	testutils.RunSQL(t, "CREATE TABLE charsetdst (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, b VARCHAR(100) NOT NULL) CHARSET=utf8mb4")

	// Insert rows with special characters that exist in latin1
	// 'à' (U+00E0) and '€' (U+20AC) are both valid in latin1
	testutils.RunSQL(t, "INSERT INTO charsetsrc VALUES (NULL, 'à'), (NULL, '€')")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "charsetsrc")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "charsetdst")
	assert.NoError(t, t2.SetInfo(t.Context()))

	cfg := NewCopierDefaultConfig()
	cfg.UseExperimentalBufferedCopier = true
	cfg.Applier = applier.NewSingleTargetApplier(applier.Target{DB: db}, dbconn.NewDBConfig(), slog.Default())
	chunker, err := table.NewChunker(t1, t2, cfg.TargetChunkTime, cfg.Logger)
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())

	copier, err := NewCopier(db, chunker, cfg)
	assert.NoError(t, err)

	// The copy should succeed because we set the connection charset to utf8mb4
	// On read from the src it will be converted from latin1 to utf8mb4
	err = copier.Run(t.Context())
	assert.NoError(t, err, "Charset conversion from latin1 to utf8mb4 should succeed")

	// Reverse the copy to show the other direction works too
	// Start by emptying the "src" table, which is our intended destination.
	testutils.RunSQL(t, "TRUNCATE TABLE charsetsrc")
	chunker, err = table.NewChunker(t2, t1, cfg.TargetChunkTime, cfg.Logger)
	assert.NoError(t, err)
	copier, err = NewCopier(db, chunker, cfg)
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
	err = copier.Run(t.Context())
	assert.NoError(t, err, "Charset conversion from utf8mb4 to latin1 should succeed")
}
