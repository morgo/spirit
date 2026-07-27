package migration

import (
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChecksumChunkerIsSizedSeparatelyFromTheCopier pins the wiring that gives
// the checksum its own chunk-sizing profile.
//
// The two phases share a chunker implementation but not its calibration: a copy
// chunk's duration bounds a write transaction, a checksum chunk's bounds a
// server-side aggregate inside a snapshot that is already held. Before this was
// separated, the checksum inherited the copier's 500ms target and 1000-row start
// and — because growth is capped per feedback window — spent whole passes ramping
// without ever reaching the size it had measured as correct.
func TestChecksumChunkerIsSizedSeparatelyFromTheCopier(t *testing.T) {
	tableName := "chunksize_profile"
	dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s, %s, %s",
		tableName, utils.NewTableName(tableName), utils.CheckpointTableName(tableName))
	testutils.RunSQL(t, dropStmt)
	t.Cleanup(func() { testutils.RunSQL(t, dropStmt) })
	testutils.RunSQL(t, "CREATE TABLE "+tableName+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(10))")
	testutils.RunSQL(t, "INSERT INTO "+tableName+" (pad) VALUES ('a'), ('b'), ('c')")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Migration{
		Host:         cfg.Addr,
		Username:     cfg.User,
		Password:     &cfg.Passwd,
		Database:     cfg.DBName,
		Threads:      1,
		WriteThreads: 1,
		Table:        tableName,
		Alter:        "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(r) })

	// A programmatically-constructed Migration leaves the Kong defaults at zero,
	// so normalizeOptions is what has to fill this in — otherwise a library caller
	// would get a zero target and the chunker's own fallback.
	assert.Equal(t, checksum.DefaultTargetChunkTime, r.migration.ChecksumTargetChunkTime)
	assert.NotEqual(t, r.migration.TargetChunkTime, r.migration.ChecksumTargetChunkTime,
		"the checksum must not be sharing the copier's target")

	dbCfg := dbconn.NewDBConfig()
	r.db, err = dbconn.New(testutils.DSN(), dbCfg)
	require.NoError(t, err)
	r.dbConfig = dbCfg
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.changes[0].dropOldTable(t.Context()))
	require.NoError(t, r.changes[0].createNewTable(t.Context()))
	require.NoError(t, r.changes[0].alterNewTable(t.Context()))
	require.NoError(t, r.initChunkers())

	require.NoError(t, r.copyChunker.Open())
	require.NoError(t, r.checksumChunker.Open())

	copyChunk, err := r.copyChunker.Next()
	require.NoError(t, err)
	assert.Equal(t, uint64(table.StartingChunkSize), copyChunk.ChunkSize,
		"the copy phase keeps its conservative ramp")

	checksumChunk, err := r.checksumChunker.Next()
	require.NoError(t, err)
	assert.Equal(t, uint64(checksum.ChunkStartRows), checksumChunk.ChunkSize,
		"the checksum starts at the row cap instead of ramping up to it")
}

// TestChecksumTargetChunkTimeValidation covers the flag's own bounds. It is a
// duration read straight from the command line, and a negative value would make
// every chunk look late and pin the chunk size at the row floor.
func TestChecksumTargetChunkTimeValidation(t *testing.T) {
	pw := ""
	m := &Migration{
		Password:                &pw,
		Table:                   "t",
		Alter:                   "ENGINE=InnoDB",
		ChecksumTargetChunkTime: -1 * time.Second,
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--checksum-target-chunk-time")
}
