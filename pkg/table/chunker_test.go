package table

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestCompositeChunker(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS composite`)
	table := `CREATE TABLE composite (
		id bigint NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "composite")
	require.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
}

func TestOptimisticChunker(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS optimistic`)
	table := `CREATE TABLE optimistic (
		id bigint NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "optimistic")
	require.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerOptimistic{}, chunker)
}

func TestNewCompositeChunkerWithKeyAndWhere(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS composite`)
	table := `CREATE TABLE composite (
		id bigint NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id),
        KEY age_idx (age)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "composite")
	require.NoError(t, t1.SetInfo(t.Context()))

	// When Key and Where are specified, NewChunker should always return a
	// composite chunker even though this table has a single-column auto-inc PK.
	chunker, err := NewChunker(t1, ChunkerConfig{
		Key:   "age_idx",
		Where: "age > 50",
	})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
	require.Equal(t, "age_idx", chunker.(*chunkerComposite).keyName)
	require.Equal(t, "age > 50", chunker.(*chunkerComposite).where)
}

func TestChunkerSizingSignal(t *testing.T) {
	const rowBytes = 1024
	const targetBytes = 32 * rowBytes
	for _, tc := range []struct {
		name       string
		primaryKey string
		idType     string
		key        string
		where      string
		bytes      uint64
	}{
		{name: "optimistic", primaryKey: "id", idType: "BIGINT AUTO_INCREMENT", bytes: targetBytes},
		{name: "composite_primary_key", primaryKey: "id, grp", idType: "BIGINT AUTO_INCREMENT", bytes: targetBytes},
		{name: "non_auto_increment", primaryKey: "id", idType: "BINARY(16)", bytes: targetBytes},
		{name: "alternate_key", primaryKey: "id", idType: "BIGINT AUTO_INCREMENT", key: "grp_idx", where: "grp = 1", bytes: targetBytes},
		{name: "checksum_time_signal", primaryKey: "id, grp", idType: "BIGINT AUTO_INCREMENT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, "chunker_sizing_signal", "CREATE TABLE chunker_sizing_signal ("+
				"id "+tc.idType+" NOT NULL, grp INT NOT NULL DEFAULT 1, pad VARBINARY(1024) NOT NULL, "+
				"PRIMARY KEY ("+tc.primaryKey+"), KEY grp_idx (grp, id))")
			seed := "INSERT INTO chunker_sizing_signal (pad) SELECT REPEAT('a', 1024)"
			if tc.idType == "BINARY(16)" {
				seed = "INSERT INTO chunker_sizing_signal (id, pad) SELECT UUID_TO_BIN(UUID()), REPEAT('a', 1024)"
			}
			tt.SeedRows(t, seed, 8192)
			var schema string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema))
			ti := NewTableInfo(tt.DB, schema, tt.Name)
			require.NoError(t, ti.SetInfo(t.Context()))
			chunker, err := NewChunker(ti, ChunkerConfig{TargetChunkBytes: tc.bytes, Key: tc.key, Where: tc.where})
			require.NoError(t, err)
			require.NoError(t, chunker.Open())
			defer func() { require.NoError(t, chunker.Close()) }()

			var chunk *Chunk
			for range 25 {
				chunk, err = chunker.Next()
				require.NoError(t, err)
				var rows uint64
				require.NoError(t, tt.DB.QueryRowContext(t.Context(),
					"SELECT COUNT(*), COALESCE(SUM(OCTET_LENGTH(pad)), 0) FROM "+ti.QuotedTableName+" WHERE "+chunk.String(),
				).Scan(&rows, &chunk.ActualBytes))
				// Model the buffered copier's size measurement with a long write
				// queue delay. Only a time-based chunker should shrink for this.
				chunker.Feedback(chunk, 30*time.Second, rows)
			}
			if tc.bytes == 0 {
				require.EqualValues(t, MinDynamicRowSize, chunk.ChunkSize,
					"without a byte budget, the checksum must still react to slow chunks")
			} else {
				require.InEpsilon(t, targetBytes, chunk.ActualBytes, 0.1,
					"copy chunks must follow the byte budget, not write queue delays")
			}
		})
	}
}
