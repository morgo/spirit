package table

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startSizeFixture creates a densely-populated table with an auto-increment
// primary key (so NewChunker picks the optimistic chunker) plus a second table
// with a composite key, and returns both.
func startSizeFixture(t *testing.T) (autoInc, composite *TableInfo) {
	t.Helper()
	testutils.RunSQL(t, "DROP TABLE IF EXISTS startsize_autoinc, startsize_composite")
	testutils.RunSQL(t, "CREATE TABLE startsize_autoinc (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(10))")
	testutils.RunSQL(t, "CREATE TABLE startsize_composite (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))")
	testutils.RunSQL(t, `INSERT INTO startsize_autoinc (pad)
		WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 500) SELECT 'x' FROM s`)
	testutils.RunSQL(t, `INSERT INTO startsize_composite (a, b)
		WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 30) SELECT x.i, y.i FROM s x, s y`)

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	autoInc = NewTableInfo(db, "test", "startsize_autoinc")
	require.NoError(t, autoInc.SetInfo(t.Context()))
	composite = NewTableInfo(db, "test", "startsize_composite")
	require.NoError(t, composite.SetInfo(t.Context()))
	return autoInc, composite
}

// TestStartingChunkSizeIsUsedByBothChunkers covers the config knob that lets a
// read-only consumer (the checksum) skip the ramp the copier needs. Both chunker
// implementations have to honour it, and it has to survive Reset — otherwise a
// checksum retry would start over at the copier's size.
func TestStartingChunkSizeIsUsedByBothChunkers(t *testing.T) {
	autoInc, composite := startSizeFixture(t)

	for _, tc := range []struct {
		name string
		ti   *TableInfo
	}{
		{"optimistic", autoInc},
		{"composite", composite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def, err := NewChunker(tc.ti, ChunkerConfig{})
			require.NoError(t, err)
			require.NoError(t, def.Open())
			first, err := def.Next()
			require.NoError(t, err)
			assert.Equal(t, uint64(StartingChunkSize), first.ChunkSize,
				"an unset StartingChunkSize must keep the existing default")

			c, err := NewChunker(tc.ti, ChunkerConfig{StartingChunkSize: 40000})
			require.NoError(t, err)
			require.NoError(t, c.Open())
			first, err = c.Next()
			require.NoError(t, err)
			assert.Equal(t, uint64(40000), first.ChunkSize, "the first chunk must already be at the configured size")

			// A checksum that hits its yield timeout or a retryable error resets and
			// re-reads the table; resetting to the copier's 1000 rows would put the
			// ramp back in the way.
			require.NoError(t, c.Reset())
			first, err = c.Next()
			require.NoError(t, err)
			assert.Equal(t, uint64(40000), first.ChunkSize, "Reset must not drop back to the default")
		})
	}
}

// TestStartingChunkSizeIsClamped guards the bounds. The value reaches the sizer
// from a config struct, so it cannot be assumed sane; and it deliberately does
// not go through boundaryCheckTargetChunkSize, whose growth cap is relative to
// the current chunk size and would clamp any starting value to the row floor.
func TestStartingChunkSizeIsClamped(t *testing.T) {
	d := dynamicChunkSizer{}
	assert.Equal(t, uint64(StartingChunkSize), d.startingChunkSize(), "zero means the default")

	d.startSize = MaxDynamicRowSize * 10
	assert.Equal(t, uint64(MaxDynamicRowSize), d.startingChunkSize())

	d.startSize = 1
	assert.Equal(t, uint64(MinDynamicRowSize), d.startingChunkSize())

	d.startSize = MaxDynamicRowSize
	assert.Equal(t, uint64(MaxDynamicRowSize), d.startingChunkSize(),
		"starting at the ceiling is the checksum's configuration and must survive clamping")
}
