package statement

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// diffContract is one MySQL round-trip contract for CreateTable.Diff. A table
// is created from target and its SHOW CREATE TABLE captured as the expected end
// state; the table is then recreated from source, the live definition is diffed
// against target, every emitted statement is executed, and the resulting
// SHOW CREATE TABLE must equal the expected one byte for byte. A second diff of
// the result against target must then be empty.
//
// Comparing against MySQL's own rendering of the target, rather than only
// checking that the second diff is empty, is what catches a silent omission: an
// attribute the parser drops on both sides diffs clean while the live table
// never had it.
type diffContract struct {
	name   string
	source string // the body of CREATE TABLE t, e.g. "(id INT PRIMARY KEY)"
	target string
	// sourceSessionCharset creates the source table from a session with that
	// character_set_client (SET NAMES), which is how MySQL comes to report a
	// non-utf8mb4 introducer on a literal the user wrote bare.
	sourceSessionCharset string
	// wantNoop asserts that the live source already satisfies target: the diff
	// must be empty, and the SHOW CREATE TABLE comparison is skipped because
	// the two definitions are equivalent rather than identical.
	wantNoop bool
	opts     *DiffOptions // nil selects the defaults
}

func runDiffContract(t *testing.T, c diffContract) {
	t.Helper()
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	ctx := t.Context()
	exec := func(stmt string) {
		t.Helper()
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "executing: %s", stmt)
	}

	exec("CREATE TABLE t " + c.target)
	expected := showCreateTable(t, db, "t")
	exec("DROP TABLE t")

	if c.sourceSessionCharset == "" {
		exec("CREATE TABLE t " + c.source)
	} else {
		// A separate pool, so the SET NAMES never leaks into db's connections.
		sdb, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
		require.NoError(t, err)
		conn, err := sdb.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "SET NAMES "+c.sourceSessionCharset)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE TABLE t "+c.source)
		require.NoError(t, err)
		require.NoError(t, conn.Close())
		require.NoError(t, sdb.Close())
	}

	live := showCreateTable(t, db, "t")
	src, err := ParseCreateTable(live)
	require.NoError(t, err, live)
	dst, err := ParseCreateTable("CREATE TABLE t " + c.target)
	require.NoError(t, err)
	stmts, err := src.Diff(dst, c.opts)
	require.NoError(t, err)
	applied := make([]string, 0, len(stmts))
	for _, s := range stmts {
		applied = append(applied, s.Statement)
	}
	if c.wantNoop {
		assert.Empty(t, applied, "live:\n%s", live)
		return
	}
	for _, s := range applied {
		exec(s)
	}
	actual := showCreateTable(t, db, "t")
	require.Equal(t, expected, actual, "live:\n%s\napplied:\n%s", live, strings.Join(applied, "\n"))

	again, err := ParseCreateTable(actual)
	require.NoError(t, err)
	stmts, err = again.Diff(dst, c.opts)
	require.NoError(t, err)
	assert.Empty(t, stmts, "second diff must be empty")
}

// TestDiffMySQLContracts runs the round-trip contracts. Each case is one
// correctness finding; the comment above a group names what used to go wrong.
func TestDiffMySQLContracts(t *testing.T) {
	contracts := []diffContract{
		// Charset introducers. Every introducer used to be stripped from a
		// restored expression, which changed its meaning (CHAR_LENGTH of a
		// _binary literal counts bytes) or made it invalid (a COLLATE whose
		// charset came from the introducer, error 1253). See restoreExprText.
		{
			name:   "generated column keeps a _binary introducer",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, g INT AS (CHAR_LENGTH(_binary'€') + id) STORED)",
		},
		{
			name:   "check keeps a _binary introducer",
			source: "(id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> 'q'))",
			target: "(id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> _binary'q'))",
		},
		{
			name:   "functional index keeps a _binary introducer",
			source: "(id INT PRIMARY KEY, z VARCHAR(10))",
			target: "(id INT PRIMARY KEY, z VARCHAR(10), KEY fk ((CONCAT(z, _binary'x'))))",
		},
		{
			name:   "expression default keeps the introducer under COLLATE",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (_latin1'a' COLLATE latin1_bin))",
		},
		{
			name:                 "literal stored from a utf8mb3 session is the bare literal",
			source:               "(id INT PRIMARY KEY, c VARCHAR(10), CONSTRAINT ck CHECK (c <> 'A'))",
			target:               "(id INT PRIMARY KEY, c VARCHAR(10), CONSTRAINT ck CHECK (c <> 'A'))",
			sourceSessionCharset: "utf8",
			wantNoop:             true,
		},
		{
			name:                 "ASCII literal stored from a latin1 session is the bare literal",
			source:               "(id INT PRIMARY KEY, g INT AS (LENGTH('abc')) STORED)",
			target:               "(id INT PRIMARY KEY, g INT AS (LENGTH('abc')) STORED)",
			sourceSessionCharset: "latin1",
			wantNoop:             true,
		},
		{
			// From a latin1 session the UTF-8 bytes of 'é' are read as two
			// latin1 characters, so the stored expression is a different one.
			name:                 "non-ASCII literal stored from a latin1 session is not the bare literal",
			source:               "(id INT PRIMARY KEY, g INT AS (CHAR_LENGTH('é')) STORED)",
			target:               "(id INT PRIMARY KEY, g INT AS (CHAR_LENGTH('é')) STORED)",
			sourceSessionCharset: "latin1",
		},
		// Column-level CHECKs: every one a column carries is kept, with its
		// enforcement. Only the last used to survive, and NOT ENFORCED was lost.
		{
			name:   "both column checks are added",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
		},
		{
			name:     "both column checks are kept",
			source:   "(id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
			target:   "(id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
			wantNoop: true,
		},
		{
			name:   "column check stays not enforced",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0) NOT ENFORCED)",
		},
		{
			name:   "column check enforcement is toggled",
			source: "(id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0))",
			target: "(id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0) NOT ENFORCED)",
		},
		// Invisible columns and the other per-column attributes. They were
		// not modeled, so a change to one was never emitted, and a MODIFY
		// for any other reason silently cleared them.
		{
			name:   "column becomes invisible",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT INVISIBLE)",
		},
		{
			name:   "column becomes visible",
			source: "(id INT PRIMARY KEY, c INT INVISIBLE)",
			target: "(id INT PRIMARY KEY, c INT)",
		},
		{
			name:   "invisible column survives a comment change",
			source: "(id INT PRIMARY KEY, c INT INVISIBLE)",
			target: "(id INT PRIMARY KEY, c INT INVISIBLE COMMENT 'x')",
		},
		{
			name:   "secondary engine attribute survives a comment change",
			source: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
			target: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}' COMMENT 'x')`,
		},
		{
			name:     "secondary engine attribute as MySQL re-serializes it is a no-op",
			source:   `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"b":1,"a":[1,2]}')`,
			target:   `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"b":1,"a":[1,2]}')`,
			wantNoop: true,
		},
		{
			name:   "not secondary, column format and storage survive a comment change",
			source: "(id INT PRIMARY KEY, c INT NOT SECONDARY COLUMN_FORMAT FIXED STORAGE DISK)",
			target: "(id INT PRIMARY KEY, c INT NOT SECONDARY COLUMN_FORMAT FIXED STORAGE DISK COMMENT 'x')",
		},
		{
			name:   "generated invisible column is added with its attributes",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, g INT GENERATED ALWAYS AS (id + 1) STORED NOT NULL INVISIBLE COMMENT 'g')",
		},
		// Column order. Positions are decided by replaying the clauses the
		// way MySQL applies them; a DROP used to suppress the reorder of the
		// column after it.
		{
			name:   "columns are reordered after a drop",
			source: "(id INT PRIMARY KEY, a INT, b INT, c INT, d INT)",
			target: "(id INT PRIMARY KEY, d INT, b INT)",
		},
		{
			name:   "columns are reordered around an added column",
			source: "(id INT PRIMARY KEY, a INT, b INT)",
			target: "(id INT PRIMARY KEY, x INT, b INT, a INT)",
		},
		{
			name:   "columns are rotated",
			source: "(a INT, b INT, c INT, d INT)",
			target: "(c INT, d INT, a INT, b INT)",
		},
		{
			name:   "columns are reversed",
			source: "(a INT, b INT, c INT, d INT)",
			target: "(d INT, c INT, b INT, a INT)",
		},
		// Table options SHOW CREATE TABLE reports and Diff used to discard.
		{
			name:   "table statistics options are applied",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY) STATS_PERSISTENT=0 STATS_AUTO_RECALC=1 STATS_SAMPLE_PAGES=42",
		},
		{
			name:   "table statistics options are reset",
			source: "(id INT PRIMARY KEY) STATS_PERSISTENT=0 STATS_AUTO_RECALC=1 STATS_SAMPLE_PAGES=42",
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:     "AUTOEXTEND_SIZE suffix matches the live byte count",
			source:   "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4194304",
			target:   "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
			wantNoop: true,
		},
		{
			name:   "AUTOEXTEND_SIZE is applied",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
		},
		{
			name:   "AUTOEXTEND_SIZE is reset",
			source: "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:   "table SECONDARY_ENGINE_ATTRIBUTE is applied",
			source: "(id INT PRIMARY KEY)",
			target: `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"t":1}'`,
		},
		{
			name:   "table SECONDARY_ENGINE_ATTRIBUTE is reset",
			source: `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"t":1}'`,
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:     "table SECONDARY_ENGINE_ATTRIBUTE compares as JSON",
			source:   `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"b":1,"a":[1,2]}'`,
			target:   `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"a": [1, 2], "b": 1}'`,
			wantNoop: true,
		},
		{
			name:   "table storage hints are applied",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY) MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=100 PACK_KEYS=1 CHECKSUM=1 DELAY_KEY_WRITE=1",
		},
		{
			name:   "table storage hints are reset",
			source: "(id INT PRIMARY KEY) MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=100 PACK_KEYS=1 CHECKSUM=1 DELAY_KEY_WRITE=1",
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:   "table KEY_BLOCK_SIZE is applied with the row format",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		{
			// InnoDB rejects ROW_FORMAT=DYNAMIC while a KEY_BLOCK_SIZE is set,
			// so the clearing KEY_BLOCK_SIZE=0 must travel in the same ALTER.
			name:   "table KEY_BLOCK_SIZE is cleared with the row format",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			target: "(id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		// An index's SECONDARY_ENGINE_ATTRIBUTE.
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE is applied",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
		},
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE is removed",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
			target: "(id INT PRIMARY KEY, c INT, KEY k (c))",
		},
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE survives a comment change",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) COMMENT 'a' SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) COMMENT 'b' SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
		},
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE survives a visibility change",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}' INVISIBLE)`,
		},
		// Index options MySQL accepts and does not store.
		{
			name:     "primary key VISIBLE is a no-op",
			source:   "(id INT PRIMARY KEY)",
			target:   "(id INT, PRIMARY KEY (id) VISIBLE)",
			wantNoop: true,
		},
		{
			name:     "secondary index VISIBLE is a no-op",
			source:   "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target:   "(id INT PRIMARY KEY, c INT, KEY k (c) VISIBLE)",
			wantNoop: true,
		},
		{
			name:     "USING HASH is a no-op on InnoDB",
			source:   "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target:   "(id INT PRIMARY KEY, c INT, KEY k (c) USING HASH)",
			wantNoop: true,
		},
		{
			name:   "USING BTREE is applied",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c) USING BTREE)",
		},
		{
			name:   "USING BTREE is removed",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c) USING BTREE)",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c))",
		},
		{
			name:     "index KEY_BLOCK_SIZE is a no-op on an uncompressed table",
			source:   "(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id) KEY_BLOCK_SIZE=8)",
			wantNoop: true,
		},
		{
			name:   "index KEY_BLOCK_SIZE is applied on a compressed table",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
		},

		// AUTO_INCREMENT implies NOT NULL (autoIncrementNotNullNormalizer).
		{
			name:     "AUTO_INCREMENT without NOT NULL is NOT NULL",
			source:   "(id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			wantNoop: true,
		},
		{
			name:     "NULL before AUTO_INCREMENT is NOT NULL",
			source:   "(id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			wantNoop: true,
		},
		{
			name:     "AUTO_INCREMENT DEFAULT NULL is NOT NULL with no default",
			source:   "(id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT AUTO_INCREMENT DEFAULT NULL, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			wantNoop: true,
		},
		{
			name:   "adding an AUTO_INCREMENT column without NOT NULL",
			source: "(x INT PRIMARY KEY)",
			target: "(x INT PRIMARY KEY, id INT AUTO_INCREMENT, UNIQUE KEY k (id))",
		},
	}
	for _, c := range contracts {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			runDiffContract(t, c)
		})
	}
}

// TestDiffContractInlineUniqueKeepsLiveName checks the pairing of an inline
// `c INT UNIQUE` with a live unique index under another name: the emitted
// ALTER keeps the live name and brings the index's options in line with the
// declaration, and the result diffs clean. The SHOW CREATE TABLE text differs
// from the declaration's by the index name, which is why this is not a
// diffContract case.
func TestDiffContractInlineUniqueKeepsLiveName(t *testing.T) {
	t.Parallel()
	_, db := testutils.CreateUniqueTestDatabase(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, "CREATE TABLE t (id INT PRIMARY KEY, c INT, UNIQUE KEY c_2 (c) COMMENT 'x' INVISIBLE)")
	require.NoError(t, err)
	dst, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, c INT UNIQUE)")
	require.NoError(t, err)
	src, err := ParseCreateTable(showCreateTable(t, db, "t"))
	require.NoError(t, err)
	stmts, err := src.Diff(dst, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stmts, "the live index carries a comment and is invisible; the declaration has neither")
	for _, stmt := range stmts {
		_, err = db.ExecContext(ctx, stmt.Statement)
		require.NoError(t, err, stmt.Statement)
	}
	live := showCreateTable(t, db, "t")
	require.Contains(t, live, "UNIQUE KEY `c_2` (`c`)\n", "the live name is kept and the options are cleared")
	again, err := ParseCreateTable(live)
	require.NoError(t, err)
	stmts, err = again.Diff(dst, nil)
	require.NoError(t, err)
	require.Empty(t, stmts)
}

// TestDiffContractNullableAutoIncrement: a NULL written after the
// AUTO_INCREMENT is the one spelling of a nullable AUTO_INCREMENT column, and
// the emitted MODIFY has to keep that order, because MySQL applies the
// attributes in order and `int NULL AUTO_INCREMENT` is stored NOT NULL. This
// is not a diffContract because the result cannot converge: MySQL reports the
// nullable column as `int AUTO_INCREMENT`, which as CREATE TABLE input means
// NOT NULL, so a second diff reads the live column back as NOT NULL.
func TestDiffContractNullableAutoIncrement(t *testing.T) {
	_, db := testutils.CreateUniqueTestDatabase(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))")
	require.NoError(t, err)
	src, err := ParseCreateTable(showCreateTable(t, db, "t"))
	require.NoError(t, err)
	dst, err := ParseCreateTable("CREATE TABLE t (id INT AUTO_INCREMENT NULL, x INT PRIMARY KEY, UNIQUE KEY k (id))")
	require.NoError(t, err)
	stmts, err := src.Diff(dst, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE `t` MODIFY COLUMN `id` int AUTO_INCREMENT NULL", stmts[0].Statement)
	_, err = db.ExecContext(ctx, stmts[0].Statement)
	require.NoError(t, err)
	var isNullable string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 't' AND COLUMN_NAME = 'id'").Scan(&isNullable))
	assert.Equal(t, "YES", isNullable)
	assert.Contains(t, showCreateTable(t, db, "t"), "`id` int AUTO_INCREMENT,\n")
}
