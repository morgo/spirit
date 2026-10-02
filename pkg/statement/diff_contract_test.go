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
	}
	for _, c := range contracts {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			runDiffContract(t, c)
		})
	}
}
