package statement

import (
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// showCreateTable returns the SHOW CREATE TABLE output for a test table.
func showCreateTable(t *testing.T, tt *testutils.TestTable) string {
	t.Helper()
	var name, createSQL string
	err := tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SHOW CREATE TABLE `%s`", tt.Name)).Scan(&name, &createSQL)
	require.NoError(t, err)
	return createSQL
}

// TestDiffIntegrationRemoveTableComment verifies that the empty COMMENT
// clause emitted when a table comment is removed actually clears the comment
// on a real MySQL server, and that a re-diff afterwards converges to nil.
func TestDiffIntegrationRemoveTableComment(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_comment_removal",
		"CREATE TABLE diff_comment_removal (a int) COMMENT='old comment'")

	target, err := ParseCreateTable("CREATE TABLE diff_comment_removal (a int)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_comment_removal` COMMENT=''", stmts[0].Statement)

	// Apply the diff and verify the comment is really gone.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt)
	require.NotContains(t, postAlter, "COMMENT")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationFulltextParser verifies that adding WITH PARSER to an
// index whose column list is unchanged is detected, and that executing the
// statements Diff() emits — exactly as Spirit's Runner would — actually applies
// the parser change on a real MySQL server.
//
// This is a regression test for a silent-no-op bug: MySQL treats a combined
// `DROP INDEX x, ADD INDEX x (<same cols>)` in a single ALTER as a no-op and
// keeps the existing index. Diff() therefore emits the DROP and ADD as two
// separate statements. The test executes only those emitted statements (no
// extra manual ALTERs) and asserts the parser change took effect.
func TestDiffIntegrationFulltextParser(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_ft_parser",
		"CREATE TABLE diff_ft_parser (id int primary key, b text, FULLTEXT KEY ft_b (b))")

	target, err := ParseCreateTable("CREATE TABLE diff_ft_parser (id int primary key, b text, FULLTEXT KEY ft_b (b) WITH PARSER ngram)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 2, "option-only index change must be two separate statements")
	require.Equal(t, "ALTER TABLE `diff_ft_parser` DROP INDEX `ft_b`", stmts[0].Statement)
	require.Equal(t, "ALTER TABLE `diff_ft_parser` ADD FULLTEXT INDEX `ft_b` (`b`) WITH PARSER ngram", stmts[1].Statement)

	// Execute the emitted statements exactly as the Runner would, and verify
	// the parser change actually took effect — no extra manual ALTERs.
	for _, stmt := range stmts {
		_, err = tt.DB.ExecContext(t.Context(), stmt.Statement)
		require.NoError(t, err)
	}
	postAlter := showCreateTable(t, tt)
	require.Contains(t, postAlter, "WITH PARSER `ngram`")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationFulltextRebuildPreservesParser verifies that an index
// rebuilt for an unrelated reason (a column list change) preserves WITH PARSER
// in the re-add, and that applying the diff to a real MySQL server converges.
//
// Unlike the option-only cases above, the column list genuinely changes here,
// so MySQL really rebuilds the index. A combined `DROP INDEX x, ADD INDEX x
// (<new cols>)` in a single ALTER is therefore NOT a no-op, and Diff() keeps it
// as one statement.
func TestDiffIntegrationFulltextRebuildPreservesParser(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_ft_rebuild",
		"CREATE TABLE diff_ft_rebuild (id int primary key, b text, c text, FULLTEXT KEY ft_b (b) WITH PARSER ngram)")

	target, err := ParseCreateTable("CREATE TABLE diff_ft_rebuild (id int primary key, b text, c text, FULLTEXT KEY ft_b (b, c) WITH PARSER ngram)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_ft_rebuild` DROP INDEX `ft_b`, ADD FULLTEXT INDEX `ft_b` (`b`, `c`) WITH PARSER ngram", stmts[0].Statement)

	// The column list changed, so MySQL really rebuilds the index and the
	// parser must survive the rebuild.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt)
	require.Contains(t, postAlter, "FULLTEXT KEY `ft_b` (`b`,`c`)")
	require.Contains(t, postAlter, "WITH PARSER `ngram`")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationKeyBlockSize verifies that a KEY_BLOCK_SIZE difference on
// an index whose column list is unchanged is detected, and that executing the
// statements Diff() emits actually applies the change on a real MySQL server.
// The table uses ROW_FORMAT=COMPRESSED because InnoDB silently ignores
// index-level KEY_BLOCK_SIZE on uncompressed tables.
//
// Like the parser case, this is option-only, so Diff() emits a separate DROP
// and ADD; a combined single ALTER would be a MySQL no-op. The test executes
// only the emitted statements (no extra manual ALTERs).
func TestDiffIntegrationKeyBlockSize(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_kbs",
		"CREATE TABLE diff_kbs (id int primary key, b varchar(100), KEY idx_b (b)) ROW_FORMAT=COMPRESSED")

	target, err := ParseCreateTable("CREATE TABLE diff_kbs (id int primary key, b varchar(100), KEY idx_b (b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 2, "option-only index change must be two separate statements")
	require.Equal(t, "ALTER TABLE `diff_kbs` DROP INDEX `idx_b`", stmts[0].Statement)
	require.Equal(t, "ALTER TABLE `diff_kbs` ADD INDEX `idx_b` (`b`) KEY_BLOCK_SIZE=8", stmts[1].Statement)

	// Execute the emitted statements exactly as the Runner would, and verify
	// KEY_BLOCK_SIZE actually took effect — no extra manual ALTERs.
	for _, stmt := range stmts {
		_, err = tt.DB.ExecContext(t.Context(), stmt.Statement)
		require.NoError(t, err)
	}
	postAlter := showCreateTable(t, tt)
	require.Contains(t, postAlter, "KEY `idx_b` (`b`) KEY_BLOCK_SIZE=8")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}
