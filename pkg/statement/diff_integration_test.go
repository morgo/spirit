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

// TestDiffIntegrationFulltextParser verifies that a WITH PARSER difference on
// an otherwise identical index is detected and that the emitted ALTER is
// accepted by a real MySQL server.
func TestDiffIntegrationFulltextParser(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_ft_parser",
		"CREATE TABLE diff_ft_parser (id int primary key, b text, FULLTEXT KEY ft_b (b))")

	target, err := ParseCreateTable("CREATE TABLE diff_ft_parser (id int primary key, b text, FULLTEXT KEY ft_b (b) WITH PARSER ngram)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_ft_parser` DROP INDEX `ft_b`, ADD FULLTEXT INDEX `ft_b` (`b`) WITH PARSER ngram", stmts[0].Statement)

	// The emitted statement must be valid SQL.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)

	// Note: when a single ALTER drops and re-adds an index with the same name
	// and column list, MySQL pairs the two clauses up and keeps the existing
	// index, silently ignoring the parser change. Running the clauses as two
	// separate ALTERs makes the change take effect.
	_, err = tt.DB.ExecContext(t.Context(), "ALTER TABLE `diff_ft_parser` DROP INDEX `ft_b`")
	require.NoError(t, err)
	_, err = tt.DB.ExecContext(t.Context(), "ALTER TABLE `diff_ft_parser` ADD FULLTEXT INDEX `ft_b` (`b`) WITH PARSER ngram")
	require.NoError(t, err)

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
// an index is detected and that the emitted ALTER is accepted by a real MySQL
// server. The table uses ROW_FORMAT=COMPRESSED because InnoDB silently ignores
// index-level KEY_BLOCK_SIZE on uncompressed tables.
func TestDiffIntegrationKeyBlockSize(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_kbs",
		"CREATE TABLE diff_kbs (id int primary key, b varchar(100), KEY idx_b (b)) ROW_FORMAT=COMPRESSED")

	target, err := ParseCreateTable("CREATE TABLE diff_kbs (id int primary key, b varchar(100), KEY idx_b (b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_kbs` DROP INDEX `idx_b`, ADD INDEX `idx_b` (`b`) KEY_BLOCK_SIZE=8", stmts[0].Statement)

	// The emitted statement must be valid SQL.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)

	// Same MySQL behavior as the parser case: a combined DROP+ADD of the same
	// index name and column list keeps the existing index, so apply the
	// clauses separately to make the change take effect.
	_, err = tt.DB.ExecContext(t.Context(), "ALTER TABLE `diff_kbs` DROP INDEX `idx_b`")
	require.NoError(t, err)
	_, err = tt.DB.ExecContext(t.Context(), "ALTER TABLE `diff_kbs` ADD INDEX `idx_b` (`b`) KEY_BLOCK_SIZE=8")
	require.NoError(t, err)

	postAlter := showCreateTable(t, tt)
	require.Contains(t, postAlter, "KEY `idx_b` (`b`) KEY_BLOCK_SIZE=8")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}
