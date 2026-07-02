package statement

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// showCreateTable (defined in diff_column_options_test.go) takes (t, db,
// tableName); call it as showCreateTable(t, tt.DB, tt.Name) below.

// TestDiffIntegrationRemoveTableComment verifies that the empty COMMENT
// clause emitted when a table comment is removed actually clears the comment
// on a real MySQL server, and that a re-diff afterwards converges to nil.
func TestDiffIntegrationRemoveTableComment(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_comment_removal",
		"CREATE TABLE diff_comment_removal (a int) COMMENT='old comment'")

	target, err := ParseCreateTable("CREATE TABLE diff_comment_removal (a int)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_comment_removal` COMMENT=''", stmts[0].Statement)

	// Apply the diff and verify the comment is really gone.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
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

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
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
	postAlter := showCreateTable(t, tt.DB, tt.Name)
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

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_ft_rebuild` DROP INDEX `ft_b`, ADD FULLTEXT INDEX `ft_b` (`b`, `c`) WITH PARSER ngram", stmts[0].Statement)

	// The column list changed, so MySQL really rebuilds the index and the
	// parser must survive the rebuild.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "FULLTEXT KEY `ft_b` (`b`,`c`)")
	require.Contains(t, postAlter, "WITH PARSER `ngram`")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationInlineUnique verifies that a desired schema written with
// a column-level UNIQUE (`c int unique`) diffs cleanly against the live
// canonical form MySQL reports (`c int` + `UNIQUE KEY c (c)`), and that when
// the unique index is missing the emitted DDL creates it exactly once and the
// re-diff converges.
//
// Regression: inline UNIQUE only existed as Column.Unique and was invisible to
// diffIndexes, so this diff used to emit
// `MODIFY COLUMN c int(11) NULL, DROP INDEX c` — silently dropping the live
// uniqueness constraint — and never converged (a MODIFY COLUMN cannot
// re-express UNIQUE).
func TestDiffIntegrationInlineUnique(t *testing.T) {
	// Create the table from the inline form; MySQL canonicalizes it.
	tt := testutils.NewTestTable(t, "diff_inline_unique",
		"CREATE TABLE diff_inline_unique (id int primary key, c int unique)")

	desired, err := ParseCreateTable("CREATE TABLE diff_inline_unique (id int primary key, c int unique)")
	require.NoError(t, err)

	// The server names the canonicalized unique index after the column.
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "UNIQUE KEY `c` (`c`)")

	source, err := ParseCreateTable(live)
	require.NoError(t, err)

	// Headline regression: live-canonical vs desired-inline must be a no-op.
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)

	// Drop the unique index out from under the desired schema; the diff must
	// re-add it, exactly once.
	_, err = tt.DB.ExecContext(t.Context(), "ALTER TABLE diff_inline_unique DROP INDEX c")
	require.NoError(t, err)

	source, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_inline_unique` ADD UNIQUE INDEX `c` (`c`)", stmts[0].Statement)

	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)

	// Convergence: the index is back under its canonical name and a re-diff
	// yields nil.
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "UNIQUE KEY `c` (`c`)")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationInlineUniqueNameCollision verifies the synthesized names
// follow the server's declaration-order naming when an inline unique and an
// unnamed table-level key share a base name: the inline unique on c claims
// `c` and the unnamed KEY (c, d) is pushed to `c_2`. The live canonical form
// must diff as a no-op against the original inline form.
func TestDiffIntegrationInlineUniqueNameCollision(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_inline_uniq_col",
		"CREATE TABLE diff_inline_uniq_col (id int primary key, c int unique, d int, key (c, d))")

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "UNIQUE KEY `c` (`c`)")
	require.Contains(t, live, "KEY `c_2` (`c`,`d`)",
		"server is expected to push the unnamed key past the inline unique's name")

	desired, err := ParseCreateTable("CREATE TABLE diff_inline_uniq_col (id int primary key, c int unique, d int, key (c, d))")
	require.NoError(t, err)
	source, err := ParseCreateTable(live)
	require.NoError(t, err)

	stmts, err := source.Diff(desired, nil)
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

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
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
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "KEY `idx_b` (`b`) KEY_BLOCK_SIZE=8")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationForeignKeyNoAction verifies against a real MySQL server
// that a desired schema spelling out ON DELETE NO ACTION / ON UPDATE NO ACTION
// converges with the live table. MySQL omits NO ACTION from SHOW CREATE TABLE
// output (it is the default action, a synonym for RESTRICT in InnoDB), so
// before the parse-time normalization this produced the same DROP+ADD FOREIGN
// KEY on every diff — an ALTER that never changed SHOW CREATE output.
func TestDiffIntegrationForeignKeyNoAction(t *testing.T) {
	// Parent must be created first; TestTable cleanup is LIFO so the child
	// (created last) is dropped before the parent.
	_ = testutils.NewTestTable(t, "diff_fkna_parent",
		"CREATE TABLE diff_fkna_parent (id int primary key)")
	tt := testutils.NewTestTable(t, "diff_fkna_child",
		"CREATE TABLE diff_fkna_child (id int primary key, pid int, KEY fk_fkna_pid (pid), "+
			"CONSTRAINT fk_fkna_pid FOREIGN KEY (pid) REFERENCES diff_fkna_parent (id) ON DELETE NO ACTION ON UPDATE NO ACTION)")

	// Document the server behavior this fix depends on: SHOW CREATE TABLE
	// omits NO ACTION.
	live := showCreateTable(t, tt.DB, tt.Name)
	require.NotContains(t, live, "NO ACTION")

	desired, err := ParseCreateTable(
		"CREATE TABLE diff_fkna_child (id int primary key, pid int, KEY fk_fkna_pid (pid), " +
			"CONSTRAINT fk_fkna_pid FOREIGN KEY (pid) REFERENCES diff_fkna_parent (id) ON DELETE NO ACTION ON UPDATE NO ACTION)")
	require.NoError(t, err)

	source, err := ParseCreateTable(live)
	require.NoError(t, err)

	// The explicit NO ACTION spelling converges with the live table.
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "explicit NO ACTION must converge with live schema")

	// A genuine action change (NO ACTION -> CASCADE) still produces a diff,
	// and applying it converges. The desired FK uses a different constraint
	// name because MySQL rejects a same-name DROP FOREIGN KEY + ADD
	// CONSTRAINT within a single ALTER (Error 1826).
	desiredCascade, err := ParseCreateTable(
		"CREATE TABLE diff_fkna_child (id int primary key, pid int, KEY fk_fkna_pid (pid), " +
			"CONSTRAINT fk_fkna_pid2 FOREIGN KEY (pid) REFERENCES diff_fkna_parent (id) ON DELETE CASCADE)")
	require.NoError(t, err)
	stmts, err = source.Diff(desiredCascade, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_fkna_child` DROP FOREIGN KEY `fk_fkna_pid`, ADD CONSTRAINT `fk_fkna_pid2` FOREIGN KEY (`pid`) REFERENCES `diff_fkna_parent` (`id`) ON DELETE CASCADE", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	source, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = source.Diff(desiredCascade, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationForeignKeyRestrict is the regression guard for the
// NO ACTION normalization: RESTRICT has identical semantics in InnoDB but IS
// printed by SHOW CREATE TABLE, so it must round-trip verbatim — neither
// normalized away nor producing a spurious diff.
func TestDiffIntegrationForeignKeyRestrict(t *testing.T) {
	_ = testutils.NewTestTable(t, "diff_fkr_parent",
		"CREATE TABLE diff_fkr_parent (id int primary key)")
	tt := testutils.NewTestTable(t, "diff_fkr_child",
		"CREATE TABLE diff_fkr_child (id int primary key, pid int, KEY fk_fkr_pid (pid), "+
			"CONSTRAINT fk_fkr_pid FOREIGN KEY (pid) REFERENCES diff_fkr_parent (id) ON DELETE RESTRICT ON UPDATE RESTRICT)")

	// Document the server behavior: RESTRICT is printed (unlike NO ACTION).
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "ON DELETE RESTRICT ON UPDATE RESTRICT")

	desired, err := ParseCreateTable(
		"CREATE TABLE diff_fkr_child (id int primary key, pid int, KEY fk_fkr_pid (pid), " +
			"CONSTRAINT fk_fkr_pid FOREIGN KEY (pid) REFERENCES diff_fkr_parent (id) ON DELETE RESTRICT ON UPDATE RESTRICT)")
	require.NoError(t, err)

	source, err := ParseCreateTable(live)
	require.NoError(t, err)
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "RESTRICT must round-trip unchanged")
}

// TestDiffIntegrationCheckEnforcement verifies against a real MySQL server
// that the [NOT] ENFORCED state of CHECK constraints round-trips through
// SHOW CREATE TABLE (which renders it as /*!80016 NOT ENFORCED */), that
// enforcement flips are applied in place with ALTER CHECK, and that a
// NOT ENFORCED check re-added for an expression change stays NOT ENFORCED
// instead of silently re-enabling enforcement.
func TestDiffIntegrationCheckEnforcement(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_chk_enf",
		"CREATE TABLE diff_chk_enf (id int primary key, age int, "+
			"CONSTRAINT chk_dce_age CHECK (age >= 0) NOT ENFORCED)")

	// Document the server's canonical rendering.
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "/*!80016 NOT ENFORCED */")

	// Desired NOT ENFORCED vs live NOT ENFORCED converges.
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_chk_enf (id int primary key, age int, " +
			"CONSTRAINT chk_dce_age CHECK (age >= 0) NOT ENFORCED)")
	require.NoError(t, err)
	source, err := ParseCreateTable(live)
	require.NoError(t, err)
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "NOT ENFORCED on both sides must converge")

	// A row violating the (unenforced) check: flipping enforcement ON must
	// surface MySQL's validation error rather than silently passing, which
	// also proves ALTER CHECK ... ENFORCED validates existing rows just as
	// an enforced ADD CONSTRAINT would.
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO diff_chk_enf VALUES (1, -5)")
	require.NoError(t, err)

	desiredEnforced, err := ParseCreateTable(
		"CREATE TABLE diff_chk_enf (id int primary key, age int, " +
			"CONSTRAINT chk_dce_age CHECK (age >= 0))")
	require.NoError(t, err)
	stmts, err = source.Diff(desiredEnforced, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_chk_enf` ALTER CHECK `chk_dce_age` ENFORCED", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.ErrorContains(t, err, "chk_dce_age", "enforcing over violating rows must fail")

	// Remove the violating row; the same ALTER now applies and converges.
	_, err = tt.DB.ExecContext(t.Context(), "DELETE FROM diff_chk_enf")
	require.NoError(t, err)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.NotContains(t, postAlter, "NOT ENFORCED")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desiredEnforced, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)

	// Flip back to NOT ENFORCED: exactly one in-place ALTER, then converges.
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_chk_enf` ALTER CHECK `chk_dce_age` NOT ENFORCED", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter = showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "/*!80016 NOT ENFORCED */")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)

	// Expression change on a NOT ENFORCED check: the re-add must carry
	// NOT ENFORCED through to the server (previously it silently flipped
	// enforcement back on).
	desiredNewExpr, err := ParseCreateTable(
		"CREATE TABLE diff_chk_enf (id int primary key, age int, " +
			"CONSTRAINT chk_dce_age CHECK (age >= 18) NOT ENFORCED)")
	require.NoError(t, err)
	stmts, err = source.Diff(desiredNewExpr, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_chk_enf` DROP CHECK `chk_dce_age`, ADD CONSTRAINT `chk_dce_age` CHECK (`age`>=18) NOT ENFORCED", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter = showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "/*!80016 NOT ENFORCED */")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desiredNewExpr, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationDescIndex verifies that changing an index key part from
// ascending to descending (MySQL 8.0+) is detected by Diff(), that the emitted
// combined `DROP INDEX k, ADD INDEX k (a DESC)` really rebuilds the index on a
// real MySQL server (unlike the option-only cases, MySQL does not no-op a
// direction change), and that a re-diff afterwards converges to nil.
//
// This is a regression test: the DESC modifier used to be dropped during
// parsing, so KEY k (a) and KEY k (a DESC) diffed as equal and restored
// descending indexes silently became ascending.
func TestDiffIntegrationDescIndex(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_desc_idx",
		"CREATE TABLE diff_desc_idx (id int primary key, a int, b int, KEY k (a, b))")

	target, err := ParseCreateTable("CREATE TABLE diff_desc_idx (id int primary key, a int, b int, KEY k (a DESC, b))")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_desc_idx` DROP INDEX `k`, ADD INDEX `k` (`a` DESC, `b`)", stmts[0].Statement)

	// Execute the emitted statement exactly as the Runner would, and verify
	// the index really became descending.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "KEY `k` (`a` DESC,`b`)")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}
