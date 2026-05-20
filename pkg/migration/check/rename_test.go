package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

func TestRename(t *testing.T) {
	// Table rename is always blocked
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 RENAME TO newtablename")[0],
	}
	err := renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "table renames are not supported")

	// Non-PK column rename via RENAME COLUMN is now allowed
	r.Statement = statement.MustNew("ALTER TABLE t1 RENAME COLUMN c1 TO c2")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Non-PK column rename via CHANGE COLUMN is now allowed
	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c2 VARCHAR(100)")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// CHANGE COLUMN without rename (same name) is still allowed
	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c1 VARCHAR(100)")[0] //nolint: dupword
	err = renameCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Non-rename ALTER is still allowed
	r.Statement = statement.MustNew("ALTER TABLE t1 ADD INDEX (anothercol)")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

func TestRenamePKColumnBlocked(t *testing.T) {
	ti := table.NewTableInfo(nil, "test", "t1")
	ti.KeyColumns = []string{"id"}

	// Renaming a PK column via RENAME COLUMN is blocked
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 RENAME COLUMN id TO new_id")[0],
		Table:     ti,
	}
	err := renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "renaming primary key column")

	// Renaming a PK column via CHANGE COLUMN is blocked
	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE id new_id BIGINT")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "renaming primary key column")

	// Renaming a non-PK column is allowed even when table has PK info
	r.Statement = statement.MustNew("ALTER TABLE t1 RENAME COLUMN name TO full_name")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

// TestRenameBlockedWithBufferedCopier covers the gate on column
// renames when the buffered copier (--buffered) is opted in. The
// always-buffered binlog subscription is not the concern — see the
// renameCheck doc comment for the generated/virtual-column edge cases
// that motivate the gate.
func TestRenameBlockedWithBufferedCopier(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 RENAME COLUMN c1 TO c2")[0],
		Buffered:  true,
	}
	err := renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "buffered copier")

	// CHANGE COLUMN rename is also blocked under the buffered copier.
	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c2 VARCHAR(100)")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "buffered copier")

	// CHANGE COLUMN without a rename is fine under the buffered copier.
	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c1 VARCHAR(100)")[0] //nolint: dupword
	require.NoError(t, renameCheck(t.Context(), r, slog.Default()))
}

func TestRenameColumnNameOverlap(t *testing.T) {
	// Pattern 1: RENAME COLUMN c1 TO n1, ADD COLUMN c1 ...
	// The old name is reused by a new column — data corruption risk.
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 RENAME COLUMN c1 TO n1, ADD COLUMN c1 VARCHAR(100)")[0],
	}
	err := renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "conflicts with added column")
	require.ErrorContains(t, err, "old column name is reused")

	// Same pattern via CHANGE COLUMN
	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 n1 VARCHAR(200), ADD COLUMN c1 VARCHAR(100)")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "conflicts with added column")

	// Pattern 2: RENAME COLUMN a TO c, ADD COLUMN c ...
	// The new name collides with an added column.
	r.Statement = statement.MustNew("ALTER TABLE t1 RENAME COLUMN a TO c, ADD COLUMN c INT")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "conflicts with added column")
	require.ErrorContains(t, err, "new column name collides")

	// Safe: rename + add a different column (no overlap)
	r.Statement = statement.MustNew("ALTER TABLE t1 RENAME COLUMN c1 TO n1, ADD COLUMN c2 VARCHAR(100)")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Safe: rename + drop column (no overlap)
	r.Statement = statement.MustNew("ALTER TABLE t1 RENAME COLUMN c1 TO n1, DROP COLUMN c2")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}
