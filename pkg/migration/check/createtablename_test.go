package check

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestCreateTableNameCheck(t *testing.T) {
	// Non-CREATE TABLE statements should pass (no-op)
	alterStmt := statement.MustNew("ALTER TABLE t1 ADD COLUMN a INT")[0]
	r := Resources{
		Statement: alterStmt,
	}
	require.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// Nil statement should pass (no-op)
	r = Resources{Statement: nil}
	require.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// CREATE TABLE with a short name should pass
	shortStmt := statement.MustNew("CREATE TABLE short_name (id INT NOT NULL PRIMARY KEY)")[0]
	r = Resources{Statement: shortStmt}
	require.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// CREATE TABLE with exactly the max manageable length should pass
	nameMax := strings.Repeat("x", MaxMigratableTableNameLength)
	stmtMax := statement.MustNew("CREATE TABLE `" + nameMax + "` (id INT NOT NULL PRIMARY KEY)")[0]
	r = Resources{Statement: stmtMax}
	require.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// CREATE TABLE with one character over the max should fail
	nameTooLong := strings.Repeat("y", MaxMigratableTableNameLength+1)
	stmtTooLong := statement.MustNew("CREATE TABLE `" + nameTooLong + "` (id INT NOT NULL PRIMARY KEY)")[0]
	r = Resources{Statement: stmtTooLong}
	err := createTableNameCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("exceeds the maximum length of %d characters that Spirit can manage", MaxMigratableTableNameLength))
}
