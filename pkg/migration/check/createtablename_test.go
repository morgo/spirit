package check

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestCreateTableNameCheck(t *testing.T) {
	// Non-CREATE TABLE statements should pass (no-op)
	alterStmt := statement.MustNew("ALTER TABLE t1 ADD COLUMN a INT")[0]
	r := Resources{
		Statement: alterStmt,
	}
	assert.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// Nil statement should pass (no-op)
	r = Resources{Statement: nil}
	assert.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// CREATE TABLE with a short name should pass
	shortStmt := statement.MustNew("CREATE TABLE short_name (id INT NOT NULL PRIMARY KEY)")[0]
	r = Resources{Statement: shortStmt}
	assert.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// CREATE TABLE with exactly 64 characters should pass
	name64 := strings.Repeat("x", 64)
	stmt64 := statement.MustNew("CREATE TABLE `" + name64 + "` (id INT NOT NULL PRIMARY KEY)")[0]
	r = Resources{Statement: stmt64}
	assert.NoError(t, createTableNameCheck(t.Context(), r, slog.Default()))

	// CREATE TABLE with more than 64 characters should fail
	name65 := strings.Repeat("y", utils.MaxTableNameLength+1)
	stmt65 := statement.MustNew("CREATE TABLE `" + name65 + "` (id INT NOT NULL PRIMARY KEY)")[0]
	r = Resources{Statement: stmt65}
	err := createTableNameCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "exceeds MySQL's maximum length of 64 characters")
}
