package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKey(t *testing.T) {
	r := Resources{
		Runtime: Migration,
		Change:  Change{Statement: statement.MustNew("ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)")[0]},
	}
	err := primaryKeyCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // drop primary key

	r.Change.Statement = statement.MustNew("ALTER TABLE t1 ADD INDEX (anothercol)")[0]
	err = primaryKeyCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // safe modification
}
