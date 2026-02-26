package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetReorderCheck(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS setchk`)
	testutils.RunSQL(t, `CREATE TABLE setchk (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		flags SET('read', 'write', 'execute') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "setchk")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + reorder SET: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk MODIFY COLUMN flags SET('write', 'read', 'execute') NOT NULL")[0],
		Buffered:  true,
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe SET value reorder")
	assert.ErrorContains(t, err, "checksum")

	// Unbuffered + reorder SET: should also fail (checksum always fails for SET reorder)
	r.Buffered = false
	err = setReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe SET value reorder")

	// Buffered + append SET (safe): should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk MODIFY COLUMN flags SET('read', 'write', 'execute', 'admin') NOT NULL")[0],
		Buffered:  true,
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)

	// Unbuffered + append SET (safe): should pass
	r.Buffered = false
	err = setReorderCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)

	// Buffered + CHANGE COLUMN with reorder: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk CHANGE COLUMN flags flags SET('execute', 'read', 'write') NOT NULL")[0],
		Buffered:  true,
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe SET value reorder")

	// Buffered + remove SET value: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk MODIFY COLUMN flags SET('read', 'execute') NOT NULL")[0],
		Buffered:  true,
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe SET value reorder")

	// Buffered + non-SET column modification: should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk ADD COLUMN name VARCHAR(255)")[0],
		Buffered:  true,
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)
}
