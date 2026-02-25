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

func TestEnumReorderCheck(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumchk`)
	testutils.RunSQL(t, `CREATE TABLE enumchk (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumchk")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + reorder ENUM: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('pending', 'active', 'inactive') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe ENUM value reorder")
	assert.ErrorContains(t, err, "buffered mode")

	// Unbuffered + reorder ENUM: should pass (unbuffered is safe for ENUM)
	r.Buffered = false
	err = enumReorderCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)

	// Buffered + append ENUM (safe): should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active', 'inactive', 'pending', 'archived') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)

	// Buffered + insert new ENUM value in middle: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active', 'suspended', 'inactive', 'pending') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe ENUM value reorder")

	// Buffered + CHANGE COLUMN with reorder: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk CHANGE COLUMN status status ENUM('pending', 'active', 'inactive') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe ENUM value reorder")

	// Buffered + remove ENUM value: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active', 'pending') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsafe ENUM value reorder")

	// Buffered + non-ENUM column modification: should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk ADD COLUMN name VARCHAR(255)")[0],
		Buffered:  true,
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)
}
