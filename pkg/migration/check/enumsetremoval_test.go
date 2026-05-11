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
	"github.com/stretchr/testify/require"
)

func TestEnumSetRemovalCheckEnumToVarchar(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + ENUM→VARCHAR: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval MODIFY COLUMN status VARCHAR(255) NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM to non-ENUM")

	// Unbuffered + ENUM→VARCHAR: should also fail — bufferedMap is now the
	// binlog replay path for memory-comparable PKs regardless of the copy mode.
	r.Buffered = false
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM to non-ENUM")
}

func TestEnumSetRemovalCheckEnumToText(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval2`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval2")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + ENUM→TEXT: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval2 MODIFY COLUMN status TEXT NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM to non-ENUM")
}

func TestEnumSetRemovalCheckEnumToInt(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval3`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval3 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval3")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + ENUM→INT: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval3 MODIFY COLUMN status INT NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM to non-ENUM")
}

func TestEnumSetRemovalCheckSetToVarchar(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS setremoval`)
	testutils.RunSQL(t, `CREATE TABLE setremoval (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		flags SET('read', 'write', 'execute') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "setremoval")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + SET→VARCHAR: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setremoval MODIFY COLUMN flags VARCHAR(255) NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET to non-SET")

	// Unbuffered + SET→VARCHAR: should also fail — bufferedMap is now the
	// binlog replay path for memory-comparable PKs regardless of the copy mode.
	r.Buffered = false
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET to non-SET")
}

func TestEnumSetRemovalCheckChangeColumn(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval_change`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval_change (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval_change")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + CHANGE COLUMN ENUM→VARCHAR: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval_change CHANGE COLUMN status status VARCHAR(255) NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM to non-ENUM")
}

func TestEnumSetRemovalCheckEnumToEnum(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval_safe`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval_safe (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval_safe")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + ENUM→ENUM (append): should pass (handled by enumReorderCheck, not this check)
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval_safe MODIFY COLUMN status ENUM('active', 'inactive', 'pending') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

func TestEnumSetRemovalCheckNonEnumColumn(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval_nochange`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval_nochange (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval_nochange")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + VARCHAR→TEXT: should pass (not an ENUM/SET removal)
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval_nochange MODIFY COLUMN name TEXT NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

func TestEnumSetRemovalCheckEnumToSet(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumtoset`)
	testutils.RunSQL(t, `CREATE TABLE enumtoset (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumtoset")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + ENUM→SET: should fail (ordinal vs bitmask mismatch)
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumtoset MODIFY COLUMN status SET('active', 'inactive') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM to SET")

	// Unbuffered + ENUM→SET: should also fail — bufferedMap is now the
	// binlog replay path for memory-comparable PKs regardless of the copy mode.
	r.Buffered = false
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM to SET")
}

func TestEnumSetRemovalCheckSetToEnum(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS settoenum`)
	testutils.RunSQL(t, `CREATE TABLE settoenum (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		flags SET('read', 'write') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "settoenum")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Buffered + SET→ENUM: should fail (bitmask vs ordinal mismatch)
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE settoenum MODIFY COLUMN flags ENUM('read', 'write') NOT NULL")[0],
		Buffered:  true,
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET to ENUM")

	// Unbuffered + SET→ENUM: should also fail — bufferedMap is now the
	// binlog replay path for memory-comparable PKs regardless of the copy mode.
	r.Buffered = false
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET to ENUM")
}
