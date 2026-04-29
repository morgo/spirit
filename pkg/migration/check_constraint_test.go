package migration

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for issue #351: Add more tests for CHECK constraints.
// CHECK constraints have a schema-level namespace, so creating two tables with
// the same named CHECK constraint would fail. Spirit uses CREATE TABLE .. LIKE
// which auto-renames constraints, avoiding this issue. These tests verify that
// behavior is correct and constraints are preserved after migration.

// TestCheckConstraintNamed tests migration of a table with named CHECK constraints.
// This is the scenario from issue #351 where named constraints like
// CONSTRAINT c2_positive CHECK (c2 > 0) would conflict if CREATE TABLE .. LIKE
// didn't auto-rename them.
func TestCheckConstraintNamed(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chk_named, _chk_named_new`)
	table := `CREATE TABLE chk_named (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT DEFAULT NULL,
		c2 INT DEFAULT NULL,
		c3 INT DEFAULT NULL,
		CONSTRAINT chk_named_c1pos CHECK (c1 > 0),
		CONSTRAINT chk_named_c2pos CHECK (c2 > 0),
		CONSTRAINT chk_named_c3rng CHECK (c3 BETWEEN 1 AND 100)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 50), (10, 20, 99), (5, 5, 1)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "chk_named",
		Alter:    "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_named").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify CHECK constraints are still enforced after migration.
	// Inserting a row that violates chk_named_c1pos should fail.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (-1, 1, 50)")
	assert.Error(t, err, "CHECK constraint on c1 should reject negative values")

	// Inserting a row that violates chk_named_c2pos should fail.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, -1, 50)")
	assert.Error(t, err, "CHECK constraint on c2 should reject negative values")

	// Inserting a row that violates chk_named_c3rng should fail.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 200)")
	assert.Error(t, err, "CHECK constraint on c3 should reject out-of-range values")

	// Inserting a valid row should succeed.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 50)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintUnnamed tests migration of a table with unnamed (auto-generated)
// CHECK constraints. MySQL auto-generates names like <table>_chk_1, <table>_chk_2, etc.
func TestCheckConstraintUnnamed(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chk_unnamed, _chk_unnamed_new`)
	table := `CREATE TABLE chk_unnamed (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT CHECK (c1 > 10),
		c2 INT CHECK (c2 > 0),
		CHECK (c1 <> c2)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO chk_unnamed (c1, c2) VALUES (11, 1), (20, 2), (100, 3)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "chk_unnamed",
		Alter:    "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_unnamed").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify CHECK constraints are still enforced.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (5, 1)")
	assert.Error(t, err, "CHECK constraint should reject c1 <= 10")

	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, -1)")
	assert.Error(t, err, "CHECK constraint should reject c2 <= 0")

	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, 11)")
	assert.Error(t, err, "CHECK constraint should reject c1 == c2")
}

// TestCheckConstraintMixed tests migration of a table with both named and unnamed
// CHECK constraints — the exact scenario from issue #351's example.
func TestCheckConstraintMixed(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chk_mixed, _chk_mixed_new`)
	// This is based on the table definition from issue #351,
	// with constraint names prefixed to avoid schema-level conflicts.
	table := `CREATE TABLE chk_mixed (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		CHECK (c1 <> c2),
		c1 INT CHECK (c1 > 10),
		c2 INT CONSTRAINT chk_mixed_c2pos CHECK (c2 > 0),
		c3 INT CHECK (c3 < 100),
		CONSTRAINT chk_mixed_c1nz CHECK (c1 <> 0),
		CHECK (c1 > c3)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 10), (20, 5, 15), (80, 3, 70)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "chk_mixed",
		Alter:    "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_mixed").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify all CHECK constraints are enforced after migration.
	// Violate c1 > 10
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (5, 1, 1)")
	assert.Error(t, err, "CHECK constraint should reject c1 <= 10")

	// Violate c2 > 0
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, -1, 10)")
	assert.Error(t, err, "CHECK constraint should reject c2 <= 0")

	// Violate c3 < 100
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 200)")
	assert.Error(t, err, "CHECK constraint should reject c3 >= 100")

	// Violate c1 <> 0
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (0, 1, 10)")
	assert.Error(t, err, "CHECK constraint should reject c1 == 0")

	// Violate c1 <> c2
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 50, 10)")
	assert.Error(t, err, "CHECK constraint should reject c1 == c2")

	// Violate c1 > c3
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 90)")
	assert.Error(t, err, "CHECK constraint should reject c1 <= c3")

	// Valid row should succeed.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 10)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintWithDML tests migration of a table with CHECK constraints
// while concurrent DML is happening. This ensures the binlog replay path
// correctly handles rows that are subject to CHECK constraints.
func TestCheckConstraintWithDML(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chk_dml, _chk_dml_new, _chk_dml_chkpnt`)
	table := `CREATE TABLE chk_dml (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_dml_valpos CHECK (val > 0),
		CONSTRAINT chk_dml_valrng CHECK (val < 10000)
	)`
	testutils.RunSQL(t, table)

	// Insert enough data so the copy phase takes a measurable amount of time.
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) VALUES (1)`)
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 2
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 4
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 8
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 16
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 32
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 64
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 128
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 256
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 512
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 1024
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 2048
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT val FROM chk_dml`)                // 4096
	testutils.RunSQL(t, `INSERT INTO chk_dml (val) SELECT a.val FROM chk_dml a LIMIT 2000`) // ~6096

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "chk_dml",
		Alter:           "ENGINE=InnoDB",
	})
	require.NoError(t, err)

	dmlDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(dmlDB)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		for i := 0; i < 100; i++ {
			if ctx.Err() != nil {
				return
			}
			// Insert valid rows (val must be > 0 and < 10000).
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO chk_dml (val) VALUES (?)`, i+1)
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE chk_dml SET val = %d WHERE id = %d`, (i%9999)+1, i+1))
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`DELETE FROM chk_dml WHERE id = %d`, i+50))
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())
	require.NoError(t, migrationErr)

	// Verify CHECK constraints are still enforced after migration with DML.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (0)")
	assert.Error(t, err, "CHECK constraint should reject val <= 0")

	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (10000)")
	assert.Error(t, err, "CHECK constraint should reject val >= 10000")

	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (5000)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintAddColumn tests adding a column to a table that has
// CHECK constraints. This verifies that the ALTER is applied correctly
// on top of the shadow table created via CREATE TABLE .. LIKE.
func TestCheckConstraintAddColumn(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chk_addcol, _chk_addcol_new`)
	table := `CREATE TABLE chk_addcol (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_addcol_valchk CHECK (val > 0)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO chk_addcol (val) VALUES (1), (2), (3)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "chk_addcol",
		Alter:    "ADD COLUMN extra VARCHAR(100) DEFAULT 'hello', ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact and new column exists.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_addcol WHERE extra = 'hello'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify CHECK constraint is still enforced.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (0)")
	assert.Error(t, err, "CHECK constraint should still reject val <= 0 after adding column")

	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (1)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintAddNewCheckConstraint tests adding a new CHECK constraint
// to a table that already has CHECK constraints.
func TestCheckConstraintAddNewCheckConstraint(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chk_addchk, _chk_addchk_new`)
	table := `CREATE TABLE chk_addchk (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT NOT NULL,
		c2 INT NOT NULL,
		CONSTRAINT chk_addchk_c1pos CHECK (c1 > 0)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO chk_addchk (c1, c2) VALUES (1, 50), (10, 50), (5, 50)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "chk_addchk",
		Alter:    "ADD CONSTRAINT chk_addchk_c2rng CHECK (c2 BETWEEN 1 AND 100)",
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())

	// Verify both old and new CHECK constraints are enforced.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Violate original constraint c1 > 0
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (0, 50)")
	assert.Error(t, err, "original CHECK constraint should still reject c1 <= 0")

	// Violate new constraint chk_addchk_c2rng: c2 BETWEEN 1 AND 100
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 200)")
	assert.Error(t, err, "new CHECK constraint should reject c2 > 100")

	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 0)")
	assert.Error(t, err, "new CHECK constraint should reject c2 < 1")

	// Valid row should succeed.
	_, err = db.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 50)")
	assert.NoError(t, err, "valid row should be accepted")
}
