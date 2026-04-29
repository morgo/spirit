package migration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
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
	tt := testutils.NewTestTable(t, "chk_named",
		`CREATE TABLE chk_named (
			id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			c1 INT DEFAULT NULL,
			c2 INT DEFAULT NULL,
			c3 INT DEFAULT NULL,
			CONSTRAINT chk_named_c1pos CHECK (c1 > 0),
			CONSTRAINT chk_named_c2pos CHECK (c2 > 0),
			CONSTRAINT chk_named_c3rng CHECK (c3 BETWEEN 1 AND 100)
		)`)
	tt.SeedRows(t, "INSERT INTO chk_named (c1, c2, c3) SELECT 1, 1, 50", 3)

	m := NewTestRunner(t, "chk_named", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_named").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	// Verify CHECK constraints are still enforced after migration.
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (-1, 1, 50)")
	assert.Error(t, err, "CHECK constraint on c1 should reject negative values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, -1, 50)")
	assert.Error(t, err, "CHECK constraint on c2 should reject negative values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 200)")
	assert.Error(t, err, "CHECK constraint on c3 should reject out-of-range values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 50)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintUnnamed tests migration of a table with unnamed (auto-generated)
// CHECK constraints. MySQL auto-generates names like <table>_chk_1, <table>_chk_2, etc.
func TestCheckConstraintUnnamed(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_unnamed",
		`CREATE TABLE chk_unnamed (
			id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			c1 INT CHECK (c1 > 10),
			c2 INT CHECK (c2 > 0),
			CHECK (c1 <> c2)
		)`)
	tt.SeedRows(t, "INSERT INTO chk_unnamed (c1, c2) SELECT 11, 1", 3)

	m := NewTestRunner(t, "chk_unnamed", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_unnamed").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	// Verify CHECK constraints are still enforced.
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (5, 1)")
	assert.Error(t, err, "CHECK constraint should reject c1 <= 10")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, -1)")
	assert.Error(t, err, "CHECK constraint should reject c2 <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, 11)")
	assert.Error(t, err, "CHECK constraint should reject c1 == c2")
}

// TestCheckConstraintMixed tests migration of a table with both named and unnamed
// CHECK constraints — the exact scenario from issue #351's example.
func TestCheckConstraintMixed(t *testing.T) {
	t.Parallel()
	// Based on the table definition from issue #351,
	// with constraint names prefixed to avoid schema-level conflicts.
	tt := testutils.NewTestTable(t, "chk_mixed",
		`CREATE TABLE chk_mixed (
			id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			CHECK (c1 <> c2),
			c1 INT CHECK (c1 > 10),
			c2 INT CONSTRAINT chk_mixed_c2pos CHECK (c2 > 0),
			c3 INT CHECK (c3 < 100),
			CONSTRAINT chk_mixed_c1nz CHECK (c1 <> 0),
			CHECK (c1 > c3)
		)`)
	tt.SeedRows(t, "INSERT INTO chk_mixed (c1, c2, c3) SELECT 50, 1, 10", 3)

	m := NewTestRunner(t, "chk_mixed", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_mixed").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	// Verify all CHECK constraints are enforced after migration.
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (5, 1, 1)")
	assert.Error(t, err, "CHECK constraint should reject c1 <= 10")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, -1, 10)")
	assert.Error(t, err, "CHECK constraint should reject c2 <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 200)")
	assert.Error(t, err, "CHECK constraint should reject c3 >= 100")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (0, 1, 10)")
	assert.Error(t, err, "CHECK constraint should reject c1 == 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 50, 10)")
	assert.Error(t, err, "CHECK constraint should reject c1 == c2")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 90)")
	assert.Error(t, err, "CHECK constraint should reject c1 <= c3")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 10)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintWithDML tests migration of a table with CHECK constraints
// while concurrent DML is happening. This ensures the binlog replay path
// correctly handles rows that are subject to CHECK constraints.
func TestCheckConstraintWithDML(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_dml",
		`CREATE TABLE chk_dml (
			id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			val INT NOT NULL,
			CONSTRAINT chk_dml_valpos CHECK (val > 0),
			CONSTRAINT chk_dml_valrng CHECK (val < 10000)
		)`)
	tt.SeedRows(t, "INSERT INTO chk_dml (val) SELECT 1", 4096)

	m := NewTestRunner(t, "chk_dml", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
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
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO chk_dml (val) VALUES (?)`, i+1)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE chk_dml SET val = %d WHERE id = %d`, (i%9999)+1, i+1))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`DELETE FROM chk_dml WHERE id = %d`, i+50))
		}
	})

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())
	require.NoError(t, migrationErr)

	// Verify CHECK constraints are still enforced after migration with DML.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (0)")
	assert.Error(t, err, "CHECK constraint should reject val <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (10000)")
	assert.Error(t, err, "CHECK constraint should reject val >= 10000")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (5000)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintAddColumn tests adding a column to a table that has
// CHECK constraints. This verifies that the ALTER is applied correctly
// on top of the shadow table created via CREATE TABLE .. LIKE.
func TestCheckConstraintAddColumn(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_addcol",
		`CREATE TABLE chk_addcol (
			id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			val INT NOT NULL,
			CONSTRAINT chk_addcol_valchk CHECK (val > 0)
		)`)
	tt.SeedRows(t, "INSERT INTO chk_addcol (val) SELECT 1", 3)

	m := NewTestRunner(t, "chk_addcol", "ADD COLUMN extra VARCHAR(100) DEFAULT 'hello', ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_addcol WHERE extra = 'hello'").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	// Verify CHECK constraint is still enforced.
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (0)")
	assert.Error(t, err, "CHECK constraint should still reject val <= 0 after adding column")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (1)")
	assert.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintAddNewCheckConstraint tests adding a new CHECK constraint
// to a table that already has CHECK constraints.
func TestCheckConstraintAddNewCheckConstraint(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_addchk",
		`CREATE TABLE chk_addchk (
			id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			c1 INT NOT NULL,
			c2 INT NOT NULL,
			CONSTRAINT chk_addchk_c1pos CHECK (c1 > 0)
		)`)
	tt.SeedRows(t, "INSERT INTO chk_addchk (c1, c2) SELECT 1, 50", 3)

	m := NewTestRunner(t, "chk_addchk", "ADD CONSTRAINT chk_addchk_c2rng CHECK (c2 BETWEEN 1 AND 100)")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())

	// Verify both old and new CHECK constraints are enforced.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (0, 50)")
	assert.Error(t, err, "original CHECK constraint should still reject c1 <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 200)")
	assert.Error(t, err, "new CHECK constraint should reject c2 > 100")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 0)")
	assert.Error(t, err, "new CHECK constraint should reject c2 < 1")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 50)")
	assert.NoError(t, err, "valid row should be accepted")
}
