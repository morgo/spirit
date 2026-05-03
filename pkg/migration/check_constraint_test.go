package migration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// Tests for issue #351: Add more tests for CHECK constraints.
// CHECK constraints have a schema-level namespace, so creating two tables with
// the same named CHECK constraint would fail. Spirit uses CREATE TABLE .. LIKE
// which auto-renames constraints, avoiding this issue.

// TestCheckConstraintNamed tests migration of a table with named CHECK constraints.
func TestCheckConstraintNamed(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_named", `CREATE TABLE chk_named (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT DEFAULT NULL,
		c2 INT DEFAULT NULL,
		c3 INT DEFAULT NULL,
		CONSTRAINT chk_named_c1pos CHECK (c1 > 0),
		CONSTRAINT chk_named_c2pos CHECK (c2 > 0),
		CONSTRAINT chk_named_c3rng CHECK (c3 BETWEEN 1 AND 100)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 50), (10, 20, 99), (5, 5, 1)`)

	m := NewTestRunner(t, "chk_named", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_named").Scan(&count))
	require.Equal(t, 3, count)

	// Verify CHECK constraints are still enforced after migration.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (-1, 1, 50)")
	require.Error(t, err, "CHECK constraint on c1 should reject negative values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, -1, 50)")
	require.Error(t, err, "CHECK constraint on c2 should reject negative values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 200)")
	require.Error(t, err, "CHECK constraint on c3 should reject out-of-range values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 50)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintUnnamed tests migration of a table with unnamed (auto-generated) CHECK constraints.
func TestCheckConstraintUnnamed(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_unnamed", `CREATE TABLE chk_unnamed (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT CHECK (c1 > 10),
		c2 INT CHECK (c2 > 0),
		CHECK (c1 <> c2)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_unnamed (c1, c2) VALUES (11, 1), (20, 2), (100, 3)`)

	m := NewTestRunner(t, "chk_unnamed", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_unnamed").Scan(&count))
	require.Equal(t, 3, count)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (5, 1)")
	require.Error(t, err, "CHECK constraint should reject c1 <= 10")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, -1)")
	require.Error(t, err, "CHECK constraint should reject c2 <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, 11)")
	require.Error(t, err, "CHECK constraint should reject c1 == c2")
}

// TestCheckConstraintMixed tests migration of a table with both named and unnamed CHECK constraints.
func TestCheckConstraintMixed(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_mixed", `CREATE TABLE chk_mixed (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		CHECK (c1 <> c2),
		c1 INT CHECK (c1 > 10),
		c2 INT CONSTRAINT chk_mixed_c2pos CHECK (c2 > 0),
		c3 INT CHECK (c3 < 100),
		CONSTRAINT chk_mixed_c1nz CHECK (c1 <> 0),
		CHECK (c1 > c3)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 10), (20, 5, 15), (80, 3, 70)`)

	m := NewTestRunner(t, "chk_mixed", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_mixed").Scan(&count))
	require.Equal(t, 3, count)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (5, 1, 1)")
	require.Error(t, err, "CHECK constraint should reject c1 <= 10")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, -1, 10)")
	require.Error(t, err, "CHECK constraint should reject c2 <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 200)")
	require.Error(t, err, "CHECK constraint should reject c3 >= 100")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 50, 10)")
	require.Error(t, err, "CHECK constraint should reject c1 == c2")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 10)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintWithDML tests migration with concurrent DML on a table with CHECK constraints.
func TestCheckConstraintWithDML(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_dml", `CREATE TABLE chk_dml (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_dml_valpos CHECK (val > 0),
		CONSTRAINT chk_dml_valrng CHECK (val < 10000)
	)`)
	tt.SeedRows(t, "INSERT INTO chk_dml (val) SELECT 1", 6000)

	m := NewTestRunner(t, "chk_dml", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	var wg sync.WaitGroup
	wg.Go(func() {
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
		}
		for i := range 100 {
			// Insert valid rows (val must be > 0 and < 10000).
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf(`INSERT INTO chk_dml (val) VALUES (%d)`, i+1))
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf(`UPDATE chk_dml SET val = %d WHERE id = %d`, (i%9999)+1, i+1))
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf(`DELETE FROM chk_dml WHERE id = %d`, i+50))
		}
	})

	require.NoError(t, m.Run(t.Context()))
	wg.Wait()
	require.NoError(t, m.Close())

	// Verify CHECK constraints are still enforced after migration with DML.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (0)")
	require.Error(t, err, "CHECK constraint should reject val <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (10000)")
	require.Error(t, err, "CHECK constraint should reject val >= 10000")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (5000)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintAddColumn tests adding a column to a table that has CHECK constraints.
func TestCheckConstraintAddColumn(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_addcol", `CREATE TABLE chk_addcol (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_addcol_valchk CHECK (val > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_addcol (val) VALUES (1), (2), (3)`)

	m := NewTestRunner(t, "chk_addcol", "ADD COLUMN extra VARCHAR(100) DEFAULT 'hello', ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_addcol WHERE extra = 'hello'").Scan(&count))
	require.Equal(t, 3, count)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (0)")
	require.Error(t, err, "CHECK constraint should still reject val <= 0 after adding column")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (1)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintAddNewCheckConstraint tests adding a new CHECK constraint
// to a table that already has CHECK constraints.
func TestCheckConstraintAddNewCheckConstraint(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_addchk", `CREATE TABLE chk_addchk (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT NOT NULL,
		c2 INT NOT NULL,
		CONSTRAINT chk_addchk_c1pos CHECK (c1 > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_addchk (c1, c2) VALUES (1, 50), (10, 50), (5, 50)`)

	m := NewTestRunner(t, "chk_addchk", "ADD CONSTRAINT chk_addchk_c2rng CHECK (c2 BETWEEN 1 AND 100)")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	// Violate original constraint c1 > 0
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (0, 50)")
	require.Error(t, err, "original CHECK constraint should still reject c1 <= 0")

	// Violate new constraint c2 BETWEEN 1 AND 100
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 200)")
	require.Error(t, err, "new CHECK constraint should reject c2 > 100")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 0)")
	require.Error(t, err, "new CHECK constraint should reject c2 < 1")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 50)")
	require.NoError(t, err, "valid row should be accepted")
}
