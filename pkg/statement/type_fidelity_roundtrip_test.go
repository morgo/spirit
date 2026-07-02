package statement

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// This file holds probe-style round-trip tests for parse/diff/emission
// fidelity of type and partition attributes: each test drives the emitted
// DDL against a real MySQL server (see openScratch) and asserts both that
// MySQL accepts it and that a re-diff converges.

// TestRoundTrip_PartitionMaxValue verifies that the MAXVALUE partition bound
// survives parse -> diff -> emission as the bare keyword. Before the fix,
// MAXVALUE was stored as a plain string and emitted as the quoted string
// literal 'MAXVALUE', which MySQL rejects with error 1697.
func TestRoundTrip_PartitionMaxValue(t *testing.T) {
	db := openScratch(t)

	t.Run("emits_bare_maxvalue_and_applies", func(t *testing.T) {
		createSQL := "CREATE TABLE tf_maxvalue (id INT NOT NULL PRIMARY KEY)"
		targetSQL := "CREATE TABLE tf_maxvalue (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) " +
			"(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB, " +
			"PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB)"

		// The diff that re-creates partitioning must emit the bare MAXVALUE
		// keyword, never the quoted string literal 'MAXVALUE'.
		source, err := ParseCreateTable(createSQL)
		require.NoError(t, err)
		target, err := ParseCreateTable(targetSQL)
		require.NoError(t, err)
		stmts, err := source.Diff(target, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "VALUES LESS THAN MAXVALUE")
		require.NotContains(t, stmts[0].Statement, "'MAXVALUE'")

		// The emitted ALTER must be accepted by real MySQL, and a re-diff of
		// the partitioned table against the target must be nil (converged).
		afterCreate := applyAndConverge(t, db, "tf_maxvalue", createSQL, targetSQL)
		require.Contains(t, afterCreate, "VALUES LESS THAN MAXVALUE")
	})

	t.Run("tuple_maxvalue_emits_bare_and_applies", func(t *testing.T) {
		// Multi-column RANGE COLUMNS: MAXVALUE appears as a tuple element and
		// must render bare inside the parenthesized value list.
		createSQL := "CREATE TABLE tf_maxvalue_mc (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))"
		targetSQL := "CREATE TABLE tf_maxvalue_mc (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) " +
			"PARTITION BY RANGE COLUMNS (a, b) " +
			"(PARTITION p0 VALUES LESS THAN (10, 20) ENGINE = InnoDB, " +
			"PARTITION pmax VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)"

		source, err := ParseCreateTable(createSQL)
		require.NoError(t, err)
		target, err := ParseCreateTable(targetSQL)
		require.NoError(t, err)
		stmts, err := source.Diff(target, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "VALUES LESS THAN (MAXVALUE, MAXVALUE)")
		require.NotContains(t, stmts[0].Statement, "'MAXVALUE'")

		afterCreate := applyAndConverge(t, db, "tf_maxvalue_mc", createSQL, targetSQL)
		require.Contains(t, afterCreate, "(MAXVALUE,MAXVALUE)")
	})

	t.Run("maxvalue_equal_across_spellings", func(t *testing.T) {
		// VALUES LESS THAN MAXVALUE (canonical for RANGE) and
		// VALUES LESS THAN (MAXVALUE) (canonical for RANGE COLUMNS) must
		// compare equal: MySQL treats them as the same bound.
		bare, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN MAXVALUE)")
		require.NoError(t, err)
		parens, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN (MAXVALUE))")
		require.NoError(t, err)

		stmts, err := bare.Diff(parens, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "MAXVALUE must equal (MAXVALUE); got: %+v", stmts)
		stmts, err = parens.Diff(bare, nil)
		require.NoError(t, err)
		require.Nil(t, stmts)
	})

	t.Run("maxvalue_vs_number_differs", func(t *testing.T) {
		maxed, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN MAXVALUE)")
		require.NoError(t, err)
		bounded, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN (100))")
		require.NoError(t, err)

		stmts, err := maxed.Diff(bounded, nil)
		require.NoError(t, err)
		require.NotEmpty(t, stmts, "MAXVALUE must differ from a finite bound")
	})
}
