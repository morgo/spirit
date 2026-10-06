package migration

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// foreignKeyFixture creates, in a database of its own, a parent table and a
// child table with two foreign keys to it: one named, and one with a name
// MySQL generates (child_ibfk_1). Foreign key names are unique per schema, so
// the tests cannot share a database.
func foreignKeyFixture(t *testing.T) (string, *sql.DB) {
	t.Helper()
	testutils.SkipBeforeMySQLVersion(t, check.MinForeignKeyVersion, "changes made by foreign key cascades are only in the binary log from MySQL 9.6")
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE parent (id INT NOT NULL PRIMARY KEY, v INT)`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE child (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pid INT,
		pid2 INT,
		pad VARCHAR(100) NOT NULL DEFAULT '',
		KEY explicit_pad (pad),
		CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE,
		FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE
	)`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO parent (id, v)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100) SELECT n, n FROM seq`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO child (pid, pid2, pad)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 900)
		SELECT n % 100 + 1, (n * 7) % 100 + 1, REPEAT('a', 50) FROM seq`)
	return dbName, db
}

// copyAlter adds a column. ENGINE=InnoDB makes it a copy: without it, the ALTER
// is INSTANT, which spirit leaves to MySQL before any of its checks run.
const copyAlter = "ADD COLUMN c INT, ENGINE=InnoDB"

func parsedCreateTable(t *testing.T, db *sql.DB, tableName string) *statement.CreateTable {
	t.Helper()
	var name, createTable string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE "+tableName).Scan(&name, &createTable))
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)
	return ct
}

// foreignKeysAndIndexes renders the foreign keys and indexes of ct, sorted. The
// copy can change the order of the indexes, which nothing depends on (Diff
// ignores it too): copying a named foreign key replaces its index, which is
// renamed back, but stays at the end.
func foreignKeysAndIndexes(ct *statement.CreateTable) []string {
	var out []string
	for _, c := range ct.GetConstraints() {
		if c.Type == "FOREIGN KEY" {
			out = append(out, "CONSTRAINT "+c.Name+" "+*c.Definition)
		}
	}
	for _, idx := range ct.GetIndexes() {
		out = append(out, idx.Type+" "+idx.Name+" ("+strings.Join(idx.Columns, ",")+")")
	}
	slices.Sort(out)
	return out
}

func countRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), query).Scan(&n))
	return n
}

// TestForeignKeysCascadeAfterChecksum changes the child table only through
// foreign key cascades, while the migration waits on the sentinel. The initial
// checksum has run by then and the continuous checksum never repairs, so the
// new table only ends up right if the change feed carried the cascaded
// changes. The cutover must also leave the foreign keys and indexes exactly as
// they were.
func TestForeignKeysCascadeAfterChecksum(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))
	require.Contains(t, before, "CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE")

	deletedByCascade := countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid <= 10")
	setNullByCascade := countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid2 <= 10 AND pid > 10")
	updatedByCascade := countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid = 50")
	total := countRows(t, db, "SELECT COUNT(*) FROM child")
	require.Positive(t, deletedByCascade)
	require.Positive(t, setNullByCascade)
	require.Positive(t, updatedByCascade)

	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName),
		WithDeferCutOver(),
		WithExperimentalForeignKeys())
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)

	_, err := db.ExecContext(t.Context(), "DELETE FROM parent WHERE id <= 10")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "UPDATE parent SET id = 1050 WHERE id = 50")
	require.NoError(t, err)
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, running.wait(t))

	after := parsedCreateTable(t, db, "child")
	assert.Equal(t, before, foreignKeysAndIndexes(after))
	assert.True(t, slicesContainColumn(after, "c"))

	assert.Equal(t, total-deletedByCascade, countRows(t, db, "SELECT COUNT(*) FROM child"))
	assert.Zero(t, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid <= 10 OR pid2 <= 10"))
	assert.Equal(t, setNullByCascade, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid2 IS NULL"))
	assert.Equal(t, updatedByCascade, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid = 1050"))
	assert.Zero(t, countRows(t, db, `SELECT COUNT(*) FROM child LEFT JOIN parent ON child.pid = parent.id
		WHERE child.pid IS NOT NULL AND parent.id IS NULL`))
	// The foreign keys are enforced against parent, not a leftover table.
	assert.Zero(t, countRows(t, db, fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.referential_constraints
		WHERE constraint_schema = '%s' AND referenced_table_name <> 'parent'`, dbName)))
	_, err = db.ExecContext(t.Context(), "INSERT INTO child (pid) VALUES (999)")
	require.ErrorContains(t, err, "a foreign key constraint fails")
}

func slicesContainColumn(ct *statement.CreateTable, name string) bool {
	for _, col := range ct.GetColumns() {
		if col.Name == name {
			return true
		}
	}
	return false
}

// TestForeignKeysAlter covers ALTERs that touch the foreign keys the copy
// supports: dropping one, and renaming a column one is on.
func TestForeignKeysAlter(t *testing.T) {
	t.Parallel()
	t.Run("drop foreign key", func(t *testing.T) {
		t.Parallel()
		dbName, db := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "DROP FOREIGN KEY fk_child_parent, "+copyAlter,
			WithDBName(dbName), WithExperimentalForeignKeys())
		require.NoError(t, m.Run(t.Context()))
		require.NoError(t, m.Close())
		assert.Equal(t, []string{
			"CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE",
			"INDEX explicit_pad (pad)",
			"INDEX fk_child_parent (pid)", // MySQL keeps the index of a dropped foreign key
			"INDEX pid2 (pid2)",
			"PRIMARY KEY PRIMARY (id)",
		}, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	})
	t.Run("drop generated foreign key", func(t *testing.T) {
		t.Parallel()
		dbName, db := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "DROP FOREIGN KEY child_ibfk_1, "+copyAlter,
			WithDBName(dbName), WithExperimentalForeignKeys())
		require.NoError(t, m.Run(t.Context()))
		require.NoError(t, m.Close())
		assert.Equal(t, []string{
			"CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE",
			"INDEX explicit_pad (pad)",
			"INDEX fk_child_parent (pid)",
			"INDEX pid2 (pid2)",
			"PRIMARY KEY PRIMARY (id)",
		}, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	})
	t.Run("rename column", func(t *testing.T) {
		t.Parallel()
		dbName, db := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "RENAME COLUMN pid TO parent_id, "+copyAlter,
			WithDBName(dbName), WithExperimentalForeignKeys())
		require.NoError(t, m.Run(t.Context()))
		require.NoError(t, m.Close())
		assert.Equal(t, []string{
			"CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE",
			"CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE",
			"INDEX explicit_pad (pad)",
			"INDEX fk_child_parent (parent_id)",
			"INDEX pid2 (pid2)",
			"PRIMARY KEY PRIMARY (id)",
		}, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	})
	t.Run("column type change MySQL refuses", func(t *testing.T) {
		t.Parallel()
		dbName, _ := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "MODIFY pid BIGINT, ENGINE=InnoDB",
			WithDBName(dbName), WithExperimentalForeignKeys())
		err := m.Run(t.Context())
		require.ErrorContains(t, err, "incompatible") // error 3780, as from a native ALTER
		require.NoError(t, m.Close())
	})
}

// TestForeignKeysSkipDropAfterCutover keeps the old table, which keeps the
// name of the named foreign key, so its copy cannot be renamed back. The
// generated name is renamed by the cutover itself.
func TestForeignKeysSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName), WithExperimentalForeignKeys(), WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	assert.Equal(t, []string{
		"CONSTRAINT _fk_child_parent_new FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE",
		"CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE",
		"INDEX explicit_pad (pad)",
		"INDEX fk_child_parent (pid)",
		"INDEX pid2 (pid2)",
		"PRIMARY KEY PRIMARY (id)",
	}, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
}

// TestForeignKeysRefused covers what experimental support still refuses.
func TestForeignKeysRefused(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, table, alter, err string
		experimental            bool
	}{
		{"without the flag", "child", copyAlter, "tables with existing foreign key constraints are not supported", false},
		{"parent table", "parent", copyAlter, "tables referenced by a foreign key are not supported", true},
		{"add foreign key", "child", "ADD CONSTRAINT fk_new FOREIGN KEY (pid) REFERENCES parent (id)", "adding foreign key constraints is not supported", true},
		{"drop constraint", "child", "DROP CONSTRAINT fk_child_parent, " + copyAlter, "DROP CONSTRAINT fk_child_parent names a foreign key", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dbName, _ := foreignKeyFixture(t)
			opts := []RunnerOption{WithDBName(dbName)}
			if test.experimental {
				opts = append(opts, WithExperimentalForeignKeys())
			}
			m := NewTestRunner(t, test.table, test.alter, opts...)
			require.ErrorContains(t, m.Run(t.Context()), test.err)
			require.NoError(t, m.Close())
		})
	}
	t.Run("self-referencing table", func(t *testing.T) {
		t.Parallel()
		dbName, _ := foreignKeyFixture(t)
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE tree (id INT NOT NULL PRIMARY KEY, parent_id INT,
			FOREIGN KEY (parent_id) REFERENCES tree (id))`)
		m := NewTestRunner(t, "tree", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys())
		require.ErrorContains(t, m.Run(t.Context()), "tables referenced by a foreign key are not supported")
		require.NoError(t, m.Close())
	})
}

// TestForeignKeysRequireMySQL97 runs where the binary log does not carry
// cascaded changes, and must refuse the table even with the flag set.
func TestForeignKeysRequireMySQL97(t *testing.T) {
	t.Parallel()
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT VERSION()").Scan(&version))
	if utils.CompareMySQLVersions(version, check.MinForeignKeyVersion) >= 0 {
		t.Skipf("MySQL %s supports foreign keys", version)
	}
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT,
		FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE)`)
	m := NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys())
	require.ErrorContains(t, m.Run(t.Context()), "tables with foreign keys require MySQL 9.7 or later")
	require.NoError(t, m.Close())
}

// TestForeignKeysResume resumes a migration whose new table already has the
// copied foreign keys, and cascades into it before and after the resume.
func TestForeignKeysResume(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	// More rows than WithCopyStalledAfterChunks(2) copies.
	for range 3 {
		testutils.RunSQLInDatabase(t, dbName, "INSERT INTO child (pid, pid2, pad) SELECT pid, pid2, pad FROM child")
	}
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))

	m := NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys(),
		WithDeferCutOver(), WithThreads(1), WithCopyStalledAfterChunks(2))
	runUntilCheckpointThenCancel(t, m) // creates the sentinel, which a resume does not
	_, err := db.ExecContext(t.Context(), "DELETE FROM parent WHERE id <= 10")
	require.NoError(t, err)

	m = NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys(),
		WithDeferCutOver())
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)
	require.True(t, m.usedResumeFromCheckpoint.Load())
	_, err = db.ExecContext(t.Context(), "DELETE FROM parent WHERE id BETWEEN 11 AND 20")
	require.NoError(t, err)
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, running.wait(t))

	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	assert.Zero(t, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid <= 20"))
	assert.Zero(t, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid2 <= 20"))
}

// TestRestoreForeignKeyNamesAfterCrash renames the foreign keys back when the
// migration stopped between the cutover and the rename, as from a crash: the
// names to restore are read off the table, not remembered.
func TestRestoreForeignKeyNamesAfterCrash(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))
	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName), WithExperimentalForeignKeys(), WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	var oldName string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT table_name FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name LIKE '\_child\_old%'`).Scan(&oldName))
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+oldName)

	names, err := restoreForeignKeyNames(t.Context(), db, "child", false)
	require.NoError(t, err)
	require.Equal(t, []string{"_fk_child_parent_new"}, names)
	names, err = restoreForeignKeyNames(t.Context(), db, "child", true)
	require.NoError(t, err)
	require.Equal(t, []string{"_fk_child_parent_new"}, names)
	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))

	names, err = restoreForeignKeyNames(t.Context(), db, "child", true)
	require.NoError(t, err)
	require.Empty(t, names)
}
