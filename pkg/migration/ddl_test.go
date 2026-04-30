package migration

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestForNonInstantBurn tests that when a table has exhausted all 64 instant DDL
// row versions, Spirit falls back to a copy operation and resets the counter to zero.
func TestForNonInstantBurn(t *testing.T) {
	t.Parallel()

	// Skip on MySQL 8.0.28 (uses INSTANT_COLS, not total_row_versions) and
	// MySQL 9.x (total_row_versions limit was raised beyond 64).
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT version()`).Scan(&version))
	if version == "8.0.28" {
		t.Skip("Skipping this test for MySQL 8.0.28")
	}
	if strings.HasPrefix(version, "9.") {
		t.Skip("Skipping this test for MySQL 9.x: total_row_versions limit was raised beyond 64")
	}

	tt := testutils.NewTestTable(t, "instantburn", `CREATE TABLE instantburn (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`)

	rowVersions := func() int {
		var rv int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			`SELECT total_row_versions FROM INFORMATION_SCHEMA.INNODB_TABLES WHERE name=CONCAT(DATABASE(), '/instantburn')`).Scan(&rv))
		return rv
	}

	// Exhaust all 64 instant DDL row versions.
	for range 32 {
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, ADD newcol INT")
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, DROP newcol")
	}
	require.Equal(t, 64, rowVersions())

	m := NewTestRunner(t, "instantburn", "add newcol2 int", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL) // must use copy since instant is exhausted
	require.Equal(t, 0, rowVersions()) // reset to zero, not 1 (no burn)
	require.NoError(t, m.Close())
}

// TestIndexVisibility tests ALTER INDEX INVISIBLE/VISIBLE operations.
func TestIndexVisibility(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "indexvisibility", `CREATE TABLE indexvisibility (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`)

	// INVISIBLE — should use inplace DDL
	m := NewTestRunner(t, "indexvisibility", "ALTER INDEX b INVISIBLE", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// VISIBLE — should use inplace DDL
	m = NewTestRunner(t, "indexvisibility", "ALTER INDEX b VISIBLE", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// VISIBLE + ADD INDEX — mixed operations should fail
	m = NewTestRunner(t, "indexvisibility", "ALTER INDEX b VISIBLE, ADD INDEX (c)", WithThreads(1))
	err := m.Run(t.Context())
	require.Error(t, err)
	require.NoError(t, m.Close())

	// VISIBLE + CHANGE COLUMN — mixed with table-rebuilding should fail
	m = NewTestRunner(t, "indexvisibility", "ALTER INDEX b VISIBLE, CHANGE c cc BIGINT NOT NULL", WithThreads(1))
	err = m.Run(t.Context())
	require.Error(t, err)
	require.NoError(t, m.Close())
}

// TestStatementWorkflowStillInstant tests that a Statement-based ALTER that qualifies
// for instant DDL is still detected and applied instantly.
func TestStatementWorkflowStillInstant(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "stmtworkflow", `CREATE TABLE stmtworkflow (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE stmtworkflow ADD newcol INT", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

// TestTrailingSemicolon tests that ALTER statements with trailing semicolons
// and spaces are handled correctly.
func TestTrailingSemicolon(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "multiSecondary", `CREATE TABLE multiSecondary (
		id int unsigned NOT NULL AUTO_INCREMENT,
		v varchar(32) DEFAULT NULL,
		PRIMARY KEY (id),
		KEY idx5 (v),
		KEY idx1 (v),
		KEY idx2 (v),
		KEY idx3 (v),
		KEY idx4 (v)
	)`)

	// DROP INDEX — should use inplace DDL
	m := NewTestRunner(t, "multiSecondary", "drop index idx1, drop index idx2, drop index idx3, drop index idx4",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// ADD INDEX via Statement (with trailing semicolon) — should use copy
	m = NewTestRunnerFromStatement(t,
		"alter table multiSecondary add index idx1(v), add index idx2(v), add index idx3(v), add index idx4(v);",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInplaceDDL) // ADD INDEX uses copy for replica safety
	require.NoError(t, m.Close())

	// DROP INDEX with trailing semicolon+space (https://github.com/block/spirit/issues/384)
	m = NewTestRunner(t, "multiSecondary", "drop index idx1, drop index idx2, drop index idx3, drop index idx4; ",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
}

// TestAlterExtendVarcharE2E tests that extending varchar columns within the same
// length prefix (<=255 bytes) uses inplace DDL, while crossing the boundary requires copy.
func TestAlterExtendVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1extendvarchar", `CREATE TABLE t1extendvarchar (
		id int not null primary key auto_increment,
		col1 varchar(10),
		col2 varchar(10)
	) character set utf8mb4`)

	type alterAttempt struct {
		Statement string
		InPlace   bool
	}
	alters := []alterAttempt{
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(20)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar CHANGE col1 col1 varchar(21)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(22), CHANGE col2 col2 varchar(22)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(23), CHANGE col2 col2 varchar(200)`, InPlace: false},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(200)`, InPlace: false},
	}

	for _, attempt := range alters {
		m := NewTestRunnerFromStatement(t, attempt.Statement, WithThreads(1))
		require.NoError(t, m.Run(t.Context()))
		require.Equal(t, attempt.InPlace, m.usedInplaceDDL, "Statement: %s", attempt.Statement)
		require.NoError(t, m.Close())
	}
}

// TestMigrationWithSQLCommentsInStatement verifies that Spirit correctly handles
// SQL comments prepended to ALTER TABLE statements.
func TestMigrationWithSQLCommentsInStatement(t *testing.T) {
	tt := testutils.NewTestTable(t, "t1_comment_test", `CREATE TABLE t1_comment_test (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		a INT
	)`)
	testutils.RunSQL(t, "INSERT INTO t1_comment_test (a) VALUES (1), (2), (3)")

	statementWithComments := `-- Migration for JIRA-1234
-- Author: someone@block.xyz
-- Date: 2025-07-01
-- This migration adds an index on column a
-- for improved query performance on the dashboard.
ALTER TABLE t1_comment_test ADD INDEX idx_a (a)`

	m := NewTestMigration(t, WithStatement(statementWithComments))
	r, err := NewRunner(m)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	require.Len(t, r.changes, 1)
	require.Equal(t, "ADD INDEX `idx_a`(`a`)", r.changes[0].stmt.Alter)

	require.NoError(t, m.Run())

	// Verify the index was actually created.
	var indexName string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t1_comment_test' AND INDEX_NAME='idx_a'").Scan(&indexName))
	require.Equal(t, "idx_a", indexName)
}
