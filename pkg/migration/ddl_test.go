package migration

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// From https://github.com/block/spirit/issues/241
// If an ALTER qualifies as instant, but an instant can't apply, don't burn an instant version.
func TestForNonInstantBurn(t *testing.T) {
	t.Parallel()
	// We skip this test in MySQL 8.0.28. It uses INSTANT_COLS instead of total_row_versions
	// and it supports instant add col, but not instant drop col.
	// It's safe to skip, but we need 8.0.28 in tests because it's the minor version
	// used by Aurora's LTS.
	tt := testutils.NewTestTable(t, "instantburn",
		`CREATE TABLE instantburn (
			id int(11) NOT NULL AUTO_INCREMENT,
			pad varbinary(1024) NOT NULL,
			PRIMARY KEY (id)
		)`)
	var version string
	err := tt.DB.QueryRowContext(t.Context(), `SELECT version()`).Scan(&version)
	require.NoError(t, err)
	if version == "8.0.28" {
		t.Skip("Skipping this test for MySQL 8.0.28")
	}
	if strings.HasPrefix(version, "9.") {
		t.Skip("Skipping this test for MySQL 9.x: total_row_versions limit was raised beyond 64")
	}

	rowVersions := func() int {
		var rv int
		err := tt.DB.QueryRowContext(t.Context(), `SELECT total_row_versions FROM INFORMATION_SCHEMA.INNODB_TABLES where name='test/instantburn'`).Scan(&rv)
		require.NoError(t, err)
		return rv
	}

	for range 32 { // requires 64 instants
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, ADD newcol INT")
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, DROP newcol")
	}
	assert.Equal(t, 64, rowVersions()) // confirm all 64 are used.

	m := NewTestRunner(t, "instantburn", "add newcol2 int", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL) // it would have had to apply a copy.
	assert.Equal(t, 0, rowVersions()) // confirm we reset to zero, not 1 (no burn)
	assert.NoError(t, m.Close())
}

// From https://github.com/block/spirit/issues/283
// ALTER INDEX .. VISIBLE is INPLACE which is really weird.
// it only makes sense to be instant, so we attempt it as a "safe inplace".
// If it's not with a set of safe changes, then we error.
// This means the user is expected to split their DDL into two separate ALTERs.
//
// There is a partial workaround for users to use --force-inplace, which would
// help only if the other included changes are also INPLACE and not copy.
// We *do* document this under --force-inplace docs, but it's
// really not a typical use case to ever mix invisible with any other change.
func TestIndexVisibility(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "indexvisibility",
		`CREATE TABLE indexvisibility (
			id int(11) NOT NULL AUTO_INCREMENT,
			b INT NOT NULL,
			c INT NOT NULL,
			PRIMARY KEY (id),
			INDEX (b)
		)`)

	// ALTER INDEX INVISIBLE uses inplace.
	m := NewTestRunner(t, "indexvisibility", "ALTER INDEX b INVISIBLE", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.True(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	// ALTER INDEX VISIBLE uses inplace.
	m = NewTestRunner(t, "indexvisibility", "ALTER INDEX b VISIBLE", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.True(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	// Index visibility mixed with ADD INDEX should fail.
	m = NewTestRunner(t, "indexvisibility", "ALTER INDEX b VISIBLE, ADD INDEX (c)", WithThreads(1))
	err := m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close())

	// Index visibility mixed with table-rebuilding operations should fail.
	// This is important because invisible should never be mixed with copy
	// (the semantics are weird since it's for experiments).
	m = NewTestRunner(t, "indexvisibility", "ALTER INDEX b VISIBLE, CHANGE c cc BIGINT NOT NULL", WithThreads(1))
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close())
}

func TestStatementWorkflowStillInstant(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "stmtworkflow",
		`CREATE TABLE stmtworkflow (
			id int(11) NOT NULL AUTO_INCREMENT,
			b INT NOT NULL,
			c INT NOT NULL,
			PRIMARY KEY (id),
			INDEX (b)
		)`)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE stmtworkflow ADD newcol INT", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.True(t, m.usedInstantDDL) // expected to count as instant.
	assert.NoError(t, m.Close())
}

func TestTrailingSemicolon(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "multiSecondary",
		`CREATE TABLE multiSecondary (
			id int unsigned NOT NULL AUTO_INCREMENT,
			v varchar(32) DEFAULT NULL,
			PRIMARY KEY (id),
			KEY idx5 (v),
			KEY idx1 (v),
			KEY idx2 (v),
			KEY idx3 (v),
			KEY idx4 (v)
		)`)
	dropIndexesAlter := "drop index idx1, drop index idx2, drop index idx3, drop index idx4"

	m := NewTestRunner(t, "multiSecondary", dropIndexesAlter, WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.True(t, m.usedInplaceDDL) // DROP INDEX operations now use INPLACE for better performance
	assert.NoError(t, m.Close())

	m = NewTestRunnerFromStatement(t,
		"alter table multiSecondary add index idx1(v), add index idx2(v), add index idx3(v), add index idx4(v);",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInplaceDDL) // ADD INDEX operations now use copy process for replica safety
	require.NoError(t, m.Close())

	// https://github.com/block/spirit/issues/384
	m = NewTestRunner(t, "multiSecondary", dropIndexesAlter+"; ", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
}

func TestAlterExtendVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1extendvarchar",
		`CREATE TABLE t1extendvarchar (
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
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(22), CHANGE col2 col2 varchar(22) `, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(23), CHANGE col2 col2 varchar(200) `, InPlace: false},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(200)`, InPlace: false},
	}

	for _, attempt := range alters {
		m := NewTestRunnerFromStatement(t, attempt.Statement, WithThreads(1))
		require.NoError(t, m.Run(t.Context()))
		assert.Equal(t, attempt.InPlace, m.usedInplaceDDL)
		assert.NoError(t, m.Close())
	}
}

func TestMigrationWithSQLCommentsInStatement(t *testing.T) {
	// This test verifies that Spirit correctly handles SQL comments
	// prepended to ALTER TABLE statements when using the Statement field
	tt := testutils.NewTestTable(t, "t1_comment_test",
		`CREATE TABLE t1_comment_test (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT)`)
	tt.SeedRows(t, "INSERT INTO t1_comment_test (a) SELECT 1", 3)

	// Statement with multiple SQL comments before the ALTER — this is exactly
	// what our tool passes when users include comments in their .sql files.
	migration := NewTestMigration(t, WithStatement(`-- Migration for JIRA-1234
-- Author: someone@block.xyz
-- Date: 2025-07-01
-- This migration adds an index on column a
-- for improved query performance on the dashboard.
ALTER TABLE t1_comment_test ADD INDEX idx_a (a)`))

	// Use NewRunner so we can inspect the parsed changes before running.
	r, err := NewRunner(migration)
	require.NoError(t, err)
	require.Len(t, r.changes, 1)
	assert.Equal(t, "ADD INDEX `idx_a`(`a`)", r.changes[0].stmt.Alter)

	require.NoError(t, migration.Run())

	// Verify the index was actually created
	var indexName string
	err = tt.DB.QueryRowContext(t.Context(), "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t1_comment_test' AND INDEX_NAME='idx_a'").Scan(&indexName)
	require.NoError(t, err)
	assert.Equal(t, "idx_a", indexName)
}
