//nolint:dupword
package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVarcharNonBinaryComparable(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS nonbinarycompatt1, _nonbinarycompatt1_new`)
	table := `CREATE TABLE nonbinarycompatt1 (
		uuid varchar(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "nonbinarycompatt1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified.
	assert.NoError(t, m.Run(t.Context())) // it's a non-binary comparable type (varchar)
	assert.NoError(t, m.Close())
}

// TestPartitioningSyntax tests that ALTERs that don't support ALGORITHM assertion
// are still supported. From https://github.com/block/spirit/issues/277
func TestPartitioningSyntax(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS partt1, _partt1_new`)
	table := `CREATE TABLE partt1 (
		id INT NOT NULL PRIMARY KEY auto_increment,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "partt1",
		Alter:    "PARTITION BY KEY() PARTITIONS 8",
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestVarbinary(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS varbinaryt1, _varbinaryt1_new`)
	table := `CREATE TABLE varbinaryt1 (
		uuid varbinary(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO varbinaryt1 (uuid, name) VALUES (UUID(), REPEAT('a', 200))")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "varbinaryt1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified correctly.
	assert.NoError(t, m.Run(t.Context())) // varbinary is compatible.
	assert.False(t, m.usedInstantDDL)     // not possible
	assert.NoError(t, m.Close())
}

// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00 can still be migrated.
func TestDataFromBadSqlMode(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS badsqlt1, _badsqlt1_new`)
	table := `CREATE TABLE badsqlt1 (
		id int not null primary key auto_increment,
		d date NOT NULL,
		t timestamp NOT NULL
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT IGNORE INTO badsqlt1 (d, t) VALUES ('0000-00-00', '0000-00-00 00:00:00'),('2020-02-00', '2020-02-30 00:00:00')")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "badsqlt1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified correctly.
	assert.NoError(t, m.Run(t.Context())) // pk is compatible.
	assert.False(t, m.usedInstantDDL)     // not possible
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeNoData(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS cdatatypemytable`)
	table := `CREATE TABLE cdatatypemytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "cdatatypemytable",
		Alter:    "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)                // everything is specified correctly.
	assert.NoError(t, m.Run(t.Context())) // no data so no truncation is possible.
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeDataLoss(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS cdatalossmytable`)
	table := `CREATE TABLE cdatalossmytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO cdatalossmytable (name, b) VALUES ('a', 'b')")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "cdatalossmytable",
		Alter:    "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)              // everything is specified correctly.
	assert.Error(t, m.Run(t.Context())) // value 'b' can no convert cleanly to int.
	assert.NoError(t, m.Close())
}

func TestOnline(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline`)
	table := `CREATE TABLE testonline (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline",
		Alter:    "CHANGE COLUMN b b int(11) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInplaceDDL) // not possible
	assert.NoError(t, m.Close())

	// Create another table.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline2`)
	table = `CREATE TABLE testonline2 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline2",
		Alter:    "ADD c int(11) NOT NULL",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInplaceDDL) // uses instant DDL first

	// can only check this against 8.0
	assert.True(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Finally, this will work.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline3`)
	table = `CREATE TABLE testonline3 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline3",
		Alter:    "ADD INDEX(b)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // not possible
	assert.False(t, m.usedInplaceDDL) // ADD INDEX operations now always require copy
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline4`)
	table = `CREATE TABLE testonline4 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline4",
		Alter:    "drop index name, drop index b",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	assert.True(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline5`)
	table = `CREATE TABLE testonline5 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline5",
		Alter:    "drop index name, add column c int",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	assert.False(t, m.usedInplaceDDL) // unfortunately false, since it combines INSTANT and INPLACE operations
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline6`)
	table = `CREATE TABLE testonline6 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
		)
		PARTITION BY HASH (id)
		PARTITIONS 4
	`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline6",
		Alter:    "add partition partitions 4",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL)
	assert.False(t, m.usedInplaceDDL) // false, hash/key partitioned tables require a lock
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline7`)
	table = `CREATE TABLE testonline7 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
		)
		PARTITION BY RANGE (id) (
			PARTITION p0 VALUES LESS THAN (100000), 
			PARTITION p1 VALUES LESS THAN (200000)
		)
	`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline7",
		Alter:    "add partition (partition p2 values less than (300000))",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL)
	assert.True(t, m.usedInplaceDDL) // true, range/list partitioned tables can run inplace without a lock
	assert.NoError(t, m.Close())
}

func TestTableLength(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS thisisareallylongtablenamethisisareallylongtablename60charac`)
	table := `CREATE TABLE thisisareallylongtablenamethisisareallylongtablename60charac (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "thisisareallylongtablenamethisisareallylongtablename60charac",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name must be less than 56 characters")
	assert.NoError(t, m.Close())

	// There is another condition where the error will be in dropping the _old table first
	// if the character limit is exceeded in that query.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "thisisareallylongtablenamethisisareallylongtablename60charac",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name must be less than 56 characters")
	assert.NoError(t, m.Close())
}

func TestCreateTableNameLength(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// A CREATE TABLE with a table name exceeding Spirit's manageable limit (56 chars)
	// should be rejected, since Spirit needs room for metadata suffixes like _<table>_chkpnt.
	longName := strings.Repeat("z", 57)
	assert.Greater(t, len(longName), check.MaxMigratableTableNameLength)

	m, err := NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   2,
		Statement: fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL PRIMARY KEY)", longName),
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf("exceeds the maximum length of %d characters that Spirit can manage", check.MaxMigratableTableNameLength))
	assert.NoError(t, m.Close())

	// A CREATE TABLE with a table name at exactly the max manageable length (56 chars)
	// should be allowed.
	exactName := strings.Repeat("x", check.MaxMigratableTableNameLength)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", exactName))

	m, err = NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   2,
		Statement: fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL PRIMARY KEY)", exactName),
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())

	// Cleanup
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", exactName))
}

func TestSkipDropAfterCutoverLongTableName(t *testing.T) {
	t.Parallel()

	// A table name at the normal max (56 chars) should work with SkipDropAfterCutover.
	// Previously this would have been rejected because the timestamp format exceeds 64 chars,
	// but now we truncate the table name portion in the old table name.
	tableName := "a_fifty_six_character_table_name_that_fits_normal_limits"
	assert.Equal(t, 56, len(tableName))

	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	testutils.RunSQL(t, fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL AUTO_INCREMENT,
		PRIMARY KEY(pk)
	)`, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: true,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	// Verify the old table exists (with truncated name + timestamp)
	oldName := m.changes[0].oldTableName()
	assert.LessOrEqual(t, len(oldName), 64, "old table name should fit within 64 chars")

	var tableCount int
	err = m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, oldName)).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount, "old table should exist after SkipDropAfterCutover")
	assert.NoError(t, m.Close())
}

func TestBadOptions(t *testing.T) {
	// N.B. Because host, user, password and database have defaults enforced, we expect to
	// fail in the same way when they're not provided.
	t.Parallel()
	_, err := NewRunner(&Migration{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	_, err = NewRunner(&Migration{
		Host: cfg.Addr,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mydatabase",
		Table:    "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadAlter(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS bot1, bot2`)
	table := `CREATE TABLE bot1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	table = `CREATE TABLE bot2 (
		id int(11) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "badalter",
	})
	assert.Error(t, err) // parses and fails.
	assert.Nil(t, m)

	// Column renames are now supported, but this ALTER is invalid because
	// it references the old column name in the ADD INDEX after renaming it.
	// MySQL rejects this when Spirit applies the ALTER to the shadow table.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "RENAME COLUMN name TO name2, ADD INDEX(name)", // ADD INDEX references old name after rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err) // alter is invalid (MySQL rejects it)
	assert.NoError(t, m.Close())

	// Same issue via CHANGE COLUMN syntax
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "CHANGE name name2 VARCHAR(255), ADD INDEX(name)", // ADD INDEX references old name after rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err) // alter is invalid (MySQL rejects it)
	assert.NoError(t, m.Close())

	// But this is supported (no rename)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "CHANGE name name VARCHAR(200), ADD INDEX(name)", //nolint: dupword
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.NoError(t, err) // its valid, no rename
	assert.NoError(t, m.Close())

	// Test DROP PRIMARY KEY, change primary key.
	// The REPLACE statement likely relies on the same PRIMARY KEY on the new table,
	// so things get a lot more complicated if the primary key changes.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot2",
		Alter:    "DROP PRIMARY KEY",
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "dropping primary key")
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyNoAutoInc is a good test of the how much the
// chunker will boil the ocean:
//   - There is a MIN(key)=1 and a MAX(key)=8589934592
//   - There is no auto-increment so the chunker is allowed to expand each chunk
//     based on estimated rows (which is low).
//
// Only the key=8589934592 will fail to be converted. On my system this test
// currently runs in 0.4 seconds which is "acceptable" for chunker performance.
// The generated number of chunks should also be very low because of prefetching.
func TestChangeDatatypeLossyNoAutoInc(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lossychange2`)
	table := `CREATE TABLE lossychange2 (
					id BIGINT NOT NULL,
					name varchar(255) NOT NULL,
					b varchar(255) NOT NULL,
					PRIMARY KEY (id)
				)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (1, 'a', REPEAT('a', 200))")          // will pass in migration
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))") // will fail in migration

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "lossychange2",
		Alter:    "CHANGE COLUMN id id INT NOT NULL auto_increment", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Out of range value") // Error 1264: Out of range value for column 'id' at row 1
	// Check that the chunker processed fewer than 500 chunks
	_, chunksCopied, _ := m.copier.GetChunker().Progress()
	assert.Less(t, chunksCopied, uint64(500))
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossy3 has a data type change that is "lossy" but
// given the current stored data set does not cause errors.
func TestChangeDatatypeLossless(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lossychange3`)
	table := `CREATE TABLE lossychange3 (
				id BIGINT NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NULL,
				PRIMARY KEY (id)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO lossychange3 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange3 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "lossychange3",
		Alter:    "CHANGE COLUMN b b varchar(200) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err) // works because there are no violations.
	// Check that the chunker processed fewer than 500 chunks
	_, chunksCopied, _ := m.copier.GetChunker().Progress()
	assert.Less(t, chunksCopied, uint64(500))
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyFailEarly tests a scenario where there is an error
// immediately so the DDL should halt.
// So if it does try to exhaustively run the DDL it will take forever:
// [1, 8589934592] / 1000 = 8589934.592 chunks

func TestChangeDatatypeLossyFailEarly(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lossychange4`)
	table := `CREATE TABLE lossychange4 (
				id BIGINT NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NULL,
				PRIMARY KEY (id)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name) VALUES ('a')")
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange4 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "lossychange4",
		Alter:    "CHANGE COLUMN b b varchar(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err) // there is a violation where row 1 is NULL
	assert.NoError(t, m.Close())
}

// TestAddUniqueIndex is a really interesting test *because* resuming from checkpoint
// will cause duplicate key errors. It's not straight-forward to differentiate between
// duplicate errors from a resume, and a constraint violation. So what we do is:
// 0) *FORCE* checksum to be enabled (regardless now, its always on)
func TestAddUniqueIndexChecksumEnabled(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS uniqmytable`)
	table := `CREATE TABLE uniqmytable (
				id int(11) NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('b', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('c', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))") // duplicate

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "uniqmytable",
		Alter:    "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)         // not unique
	assert.NoError(t, m.Close()) // need to close now otherwise we'll get an error on re-opening it.

	testutils.RunSQL(t, "DELETE FROM uniqmytable WHERE b = REPEAT('a', 200) LIMIT 1") // make unique
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_chkpnt`)                   // make sure no checkpoint exists, we need to start again.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_new`)                      // cleanup temp table from first run
	m2, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "uniqmytable",
		Alter:    "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m2.Run(t.Context())
	assert.NoError(t, err) // works fine.
	assert.NoError(t, m2.Close())
}

func TestChangeNonIntPK(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS nonintpk`)
	table := `CREATE TABLE nonintpk (
			pk varbinary(36) NOT NULL PRIMARY KEY,
			name varchar(255) NOT NULL,
			b varchar(10) NOT NULL -- change to varchar(255)
		)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO nonintpk (pk, name, b) VALUES (UUID(), 'a', REPEAT('a', 5))")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "nonintpk",
		Alter:    "CHANGE COLUMN b b VARCHAR(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

// TestForRemainingTableArtifacts tests that the table is left after
// the migration is complete, but no _chkpnt or _new or _old table.
func TestForRemainingTableArtifacts(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS remainingtbl, _remainingtbl_new, _remainingtbl_old, _remainingtbl_chkpnt`)
	table := `CREATE TABLE remainingtbl (
		id INT NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "remainingtbl",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified.
	assert.NoError(t, m.Run(t.Context())) // it's an accepted type.
	assert.NoError(t, m.Close())

	// Now we should have a _remainingtbl_old table and a remainingtbl table
	// but no _remainingtbl_new table or _remainingtbl_chkpnt table.
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	stmt := `SELECT GROUP_CONCAT(table_name) FROM information_schema.tables where table_schema='test' and table_name LIKE '%remainingtbl%' ORDER BY table_name;`
	var tables string
	assert.NoError(t, db.QueryRowContext(t.Context(), stmt).Scan(&tables))
	assert.Equal(t, "remainingtbl", tables)
}

func TestDropColumn(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS dropcol, _dropcol_new`)
	table := `CREATE TABLE dropcol (
		id int(11) NOT NULL AUTO_INCREMENT,
		a varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		c varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into dropcol (id, a,b,c) values (1, 'a', 'b', 'c')`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "dropcol",
		Alter:    "DROP COLUMN b, ENGINE=InnoDB", // need both to ensure it is not instant!
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(t.Context()))

	assert.False(t, m.usedInstantDDL) // need to ensure it uses full process.
	assert.NoError(t, m.Close())
}

func TestDefaultPort(t *testing.T) {
	t.Parallel()
	m, err := NewRunner(&Migration{
		Host:     "localhost",
		Username: "root",
		Password: mkPtr("mypassword"),
		Database: "test",
		Threads:  2,
		Table:    "t1",
		Alter:    "DROP COLUMN b, ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	assert.Equal(t, "localhost:3306", m.migration.Host)
	m.SetLogger(slog.Default())
}

func TestNullToNotNull(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS autodatetime`)
	table := `CREATE TABLE autodatetime (
		id INT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO autodatetime (created_at) VALUES (NULL)`)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "autodatetime",
		Alter:    "modify column created_at datetime(3) not null default current_timestamp(3)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Column 'created_at' cannot be null")
	assert.NoError(t, m.Close())
}

func TestChunkerPrefetching(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS prefetchtest`)
	table := `CREATE TABLE prefetchtest (
		id BIGINT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	// insert about 11K rows.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) VALUES (NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 10000`)

	// the max id should be able 11040
	// lets insert one far off ID: 300B
	// and then continue inserting at greater than the max dynamic chunk size.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (300000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)

	// and then another big gap
	// and then continue inserting at greater than the max dynamic chunk size.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (600000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)
	// and then one final value which is way out there.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (900000000000, NULL)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "prefetchtest",
		Alter:    "engine=innodb",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestTpConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS tpconvert")
	testutils.RunSQL(t, `CREATE TABLE tpconvert (
	id bigint NOT NULL AUTO_INCREMENT primary key,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	issued_at timestamp NULL DEFAULT NULL,
	activated_at timestamp NULL DEFAULT NULL,
	deactivated_at timestamp NULL DEFAULT NULL,
	intasstring varchar(255) NULL DEFAULT NULL,
	floatcol FLOAT NULL DEFAULT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO tpconvert (created_at, updated_at, issued_at, activated_at, deactivated_at, intasstring, floatcol) VALUES
	('2023-05-18 09:28:46', '2023-05-18 09:33:27', '2023-05-18 09:28:45', '2023-05-18 09:28:45', NULL, '0001', 9.3),
	('2023-05-18 09:34:38', '2023-05-24 07:38:25', '2023-05-18 09:34:37', '2023-05-18 09:34:37', '2023-05-24 07:38:25', '10', 9.3),
	('2023-05-24 07:34:36', '2023-05-24 07:34:36', '2023-05-24 07:34:35', NULL, null, '01234', 9.3),
	('2023-05-24 07:41:05', '2023-05-25 06:15:37', '2023-05-24 07:41:04', '2023-05-24 07:41:04', '2023-05-25 06:15:37', '10', 2.2),
	('2023-05-25 06:17:30', '2023-05-25 06:17:30', '2023-05-25 06:17:29', '2023-05-25 06:17:29', NULL, '10', 9.3),
	('2023-05-25 06:18:33', '2023-05-25 06:41:13', '2023-05-25 06:18:32', '2023-05-25 06:18:32', '2023-05-25 06:41:13', '10', 1.1),
	('2023-05-25 06:24:23', '2023-05-25 06:24:23', '2023-05-25 06:24:22', NULL, null, '10', 9.3),
	('2023-05-25 06:41:35', '2023-05-28 23:45:09', '2023-05-25 06:41:34', '2023-05-25 06:41:34', '2023-05-28 23:45:09', '10', 9.3),
	('2023-05-25 06:44:41', '2023-05-28 23:45:03', '2023-05-25 06:44:40', '2023-05-25 06:46:48', '2023-05-28 23:45:03', '10', 9.3),
	('2023-05-26 06:24:24', '2023-05-28 23:45:01', '2023-05-26 06:24:23', '2023-05-26 06:24:42', '2023-05-28 23:45:01', '10', 9.3),
	('2023-05-28 23:46:07', '2023-05-29 00:57:55', '2023-05-28 23:46:05', '2023-05-28 23:46:05', NULL, '10', 9.3),
	('2023-05-28 23:53:34', '2023-05-29 00:57:56', '2023-05-28 23:53:33', '2023-05-28 23:58:09', NULL, '10', 9.3);`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "tpconvert",
		Alter: `MODIFY COLUMN created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		MODIFY COLUMN issued_at TIMESTAMP(6) NULL,
		MODIFY COLUMN activated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN deactivated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN intasstring INT NULL DEFAULT NULL
		`,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestPartitionedTable(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS part1, _part1_new`)
	table := `CREATE TABLE part1 (
			id bigint(20) NOT NULL AUTO_INCREMENT,
			partition_id smallint(6) NOT NULL,
			created_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			initiated_at timestamp(3) NULL DEFAULT NULL,
			version int(11) NOT NULL DEFAULT '0',
			type varchar(50) DEFAULT NULL,
			token varchar(255) DEFAULT NULL,
			PRIMARY KEY (id,partition_id),
			UNIQUE KEY idx_token (token,partition_id)
		  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC
		  /*!50100 PARTITION BY LIST (partition_id)
		  (PARTITION p0 VALUES IN (0) ENGINE = InnoDB,
		   PARTITION p1 VALUES IN (1) ENGINE = InnoDB,
		   PARTITION p2 VALUES IN (2) ENGINE = InnoDB,
		   PARTITION p3 VALUES IN (3) ENGINE = InnoDB,
		   PARTITION p4 VALUES IN (4) ENGINE = InnoDB,
		   PARTITION p5 VALUES IN (5) ENGINE = InnoDB,
		   PARTITION p6 VALUES IN (6) ENGINE = InnoDB,
		   PARTITION p7 VALUES IN (7) ENGINE = InnoDB) */`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into part1 values (1, 1, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 2, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 3, NOW(), NOW(), NOW(), 1, 'type', 'token2')`) //nolint: dupword

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "part1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified.
	assert.NoError(t, m.Run(t.Context())) // should work.
	assert.NoError(t, m.Close())
}

func TestVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS varchart1`)
	table := `CREATE TABLE varchart1 (
				pk varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (pk)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM dual ")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "varchart1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	tableName := `skipdrop_test`

	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	table := fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName)

	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "skipdrop_test",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: true,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = m.db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount)
	assert.NoError(t, m.Close())
}

func TestDropAfterCutover(t *testing.T) {
	t.Parallel()
	tableName := `drop_test`
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	table := fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName)

	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "drop_test",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = m.db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOver(t *testing.T) {
	t.Skip("skipping: this test waits for sentinelWaitLimit to expire, which is too slow with the current 48 hour limit")
	t.Parallel()

	// Create unique database for this test
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	tableName := `deferred_cutover`
	newName := fmt.Sprintf("_%s_new", tableName)
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "deferred_cutover",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Go(func() {
		err = m.Run(t.Context())
		assert.Error(t, err)
		assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	})

	// While it's waiting, check the Progress.
	waitForStatus(t, m, status.WaitingOnSentinelTable)
	wg.Wait()

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, newName)
	var tableCount int
	err = m.db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOverE2E(t *testing.T) {
	t.Parallel()

	// Create unique database for this test
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e`
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	// Add cleanup handler to guarantee table cleanup even on failure/timeout
	t.Cleanup(func() {
		db, _ := sql.Open("mysql", testutils.DSNForDatabase(dbName))
		defer func() { _ = db.Close() }()
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(
			"DROP TABLE IF EXISTS %s, _%s_new, _%s_old, _%s_chkpnt, %s",
			tableName, tableName, tableName, tableName, sentinelTableName))
	})

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              1,
		Table:                "deferred_cutover_e2e",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	go func() {
		err := m.Run(t.Context())
		assert.NoError(t, err)
		c <- err
	}()

	// wait until the sentinel table exists
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	for {
		var rowCount int
		sql := fmt.Sprintf(
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'`, dbName, sentinelTableName)
		err = db.QueryRowContext(t.Context(), sql).Scan(&rowCount)
		assert.NoError(t, err)
		if rowCount > 0 {
			break
		}
	}
	assert.NoError(t, err)

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c // wait for the migration to finish
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOverE2EBinlogAdvance(t *testing.T) {
	t.Parallel()
	// Create unique database for this test
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e_stage`
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	// Add cleanup handler to guarantee table cleanup even on failure/timeout
	t.Cleanup(func() {
		db, _ := sql.Open("mysql", testutils.DSNForDatabase(dbName))
		defer func() { _ = db.Close() }()
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(
			"DROP TABLE IF EXISTS %s, _%s_new, _%s_old, _%s_chkpnt, %s",
			tableName, tableName, tableName, tableName, sentinelTableName))
	})

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              1,
		Table:                "deferred_cutover_e2e_stage",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	go func() {
		err := m.Run(t.Context())
		assert.NoError(t, err)
		c <- err
	}()

	// wait until the sentinel table exists
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	waitForStatus(t, m, status.WaitingOnSentinelTable)

	binlogPos := m.replClient.GetBinlogApplyPosition()
	for range 4 {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))
		assert.NoError(t, m.replClient.BlockWait(t.Context()))
		assert.NoError(t, m.replClient.Flush(t.Context()))
		newBinlogPos := m.replClient.GetBinlogApplyPosition()
		assert.Equal(t, 1, newBinlogPos.Compare(binlogPos))
		binlogPos = newBinlogPos
	}

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c // wait for the migration to finish
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = db.QueryRowContext(t.Context(), sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestPreRunChecksE2E(t *testing.T) {
	t.Parallel()
	// We test the checks in tests for that package, but we also want to test
	// that the checks run correctly when instantiating a migration.

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "test_checks_e2e",
		Alter:    "engine=innodb",
	})
	assert.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	err = m.runChecks(t.Context(), check.ScopePreRun)
	assert.NoError(t, err)
}

// From https://github.com/block/spirit/issues/241
// If an ALTER qualifies as instant, but an instant can't apply, don't burn an instant version.
func TestForNonInstantBurn(t *testing.T) {
	t.Parallel()
	// We skip this test in MySQL 8.0.28. It uses INSTANT_COLS instead of total_row_versions
	// and it supports instant add col, but not instant drop col.
	// It's safe to skip, but we need 8.0.28 in tests because it's the minor version
	// used by Aurora's LTS.
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	err = db.QueryRowContext(t.Context(), `SELECT version()`).Scan(&version)
	assert.NoError(t, err)
	if version == "8.0.28" {
		t.Skip("Skipping this test for MySQL 8.0.28")
	}
	if strings.HasPrefix(version, "9.") {
		t.Skip("Skipping this test for MySQL 9.x: total_row_versions limit was raised beyond 64")
	}
	// Continue with the test.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS instantburn`)
	table := `CREATE TABLE instantburn (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	rowVersions := func() int {
		// Check that the number of total_row_versions is Zero (i'e doesn't burn)
		db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		defer utils.CloseAndLog(db)
		var rowVersions int
		err = db.QueryRowContext(t.Context(), `SELECT total_row_versions FROM INFORMATION_SCHEMA.INNODB_TABLES where name='test/instantburn'`).Scan(&rowVersions)
		assert.NoError(t, err)
		return rowVersions
	}

	testutils.RunSQL(t, table)
	for range 32 { // requires 64 instants
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, ADD newcol INT")
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, DROP newcol")
	}
	assert.Equal(t, 64, rowVersions()) // confirm all 64 are used.
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "instantburn",
		Alter:    "add newcol2 int",
	})
	assert.NoError(t, err)
	defer utils.CloseAndLog(m)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.False(t, m.usedInstantDDL) // it would have had to apply a copy.
	assert.Equal(t, 0, rowVersions()) // confirm we reset to zero, not 1 (no burn)
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
// i.e. if anything it's more a side-effect than a workaround.
func TestIndexVisibility(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS indexvisibility`)
	table := `CREATE TABLE indexvisibility (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`
	testutils.RunSQL(t, table)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b INVISIBLE",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.True(t, m.usedInplaceDDL) // expected to count as safe.
	assert.NoError(t, m.Close())

	// Test again with visible
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m.usedInplaceDDL) // expected to count as safe.
	assert.NoError(t, m.Close())

	// Test again but include an unsafe INPLACE change at the same time.
	// This won't work by default.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE, ADD INDEX (c)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close()) // it's errored, we don't need to try again. We can close.

	// The above should now fail with enhanced automatic detection.
	// Index visibility mixed with table-rebuilding operations should be rejected.

	// Index visibility mixed with table-rebuilding operations should fail.
	// This is important because invisible should never be mixed with copy
	// (the semantics are weird since it's for experiments).
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE, CHANGE c cc BIGINT NOT NULL",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close()) // it's errored, we don't need to try again. We can close.
}

// TestPreventConcurrentRuns ensures that metadata locking
// prevents two concurrent migrations on the same table.
// We use DeferCutOver=true option to force the first migration
// to stay running.
func TestPreventConcurrentRuns(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `prevent_concurrent_runs`

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	defer utils.CloseAndLog(m)
	wg := sync.WaitGroup{}
	wg.Go(func() {
		// Shadow err to avoid a data race
		err := m.Run(t.Context())
		assert.Error(t, err)
		// The error can either be context cancelled or timed out.
		// Both are acceptable
		if !errors.Is(err, context.Canceled) {
			assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
		}
	})

	// Wait until m has reached the sentinel wait phase before starting m2.
	waitForStatus(t, m, status.WaitingOnSentinelTable)

	m2, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             &cfg.Passwd,
		Database:             dbName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
	})
	assert.NoError(t, err)
	err = m2.Run(t.Context())
	defer utils.CloseAndLog(m2)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not acquire metadata lock")

	// Cancel the first migration rather than waiting for the sentinel timeout
	// (which could take up to sentinelWaitLimit).
	m.Cancel()
	wg.Wait()
}

func TestStatementWorkflowStillInstant(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS stmtworkflow`)
	table := `CREATE TABLE stmtworkflow (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`
	testutils.RunSQL(t, table)
	m, err := NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "ALTER TABLE stmtworkflow ADD newcol INT",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.True(t, m.usedInstantDDL) // expected to count as instant.
	assert.NoError(t, m.Close())
}

func TestTrailingSemicolon(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS multiSecondary`)
	testutils.RunSQL(t, `CREATE TABLE multiSecondary (
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
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "multiSecondary",
		Alter:    dropIndexesAlter,
		Threads:  1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	assert.True(t, m.usedInplaceDDL) // DROP INDEX operations now use INPLACE for better performance
	assert.NoError(t, m.Close())

	m, err = NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Statement: "alter table multiSecondary add index idx1(v), add index idx2(v), add index idx3(v), add index idx4(v);",
		Threads:   1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	require.False(t, m.usedInplaceDDL) // ADD INDEX operations now use copy process for replica safety
	require.NoError(t, m.Close())

	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "multiSecondary",
		// https://github.com/block/spirit/issues/384
		Alter:   dropIndexesAlter + "; ",
		Threads: 1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
}
func TestAlterExtendVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1extendvarchar, _t1extendvarchar_new`)
	table := `CREATE TABLE t1extendvarchar (
		id int not null primary key auto_increment,
		col1 varchar(10),
		col2 varchar(10)
	) character set utf8mb4`
	testutils.RunSQL(t, table)

	type alterAttempt struct {
		Statement string
		Error     bool
		InPlace   bool
	}
	alters := []alterAttempt{
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(20)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar CHANGE col1 col1 varchar(21)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(22), CHANGE col2 col2 varchar(22) `, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(23), CHANGE col2 col2 varchar(200) `, InPlace: false},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(200)`, InPlace: false},
	}

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	for _, attempt := range alters {
		m, err := NewRunner(&Migration{
			Host:      cfg.Addr,
			Username:  cfg.User,
			Password:  &cfg.Passwd,
			Database:  cfg.DBName,
			Threads:   1,
			Statement: attempt.Statement,
		})
		require.NoError(t, err)
		err = m.Run(t.Context())
		require.NoError(t, err)
		assert.Equal(t, attempt.InPlace, m.usedInplaceDDL)

		// go test howls about resource leaks if we don't close all these things
		err = m.Close()
		assert.NoError(t, err)
	}
}

func TestMigrationCancelledFromTableModification(t *testing.T) {
	t.Parallel()
	// This test covers the case where a migration is running
	// and the user modifies the table (e.g. with another ALTER).
	// The migration should detect this and cancel itself.
	// We use a long-running copy phase to give us time to do the modification.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1modification, _t1modification_new`)
	table := `CREATE TABLE t1modification (
		id int not null primary key auto_increment,
		col1 varbinary(1024),
		col2 varbinary(1024)
	) character set utf8mb4`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM dual ")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond, // weak performance at copying.
		Statement:       "ALTER TABLE t1modification ENGINE=InnoDB",
	})
	require.NoError(t, err)

	// Start the migration in a goroutine
	wg := sync.WaitGroup{}
	wg.Add(1)
	var gErr error
	go func() {
		defer wg.Done()
		gErr = m.Run(t.Context())
	}()

	// Wait until the copy phase has started.
	waitForStatus(t, m, status.CopyRows)

	// Now modify the table
	// instant DDL (applies quickly and will cause the migration to cancel)
	testutils.RunSQL(t, "ALTER TABLE t1modification ADD col3 INT")

	wg.Wait() // wait for the error to occur.

	require.Error(t, gErr)
	require.NoError(t, m.Close())
}

func TestPasswordMasking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "basic DSN with password",
			input:    "user:password@tcp(localhost:3306)/database",
			expected: "user:***@tcp(localhost:3306)/database",
		},
		{
			name:     "DSN with complex password",
			input:    "myuser:c0mplex!Pa$$w0rd@tcp(db.example.com:3306)/mydb",
			expected: "myuser:***@tcp(db.example.com:3306)/mydb",
		},
		{
			name:     "DSN without password",
			input:    "user@tcp(localhost:3306)/database",
			expected: "user@tcp(localhost:3306)/database",
		},
		{
			name:     "DSN with empty password",
			input:    "user:@tcp(localhost:3306)/database",
			expected: "user:***@tcp(localhost:3306)/database",
		},
		{
			name:     "empty DSN",
			input:    "",
			expected: "",
		},
		{
			name:     "malformed DSN without @",
			input:    "user:password",
			expected: "user:password",
		},
		{
			name:     "DSN with colon in password",
			input:    "user:pass:word@tcp(localhost:3306)/database",
			expected: "user:***@tcp(localhost:3306)/database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := maskPasswordInDSN(tt.input)
			assert.Equal(t, tt.expected, result, "Password masking failed for input: %s", tt.input)
		})
	}
}

func TestDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		user     string
		password string
		host     string
		schema   string
	}{
		{
			name:     "simple password",
			user:     "root",
			password: "secret",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "password with @",
			user:     "root",
			password: "p@ssword",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "password with multiple @",
			user:     "root",
			password: "p@ss@word",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "password with special characters",
			user:     "root",
			password: "p@ss:word/with#special!chars",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "empty password",
			user:     "root",
			password: "",
			host:     "127.0.0.1:3306",
			schema:   "testdb",
		},
		{
			name:     "AWS IAM-style token",
			user:     "iam_user",
			password: "aaa@bbb.ccc.us-east-1.rds.amazonaws.com:3306/?Action=connect&DBUser=iam_user",
			host:     "mydb.cluster-xyz.us-east-1.rds.amazonaws.com:3306",
			schema:   "production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pw := tt.password
			r := &Runner{
				migration: &Migration{
					Username: tt.user,
					Password: &pw,
					Host:     tt.host,
				},
				changes: []*change{
					{
						stmt: &statement.AbstractStatement{
							Schema: tt.schema,
						},
					},
				},
			}

			dsn := r.dsn()

			// Parse the DSN back and verify all fields round-trip correctly.
			cfg, err := mysql.ParseDSN(dsn)
			require.NoError(t, err)
			assert.Equal(t, tt.user, cfg.User)
			assert.Equal(t, tt.password, cfg.Passwd)
			assert.Equal(t, tt.host, cfg.Addr)
			assert.Equal(t, tt.schema, cfg.DBName)
			assert.Equal(t, "tcp", cfg.Net)
		})
	}
}

// TestEnumReorder tests that reordering ENUM values in an ALTER TABLE
// produces correct data after migration.
//
// This test only works correctly in unbuffered mode because of the way
// ENUM values are represented in the binlog. We test *both* unbuffered and buffered modes
// though and we accept a pre-flight failure as a "pass", since it's not corruption.
// i.e. it's OK to refuse changes you can't handle.
//
// The unbuffered path uses
// REPLACE INTO ... SELECT (SQL-level string operations) which handles
// ENUM reordering correctly. The buffered path uses UpsertRows with
// raw binlog values, where ENUM values are represented as int64 ordinals.
// If the ENUM is reordered, the ordinals map to different string values
// in the target table, causing data corruption.
//
// This test exercises both the copier path (initial data) and the binlog
// replay path (concurrent DML during migration) to verify correctness.
func TestEnumReorder(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testEnumReorder(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testEnumReorder(t, true)
	})
}

func testEnumReorder(t *testing.T, enableBuffered bool) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumreorder, _enumreorder_new, _enumreorder_chkpnt`)
	testutils.RunSQL(t, `CREATE TABLE enumreorder (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)

	// Insert enough initial data so the copy phase takes a measurable amount of time,
	// giving the concurrent DML goroutine a window to inject changes that will be
	// captured by the binlog subscription and replayed via the flush path.
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) VALUES ('active'), ('inactive'), ('pending')`)
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 6
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 12
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 24
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 48
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 96
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 192
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 384
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 768
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 1536
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 3072
	testutils.RunSQL(t, `INSERT INTO enumreorder (status) SELECT status FROM enumreorder`) // 6144

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use NewRunner so we can access the runner's status to synchronize
	// concurrent DML injection during the copy phase.
	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "enumreorder",
		Alter:           "MODIFY COLUMN status ENUM('pending', 'active', 'inactive') NOT NULL",
		Buffered:        enableBuffered,
	})
	require.NoError(t, err)

	// Open a separate DB connection for DML that won't interfere with test assertions.
	// Spirit's force-kill mechanism may kill DML connections during the checksum/lock
	// phase, so we must not use testutils.RunSQL (which asserts no error).
	dmlDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(dmlDB)

	// Run concurrent DML in a goroutine to exercise the binlog replay path.
	// These inserts happen while the copier is running, so they'll be captured
	// by the binlog subscription and flushed via deltaMap (unbuffered) or
	// bufferedMap (buffered).
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		// Wait until we're in the copy phase
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		// Insert rows with each ENUM value during the copy phase.
		// These will be picked up by the binlog and replayed.
		// We ignore errors because Spirit may kill our connection during
		// the checksum/lock phase (force-kill of blocking transactions).
		for i := 0; i < 50; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('active')`)
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('inactive')`)
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('pending')`)
			// Also update some existing rows to exercise the update path
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'active' WHERE id = %d`, i*3+1))
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'inactive' WHERE id = %d`, i*3+2))
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'pending' WHERE id = %d`, i*3+3))
		}
	}()

	// Run the migration. For unbuffered mode this should succeed with correct data.
	// For buffered mode, the preflight check should refuse the ENUM reorder
	// because the binlog replay path uses integer ordinals that would cause corruption.
	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	assert.NoError(t, m.Close())

	if enableBuffered {
		// Buffered mode: the preflight check should refuse this migration because
		// ENUM reordering is unsafe when the binlog replay path uses integer ordinals.
		// Accepting this preflight error as a "pass" — it's not corruption, it's
		// Spirit correctly refusing a change it can't handle safely.
		require.Error(t, migrationErr)
		assert.ErrorContains(t, migrationErr, "unsafe ENUM value reorder")
		return
	}

	// Unbuffered mode: migration should succeed and data should be correct.
	require.NoError(t, migrationErr)

	// Verify that every row has a valid ENUM string value and that
	// no ordinal-based corruption occurred.
	var activeCount, inactiveCount, pendingCount int
	err = db.QueryRowContext(t.Context(), `SELECT
			SUM(status = 'active'),
			SUM(status = 'inactive'),
			SUM(status = 'pending')
			FROM enumreorder`).Scan(&activeCount, &inactiveCount, &pendingCount)
	require.NoError(t, err)

	var totalCount int
	err = db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM enumreorder`).Scan(&totalCount)
	require.NoError(t, err)

	// Every row must map to one of the three valid ENUM string values.
	assert.Equal(t, totalCount, activeCount+inactiveCount+pendingCount,
		"all rows should have a valid ENUM value (no empty strings from ordinal corruption)")
	assert.Greater(t, activeCount, 0, "should have 'active' rows")
	assert.Greater(t, inactiveCount, 0, "should have 'inactive' rows")
	assert.Greater(t, pendingCount, 0, "should have 'pending' rows")
}

// TestSetReorder mirrors TestEnumReorder but for SET columns.
// SET values are stored as bitmasks in the binlog, so reordering has the same
// corruption risk as ENUM in buffered mode.
func TestSetReorder(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testSetReorder(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testSetReorder(t, true)
	})
}

func testSetReorder(t *testing.T, enableBuffered bool) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS setreorder, _setreorder_new, _setreorder_chkpnt`)
	testutils.RunSQL(t, `CREATE TABLE setreorder (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		perms SET('read', 'write', 'execute') NOT NULL
	)`)

	testutils.RunSQL(t, `INSERT INTO setreorder (perms) VALUES ('read'), ('write'), ('execute'), ('read,write'), ('read,execute'), ('write,execute'), ('read,write,execute')`)
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 14
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 28
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 56
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 112
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 224
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 448
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 896
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 1792
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 3584
	testutils.RunSQL(t, `INSERT INTO setreorder (perms) SELECT perms FROM setreorder`) // 7168

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "setreorder",
		Alter:           "MODIFY COLUMN perms SET('execute', 'read', 'write') NOT NULL",
		Buffered:        enableBuffered,
	})
	require.NoError(t, err)

	dmlDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(dmlDB)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		for i := 0; i < 50; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('read')`)
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('write,execute')`)
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('read,write,execute')`)
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'read,write' WHERE id = %d`, i*3+1))
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'execute' WHERE id = %d`, i*3+2))
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'read,write,execute' WHERE id = %d`, i*3+3))
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	assert.NoError(t, m.Close())

	// Both buffered and unbuffered modes should refuse SET reordering.
	// In buffered mode, the binlog replay path uses bitmask ordinals that corrupt data.
	// In unbuffered mode, the data is actually correct but MySQL outputs SET members
	// in definition order, so the string representation changes (e.g. "read,execute"
	// becomes "execute,read"), causing Spirit's CRC32 checksum to always fail.
	require.Error(t, migrationErr)
	assert.ErrorContains(t, migrationErr, "unsafe SET value reorder")
}

// TestBufferedMigrationFailsGracefullyWithMinimalRBR verifies that a buffered
// migration fails gracefully when it receives minimal RBR events. The buffered
// mode uses a buffered applier which requires full row images. We simulate a
// rogue session that has SET binlog_row_image = 'minimal' at the session level,
// which causes its DML to produce minimal row images in the binlog even though
// the global setting is FULL.
func TestBufferedMigrationFailsGracefullyWithMinimalRBR(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner (global setting already minimal)")
	}

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS minrbr_buffered, _minrbr_buffered_new, _minrbr_buffered_chkpnt`)
	testutils.RunSQL(t, `CREATE TABLE minrbr_buffered (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		val INT NOT NULL DEFAULT 0
	)`)

	// Insert some data. The test throttler will slow the copier down,
	// giving the repl client time to process the minimal-RBR events.
	testutils.RunSQL(t, `INSERT INTO minrbr_buffered (name, val) VALUES ('seed', 1)`)
	for range 8 {
		testutils.RunSQL(t, `INSERT INTO minrbr_buffered (name, val) SELECT CONCAT(name, '-', id), val FROM minrbr_buffered`)
	}

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Open a dedicated connection with session-level minimal RBR.
	// DML on this connection will produce minimal row images in the binlog.
	minimalDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(minimalDB)

	// Force a single connection so the session variable sticks.
	minimalDB.SetMaxOpenConns(1)
	_, err = minimalDB.ExecContext(t.Context(), "SET binlog_row_image = 'MINIMAL'")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m, err := NewRunner(&Migration{
		Host:             cfg.Addr,
		Username:         cfg.User,
		Password:         &cfg.Passwd,
		Database:         cfg.DBName,
		Threads:          1,
		TargetChunkTime:  100 * time.Millisecond,
		Table:            "minrbr_buffered",
		Alter:            "ENGINE=InnoDB",
		Buffered:         true,
		useTestThrottler: true, // slows the copier so the repl client has time to see minimal events
	})
	require.NoError(t, err)

	// Run the migration in a goroutine so we can inject minimal-RBR writes
	// once the copy phase has started.
	var migrationErr error
	migrationDone := make(chan struct{})
	go func() {
		defer close(migrationDone)
		migrationErr = m.Run(ctx)
	}()

	// Wait until the migration is in the copy phase, then start writing
	// with minimal RBR. This ensures the repl client is actively reading
	// binlog events when the minimal row images arrive.
	waitForStatus(t, m, status.CopyRows)

	// Continuously write using the minimal-RBR session during the copy phase.
	// The test throttler slows the copier, so these writes will be picked up
	// by the binlog subscription before the copy phase finishes.
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = minimalDB.ExecContext(ctx, `UPDATE minrbr_buffered SET val = val + 1 WHERE id = ?`, (i%100)+1)
			}
		}
	}()

	// Wait for the migration to complete (it should fail), then stop the writer.
	<-migrationDone
	cancel()
	writerWg.Wait()
	assert.NoError(t, m.Close())

	// The migration should fail because the runtime check detects minimal RBR
	// events while a buffered applier is in use. The repl client cancels
	// the caller's context, so the error may be context.Canceled or may
	// contain the original "minimal RBR" message depending on which
	// operation observes the cancellation first.
	require.Error(t, migrationErr)
}

func TestMigrationWithSQLCommentsInStatement(t *testing.T) {
	// This test verifies that Spirit correctly handles SQL comments
	// prepended to ALTER TABLE statements when using the Statement field
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS t1_comment_test")
	testutils.RunSQL(t, "CREATE TABLE t1_comment_test (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT)")
	testutils.RunSQL(t, "INSERT INTO t1_comment_test (a) VALUES (1), (2), (3)")

	// Statement with multiple SQL comments before the ALTER — this is exactly
	// what our tool passes when users include comments in their .sql files.
	statementWithComments := `-- Migration for JIRA-1234
-- Author: someone@block.xyz
-- Date: 2025-07-01
-- This migration adds an index on column a
-- for improved query performance on the dashboard.
ALTER TABLE t1_comment_test ADD INDEX idx_a (a)`

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Statement: statementWithComments,
	}

	// Use NewRunner so we can inspect the parsed changes before running.
	r, err := NewRunner(migration)
	require.NoError(t, err)
	require.Len(t, r.changes, 1)
	assert.Equal(t, "ADD INDEX `idx_a`(`a`)", r.changes[0].stmt.Alter)

	err = migration.Run()
	assert.NoError(t, err)

	// Verify the index was actually created
	var indexName string
	err = db.QueryRowContext(t.Context(), "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t1_comment_test' AND INDEX_NAME='idx_a'").Scan(&indexName)
	assert.NoError(t, err)
	assert.Equal(t, "idx_a", indexName)
	testutils.RunSQL(t, "DROP TABLE IF EXISTS t1_comment_test")
}
