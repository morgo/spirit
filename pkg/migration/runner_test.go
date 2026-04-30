//nolint:dupword
package migration

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// are still supported. From https://github.com/block/spirit/issues/277
// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00 can still be migrated.
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
