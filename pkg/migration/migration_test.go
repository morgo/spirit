package migration

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	status.CheckpointDumpInterval = 100 * time.Millisecond
	status.StatusInterval = 10 * time.Millisecond // the status will be accurate to 1ms
	sentinelCheckInterval = 100 * time.Millisecond
	goleak.VerifyTestMain(m)
}

func TestE2ENullAlterEmpty(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1e2e",
		`CREATE TABLE t1e2e (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	m := NewTestRunner(t, "t1e2e", "ENGINE=InnoDB", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestE2EExplicitAutoIncrementInAlter(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1explicit_autoinc",
		`CREATE TABLE t1explicit_autoinc (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB AUTO_INCREMENT=1000`)

	// User explicitly sets AUTO_INCREMENT=5000 in the ALTER
	m := NewTestRunner(t, "t1explicit_autoinc", "ADD COLUMN test_col VARCHAR(255), AUTO_INCREMENT=5000")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())

	// Verify that new inserts start from 5000
	// Note: We use INSERT behavior rather than information_schema.TABLES because
	// InnoDB may cache AUTO_INCREMENT values and information_schema may show stale data.
	testutils.RunSQL(t, "INSERT INTO t1explicit_autoinc (name) VALUES ('test')")
	var insertedID int64
	err := tt.DB.QueryRowContext(t.Context(), "SELECT MAX(id) FROM t1explicit_autoinc").Scan(&insertedID)
	require.NoError(t, err)
	assert.Equal(t, int64(5000), insertedID, "User-specified AUTO_INCREMENT=5000 should be preserved, first insert should get ID 5000")
}

func TestMissingAlter(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1missing",
		`CREATE TABLE t1missing (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	m := NewTestMigration(t, WithTable("t1missing"), WithAlter(""))
	err := m.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadDatabaseCredentials(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1bad",
		`CREATE TABLE t1bad (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	m := NewTestMigration(t, WithTable("t1bad"), WithAlter("ENGINE=InnoDB"))
	m.Host = "127.0.0.1:9999"
	err := m.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "connect: connection refused")
}

func TestE2ENullAlter1Row(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1nullalter",
		`CREATE TABLE t1nullalter (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	tt.SeedRows(t, "INSERT INTO t1nullalter (name) SELECT 'aaa'", 1)

	m := NewTestRunner(t, "t1nullalter", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestE2ENullAlterWithReplicas(t *testing.T) {
	t.Parallel()
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping replica tests because REPLICA_DSN not set")
	}
	testutils.NewTestTable(t, "replicatest",
		`CREATE TABLE replicatest (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	m := NewTestMigration(t, WithTable("replicatest"), WithAlter("ENGINE=InnoDB"))
	m.ReplicaDSN = replicaDSN
	m.ReplicaMaxLag = 10 * time.Second
	require.NoError(t, m.Run())
}

// TestRenameInMySQL80 tests that even though renames are not supported,
// if the version is 8.0 it will apply the instant operation before
// the rename check applies. It's only when it needs to actually migrate
// that it won't allow renames.
func TestRenameInMySQL80(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "renamet1",
		`CREATE TABLE renamet1 (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	m := NewTestRunner(t, "renamet1", "CHANGE name nameNew varchar(255) not null")
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

// TestUniqueOnNonUniqueData tests that we:
// 1. Fail trying to add a unique index on non-unique data.
// 2. The error does not blame spirit, but is instead suggestive of user-data error.
func TestUniqueOnNonUniqueData(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "uniquet1",
		`CREATE TABLE uniquet1 (id int not null primary key auto_increment, b int not null, pad1 varbinary(1024))`)
	tt.SeedRows(t, "INSERT INTO uniquet1 (b, pad1) SELECT 1, RANDOM_BYTES(1024)", 100000)
	// Make almost-unique with 2 duplicates.
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = id`)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = 12345 ORDER BY RAND() LIMIT 2`)

	m := NewTestRunner(t, "uniquet1", "ADD UNIQUE (b)")
	err := m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data")
	assert.NoError(t, m.Close())
}

func TestGeneratedColumns(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testGeneratedColumns(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testGeneratedColumns(t, true)
	})
}

func testGeneratedColumns(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "t1generated",
		`CREATE TABLE t1generated (
			id int not null primary key auto_increment,
			b int not null,
			c int GENERATED ALWAYS AS (b + 1),
			d int
		)`)
	tt.SeedRows(t, "INSERT INTO t1generated (b, d) SELECT 1, 10", 3)

	m := NewTestRunner(t, "t1generated", "ENGINE=InnoDB",
		WithThreads(1), WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestStoredGeneratedColumns(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testStoredGeneratedColumns(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testStoredGeneratedColumns(t, true)
	})
}

func testStoredGeneratedColumns(t *testing.T, enableBuffered bool) {
	testutils.NewTestTable(t, "t1stored",
		`CREATE TABLE t1stored (
			id bigint NOT NULL AUTO_INCREMENT,
			pa bigint DEFAULT NULL,
			p1 bigint DEFAULT NULL,
			p2 bigint DEFAULT NULL,
			s1 tinyint(1) GENERATED ALWAYS AS (if((pa is not null),1,NULL)) STORED,
			s2 tinyint(1) GENERATED ALWAYS AS (if((p1 is not null),1,NULL)) STORED,
			s3 tinyint(1) GENERATED ALWAYS AS (if((p2 is not null),1,NULL)) STORED,
			s4 tinyint(1) GENERATED ALWAYS AS (if((pa <> p2),1,NULL)) STORED,
			p3 int not null,
			PRIMARY KEY (id)
		)`)
	//nolint: dupword
	testutils.RunSQL(t, `INSERT INTO t1stored (pa, p1, p2, p3)
VALUES
(1, 1, 1, 99),
(1, NULL, 1, 98),
(1, 1, NULL, 97),
(1, NULL, NULL, 96),
(1, 1, 0, 95),
(1, 0, 1, 94),
(1, 0, 0, 93),
(1, NULL, 0, 92),
(1, 0, NULL, 91),
(1, NULL, NULL, 90),
(NULL, NULL, NULL, 89)
`)
	// Uses Statement field because the ALTER modifies a stored generated column expression.
	m := NewTestRunnerFromStatement(t, `ALTER TABLE t1stored
MODIFY COLUMN s4 TINYINT(1)
GENERATED ALWAYS AS (
IF(
 pa <> p2
     OR (pa IS NULL AND p2 IS NOT NULL)
     OR (pa IS NOT NULL AND p2 IS NULL),
 1,
 NULL
)
) STORED`, WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

type testcase struct {
	OldType string
	NewType string
}

// TestBinaryChecksum tests that we can alter a binary column and still get a checksum match.
// It works fine from varbinary(50)->varbinary(100), but not from binary(50)->binary(100),
// without an intermediate cast.
func TestBinaryChecksum(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testBinaryChecksum(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testBinaryChecksum(t, true)
	})
}

func testBinaryChecksum(t *testing.T, enableBuffered bool) {
	tests := []testcase{
		{"binary(50)", "varbinary(100)"},
		{"binary(50)", "binary(100)"},
		{"varbinary(100)", "varbinary(50)"},
		{"varbinary(100)", "binary(50)"},
		{"blob", "tinyblob"},
		{"tinyblob", "blob"},
		{"mediumblob", "tinyblob"},
		{"longblob", "mediumblob"},
		{"binary(100)", "blob"},
		{"blob", "binary(100)"},
	}
	for _, test := range tests {
		tt := testutils.NewTestTable(t, "t1varbin",
			fmt.Sprintf(`CREATE TABLE t1varbin (
				id int not null primary key auto_increment,
				b %s not null
			)`, test.OldType))
		tt.SeedRows(t, "INSERT INTO t1varbin (b) SELECT 'abcdefg'", 1)

		m := NewTestRunner(t, "t1varbin",
			fmt.Sprintf("CHANGE b b %s not null", test.NewType), //nolint: dupword
			WithThreads(1), WithBuffered(enableBuffered))
		require.NoError(t, m.Run(t.Context()))
		assert.NoError(t, m.Close())
	}
}

// TestConvertCharset tests that we can change the character set from latin1 to utf8mb4,
// and that the non 7-bit characters that can be represented in latin1 as 1 byte,
// checksum correctly against their multi-byte utf8mb4 representations
func TestConvertCharset(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testConvertCharset(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testConvertCharset(t, true)
	})
}

func testConvertCharset(t *testing.T, enableBuffered bool) {
	testutils.NewTestTable(t, "t1charset",
		`CREATE TABLE t1charset (
			id int not null primary key auto_increment,
			b varchar(100) not null
		) charset=latin1`)
	// These specific characters test latin1→utf8mb4 multi-byte conversion.
	testutils.RunSQL(t, `insert into t1charset values (null, 'à'), (null, '€')`)

	m := NewTestRunner(t, "t1charset", "CONVERT TO CHARACTER SET UTF8MB4",
		WithThreads(1), WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())

	// Because utf8mb4 is the superset, it doesn't matter that that's
	// what the checksum casts to. We should be able to convert back as well.
	m = NewTestRunner(t, "t1charset", "CONVERT TO CHARACTER SET latin1",
		WithThreads(1), WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestStmtWorkflow(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1s, _t1s_new`)

	// CREATE TABLE via Statement path.
	migration := NewTestMigration(t, WithStatement(`CREATE TABLE t1s (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`))
	require.NoError(t, migration.Run())

	// ALTER TABLE via Statement path.
	r := NewTestRunnerFromStatement(t, "ALTER TABLE t1s ADD COLUMN c int")
	require.NoError(t, r.Run(t.Context()))
	assert.NoError(t, r.Close())
}

// TestUnparsableStatements tests that the behavior is expected in cases
// where we know the TiDB parser does not support the statement. We document
// that we require the TiDB parser to parse the statement for it to execute,
// which feels like a reasonable limitation based on its capabilities.
// Example TiDB bug: https://github.com/pingcap/tidb/issues/54700
func TestUnparsableStatements(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1parse, _t1parse_new`)

	// CREATE TABLE with BLOB DEFAULT — works via Statement path.
	m := NewTestMigration(t,
		WithStatement(`CREATE TABLE t1parse (id int not null primary key auto_increment, b BLOB DEFAULT ('abc'))`))
	require.NoError(t, m.Run())

	// Try again as ALTER TABLE, with --statement
	m = NewTestMigration(t, WithThreads(1),
		WithStatement("ALTER TABLE t1parse ADD COLUMN c BLOB DEFAULT ('abc')"))
	err := m.Run()
	assert.Error(t, err)
	// this is permitted by MySQL now, it's the TiDB parser that won't allow it.
	// TODO: we should figure this out so that usage between --statement and --alter is orthogonal.
	assert.ErrorContains(t, err, "can't have a default value")

	// With ALTER TABLE as --table and --alter
	m = NewTestMigration(t, WithThreads(1),
		WithTable("t1parse"), WithAlter("ADD COLUMN c BLOB DEFAULT ('abc')"))
	require.NoError(t, m.Run()) // in this context it works! This is a problem.

	// With CREATE TRIGGER.
	m = NewTestMigration(t, WithThreads(1),
		WithStatement("CREATE TRIGGER ins_sum BEFORE INSERT ON t1parse FOR EACH ROW SET @sum = @sum + NEW.b;"))
	err = m.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "line 1 column 14 near \"TRIGGER")

	// https://github.com/pingcap/tidb/pull/61498
	m = NewTestMigration(t, WithThreads(1),
		WithTable("t1parse"),
		WithAlter(`ADD COLUMN src_col timestamp NULL DEFAULT NULL, add column new_col timestamp NULL DEFAULT(src_col)`))
	require.NoError(t, m.Run())
}

func TestCreateIndexIsRewritten(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1createindex",
		`CREATE TABLE t1createindex (
			id int not null primary key auto_increment,
			b int not null
		)`)

	m := NewTestRunnerFromStatement(t, "CREATE INDEX idx ON test.t1createindex (b)", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestSchemaNameIncluded(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1schemaname",
		`CREATE TABLE t1schemaname (
			id int not null primary key auto_increment,
			b int not null
		)`)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE test.t1schemaname ADD COLUMN c int", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

// TestSecondaryEngineAttribute tests that we can add a secondary engine attribute
// We can't quite test to the original bug report, because the vector type + index
// may not be supported in the version of MySQL we are using:
// https://github.com/block/spirit/issues/405
func TestSecondaryEngineAttribute(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1secondary",
		`CREATE /*vt+ QUERY_TIMEOUT_MS=0 */ TABLE t1secondary (
			id int not null primary key auto_increment,
			title VARCHAR(250)
		)`)

	// ForceKill is now enabled by default, and this happens to be instant, which tests this code path too.
	m := NewTestRunnerFromStatement(t,
		`ALTER TABLE t1secondary ADD KEY (title) SECONDARY_ENGINE_ATTRIBUTE='{"type":"spann", "distance":"l2", "product_quantization":{"dimensions":96}}'`,
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

// TestLargeNumberOfMultiChanges tests the interaction between force kill and multiple table changes.
// and supporting a large number of changes in general. Apart from the MDL locks (currently 1 per table),
// this shouldn't require more connections than regular single table changes.
// https://github.com/block/spirit/issues/502
func TestLargeNumberOfMultiChanges(t *testing.T) {
	var alterStmts []string
	for i := range 50 {
		testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS mt_%d`, i))
		testutils.RunSQL(t, fmt.Sprintf(`CREATE TABLE mt_%d (id int not null primary key auto_increment, b INT NOT NULL)`, i))
		alterStmts = append(alterStmts, fmt.Sprintf(`ALTER TABLE mt_%d ENGINE=InnoDB`, i))
	}

	m := NewTestMigration(t, WithTargetChunkTime(2*time.Second),
		WithStatement(strings.Join(alterStmts, "; ")))
	require.NoError(t, m.Run())
}

func TestBufferedMultiTableMigration(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
	}
	// Create two tables with data
	tt1 := testutils.NewTestTable(t, "bmt_t1",
		`CREATE TABLE bmt_t1 (
			id int not null primary key auto_increment,
			name varchar(100) not null,
			val int not null
		)`)
	tt2 := testutils.NewTestTable(t, "bmt_t2",
		`CREATE TABLE bmt_t2 (
			id int not null primary key auto_increment,
			description varchar(200) not null,
			amount decimal(10,2) not null
		)`)
	// Insert enough rows to trigger multiple chunks
	for i := range 100 {
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO bmt_t1 (name, val) VALUES ('row_%d', %d)`, i, i*10))
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO bmt_t2 (description, amount) VALUES ('item_%d', %d.%d)`, i, i, i%100))
	}

	m := NewTestMigration(t,
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(true),
		WithStatement("ALTER TABLE bmt_t1 ADD COLUMN extra int DEFAULT 0; ALTER TABLE bmt_t2 ADD COLUMN extra int DEFAULT 0"))
	require.NoError(t, m.Run())

	// Verify both tables were altered correctly
	var count1 int
	err := tt1.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM bmt_t1 WHERE extra = 0`).Scan(&count1)
	require.NoError(t, err)
	assert.Equal(t, 100, count1)

	var count2 int
	err = tt2.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM bmt_t2 WHERE extra = 0`).Scan(&count2)
	require.NoError(t, err)
	assert.Equal(t, 100, count2)
}

func TestMigrationParamsDefaultsUsed(t *testing.T) {
	migration := &Migration{Table: "test_table", Alter: "ENGINE=INNODB"}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, defaultUsername, migration.Username)
	assert.Equal(t, defaultPassword, *migration.Password)
	assert.Equal(t, fmt.Sprintf("%s:%d", defaultHost, defaultPort), migration.Host)
	assert.Equal(t, defaultDatabase, migration.Database)
	assert.Equal(t, defaultTLSMode, migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsCLIUsed(t *testing.T) {
	migration := &Migration{
		Host:               "cli-host:3306",
		Username:           "cli-user",
		Password:           mkPtr("cli-password"),
		Database:           "cli-db",
		Table:              "testtable",
		Alter:              "ENGINE=InnoDB",
		TLSMode:            "VERIFY_CA",
		TLSCertificatePath: "/path/to/ca",
	}

	_, err := migration.normalizeOptions()
	assert.NoError(t, err)

	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "VERIFY_CA", migration.TLSMode)
	assert.Equal(t, "/path/to/ca", migration.TLSCertificatePath)
}

func TestMigrationParamsEmptyPasswordUsedIfProvided(t *testing.T) {
	migration := &Migration{
		Password: mkPtr(""),
		Table:    "test_table",
		Alter:    "ENGINE=INNODB",
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, defaultUsername, migration.Username)
	assert.Empty(t, *migration.Password)
	assert.Equal(t, fmt.Sprintf("%s:%d", defaultHost, defaultPort), migration.Host)
	assert.Equal(t, defaultDatabase, migration.Database)
	assert.Equal(t, defaultTLSMode, migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileInvalidFile(t *testing.T) {
	migration := &Migration{
		Host:     "localhost:3306",
		Username: "defaultuser",
		Password: mkPtr("defaultpass"),
		Database: "testdb",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: "/nonexistent/file.cnf",
	}

	_, err := migration.normalizeOptions()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "no such file or directory")
}

func TestMigrationParamsIniFilePreferCommandLineOptions(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
port = 5678
tls-mode = VERIFY_IDENTITY
tls-ca = /path/from/file
`)
	defer utils.CloseAndLog(tmpFile)

	migration := &Migration{
		Host:               "cli-host:1234",
		Username:           "cli-user",
		Password:           mkPtr("cli-password"),
		Database:           "cli-db",
		Table:              "testtable",
		Alter:              "ENGINE=InnoDB",
		ConfFile:           tmpFile.Name(),
		TLSMode:            "REQUIRED",
		TLSCertificatePath: "/path/to/cert",
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-host:1234", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "REQUIRED", migration.TLSMode)
	assert.Equal(t, "/path/to/cert", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileNoCommandLineOptions(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
port = 5678
tls-mode = REQUIRED
tls-ca = /path/to/cert
`)
	defer utils.CloseAndLog(tmpFile)

	migration := &Migration{
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "fileuser", migration.Username)
	assert.Equal(t, "filepass", *migration.Password)
	assert.Equal(t, "filehost:5678", migration.Host)
	assert.Equal(t, "filedb", migration.Database)
	assert.Equal(t, "REQUIRED", migration.TLSMode)
	assert.Equal(t, "/path/to/cert", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileUseDefaultPort(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
tls-mode = VERIFY_IDENTITY
tls-ca = /path/to/another/ca
`)
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "fileuser", migration.Username)
	assert.Equal(t, "filepass", *migration.Password)
	assert.Equal(t, "filehost:3306", migration.Host)
	assert.Equal(t, "filedb", migration.Database)
	assert.Equal(t, "VERIFY_IDENTITY", migration.TLSMode)
	assert.Equal(t, "/path/to/another/ca", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyUserSpecifiedInFile(t *testing.T) {
	// Test with only username in creds file
	tmpFile := mkIniFile(t, `[client]
user = fileuser
`)
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Password: mkPtr("cli-pass"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "fileuser", migration.Username)
	assert.Equal(t, "cli-pass", *migration.Password)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyPasswordSpecifiedInFile(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
password = filepass
`)
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "filepass", *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyPasswordPassedThrough(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
password =
`)
	// File will be cleaned up by t.TempDir()
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Empty(t, *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyPasswordOverridenByCommandLine(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
password =
`)
	// File will be cleaned up by t.TempDir()
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Password: mkPtr("cli-password"),
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyPortUsedFromFile(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
port=1234
`)
	// File will be cleaned up by t.TempDir()
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:1234", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyClientSection(t *testing.T) {
	// Test with empty client section
	tmpFile := mkIniFile(t, `[client]
`)
	// File will be cleaned up by t.TempDir()
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileHasNoClientSection(t *testing.T) {
	// Test with no client section at all
	tmpFile := mkIniFile(t, `[mysql]
user = mysqluser
password = mysqlpass
`)
	// File will be cleaned up by t.TempDir()
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

// --- Configuration validation tests (moved from runner_test.go) ---

func TestBadOptions(t *testing.T) {
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
	testutils.NewTestTable(t, "bot1",
		`CREATE TABLE bot1 (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	testutils.NewTestTable(t, "bot2",
		`CREATE TABLE bot2 (
			id int(11) NOT NULL,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	migration := NewTestMigration(t, WithTable("bot1"), WithAlter("badalter"))
	m, err := NewRunner(migration)
	assert.Error(t, err)
	assert.Nil(t, m)

	m2 := NewTestRunner(t, "bot1", "RENAME COLUMN name TO name2, ADD INDEX(name)")
	err = m2.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m2.Close())

	m2 = NewTestRunner(t, "bot1", "CHANGE name name2 VARCHAR(255), ADD INDEX(name)")
	err = m2.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m2.Close())

	m2 = NewTestRunner(t, "bot1", "CHANGE name name VARCHAR(200), ADD INDEX(name)") //nolint: dupword
	err = m2.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m2.Close())

	m2 = NewTestRunner(t, "bot2", "DROP PRIMARY KEY")
	err = m2.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "dropping primary key")
	assert.NoError(t, m2.Close())
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

func TestPasswordMasking(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "basic DSN with password", input: "user:password@tcp(localhost:3306)/database", expected: "user:***@tcp(localhost:3306)/database"},
		{name: "DSN with complex password", input: "myuser:c0mplex!Pa$$w0rd@tcp(db.example.com:3306)/mydb", expected: "myuser:***@tcp(db.example.com:3306)/mydb"},
		{name: "DSN without password", input: "user@tcp(localhost:3306)/database", expected: "user@tcp(localhost:3306)/database"},
		{name: "DSN with empty password", input: "user:@tcp(localhost:3306)/database", expected: "user:***@tcp(localhost:3306)/database"},
		{name: "empty DSN", input: "", expected: ""},
		{name: "malformed DSN without @", input: "user:password", expected: "user:password"},
		{name: "DSN with colon in password", input: "user:pass:word@tcp(localhost:3306)/database", expected: "user:***@tcp(localhost:3306)/database"},
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
		{name: "simple password", user: "root", password: "secret", host: "127.0.0.1:3306", schema: "testdb"},
		{name: "password with @", user: "root", password: "p@ssword", host: "127.0.0.1:3306", schema: "testdb"},
		{name: "password with multiple @", user: "root", password: "p@ss@word", host: "127.0.0.1:3306", schema: "testdb"},
		{name: "password with special characters", user: "root", password: "p@ss:word/with#special!chars", host: "127.0.0.1:3306", schema: "testdb"},
		{name: "empty password", user: "root", password: "", host: "127.0.0.1:3306", schema: "testdb"},
		{name: "AWS IAM-style token", user: "iam_user", password: "aaa@bbb.ccc.us-east-1.rds.amazonaws.com:3306/?Action=connect&DBUser=iam_user", host: "mydb.cluster-xyz.us-east-1.rds.amazonaws.com:3306", schema: "production"},
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
