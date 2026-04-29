package migration

import (
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// mkPtr returns a pointer to the given value. Useful for optional fields.
func mkPtr[T any](t T) *T {
	return &t
}

// mkIniFile creates a temporary INI file with the given content for testing.
func mkIniFile(t *testing.T, content string) *os.File {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "test_creds_*.cnf")
	require.NoError(t, err)
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	return tmpFile
}

// waitForStatus polls until the runner reaches the target status or times out.
func waitForStatus(t *testing.T, m *Runner, target status.State) {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for m.status.Get() < target {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for status >= %s, current status: %s", target, m.status.Get())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// RunnerOption is a functional option for configuring a test Runner.
type RunnerOption func(*Migration)

// WithThreads sets the number of concurrent threads.
func WithThreads(n int) RunnerOption {
	return func(m *Migration) {
		m.Threads = n
	}
}

// WithTable sets the table name for the migration.
func WithTable(t string) RunnerOption {
	return func(m *Migration) {
		m.Table = t
	}
}

// WithAlter sets the ALTER clause for the migration.
func WithAlter(a string) RunnerOption {
	return func(m *Migration) {
		m.Alter = a
	}
}

// WithStatement sets the SQL statement for the migration.
func WithStatement(s string) RunnerOption {
	return func(m *Migration) {
		m.Statement = s
	}
}

// WithTargetChunkTime sets the target chunk time.
func WithTargetChunkTime(d time.Duration) RunnerOption {
	return func(m *Migration) {
		m.TargetChunkTime = d
	}
}

// WithBuffered enables or disables the buffered copier.
func WithBuffered(b bool) RunnerOption {
	return func(m *Migration) {
		m.Buffered = b
	}
}

// WithTestThrottler enables the test throttler (slows the copier
// so the repl client has time to observe events).
func WithTestThrottler() RunnerOption {
	return func(m *Migration) {
		m.useTestThrottler = true
	}
}

// WithDeferCutOver enables deferred cutover mode.
func WithDeferCutOver() RunnerOption {
	return func(m *Migration) {
		m.DeferCutOver = true
	}
}

// WithStrict enables strict mode (mismatched ALTER detection on resume).
func WithStrict() RunnerOption {
	return func(m *Migration) {
		m.Strict = true
	}
}

// WithDBName overrides the database name (for tests using CreateUniqueTestDatabase).
func WithDBName(name string) RunnerOption {
	return func(m *Migration) {
		m.Database = name
	}
}

// WithRespectSentinel enables sentinel table detection.
func WithRespectSentinel() RunnerOption {
	return func(m *Migration) {
		m.RespectSentinel = true
	}
}

// WithSkipDropAfterCutover keeps the old table after cutover.
func WithSkipDropAfterCutover() RunnerOption {
	return func(m *Migration) {
		m.SkipDropAfterCutover = true
	}
}

// newTestMigration creates a Migration with sensible defaults for integration tests.
// It parses the test DSN and fills in Host/Username/Password/Database.
// Callers must set either Table+Alter or Statement before calling Run().
func newTestMigration(t *testing.T, opts ...RunnerOption) *Migration {
	t.Helper()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
	}
	for _, opt := range opts {
		opt(migration)
	}
	return migration
}

// NewTestRunner creates a Runner for a Table+Alter migration with sensible defaults.
//
// Defaults: Threads=2, TargetChunkTime=500ms (the production default).
//
// Example:
//
//	m := NewTestRunner(t, "mytable", "ENGINE=InnoDB")
//	require.NoError(t, m.Run(t.Context()))
//	assert.NoError(t, m.Close())
//
//	m := NewTestRunner(t, "mytable", "ADD INDEX idx_a (a)",
//	    WithThreads(1),
//	    WithTargetChunkTime(100*time.Millisecond),
//	    WithBuffered(true),
//	)
func NewTestRunner(t *testing.T, table, alter string, opts ...RunnerOption) *Runner {
	t.Helper()

	migration := newTestMigration(t, opts...)
	migration.Table = table
	migration.Alter = alter

	runner, err := NewRunner(migration)
	require.NoError(t, err)
	return runner
}

// NewTestRunnerFromStatement creates a Runner for a Statement-based migration
// with sensible defaults. Use this for tests that use full SQL statements
// (ALTER TABLE, CREATE INDEX, etc.) rather than Table+Alter.
//
// Example:
//
//	m := NewTestRunnerFromStatement(t, "ALTER TABLE mytable ADD COLUMN c INT")
//	require.NoError(t, m.Run(t.Context()))
//	assert.NoError(t, m.Close())
func NewTestRunnerFromStatement(t *testing.T, statement string, opts ...RunnerOption) *Runner {
	t.Helper()

	migration := newTestMigration(t, opts...)
	migration.Statement = statement

	runner, err := NewRunner(migration)
	require.NoError(t, err)
	return runner
}

// NewTestMigration creates a Migration struct with sensible defaults for tests
// that need to call migration.Run() directly (testing the Migration API rather
// than the Runner API). Use RunnerOption functions to configure it.
//
// Example:
//
//	m := NewTestMigration(t, WithThreads(1))
//	m.Table = "mytable"
//	m.Alter = "ENGINE=InnoDB"
//	require.NoError(t, m.Run())
func NewTestMigration(t *testing.T, opts ...RunnerOption) *Migration {
	t.Helper()
	return newTestMigration(t, opts...)
}
