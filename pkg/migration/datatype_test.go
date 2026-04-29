//nolint:dupword
package migration

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for column datatype changes — lossy conversions, lossless conversions,
// NULL-to-NOT-NULL, timestamp precision changes, etc.

func TestChangeDatatypeNoData(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cdatatypemytable",
		`CREATE TABLE cdatatypemytable (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)

	m := NewTestRunner(t, "cdatatypemytable", "CHANGE b b INT") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))                      // no data so no truncation is possible.
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeDataLoss(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "cdatalossmytable",
		`CREATE TABLE cdatalossmytable (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	tt.SeedRows(t, "INSERT INTO cdatalossmytable (name, b) SELECT 'a', 'b'", 1)

	m := NewTestRunner(t, "cdatalossmytable", "CHANGE b b INT") //nolint: dupword
	assert.Error(t, m.Run(t.Context()))                         // value 'b' cannot convert cleanly to int.
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyNoAutoInc is a good test of how much the
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
	testutils.NewTestTable(t, "lossychange2",
		`CREATE TABLE lossychange2 (
			id BIGINT NOT NULL,
			name varchar(255) NOT NULL,
			b varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`)
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (1, 'a', REPEAT('a', 200))")          // will pass
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))") // will fail

	m := NewTestRunner(t, "lossychange2", "CHANGE COLUMN id id INT NOT NULL auto_increment") //nolint: dupword
	err := m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Out of range value") // Error 1264
	// Check that the chunker processed fewer than 500 chunks
	_, chunksCopied, _ := m.copier.GetChunker().Progress()
	assert.Less(t, chunksCopied, uint64(500))
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossless has a data type change that is "lossy" but
// given the current stored data set does not cause errors.
func TestChangeDatatypeLossless(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "lossychange3",
		`CREATE TABLE lossychange3 (
			id BIGINT NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NULL,
			PRIMARY KEY (id)
		)`)
	testutils.RunSQL(t, "INSERT INTO lossychange3 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange3 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")

	m := NewTestRunner(t, "lossychange3", "CHANGE COLUMN b b varchar(200) NOT NULL") //nolint: dupword
	err := m.Run(t.Context())
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
	testutils.NewTestTable(t, "lossychange4",
		`CREATE TABLE lossychange4 (
			id BIGINT NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			b varchar(255) NULL,
			PRIMARY KEY (id)
		)`)
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name) VALUES ('a')")
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange4 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")

	m := NewTestRunner(t, "lossychange4", "CHANGE COLUMN b b varchar(255) NOT NULL") //nolint: dupword
	err := m.Run(t.Context())
	assert.Error(t, err) // there is a violation where row 1 is NULL
	assert.NoError(t, m.Close())
}

func TestNullToNotNull(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "autodatetime",
		`CREATE TABLE autodatetime (
			id INT NOT NULL AUTO_INCREMENT,
			created_at DATETIME(3) NULL,
			PRIMARY KEY (id)
		)`)
	testutils.RunSQL(t, `INSERT INTO autodatetime (created_at) VALUES (NULL)`)

	m := NewTestRunner(t, "autodatetime", "modify column created_at datetime(3) not null default current_timestamp(3)")
	err := m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Column 'created_at' cannot be null")
	assert.NoError(t, m.Close())
}

func TestTpConversion(t *testing.T) {
	testutils.NewTestTable(t, "tpconvert",
		`CREATE TABLE tpconvert (
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

	m := NewTestRunner(t, "tpconvert", `MODIFY COLUMN created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		MODIFY COLUMN issued_at TIMESTAMP(6) NULL,
		MODIFY COLUMN activated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN deactivated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN intasstring INT NULL DEFAULT NULL`)
	require.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

// --- ENUM/SET reorder and buffered mode tests ---

// TestEnumReorder tests that reordering ENUM values in an ALTER TABLE
// produces correct data after migration.
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
	tt := testutils.NewTestTable(t, "enumreorder",
		`CREATE TABLE enumreorder (
			id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
			status ENUM('active', 'inactive', 'pending') NOT NULL
		)`)
	tt.SeedRows(t, "INSERT INTO enumreorder (status) SELECT 'active'", 6000)

	m := NewTestRunner(t, "enumreorder",
		"MODIFY COLUMN status ENUM('pending', 'active', 'inactive') NOT NULL",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(enableBuffered))

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
		for i := 0; i < 50; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('active')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('inactive')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('pending')`)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'active' WHERE id = %d`, i*3+1))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'inactive' WHERE id = %d`, i*3+2))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'pending' WHERE id = %d`, i*3+3))
		}
	})

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())

	if enableBuffered {
		require.Error(t, migrationErr)
		assert.ErrorContains(t, migrationErr, "unsafe ENUM value reorder")
		return
	}

	require.NoError(t, migrationErr)

	var activeCount, inactiveCount, pendingCount int
	err := tt.DB.QueryRowContext(t.Context(), `SELECT
			SUM(status = 'active'),
			SUM(status = 'inactive'),
			SUM(status = 'pending')
			FROM enumreorder`).Scan(&activeCount, &inactiveCount, &pendingCount)
	require.NoError(t, err)

	var totalCount int
	err = tt.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM enumreorder`).Scan(&totalCount)
	require.NoError(t, err)

	assert.Equal(t, totalCount, activeCount+inactiveCount+pendingCount,
		"all rows should have a valid ENUM value")
	assert.Greater(t, activeCount, 0, "should have 'active' rows")
	assert.Greater(t, inactiveCount, 0, "should have 'inactive' rows")
	assert.Greater(t, pendingCount, 0, "should have 'pending' rows")
}

// TestSetReorder mirrors TestEnumReorder but for SET columns.
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
	tt := testutils.NewTestTable(t, "setreorder",
		`CREATE TABLE setreorder (
			id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
			perms SET('read', 'write', 'execute') NOT NULL
		)`)
	tt.SeedRows(t, "INSERT INTO setreorder (perms) SELECT 'read,write'", 7000)

	m := NewTestRunner(t, "setreorder",
		"MODIFY COLUMN perms SET('execute', 'read', 'write') NOT NULL",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(enableBuffered))

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
		for i := 0; i < 50; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('read')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('write,execute')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('read,write,execute')`)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'read,write' WHERE id = %d`, i*3+1))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'execute' WHERE id = %d`, i*3+2))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'read,write,execute' WHERE id = %d`, i*3+3))
		}
	})

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())

	require.Error(t, migrationErr)
	assert.ErrorContains(t, migrationErr, "unsafe SET value reorder")
}

// TestBufferedMigrationFailsGracefullyWithMinimalRBR verifies that a buffered
// migration fails gracefully when it receives minimal RBR events.
func TestBufferedMigrationFailsGracefullyWithMinimalRBR(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner (global setting already minimal)")
	}

	tt := testutils.NewTestTable(t, "minrbr_buffered",
		`CREATE TABLE minrbr_buffered (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			val INT NOT NULL DEFAULT 0
		)`)
	tt.SeedRows(t, "INSERT INTO minrbr_buffered (name, val) SELECT 'seed', 1", 256)

	minimalDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(minimalDB)

	minimalDB.SetMaxOpenConns(1)
	_, err = minimalDB.ExecContext(t.Context(), "SET binlog_row_image = 'MINIMAL'")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m := NewTestRunner(t, "minrbr_buffered", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(true),
		WithTestThrottler())

	var migrationErr error
	migrationDone := make(chan struct{})
	go func() {
		defer close(migrationDone)
		migrationErr = m.Run(ctx)
	}()

	waitForStatus(t, m, status.CopyRows)

	var writerWg sync.WaitGroup
	writerWg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = minimalDB.ExecContext(ctx, `UPDATE minrbr_buffered SET val = val + 1 WHERE id = ?`, (i%100)+1)
			}
		}
	})

	<-migrationDone
	cancel()
	writerWg.Wait()
	assert.NoError(t, m.Close())

	require.Error(t, migrationErr)
}

// Tests for issue #360: Add more tests for alter primary key datatype.
// Spirit supports changing the datatype on a primary key column (e.g. INT→BIGINT)
// as long as the primary key itself doesn't change (no ADD/DROP PRIMARY KEY).
// These tests verify that binlog row detection and row copying work correctly
// when the PK column's datatype changes, for both unbuffered and buffered modes.

// TestAlterPKIntToBigInt tests a simple INT→BIGINT conversion on the primary key
// column with existing data. This is the most common PK datatype change.
func TestAlterPKIntToBigInt(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testAlterPKIntToBigInt(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testAlterPKIntToBigInt(t, true)
	})
}

func testAlterPKIntToBigInt(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_int2big",
		`CREATE TABLE altpk_int2big (
			id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(255) NOT NULL,
			val int NOT NULL DEFAULT 0
		)`)
	tt.SeedRows(t, "INSERT INTO altpk_int2big (name, val) SELECT 'a', 1", 3)

	m := NewTestRunner(t, "altpk_int2big", "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_int2big").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	var colType string
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_int2big' AND COLUMN_NAME='id'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "bigint", colType)
}

// TestAlterPKIntToBigIntUnsigned tests INT→BIGINT UNSIGNED conversion on the PK.
// This is a common migration for tables approaching the INT max value.
func TestAlterPKIntToBigIntUnsigned(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testAlterPKIntToBigIntUnsigned(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testAlterPKIntToBigIntUnsigned(t, true)
	})
}

func testAlterPKIntToBigIntUnsigned(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_int2bigu",
		`CREATE TABLE altpk_int2bigu (
			id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(255) NOT NULL
		)`)
	tt.SeedRows(t, "INSERT INTO altpk_int2bigu (name) SELECT 'a'", 5)

	m := NewTestRunner(t, "altpk_int2bigu", "MODIFY COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_int2bigu").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	var colType string
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_int2bigu' AND COLUMN_NAME='id'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "bigint unsigned", colType)
}

// TestAlterPKTinyIntToInt tests TINYINT→INT conversion on the PK.
// This exercises a widening conversion on a small type.
func TestAlterPKTinyIntToInt(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testAlterPKTinyIntToInt(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testAlterPKTinyIntToInt(t, true)
	})
}

func testAlterPKTinyIntToInt(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_tiny2int",
		`CREATE TABLE altpk_tiny2int (
			id tinyint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(255) NOT NULL
		)`)
	// Stay within TINYINT UNSIGNED range (max 255): 1→2→4→8→16→32→64
	tt.SeedRows(t, "INSERT INTO altpk_tiny2int (name) SELECT 'row'", 50)

	m := NewTestRunner(t, "altpk_tiny2int", "MODIFY COLUMN id INT UNSIGNED NOT NULL AUTO_INCREMENT",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_tiny2int").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	var colType string
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_tiny2int' AND COLUMN_NAME='id'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "int unsigned", colType)
}

// TestAlterPKIntToBigIntWithDML tests INT→BIGINT on the PK column with concurrent
// DML (INSERT, UPDATE, DELETE) during the migration. This exercises the binlog
// subscription and replay path to ensure row changes are correctly detected
// when the PK column's datatype changes between the source and shadow table.
func TestAlterPKIntToBigIntWithDML(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testAlterPKIntToBigIntWithDML(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testAlterPKIntToBigIntWithDML(t, true)
	})
}

func testAlterPKIntToBigIntWithDML(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_dml",
		`CREATE TABLE altpk_dml (
			id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(255) NOT NULL,
			val int NOT NULL DEFAULT 0
		)`)
	tt.SeedRows(t, "INSERT INTO altpk_dml (name, val) SELECT 'seed', 1", 4096)

	m := NewTestRunner(t, "altpk_dml", "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(enableBuffered))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Run concurrent DML in a goroutine to exercise the binlog replay path.
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
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO altpk_dml (name, val) VALUES ('concurrent', ?)`, i)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE altpk_dml SET val = val + 1 WHERE id = %d`, i+1))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`DELETE FROM altpk_dml WHERE id = %d`, i+50))
		}
	})

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())
	require.NoError(t, migrationErr)

	var colType string
	err := tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_dml' AND COLUMN_NAME='id'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "bigint", colType)
}

// TestAlterPKCompositeDatatypeChange tests changing the datatype of the first column
// in a composite primary key. This exercises the composite chunker path with a
// PK datatype change.
func TestAlterPKCompositeDatatypeChange(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testAlterPKCompositeDatatypeChange(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testAlterPKCompositeDatatypeChange(t, true)
	})
}

func testAlterPKCompositeDatatypeChange(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_comp",
		`CREATE TABLE altpk_comp (
			id1 int NOT NULL,
			id2 int NOT NULL,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id1, id2)
		)`)
	// Insert data across both PK columns — needs unique (id1, id2) pairs,
	// so we can't use SeedRows doubling here.
	for i := 1; i <= 100; i++ {
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO altpk_comp (id1, id2, name) VALUES (%d, 1, 'row%d')", i, i))
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO altpk_comp (id1, id2, name) VALUES (%d, 2, 'row%d_2')", i, i))
	}

	m := NewTestRunner(t, "altpk_comp", "MODIFY COLUMN id1 BIGINT NOT NULL",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	var count int
	err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_comp").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 200, count)

	var colType string
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_comp' AND COLUMN_NAME='id1'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "bigint", colType)
}

// TestAlterPKIntToBigIntWithDMLAndAdditionalColumnChange tests changing the PK
// datatype while also modifying another column. This is a common real-world
// scenario where multiple changes are batched into a single ALTER.
func TestAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if testutils.IsMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t, true)
	})
}

func testAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_multi",
		`CREATE TABLE altpk_multi (
			id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(100) NOT NULL,
			val int NOT NULL DEFAULT 0
		)`)
	tt.SeedRows(t, "INSERT INTO altpk_multi (name, val) SELECT 'seed', 1", 4096)

	m := NewTestRunner(t, "altpk_multi",
		"MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT, MODIFY COLUMN name VARCHAR(255) NOT NULL",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(enableBuffered))

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
		for i := 0; i < 50; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO altpk_multi (name, val) VALUES ('dml_insert', ?)`, i)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE altpk_multi SET name = 'updated' WHERE id = %d`, i+1))
		}
	})

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())
	require.NoError(t, migrationErr)

	var colType string
	err := tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_multi' AND COLUMN_NAME='id'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "bigint", colType)

	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_multi' AND COLUMN_NAME='name'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "255", colType)
}
