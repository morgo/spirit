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
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS altpk_int2big, _altpk_int2big_new`)
	table := `CREATE TABLE altpk_int2big (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO altpk_int2big (name, val) VALUES ('a', 1), ('b', 2), ('c', 3)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "altpk_int2big",
		Alter:    "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
		Buffered: enableBuffered,
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact and column type changed.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_int2big").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	var colType string
	err = db.QueryRowContext(t.Context(),
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS altpk_int2bigu, _altpk_int2bigu_new`)
	table := `CREATE TABLE altpk_int2bigu (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO altpk_int2bigu (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "altpk_int2bigu",
		Alter:    "MODIFY COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT",
		Buffered: enableBuffered,
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact and column type changed.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_int2bigu").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	var colType string
	err = db.QueryRowContext(t.Context(),
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS altpk_tiny2int, _altpk_tiny2int_new`)
	table := `CREATE TABLE altpk_tiny2int (
		id tinyint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	// Insert enough rows to exercise chunking but stay within TINYINT UNSIGNED range (max 255).
	for i := 0; i < 50; i++ {
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO altpk_tiny2int (name) VALUES ('row%d')", i))
	}

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "altpk_tiny2int",
		Alter:    "MODIFY COLUMN id INT UNSIGNED NOT NULL AUTO_INCREMENT",
		Buffered: enableBuffered,
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_tiny2int").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 50, count)

	var colType string
	err = db.QueryRowContext(t.Context(),
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS altpk_dml, _altpk_dml_new, _altpk_dml_chkpnt`)
	table := `CREATE TABLE altpk_dml (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`
	testutils.RunSQL(t, table)

	// Insert enough data so the copy phase takes a measurable amount of time,
	// giving the concurrent DML goroutine a window to inject changes.
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) VALUES ('seed', 1)`)
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 2
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 4
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 8
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 16
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 32
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 64
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 128
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 256
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 512
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 1024
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 2048
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT name, val FROM altpk_dml`)                  // 4096
	testutils.RunSQL(t, `INSERT INTO altpk_dml (name, val) SELECT a.name, a.val FROM altpk_dml a LIMIT 2000`) // ~6096

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "altpk_dml",
		Alter:           "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
		Buffered:        enableBuffered,
	})
	require.NoError(t, err)

	// Open a separate DB connection for DML that won't interfere with test assertions.
	dmlDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(dmlDB)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Run concurrent DML in a goroutine to exercise the binlog replay path.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
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
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO altpk_dml (name, val) VALUES ('concurrent', ?)`, i)
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE altpk_dml SET val = val + 1 WHERE id = %d`, i+1))
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`DELETE FROM altpk_dml WHERE id = %d`, i+50))
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())
	require.NoError(t, migrationErr)

	// Verify column type changed.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var colType string
	err = db.QueryRowContext(t.Context(),
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS altpk_comp, _altpk_comp_new`)
	table := `CREATE TABLE altpk_comp (
		id1 int NOT NULL,
		id2 int NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id1, id2)
	)`
	testutils.RunSQL(t, table)
	// Insert data across both PK columns.
	for i := 1; i <= 100; i++ {
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO altpk_comp (id1, id2, name) VALUES (%d, 1, 'row%d')", i, i))
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO altpk_comp (id1, id2, name) VALUES (%d, 2, 'row%d_2')", i, i))
	}

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "altpk_comp",
		Alter:    "MODIFY COLUMN id1 BIGINT NOT NULL",
		Buffered: enableBuffered,
	})
	require.NoError(t, err)
	require.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Verify data is intact and column type changed.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_comp").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 200, count)

	var colType string
	err = db.QueryRowContext(t.Context(),
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS altpk_multi, _altpk_multi_new, _altpk_multi_chkpnt`)
	table := `CREATE TABLE altpk_multi (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(100) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`
	testutils.RunSQL(t, table)

	// Insert enough data for the copy phase to take time.
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) VALUES ('seed', 1)`)
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 2
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 4
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 8
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 16
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 32
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 64
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 128
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 256
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 512
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 1024
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 2048
	testutils.RunSQL(t, `INSERT INTO altpk_multi (name, val) SELECT name, val FROM altpk_multi`) // 4096

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "altpk_multi",
		Alter:           "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT, MODIFY COLUMN name VARCHAR(255) NOT NULL",
		Buffered:        enableBuffered,
	})
	require.NoError(t, err)

	// Concurrent DML during migration.
	dmlDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(dmlDB)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
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
			_, _ = dmlDB.ExecContext(ctx, `INSERT INTO altpk_multi (name, val) VALUES ('dml_insert', ?)`, i)
			_, _ = dmlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE altpk_multi SET name = 'updated' WHERE id = %d`, i+1))
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	wg.Wait()
	assert.NoError(t, m.Close())
	require.NoError(t, migrationErr)

	// Verify both column types changed.
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var colType string
	err = db.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_multi' AND COLUMN_NAME='id'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "bigint", colType)

	err = db.QueryRowContext(t.Context(),
		"SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='altpk_multi' AND COLUMN_NAME='name'",
	).Scan(&colType)
	require.NoError(t, err)
	assert.Equal(t, "255", colType)
}
