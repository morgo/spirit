// Copyright 2026 Block, Inc.

package parser_test

import (
	"testing"
)

// These statements are all valid MySQL that can appear in the binary log as
// Query events; spirit's pkg/change must be able to parse them.

func TestRepairTable(t *testing.T) {
	table := []testCase{
		{"REPAIR TABLE t1", true, "REPAIR TABLE `t1`"},
		{"repair tables t1, t2", true, "REPAIR TABLE `t1`, `t2`"},
		{"REPAIR NO_WRITE_TO_BINLOG TABLE t1 QUICK EXTENDED USE_FRM", true, "REPAIR NO_WRITE_TO_BINLOG TABLE `t1` QUICK EXTENDED USE_FRM"},
		{"REPAIR LOCAL TABLE t1 USE_FRM", true, "REPAIR NO_WRITE_TO_BINLOG TABLE `t1` USE_FRM"},
		{"REPAIR TABLE t1 EXTENDED QUICK", true, "REPAIR TABLE `t1` QUICK EXTENDED"},
		{"REPAIR TABLE", false, ""},
	}
	RunTest(t, table, false)
}

func TestRenameTables(t *testing.T) {
	table := []testCase{
		{"RENAME TABLES t1 TO t2", true, "RENAME TABLE `t1` TO `t2`"},
		{"RENAME TABLES t1 TO t2, t3 TO t4", true, "RENAME TABLE `t1` TO `t2`, `t3` TO `t4`"},
	}
	RunTest(t, table, false)
}

func TestAnalyzeTableHistograms(t *testing.T) {
	table := []testCase{
		{"ANALYZE TABLES t1, t2", true, "ANALYZE TABLE `t1`,`t2`"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c1, c2 WITH 8 BUCKETS", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c1`,`c2` WITH 8 BUCKETS"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c WITH 4 BUCKETS AUTO UPDATE", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` WITH 4 BUCKETS AUTO UPDATE"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c AUTO UPDATE", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` AUTO UPDATE"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c MANUAL UPDATE", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` MANUAL UPDATE"},
		{`ANALYZE TABLE t UPDATE HISTOGRAM ON c USING DATA '{"histogram": {}}'`, true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` USING DATA '{\"histogram\": {}}'"},
		{"ANALYZE TABLE t DROP HISTOGRAM ON c", true, "ANALYZE TABLE `t` DROP HISTOGRAM ON `c`"},
	}
	RunTest(t, table, false)
}

func TestTransactionSpellings(t *testing.T) {
	table := []testCase{
		{"BEGIN WORK", true, "START TRANSACTION"},
		{"COMMIT WORK", true, "COMMIT"},
		{"ROLLBACK WORK", true, "ROLLBACK"},
		{"COMMIT WORK AND CHAIN", true, "COMMIT AND CHAIN"},
		{"ROLLBACK WORK AND NO CHAIN NO RELEASE", true, "ROLLBACK"},
		{"ROLLBACK WORK TO SAVEPOINT sp1", true, "ROLLBACK TO sp1"},
		{"ROLLBACK WORK TO sp1", true, "ROLLBACK TO sp1"},
		{"START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT", true, "START TRANSACTION READ ONLY"},
		{"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ WRITE", true, "START TRANSACTION"},
		{"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY", true, "START TRANSACTION READ ONLY"},
	}
	RunTest(t, table, false)
}

func TestFlushOptions(t *testing.T) {
	table := []testCase{
		{"FLUSH USER_RESOURCES", true, "FLUSH USER_RESOURCES"},
		{"FLUSH OPTIMIZER_COSTS", true, "FLUSH OPTIMIZER_COSTS"},
		{"FLUSH LOCAL OPTIMIZER_COSTS", true, "FLUSH NO_WRITE_TO_BINLOG OPTIMIZER_COSTS"},
		{"FLUSH RELAY LOGS", true, "FLUSH RELAY LOGS"},
		{"FLUSH RELAY LOGS FOR CHANNEL 'group_replication_applier'", true, "FLUSH RELAY LOGS FOR CHANNEL 'group_replication_applier'"},
		{"FLUSH TABLES t1 FOR EXPORT", true, "FLUSH TABLES `t1` FOR EXPORT"},
		{"FLUSH TABLES t1, t2 WITH READ LOCK", true, "FLUSH TABLES `t1`, `t2` WITH READ LOCK"},
	}
	RunTest(t, table, false)
}

func TestAlterDatabaseReadOnly(t *testing.T) {
	table := []testCase{
		{"ALTER DATABASE d READ ONLY = 1", true, "ALTER DATABASE `d` READ ONLY = 1"},
		{"ALTER DATABASE d READ ONLY 0", true, "ALTER DATABASE `d` READ ONLY = 0"},
		{"ALTER DATABASE d READ ONLY = DEFAULT", true, "ALTER DATABASE `d` READ ONLY = DEFAULT"},
		{"ALTER DATABASE READ ONLY = 1", true, "ALTER DATABASE READ ONLY = 1"},
		{"ALTER SCHEMA d READ ONLY = 1 CHARACTER SET = utf8mb4", true, "ALTER DATABASE `d` READ ONLY = 1 CHARACTER SET = utf8mb4"},
	}
	RunTest(t, table, false)
}

func TestAlterView(t *testing.T) {
	table := []testCase{
		{"ALTER VIEW v AS SELECT * FROM t", true, "ALTER ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"ALTER ALGORITHM = MERGE DEFINER = u@h SQL SECURITY INVOKER VIEW v (a, b) AS SELECT a, b FROM t WITH LOCAL CHECK OPTION", true, "ALTER ALGORITHM = MERGE DEFINER = `u`@`h` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT `a`,`b` FROM `t` WITH LOCAL CHECK OPTION"},
		{"ALTER VIEW v AS", false, ""},
	}
	RunTest(t, table, false)
}

func TestXAStatements(t *testing.T) {
	table := []testCase{
		{"XA START 'gtrid'", true, "XA START 'gtrid'"},
		{"XA BEGIN 'gtrid'", true, "XA START 'gtrid'"},
		{"XA START 'g', 'b'", true, "XA START 'g', 'b'"},
		{"XA START 'g', 'b', 12", true, "XA START 'g', 'b', 12"},
		{"XA START X'41', X'42', 1", true, "XA START 'A', 'B', 1"},
		{"XA START 0x7465737462, 0x2030405060, 0xb", true, "XA START 'testb', ' 0@P`', 11"},
		{"XA START 'g' JOIN", true, "XA START 'g' JOIN"},
		{"XA START 'g' RESUME", true, "XA START 'g' RESUME"},
		{"XA END 'g'", true, "XA END 'g'"},
		{"XA END 'g' SUSPEND", true, "XA END 'g' SUSPEND"},
		{"XA END 'g' SUSPEND FOR MIGRATE", true, "XA END 'g' SUSPEND FOR MIGRATE"},
		{"XA PREPARE 'g'", true, "XA PREPARE 'g'"},
		{"XA COMMIT 'g'", true, "XA COMMIT 'g'"},
		{"XA COMMIT 'g' ONE PHASE", true, "XA COMMIT 'g' ONE PHASE"},
		{"XA ROLLBACK 'g'", true, "XA ROLLBACK 'g'"},
		{"XA RECOVER", true, "XA RECOVER"},
		{"XA RECOVER CONVERT XID", true, "XA RECOVER CONVERT XID"},
		{"XA START", false, ""},
	}
	RunTest(t, table, false)
}

func TestSpatialReferenceSystem(t *testing.T) {
	table := []testCase{
		{`CREATE SPATIAL REFERENCE SYSTEM 4120 NAME 'Bessel' DEFINITION 'GEOGCS[]'`, true, "CREATE SPATIAL REFERENCE SYSTEM 4120 NAME 'Bessel' DEFINITION 'GEOGCS[]'"},
		{`CREATE OR REPLACE SPATIAL REFERENCE SYSTEM 4120 ORGANIZATION 'EPSG' IDENTIFIED BY 4120 DESCRIPTION 'd' NAME 'n' DEFINITION 'def'`, true, "CREATE OR REPLACE SPATIAL REFERENCE SYSTEM 4120 ORGANIZATION 'EPSG' IDENTIFIED BY 4120 DESCRIPTION 'd' NAME 'n' DEFINITION 'def'"},
		{`CREATE SPATIAL REFERENCE SYSTEM IF NOT EXISTS 4120 NAME 'n' DEFINITION 'def'`, true, "CREATE SPATIAL REFERENCE SYSTEM IF NOT EXISTS 4120 NAME 'n' DEFINITION 'def'"},
		{"DROP SPATIAL REFERENCE SYSTEM 4120", true, "DROP SPATIAL REFERENCE SYSTEM 4120"},
		{"DROP SPATIAL REFERENCE SYSTEM IF EXISTS 4120", true, "DROP SPATIAL REFERENCE SYSTEM IF EXISTS 4120"},
		// unrelated statements starting with the same tokens still parse
		{"CREATE SPATIAL INDEX idx ON t (g)", true, "CREATE SPATIAL INDEX `idx` ON `t` (`g`)"},
	}
	RunTest(t, table, false)
}

func TestTablespaceDDL(t *testing.T) {
	table := []testCase{
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'ts1.ibd'", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'ts1.ibd'"},
		{"CREATE TABLESPACE ts1", true, "CREATE TABLESPACE `ts1`"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' ENGINE = InnoDB", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' ENGINE = `InnoDB`"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' FILE_BLOCK_SIZE = 8192 ENCRYPTION = 'Y'", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' FILE_BLOCK_SIZE = 8192 ENCRYPTION = 'Y'"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' USE LOGFILE GROUP lg1 EXTENT_SIZE 4M INITIAL_SIZE 12M ENGINE NDB", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' USE LOGFILE GROUP `lg1` EXTENT_SIZE = 4M INITIAL_SIZE = 12M ENGINE = `NDB`"},
		{"CREATE UNDO TABLESPACE u1 ADD DATAFILE 'u1.ibu'", true, "CREATE UNDO TABLESPACE `u1` ADD DATAFILE 'u1.ibu'"},
		{"ALTER TABLESPACE ts1 ADD DATAFILE 'f2' INITIAL_SIZE = 1G WAIT", true, "ALTER TABLESPACE `ts1` ADD DATAFILE 'f2' INITIAL_SIZE = 1G WAIT"},
		{"ALTER TABLESPACE ts1 DROP DATAFILE 'f2' ENGINE NDB", true, "ALTER TABLESPACE `ts1` DROP DATAFILE 'f2' ENGINE = `NDB`"},
		{"ALTER TABLESPACE ts1 RENAME TO ts2", true, "ALTER TABLESPACE `ts1` RENAME TO `ts2`"},
		{"ALTER TABLESPACE ts1 AUTOEXTEND_SIZE = 64M", true, "ALTER TABLESPACE `ts1` AUTOEXTEND_SIZE = 64M"},
		{"ALTER UNDO TABLESPACE u1 SET INACTIVE", true, "ALTER UNDO TABLESPACE `u1` SET INACTIVE"},
		{"ALTER UNDO TABLESPACE u1 SET ACTIVE ENGINE = InnoDB", true, "ALTER UNDO TABLESPACE `u1` SET ACTIVE ENGINE = `InnoDB`"},
		{"DROP TABLESPACE ts1", true, "DROP TABLESPACE `ts1`"},
		{"DROP TABLESPACE ts1 ENGINE = NDB", true, "DROP TABLESPACE `ts1` ENGINE = `NDB`"},
		{"DROP UNDO TABLESPACE u1", true, "DROP UNDO TABLESPACE `u1`"},
		{"CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' UNDO_BUFFER_SIZE = 8M ENGINE = NDB", true, "CREATE LOGFILE GROUP `lg1` ADD UNDOFILE 'undo.dat' UNDO_BUFFER_SIZE = 8M ENGINE = `NDB`"},
		{"ALTER LOGFILE GROUP lg1 ADD UNDOFILE 'undo2.dat' INITIAL_SIZE 4M WAIT ENGINE NDB", true, "ALTER LOGFILE GROUP `lg1` ADD UNDOFILE 'undo2.dat' INITIAL_SIZE = 4M WAIT ENGINE = `NDB`"},
		{"DROP LOGFILE GROUP lg1 ENGINE = NDB", true, "DROP LOGFILE GROUP `lg1` ENGINE = `NDB`"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' INITIAL_SIZE = 12M, MAX_SIZE = 100M, NODEGROUP = 3, NO_WAIT", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' INITIAL_SIZE = 12M MAX_SIZE = 100M NODEGROUP = 3 NO_WAIT"},
	}
	RunTest(t, table, false)
}

func TestAlterInstance(t *testing.T) {
	table := []testCase{
		{"ALTER INSTANCE RELOAD TLS", true, "ALTER INSTANCE RELOAD TLS"},
		{"ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR", true, "ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR"},
		{"ALTER INSTANCE RELOAD TLS FOR CHANNEL mysql_admin", true, "ALTER INSTANCE RELOAD TLS FOR CHANNEL `mysql_admin`"},
		{"ALTER INSTANCE RELOAD TLS FOR CHANNEL mysql_main NO ROLLBACK ON ERROR", true, "ALTER INSTANCE RELOAD TLS FOR CHANNEL `mysql_main` NO ROLLBACK ON ERROR"},
		{"ALTER INSTANCE ROTATE INNODB MASTER KEY", true, "ALTER INSTANCE ROTATE INNODB MASTER KEY"},
		{"ALTER INSTANCE ROTATE BINLOG MASTER KEY", true, "ALTER INSTANCE ROTATE BINLOG MASTER KEY"},
		{"ALTER INSTANCE ROTATE innodb master key", true, "ALTER INSTANCE ROTATE INNODB MASTER KEY"},
		{"ALTER INSTANCE ENABLE INNODB REDO_LOG", true, "ALTER INSTANCE ENABLE INNODB REDO_LOG"},
		{"ALTER INSTANCE DISABLE INNODB REDO_LOG", true, "ALTER INSTANCE DISABLE INNODB REDO_LOG"},
		{"ALTER INSTANCE RELOAD KEYRING", true, "ALTER INSTANCE RELOAD KEYRING"},
		{"ALTER INSTANCE ROTATE FOO MASTER KEY", false, ""},
		{"ALTER INSTANCE ENABLE FOO REDO_LOG", false, ""},
	}
	RunTest(t, table, false)
}

func TestColumnVisibility(t *testing.T) {
	table := []testCase{
		{"CREATE TABLE t (a INT, b INT INVISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT INVISIBLE)"},
		{"CREATE TABLE t (a INT, b INT VISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT VISIBLE)"},
		{"CREATE TABLE t (a INT, b INT INVISIBLE NOT NULL DEFAULT 5)", true, "CREATE TABLE `t` (`a` INT,`b` INT INVISIBLE NOT NULL DEFAULT 5)"},
		// SHOW CREATE TABLE emits the attribute inside a versioned comment.
		{"CREATE TABLE t (a INT, b INT /*!80023 INVISIBLE */ DEFAULT NULL)", true, "CREATE TABLE `t` (`a` INT,`b` INT INVISIBLE DEFAULT NULL)"},
		{"CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) VIRTUAL INVISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT GENERATED ALWAYS AS(`a`+1) VIRTUAL INVISIBLE)"},
		{"CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED INVISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT GENERATED ALWAYS AS(`a`+1) STORED INVISIBLE)"},
		{"ALTER TABLE t1 ADD COLUMN x INT INVISIBLE", true, "ALTER TABLE `t1` ADD COLUMN `x` INT INVISIBLE"},
		{"ALTER TABLE t1 MODIFY a INT INVISIBLE", true, "ALTER TABLE `t1` MODIFY COLUMN `a` INT INVISIBLE"},
		{"ALTER TABLE t1 MODIFY a INT VISIBLE", true, "ALTER TABLE `t1` MODIFY COLUMN `a` INT VISIBLE"},
		{"ALTER TABLE t1 ALTER COLUMN a SET VISIBLE", true, "ALTER TABLE `t1` ALTER COLUMN `a` SET VISIBLE"},
		{"ALTER TABLE t1 ALTER COLUMN a SET INVISIBLE", true, "ALTER TABLE `t1` ALTER COLUMN `a` SET INVISIBLE"},
		{"ALTER TABLE t1 ALTER a SET INVISIBLE", true, "ALTER TABLE `t1` ALTER COLUMN `a` SET INVISIBLE"},
	}
	RunTest(t, table, false)
}

func TestEngineAttribute(t *testing.T) {
	table := []testCase{
		{`CREATE TABLE t (c INT ENGINE_ATTRIBUTE '{"x":1}')`, true, "CREATE TABLE `t` (`c` INT ENGINE_ATTRIBUTE = '{\"x\":1}')"},
		{`CREATE TABLE t (c INT ENGINE_ATTRIBUTE = '{"x":1}' SECONDARY_ENGINE_ATTRIBUTE = '{"y":2}')`, true, "CREATE TABLE `t` (`c` INT ENGINE_ATTRIBUTE = '{\"x\":1}' SECONDARY_ENGINE_ATTRIBUTE = '{\"y\":2}')"},
		{`CREATE TABLE t (a VARCHAR(200), UNIQUE KEY k (a) VISIBLE ENGINE_ATTRIBUTE '{"a":1}')`, true, "CREATE TABLE `t` (`a` VARCHAR(200),UNIQUE `k`(`a`) VISIBLE ENGINE_ATTRIBUTE = '{\"a\":1}')"},
		{`CREATE INDEX i ON t1 ((a + b)) INVISIBLE ENGINE_ATTRIBUTE '{"x":1}'`, true, "CREATE INDEX `i` ON `t1` ((`a`+`b`)) INVISIBLE ENGINE_ATTRIBUTE = '{\"x\":1}'"},
		{`ALTER TABLE t1 ADD INDEX i (a) ENGINE_ATTRIBUTE '{"x":1}'`, true, "ALTER TABLE `t1` ADD INDEX `i`(`a`) ENGINE_ATTRIBUTE = '{\"x\":1}'"},
	}
	RunTest(t, table, false)
}

func TestVectorType(t *testing.T) {
	table := []testCase{
		{"CREATE TABLE t (v VECTOR)", true, "CREATE TABLE `t` (`v` VECTOR)"},
		{"CREATE TABLE t (v VECTOR(4))", true, "CREATE TABLE `t` (`v` VECTOR(4))"},
		{"CREATE TABLE t (v VECTOR(16383) NOT NULL)", true, "CREATE TABLE `t` (`v` VECTOR(16383) NOT NULL)"},
		{"ALTER TABLE t ADD COLUMN v VECTOR(4)", true, "ALTER TABLE `t` ADD COLUMN `v` VECTOR(4)"},
		{"SELECT CAST(x AS VECTOR) FROM t", true, "SELECT CAST(`x` AS VECTOR) FROM `t`"},
		{"SELECT CAST(x AS VECTOR(8)) FROM t", true, "SELECT CAST(`x` AS VECTOR(8)) FROM `t`"},
		// VECTOR stays usable as an identifier.
		{"CREATE TABLE vector (vector INT)", true, "CREATE TABLE `vector` (`vector` INT)"},
		{"SELECT vector FROM vector", true, "SELECT `vector` FROM `vector`"},
	}
	RunTest(t, table, false)
}

func TestACLCreateUser(t *testing.T) {
	table := []testCase{
		// IDENTIFIED BY RANDOM PASSWORD (MySQL 8.0.18+).
		{"CREATE USER pu4@localhost IDENTIFIED BY RANDOM PASSWORD", true, "CREATE USER `pu4`@`localhost` IDENTIFIED BY RANDOM PASSWORD"},
		{"CREATE USER u1@localhost IDENTIFIED WITH caching_sha2_password BY RANDOM PASSWORD", true, "CREATE USER `u1`@`localhost` IDENTIFIED WITH 'caching_sha2_password' BY RANDOM PASSWORD"},
		// Multi-factor authentication chains (MySQL 8.0.27+).
		{"CREATE USER u2@localhost IDENTIFIED BY 'pw1' AND IDENTIFIED WITH authentication_ldap_simple", true, "CREATE USER `u2`@`localhost` IDENTIFIED BY 'pw1' AND IDENTIFIED WITH 'authentication_ldap_simple'"},
		{"CREATE USER u3@localhost IDENTIFIED BY 'pw1' AND IDENTIFIED WITH authentication_ldap_simple AS 'cn=u3' AND IDENTIFIED WITH authentication_fido", true, "CREATE USER `u3`@`localhost` IDENTIFIED BY 'pw1' AND IDENTIFIED WITH 'authentication_ldap_simple' AS 'cn=u3' AND IDENTIFIED WITH 'authentication_fido'"},
		// IDENTIFIED WITH plugin AS 'hash' must not lose to GRANT's AS clause.
		{"CREATE USER x@l IDENTIFIED WITH 'p' AS 'h'", true, "CREATE USER `x`@`l` IDENTIFIED WITH 'p' AS 'h'"},
		// DEFAULT ROLE clause.
		{"CREATE USER u4 DEFAULT ROLE r1, r2", true, "CREATE USER `u4`@`%` DEFAULT ROLE `r1`@`%`, `r2`@`%`"},
		{"CREATE USER u10 IDENTIFIED BY 'x' DEFAULT ROLE r1 REQUIRE SSL PASSWORD EXPIRE NEVER", true, "CREATE USER `u10`@`%` IDENTIFIED BY 'x' DEFAULT ROLE `r1`@`%` REQUIRE SSL PASSWORD EXPIRE NEVER"},
		// INITIAL AUTHENTICATION (MySQL 8.0.27+ passwordless setup).
		{"CREATE USER u5@localhost IDENTIFIED WITH authentication_webauthn INITIAL AUTHENTICATION IDENTIFIED BY 'boot'", true, "CREATE USER `u5`@`localhost` IDENTIFIED WITH 'authentication_webauthn' INITIAL AUTHENTICATION IDENTIFIED BY 'boot'"},
		{"CREATE USER u6@localhost IDENTIFIED WITH authentication_webauthn INITIAL AUTHENTICATION IDENTIFIED BY RANDOM PASSWORD", true, "CREATE USER `u6`@`localhost` IDENTIFIED WITH 'authentication_webauthn' INITIAL AUTHENTICATION IDENTIFIED BY RANDOM PASSWORD"},
		// PASSWORD REQUIRE CURRENT [OPTIONAL | DEFAULT] (MySQL 8.0.13+).
		{"CREATE USER u7@localhost IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT", true, "CREATE USER `u7`@`localhost` IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT"},
		{"CREATE USER u8@localhost IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT OPTIONAL", true, "CREATE USER `u8`@`localhost` IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT OPTIONAL"},
		{"CREATE USER u9@localhost IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT DEFAULT", true, "CREATE USER `u9`@`localhost` IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT DEFAULT"},
	}
	RunTest(t, table, false)
}

func TestACLAlterUser(t *testing.T) {
	table := []testCase{
		// REPLACE 'current' password verification (MySQL 8.0.14+).
		{"ALTER USER u1@localhost IDENTIFIED BY 'new' REPLACE 'old' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`localhost` IDENTIFIED BY 'new' REPLACE 'old' RETAIN CURRENT PASSWORD"},
		{"ALTER USER u2@localhost IDENTIFIED BY RANDOM PASSWORD RETAIN CURRENT PASSWORD", true, "ALTER USER `u2`@`localhost` IDENTIFIED BY RANDOM PASSWORD RETAIN CURRENT PASSWORD"},
		{"ALTER USER u3@localhost IDENTIFIED BY 'new' REPLACE 'old'", true, "ALTER USER `u3`@`localhost` IDENTIFIED BY 'new' REPLACE 'old'"},
		// DEFAULT ROLE management.
		{"ALTER USER u4@localhost DEFAULT ROLE ALL", true, "ALTER USER `u4`@`localhost` DEFAULT ROLE ALL"},
		{"ALTER USER u5@localhost DEFAULT ROLE NONE", true, "ALTER USER `u5`@`localhost` DEFAULT ROLE NONE"},
		{"ALTER USER IF EXISTS u6@localhost DEFAULT ROLE r1, r2", true, "ALTER USER IF EXISTS `u6`@`localhost` DEFAULT ROLE `r1`@`%`, `r2`@`%`"},
		// Multi-factor management (MySQL 8.0.27+).
		{"ALTER USER u7@localhost ADD 2 FACTOR IDENTIFIED WITH authentication_ldap_simple", true, "ALTER USER `u7`@`localhost` ADD 2 FACTOR IDENTIFIED WITH 'authentication_ldap_simple'"},
		{"ALTER USER u8@localhost MODIFY 3 FACTOR IDENTIFIED WITH authentication_fido", true, "ALTER USER `u8`@`localhost` MODIFY 3 FACTOR IDENTIFIED WITH 'authentication_fido'"},
		{"ALTER USER u9@localhost DROP 2 FACTOR", true, "ALTER USER `u9`@`localhost` DROP 2 FACTOR"},
		{"ALTER USER u13@localhost IDENTIFIED BY 'a' AND IDENTIFIED WITH authentication_ldap_simple", true, "ALTER USER `u13`@`localhost` IDENTIFIED BY 'a' AND IDENTIFIED WITH 'authentication_ldap_simple'"},
		// WebAuthn device registration (MySQL 8.0.27+).
		{"ALTER USER u10@localhost 2 FACTOR INITIATE REGISTRATION", true, "ALTER USER `u10`@`localhost` 2 FACTOR INITIATE REGISTRATION"},
		{"ALTER USER u11@localhost 2 FACTOR FINISH REGISTRATION SET CHALLENGE_RESPONSE AS 'blob'", true, "ALTER USER `u11`@`localhost` 2 FACTOR FINISH REGISTRATION SET CHALLENGE_RESPONSE AS 'blob'"},
		{"ALTER USER u12@localhost 2 FACTOR FINISH REGISTRATION", true, "ALTER USER `u12`@`localhost` 2 FACTOR FINISH REGISTRATION"},
		{"ALTER USER u14@localhost 2 FACTOR UNREGISTER", true, "ALTER USER `u14`@`localhost` 2 FACTOR UNREGISTER"},
	}
	RunTest(t, table, false)
}

func TestACLSetPassword(t *testing.T) {
	table := []testCase{
		{"SET PASSWORD TO RANDOM", true, "SET PASSWORD TO RANDOM"},
		{"SET PASSWORD = 'new' REPLACE 'old'", true, "SET PASSWORD='new' REPLACE 'old'"},
		{"SET PASSWORD TO RANDOM REPLACE 'old'", true, "SET PASSWORD TO RANDOM REPLACE 'old'"},
		{"SET PASSWORD FOR u1@localhost TO RANDOM RETAIN CURRENT PASSWORD", true, "SET PASSWORD FOR `u1`@`localhost` TO RANDOM RETAIN CURRENT PASSWORD"},
		{"SET PASSWORD FOR u1@localhost = 'new' REPLACE 'old' RETAIN CURRENT PASSWORD", true, "SET PASSWORD FOR `u1`@`localhost`='new' REPLACE 'old' RETAIN CURRENT PASSWORD"},
		{"SET PASSWORD = 'new'", true, "SET PASSWORD='new'"},
	}
	RunTest(t, table, false)
}

func TestACLGrantRevoke(t *testing.T) {
	table := []testCase{
		// GRANT role ... WITH ADMIN OPTION (MySQL 8.0+).
		{"GRANT r1, r2 TO u1@localhost WITH ADMIN OPTION", true, "GRANT `r1`@`%`, `r2`@`%` TO `u1`@`localhost` WITH ADMIN OPTION"},
		// GRANT ... AS user [WITH ROLE ...] (MySQL 8.0.16+).
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost`"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE ALL", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE ALL"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE NONE", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE NONE"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE DEFAULT", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE DEFAULT"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE ALL EXCEPT r1", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE ALL EXCEPT `r1`@`%`"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE r1, r2", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE `r1`@`%`, `r2`@`%`"},
		{"GRANT SELECT ON db1.* TO u1@localhost WITH GRANT OPTION AS u2@localhost WITH ROLE ALL", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` WITH GRANT OPTION AS `u2`@`localhost` WITH ROLE ALL"},
		// The 5.7-era GRANT auth-hash form must keep AS bound to the plugin.
		{"GRANT SELECT ON db1.* TO u1 IDENTIFIED WITH 'p' AS 'hash57'", true, "GRANT SELECT ON `db1`.* TO `u1`@`%` IDENTIFIED WITH 'p' AS 'hash57'"},
		// REVOKE [IF EXISTS] ... [IGNORE UNKNOWN USER] (MySQL 8.0.30+).
		{"REVOKE IF EXISTS SELECT ON db1.t1 FROM u1@localhost", true, "REVOKE IF EXISTS SELECT ON `db1`.`t1` FROM `u1`@`localhost`"},
		{"REVOKE IF EXISTS SELECT ON db1.t1 FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS SELECT ON `db1`.`t1` FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		{"REVOKE SELECT ON db1.t1 FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE SELECT ON `db1`.`t1` FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		{"REVOKE IF EXISTS r1 FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS `r1`@`%` FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		{"REVOKE ALL PRIVILEGES, GRANT OPTION FROM u1@localhost", true, "REVOKE ALL, GRANT OPTION ON *.* FROM `u1`@`localhost`"},
		{"REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS ALL, GRANT OPTION ON *.* FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		// REVOKE PROXY mirrors GRANT PROXY.
		{"REVOKE PROXY ON u1@localhost FROM u2@localhost", true, "REVOKE PROXY ON `u1`@`localhost` FROM `u2`@`localhost`"},
		{"REVOKE IF EXISTS PROXY ON u1@localhost FROM u2@localhost, u3@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS PROXY ON `u1`@`localhost` FROM `u2`@`localhost`, `u3`@`localhost` IGNORE UNKNOWN USER"},
	}
	RunTest(t, table, false)
}

func TestDefaultExpression(t *testing.T) {
	// MySQL 8.0.13+ DEFAULT (expr) takes a full expression; the parentheses
	// are part of the statement's meaning and must survive the round trip.
	table := []testCase{
		{"CREATE TABLE ct06 (a INT, b INT DEFAULT (a + 1))", true, "CREATE TABLE `ct06` (`a` INT,`b` INT DEFAULT (`a`+1))"},
		{"CREATE TABLE ct07 (a INT, b INT DEFAULT (a * a))", true, "CREATE TABLE `ct07` (`a` INT,`b` INT DEFAULT (`a`*`a`))"},
		{`CREATE TABLE ct09 (j JSON DEFAULT (CAST('["x"]' AS JSON)))`, true, "CREATE TABLE `ct09` (`j` JSON DEFAULT (CAST(_UTF8MB4'[\"x\"]' AS JSON)))"},
		{"CREATE TABLE ct10 (d DATE DEFAULT (CURRENT_DATE + INTERVAL 1 YEAR))", true, "CREATE TABLE `ct10` (`d` DATE DEFAULT (DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR)))"},
		{"ALTER TABLE t1 ADD COLUMN y INT DEFAULT (a + 1)", true, "ALTER TABLE `t1` ADD COLUMN `y` INT DEFAULT (`a`+1)"},
		{"CREATE TABLE lx07 (a DATE DEFAULT (DATE '2020-01-01'))", true, "CREATE TABLE `lx07` (`a` DATE DEFAULT (DATE '2020-01-01'))"},
		{"CREATE TABLE lx08 (a TIMESTAMP DEFAULT (TIMESTAMP '2020-01-01 00:00:00'))", true, "CREATE TABLE `lx08` (`a` TIMESTAMP DEFAULT (TIMESTAMP '2020-01-01 00:00:00'))"},
		// Nested parentheses stay nested: MySQL prints DEFAULT ((1 + 2)).
		{"CREATE TABLE t (b INT DEFAULT ((1+2)))", true, "CREATE TABLE `t` (`b` INT DEFAULT ((1+2)))"},
		{"CREATE TABLE t (u VARCHAR(36) DEFAULT (UUID()))", true, "CREATE TABLE `t` (`u` VARCHAR(36) DEFAULT (UUID()))"},
		{"CREATE TABLE t (j JSON DEFAULT ('{}'))", true, "CREATE TABLE `t` (`j` JSON DEFAULT (_UTF8MB4'{}'))"},
		{"CREATE TABLE t (a INT, b INT DEFAULT (a))", true, "CREATE TABLE `t` (`a` INT,`b` INT DEFAULT (`a`))"},
		// Parenthesized NOW is an expression default and keeps its shape;
		// the bare timestamp-default form still folds to CURRENT_TIMESTAMP.
		{"CREATE TABLE t (ts DATETIME DEFAULT (NOW()))", true, "CREATE TABLE `t` (`ts` DATETIME DEFAULT (NOW()))"},
		{"CREATE TABLE t (ts DATETIME DEFAULT NOW())", true, "CREATE TABLE `t` (`ts` DATETIME DEFAULT CURRENT_TIMESTAMP())"},
		{"CREATE TABLE t (g POINT DEFAULT (POINT(0, 0)))", true, "CREATE TABLE `t` (`g` POINT DEFAULT (POINT(0, 0)))"},
		// KILL shares the builtin-function production; keep it working.
		{"KILL CONNECTION_ID()", true, "KILL CONNECTION_ID()"},
		// A bare SELECT is a syntax error in MySQL's expr too; a doubly
		// parenthesized subquery parses and is rejected semantically.
		{"CREATE TABLE t (b INT DEFAULT (SELECT 1))", false, ""},
		{"CREATE TABLE t (b INT DEFAULT ((SELECT 1)))", true, "CREATE TABLE `t` (`b` INT DEFAULT ((SELECT 1)))"},
	}
	RunTest(t, table, false)
}

// TestCharsetRegistry covers parse-level acceptance of every character set
// MySQL knows (not just the handful TiDB could store), the utf8mb3_* collation
// spellings, the utf8mb4 locale collations (IDs 310-323), and the
// BYTE/ASCII/UNICODE column-attribute shorthands from
// opt_charset_with_opt_binary. Restore shapes were verified against MySQL
// 8.0.46 SHOW CREATE TABLE.
func TestCharsetRegistry(t *testing.T) {
	table := []testCase{
		// BYTE/ASCII/UNICODE shorthands: BYTE means the binary charset (MySQL
		// prints binary(10)), ASCII means latin1, UNICODE means ucs2.
		{"CREATE TABLE t (a CHAR(10) BYTE)", true, "CREATE TABLE `t` (`a` BINARY(10))"},
		{"CREATE TABLE t (a CHAR(10) ASCII)", true, "CREATE TABLE `t` (`a` CHAR(10) CHARACTER SET LATIN1)"},
		{"CREATE TABLE t (a CHAR(10) UNICODE)", true, "CREATE TABLE `t` (`a` CHAR(10) CHARACTER SET UCS2)"},
		{"CREATE TABLE t (a CHAR ASCII)", true, "CREATE TABLE `t` (`a` CHAR CHARACTER SET LATIN1)"},
		{"CREATE TABLE t (a VARCHAR(10) ASCII)", true, "CREATE TABLE `t` (`a` VARCHAR(10) CHARACTER SET LATIN1)"},
		{"CREATE TABLE t (a VARCHAR(10) BYTE)", true, "CREATE TABLE `t` (`a` VARBINARY(10))"},
		{"SELECT CAST('a' AS CHAR(5) UNICODE)", true, "SELECT CAST(_UTF8MB4'a' AS CHAR(5) CHARSET UCS2)"},
		{"SELECT CAST('a' AS CHAR(5) BYTE)", true, "SELECT CAST(_UTF8MB4'a' AS BINARY(5))"},
		// Charset name clauses accept the full registry.
		{"CREATE TABLE t (a TEXT CHARACTER SET ucs2)", true, "CREATE TABLE `t` (`a` TEXT CHARACTER SET UCS2)"},
		{"CREATE TABLE t (a CHAR(10) CHARACTER SET latin2 COLLATE latin2_general_ci)", true, "CREATE TABLE `t` (`a` CHAR(10) CHARACTER SET LATIN2 COLLATE latin2_general_ci)"},
		{"CREATE TABLE t (a CHAR(10)) DEFAULT CHARSET=koi8r", true, "CREATE TABLE `t` (`a` CHAR(10)) DEFAULT CHARACTER SET = KOI8R"},
		{"CREATE DATABASE d CHARACTER SET ucs2", true, "CREATE DATABASE `d` CHARACTER SET = ucs2"},
		{"ALTER TABLE t CONVERT TO CHARACTER SET ucs2", true, "ALTER TABLE `t` CONVERT TO CHARACTER SET UCS2"},
		{"SELECT CONVERT('a' USING ucs2)", true, "SELECT CONVERT(_UTF8MB4'a' USING 'ucs2')"},
		{"SET NAMES ucs2", true, "SET NAMES 'ucs2'"},
		// String-literal introducers work for any known charset.
		{"SELECT _ucs2 X'0078'", true, "SELECT _UCS2 x'0078'"},
		{"SELECT _latin2'abc'", true, "SELECT _LATIN2'abc'"},
		{"SELECT _utf16 B'01111000'", true, "SELECT _UTF16 b'1111000'"},
		// utf8mb3_* spellings alias to the registry's utf8_* names, matching
		// the existing utf8mb3 -> utf8 charset canonicalization.
		{"CREATE TABLE t (a TEXT COLLATE utf8mb3_danish_ci)", true, "CREATE TABLE `t` (`a` TEXT COLLATE utf8_danish_ci)"},
		{"SELECT 'a' COLLATE utf8mb3_czech_ci", true, "SELECT _UTF8MB4'a' COLLATE utf8_czech_ci"},
		{"CREATE TABLE t (a TEXT CHARACTER SET utf8mb3)", true, "CREATE TABLE `t` (`a` TEXT CHARACTER SET UTF8)"},
		// utf8mb4 locale collations.
		{"CREATE TABLE t (a TEXT COLLATE utf8mb4_nb_0900_ai_ci)", true, "CREATE TABLE `t` (`a` TEXT COLLATE utf8mb4_nb_0900_ai_ci)"},
		{"CREATE TABLE t (a TEXT COLLATE utf8mb4_mn_cyrl_0900_as_cs)", true, "CREATE TABLE `t` (`a` TEXT COLLATE utf8mb4_mn_cyrl_0900_as_cs)"},
		{"SET NAMES utf8mb4 COLLATE utf8mb4_sr_latn_0900_ai_ci", true, "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_sr_latn_0900_ai_ci'"},
		// Names MySQL does not know are still rejected.
		{"CREATE TABLE t (a CHAR(10) CHARACTER SET nosuch)", false, ""},
		{"CREATE TABLE t (a TEXT COLLATE nosuch_ci)", false, ""},
	}
	RunTest(t, table, false)
}

// TestLibraryDDL covers CREATE/ALTER/DROP LIBRARY and GRANT ... ON LIBRARY
// (MySQL 9.x), including dollar-quoted string bodies ($tag$ ... $tag$), which
// the lexer accepts and restore normalizes to ordinary quoted strings.
// Grammar shapes were validated against MySQL 9.7.0.
func TestLibraryDDL(t *testing.T) {
	table := []testCase{
		{"CREATE LIBRARY lib1 LANGUAGE JAVASCRIPT AS 'export function f(n) {return n}'", true, "CREATE LIBRARY `lib1` LANGUAGE JAVASCRIPT AS 'export function f(n) {return n}'"},
		{"CREATE LIBRARY IF NOT EXISTS lib2 COMMENT 'x' LANGUAGE JAVASCRIPT AS $$ export function g() {} $$", true, "CREATE LIBRARY IF NOT EXISTS `lib2` COMMENT 'x' LANGUAGE JAVASCRIPT AS ' export function g() {} '"},
		{"CREATE LIBRARY lib3 LANGUAGE JAVASCRIPT AS $mark$ export function h() {} $mark$", true, "CREATE LIBRARY `lib3` LANGUAGE JAVASCRIPT AS ' export function h() {} '"},
		{"CREATE LIBRARY probedb.libq LANGUAGE javascript AS 'x'", true, "CREATE LIBRARY `probedb`.`libq` LANGUAGE JAVASCRIPT AS 'x'"},
		{"CREATE LIBRARY q LANGUAGE JAVASCRIPT AS $$it's$$", true, "CREATE LIBRARY `q` LANGUAGE JAVASCRIPT AS 'it''s'"},
		{"ALTER LIBRARY lib1 COMMENT 'y'", true, "ALTER LIBRARY `lib1` COMMENT 'y'"},
		{"ALTER LIBRARY probedb.lib1 COMMENT 'y'", true, "ALTER LIBRARY `probedb`.`lib1` COMMENT 'y'"},
		{"DROP LIBRARY IF EXISTS lib1", true, "DROP LIBRARY IF EXISTS `lib1`"},
		{"DROP LIBRARY probedb.lib2", true, "DROP LIBRARY `probedb`.`lib2`"},
		{"GRANT EXECUTE ON LIBRARY probedb.lib1 TO u1@localhost", true, "GRANT EXECUTE ON LIBRARY `probedb`.`lib1` TO `u1`@`localhost`"},
		{"GRANT ALTER ROUTINE ON LIBRARY probedb.lib1 TO u1@localhost", true, "GRANT ALTER ROUTINE ON LIBRARY `probedb`.`lib1` TO `u1`@`localhost`"},
		{"REVOKE EXECUTE ON LIBRARY probedb.lib1 FROM u1@localhost", true, "REVOKE EXECUTE ON LIBRARY `probedb`.`lib1` FROM `u1`@`localhost`"},
		// A database that happens to be named library still works: the
		// following token ('.' vs identifier) picks the interpretation.
		{"GRANT SELECT ON library.t TO u1@localhost", true, "GRANT SELECT ON `library`.`t` TO `u1`@`localhost`"},
		// Identifiers starting with or containing $ are unaffected by
		// dollar-quote lexing.
		{"SELECT $x", true, "SELECT `$x`"},
		{"SELECT 1 AS $y", true, "SELECT 1 AS `$y`"},
		{"SELECT a$b FROM t", true, "SELECT `a$b` FROM `t`"},
		// Same rejections as MySQL 9.7.
		{"CREATE LIBRARY lib9 LANGUAGE JAVASCRIPT AS $$ unterminated", false, ""},
		{"ALTER LIBRARY lib1", false, ""},
		{"CREATE OR REPLACE LIBRARY lib4 LANGUAGE JAVASCRIPT AS 'x'", false, ""},
		{"CREATE LIBRARY lib5 AS 'x' LANGUAGE JAVASCRIPT", false, ""},
	}
	RunTest(t, table, false)
}

// TestFlushTargetList covers FLUSH with a comma-separated option list; the
// table form stays exclusive as in MySQL's grammar.
func TestFlushTargetList(t *testing.T) {
	table := []testCase{
		{"FLUSH STATUS, USER_RESOURCES", true, "FLUSH STATUS, USER_RESOURCES"},
		{"FLUSH LOCAL STATUS, USER_RESOURCES, PRIVILEGES", true, "FLUSH NO_WRITE_TO_BINLOG STATUS, USER_RESOURCES, PRIVILEGES"},
		{"FLUSH NO_WRITE_TO_BINLOG ERROR LOGS, ENGINE LOGS, OPTIMIZER_COSTS", true, "FLUSH NO_WRITE_TO_BINLOG ERROR LOGS, ENGINE LOGS, OPTIMIZER_COSTS"},
		{"FLUSH RELAY LOGS FOR CHANNEL 'ch1', STATUS", true, "FLUSH RELAY LOGS FOR CHANNEL 'ch1', STATUS"},
		{"FLUSH STATUS", true, "FLUSH STATUS"},
		{"FLUSH TABLES t1, t2 WITH READ LOCK", true, "FLUSH TABLES `t1`, `t2` WITH READ LOCK"},
	}
	RunTest(t, table, false)
}

func TestNotSecondaryColumn(t *testing.T) {
	table := []testCase{
		{"CREATE TABLE t (c INT NOT SECONDARY)", true, "CREATE TABLE `t` (`c` INT NOT SECONDARY)"},
		{"CREATE TABLE t (c INT NOT NULL NOT SECONDARY DEFAULT 5)", true, "CREATE TABLE `t` (`c` INT NOT NULL NOT SECONDARY DEFAULT 5)"},
		{"ALTER TABLE t1 MODIFY c INT NOT SECONDARY", true, "ALTER TABLE `t1` MODIFY COLUMN `c` INT NOT SECONDARY"},
		// SECONDARY stays usable as an identifier.
		{"CREATE TABLE secondary (secondary INT)", true, "CREATE TABLE `secondary` (`secondary` INT)"},
		{"CREATE TABLE t (c INT SECONDARY)", false, ""},
	}
	RunTest(t, table, false)
}

func TestCreateTableStartTransaction(t *testing.T) {
	// MySQL 8.0.21+ logs CREATE TABLE ... SELECT as CREATE TABLE ... START
	// TRANSACTION followed by row events, so the binlog form must parse.
	table := []testCase{
		{"CREATE TABLE t (a INT) START TRANSACTION", true, "CREATE TABLE `t` (`a` INT) START TRANSACTION"},
		{"CREATE TABLE IF NOT EXISTS t (a INT) ENGINE = InnoDB START TRANSACTION", true, "CREATE TABLE IF NOT EXISTS `t` (`a` INT) ENGINE = InnoDB START TRANSACTION"},
		{"CREATE TABLE t (a INT) START TRANSACTION AS SELECT 1", false, ""},
		{"CREATE TABLE t (a INT) START", false, ""},
	}
	RunTest(t, table, false)
}

func TestViewWithCheckOption(t *testing.T) {
	table := []testCase{
		// Plain WITH CHECK OPTION means CASCADED; restore normalizes the
		// default away, like it already does for an explicit CASCADED.
		{"CREATE VIEW v AS SELECT a FROM t WITH CHECK OPTION", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT `a` FROM `t`"},
		{"CREATE VIEW v AS SELECT a FROM t WITH CASCADED CHECK OPTION", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT `a` FROM `t`"},
		{"CREATE VIEW v AS SELECT a FROM t WITH LOCAL CHECK OPTION", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT `a` FROM `t` WITH LOCAL CHECK OPTION"},
		{"CREATE VIEW v AS SELECT a FROM t WITH CHECK", false, ""},
	}
	RunTest(t, table, false)
}

func TestDerivedTableColumnList(t *testing.T) {
	table := []testCase{
		{"SELECT * FROM (SELECT 1, 2) dt (foo, bar)", true, "SELECT * FROM (SELECT 1,2) AS `dt`(`foo`, `bar`)"},
		{"SELECT * FROM (SELECT 1) AS dt (foo)", true, "SELECT * FROM (SELECT 1) AS `dt`(`foo`)"},
		{"CREATE VIEW pv8 AS SELECT * FROM (SELECT 1, 2) dt (foo, bar)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `pv8` AS SELECT * FROM (SELECT 1,2) AS `dt`(`foo`, `bar`)"},
		// A column list requires an alias.
		{"SELECT * FROM (SELECT 1) (foo)", false, ""},
	}
	RunTest(t, table, false)
}

func TestTableMaintenanceStatements(t *testing.T) {
	table := []testCase{
		{"CHECK TABLE t1", true, "CHECK TABLE `t1`"},
		{"CHECK TABLES t1, t2 FOR UPGRADE", true, "CHECK TABLE `t1`, `t2` FOR UPGRADE"},
		{"CHECK TABLE t1 QUICK FAST MEDIUM EXTENDED CHANGED", true, "CHECK TABLE `t1` QUICK FAST MEDIUM EXTENDED CHANGED"},
		{"CHECK TABLE t1 UPGRADE", false, ""},
		{"CHECKSUM TABLE t1, t2", true, "CHECKSUM TABLE `t1`, `t2`"},
		{"CHECKSUM TABLE t1 QUICK", true, "CHECKSUM TABLE `t1` QUICK"},
		{"CHECKSUM TABLE t1 EXTENDED", true, "CHECKSUM TABLE `t1` EXTENDED"},
	}
	RunTest(t, table, false)
}

func TestHandlerStatements(t *testing.T) {
	table := []testCase{
		{"HANDLER t1 OPEN", true, "HANDLER `t1` OPEN"},
		{"HANDLER test.t1 OPEN AS h1", true, "HANDLER `test`.`t1` OPEN AS `h1`"},
		{"HANDLER t1 OPEN h1", true, "HANDLER `t1` OPEN AS `h1`"},
		{"HANDLER h1 CLOSE", true, "HANDLER `h1` CLOSE"},
		{"HANDLER t1 READ FIRST", true, "HANDLER `t1` READ FIRST"},
		{"HANDLER t1 READ NEXT LIMIT 5", true, "HANDLER `t1` READ NEXT LIMIT 5"},
		{"HANDLER t1 READ idx PREV WHERE a > 1", true, "HANDLER `t1` READ `idx` PREV WHERE `a`>1"},
		{"HANDLER t1 READ idx LAST", true, "HANDLER `t1` READ `idx` LAST"},
		{"HANDLER t1 READ `PRIMARY` = (1, 'x')", true, "HANDLER `t1` READ `PRIMARY` = (1, _UTF8MB4'x')"},
		{"HANDLER t1 READ idx <= (10) WHERE b = 2 LIMIT 3", true, "HANDLER `t1` READ `idx` <= (10) WHERE `b`=2 LIMIT 3"},
		// Natural-order scans only go forward.
		{"HANDLER t1 READ PREV", false, ""},
		{"HANDLER t1 READ idx != (1)", false, ""},
	}
	RunTest(t, table, false)
}

func TestPurgeBinaryLogs(t *testing.T) {
	table := []testCase{
		{"PURGE BINARY LOGS TO 'binlog.000001'", true, "PURGE BINARY LOGS TO 'binlog.000001'"},
		{"PURGE MASTER LOGS TO 'binlog.000001'", true, "PURGE BINARY LOGS TO 'binlog.000001'"},
		{"PURGE BINARY LOGS BEFORE '2026-08-01 00:00:00'", true, "PURGE BINARY LOGS BEFORE _UTF8MB4'2026-08-01 00:00:00'"},
		{"PURGE BINARY LOGS BEFORE NOW() - INTERVAL 3 DAY", true, "PURGE BINARY LOGS BEFORE DATE_SUB(NOW(), INTERVAL 3 DAY)"},
		{"PURGE BINARY LOGS", false, ""},
	}
	RunTest(t, table, false)
}

func TestImportTable(t *testing.T) {
	table := []testCase{
		{"IMPORT TABLE FROM 't1.sdi'", true, "IMPORT TABLE FROM 't1.sdi'"},
		{"IMPORT TABLE FROM 'sdi_dir/*.sdi', 't2.sdi'", true, "IMPORT TABLE FROM 'sdi_dir/*.sdi', 't2.sdi'"},
		{"IMPORT TABLE FROM", false, ""},
	}
	RunTest(t, table, false)
}

func TestKeyCacheStatements(t *testing.T) {
	table := []testCase{
		{"CACHE INDEX t1 IN hot_cache", true, "CACHE INDEX `t1` IN `hot_cache`"},
		{"CACHE INDEX t1, t2 IN DEFAULT", true, "CACHE INDEX `t1`, `t2` IN `default`"},
		{"CACHE INDEX t1 INDEX (i1, i2) IN hot_cache", true, "CACHE INDEX `t1` INDEX (`i1`, `i2`) IN `hot_cache`"},
		{"CACHE INDEX t1 KEY (i1) IN hot_cache", true, "CACHE INDEX `t1` INDEX (`i1`) IN `hot_cache`"},
		{"CACHE INDEX t1 PARTITION (p0, p1) IN hot_cache", true, "CACHE INDEX `t1` PARTITION (`p0`, `p1`) IN `hot_cache`"},
		{"CACHE INDEX t1 PARTITION (ALL) INDEX (i1) IN hot_cache", true, "CACHE INDEX `t1` PARTITION (ALL) INDEX (`i1`) IN `hot_cache`"},
		{"CACHE INDEX t1", false, ""},
		{"LOAD INDEX INTO CACHE t1", true, "LOAD INDEX INTO CACHE `t1`"},
		{"LOAD INDEX INTO CACHE t1 IGNORE LEAVES, t2", true, "LOAD INDEX INTO CACHE `t1` IGNORE LEAVES, `t2`"},
		{"LOAD INDEX INTO CACHE t1 PARTITION (p0) INDEX (i1) IGNORE LEAVES", true, "LOAD INDEX INTO CACHE `t1` PARTITION (`p0`) INDEX (`i1`) IGNORE LEAVES"},
	}
	RunTest(t, table, false)
}

func TestPluginAndComponentStatements(t *testing.T) {
	table := []testCase{
		{"INSTALL PLUGIN clone SONAME 'mysql_clone.so'", true, "INSTALL PLUGIN `clone` SONAME 'mysql_clone.so'"},
		{"UNINSTALL PLUGIN clone", true, "UNINSTALL PLUGIN `clone`"},
		{"INSTALL COMPONENT 'file://component_keyring_file'", true, "INSTALL COMPONENT 'file://component_keyring_file'"},
		{"INSTALL COMPONENT 'file://a', 'file://b'", true, "INSTALL COMPONENT 'file://a', 'file://b'"},
		{"INSTALL COMPONENT 'file://a' SET GLOBAL x.y = 1", true, "INSTALL COMPONENT 'file://a' SET @@GLOBAL.`x.y`=1"},
		{"UNINSTALL COMPONENT 'file://a', 'file://b'", true, "UNINSTALL COMPONENT 'file://a', 'file://b'"},
		{"INSTALL PLUGIN clone", false, ""},
	}
	RunTest(t, table, false)
}

func TestServerStatements(t *testing.T) {
	table := []testCase{
		{"CREATE SERVER s FOREIGN DATA WRAPPER mysql OPTIONS (USER 'u', HOST '127.0.0.1', DATABASE 'db')", true, "CREATE SERVER `s` FOREIGN DATA WRAPPER `mysql` OPTIONS (USER 'u', HOST '127.0.0.1', DATABASE 'db')"},
		{"CREATE SERVER 's1' FOREIGN DATA WRAPPER 'mysql' OPTIONS (PORT 3306)", true, "CREATE SERVER `s1` FOREIGN DATA WRAPPER 'mysql' OPTIONS (PORT 3306)"},
		{"ALTER SERVER s OPTIONS (PASSWORD 'p', SOCKET '/tmp/x.sock', OWNER 'o')", true, "ALTER SERVER `s` OPTIONS (PASSWORD 'p', SOCKET '/tmp/x.sock', OWNER 'o')"},
		{"DROP SERVER IF EXISTS s", true, "DROP SERVER IF EXISTS `s`"},
		{"DROP SERVER s", true, "DROP SERVER `s`"},
		{"CREATE SERVER s FOREIGN DATA WRAPPER mysql", false, ""},
	}
	RunTest(t, table, false)
}

func TestResourceGroupStatements(t *testing.T) {
	table := []testCase{
		{"CREATE RESOURCE GROUP rg TYPE = SYSTEM", true, "CREATE RESOURCE GROUP `rg` TYPE = SYSTEM"},
		{"CREATE RESOURCE GROUP rg TYPE USER VCPU = 0-3, 8 THREAD_PRIORITY = -5 DISABLE", true, "CREATE RESOURCE GROUP `rg` TYPE = USER VCPU = 0-3, 8 THREAD_PRIORITY = -5 DISABLE"},
		{"ALTER RESOURCE GROUP rg VCPU 2 ENABLE", true, "ALTER RESOURCE GROUP `rg` VCPU = 2 ENABLE"},
		{"ALTER RESOURCE GROUP rg DISABLE FORCE", true, "ALTER RESOURCE GROUP `rg` DISABLE FORCE"},
		{"DROP RESOURCE GROUP rg FORCE", true, "DROP RESOURCE GROUP `rg` FORCE"},
		{"SET RESOURCE GROUP rg", true, "SET RESOURCE GROUP `rg`"},
		{"SET RESOURCE GROUP rg FOR 14, 78", true, "SET RESOURCE GROUP `rg` FOR 14, 78"},
		{"CREATE RESOURCE GROUP rg", false, ""},
		// SET of a variable that happens to be named resource still works.
		{"SET resource = 1", true, "SET @@SESSION.`resource`=1"},
	}
	RunTest(t, table, false)
}

func TestCloneStatements(t *testing.T) {
	table := []testCase{
		{"CLONE LOCAL DATA DIRECTORY = '/tmp/clone_dir'", true, "CLONE LOCAL DATA DIRECTORY = '/tmp/clone_dir'"},
		{"CLONE LOCAL DATA DIRECTORY '/tmp/clone_dir'", true, "CLONE LOCAL DATA DIRECTORY = '/tmp/clone_dir'"},
		{"CLONE INSTANCE FROM 'u'@'donor' : 3306 IDENTIFIED BY 'pw'", true, "CLONE INSTANCE FROM `u`@`donor`:3306 IDENTIFIED BY 'pw'"},
		{"CLONE INSTANCE FROM u@h:3306 IDENTIFIED BY 'pw' DATA DIRECTORY '/d' REQUIRE SSL", true, "CLONE INSTANCE FROM `u`@`h`:3306 IDENTIFIED BY 'pw' DATA DIRECTORY = '/d' REQUIRE SSL"},
		{"CLONE INSTANCE FROM u@h:3306 IDENTIFIED BY 'pw' REQUIRE NO SSL", true, "CLONE INSTANCE FROM `u`@`h`:3306 IDENTIFIED BY 'pw' REQUIRE NO SSL"},
		{"CLONE INSTANCE FROM u@h IDENTIFIED BY 'pw'", false, ""},
	}
	RunTest(t, table, false)
}

func TestInstanceLockStatements(t *testing.T) {
	table := []testCase{
		{"LOCK INSTANCE FOR BACKUP", true, "LOCK INSTANCE FOR BACKUP"},
		{"UNLOCK INSTANCE", true, "UNLOCK INSTANCE"},
		{"LOCK INSTANCE", false, ""},
		// LOCK TABLES gained aliases and LOW_PRIORITY WRITE.
		{"LOCK TABLES t1 AS a1 READ, t2 LOW_PRIORITY WRITE", true, "LOCK TABLES `t1` AS `a1` READ, `t2` LOW_PRIORITY WRITE"},
		{"LOCK TABLE t1 a1 WRITE", true, "LOCK TABLES `t1` AS `a1` WRITE"},
	}
	RunTest(t, table, false)
}
