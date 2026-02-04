package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiff uses a table-driven approach to test various diff scenarios
func TestDiff(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		target   string
		expected string // empty string means no diff expected
	}{
		{
			name:     "NoChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "",
		},
		{
			name:     "AddColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			expected: "ALTER TABLE `t1` ADD COLUMN `b` int(11) NULL AFTER `id`",
		},
		{
			name:     "DropColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` DROP COLUMN `b`",
		},
		{
			name:     "ModifyColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(100) NULL",
		},
		{
			name:     "ReorderColumn",
			source:   "CREATE TABLE t1 (a INT, b INT, c INT)",
			target:   "CREATE TABLE t1 (c INT, a INT, b INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int(11) NULL FIRST, MODIFY COLUMN `a` int(11) NULL AFTER `c`",
		},
		{
			name:     "AddIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX idx_b (b))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_b` (`b`)",
		},
		{
			name:     "DropIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX idx_b (b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_b`",
		},
		{
			name:     "AddUniqueIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY idx_email (email))",
			expected: "ALTER TABLE `t1` ADD UNIQUE INDEX `idx_email` (`email`)",
		},
		{
			name:     "ColumnWithDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20) DEFAULT 'active')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `status` varchar(20) NULL DEFAULT 'active'",
		},
		{
			name:     "ColumnNullability",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) NOT NULL",
		},
		{
			name:     "TableOptions",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB COMMENT='test table'",
			expected: "ALTER TABLE `t1` COMMENT='test table'",
		},
		{
			name:     "EnumColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, status ENUM('active', 'inactive'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, status ENUM('active', 'inactive', 'pending'))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `status` enum('active','inactive','pending') NULL",
		},
		{
			name:     "DecimalColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(12,4))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `price` decimal(12,4) NULL",
		},
		{
			name:     "UnsignedColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, count INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, count INT UNSIGNED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `count` int(11) unsigned NULL",
		},
		{
			name:     "ColumnComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COMMENT 'User name')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) NULL COMMENT 'User name'",
		},
		{
			name:     "ColumnCommentRogueValues1",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY COMMENT 'Line1\\nLine2\\rLine3')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int(11) NOT NULL COMMENT 'Line1\\nLine2\\rLine3'",
		},
		{
			name:     "ColumnCommentRogueValues2",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) DEFAULT "O'Brien")`,
			expected: "ALTER TABLE `t1` ADD COLUMN `name` varchar(50) NULL DEFAULT 'O\\'\\'Brien' AFTER `id`",
		},

		{
			name:     "AutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int(11) NOT NULL AUTO_INCREMENT",
		},
		{
			name:     "SetColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, permissions SET('read', 'write'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, permissions SET('read', 'write', 'execute'))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `permissions` set('read','write','execute') NULL",
		},
		{
			name:     "CheckConstraint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `chk_age` CHECK (`age`>=0)",
		},
		{
			name:     "DropCheckConstraint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT)",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_age`",
		},
		{
			name:     "TimestampDefaultCurrentTimestamp",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `created_at` timestamp NULL DEFAULT current_timestamp",
		},
		{
			name:     "TimestampExplicitDefaults",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL DEFAULT '2023-01-01 00:00:00')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `ts` timestamp NOT NULL DEFAULT '2023-01-01 00:00:00'",
		},
		{
			name:     "MultipleChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), d INT, INDEX idx_d (d))",
			expected: "ALTER TABLE `t1` DROP COLUMN `c`, MODIFY COLUMN `b` varchar(100) NULL, ADD COLUMN `d` int(11) NULL AFTER `b`, ADD INDEX `idx_d` (`d`)",
		},
		{
			name:     "DefaultValueFunction_NOW",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME DEFAULT NOW())",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `created_at` datetime NULL DEFAULT current_timestamp",
		},
		{
			name:     "DefaultValueFunction_UUID",
			source:   "CREATE TABLE t1 (id VARCHAR(36) PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id VARCHAR(36) PRIMARY KEY DEFAULT (UUID()), name VARCHAR(100))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` varchar(36) NOT NULL DEFAULT 'uuid'",
		},
		{
			name: "MultipleChanges",
			source: `CREATE TABLE products (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2),
			old_column INT
		)`,
			target: `CREATE TABLE products (
			id INT PRIMARY KEY,
			name VARCHAR(200) NOT NULL,
			price DECIMAL(12,4),
			description TEXT,
			INDEX idx_name (name)
		)`,
			expected: "ALTER TABLE `products` DROP COLUMN `old_column`, MODIFY COLUMN `name` varchar(200) NOT NULL, MODIFY COLUMN `price` decimal(12,4) NULL, ADD COLUMN `description` text NULL AFTER `price`, ADD INDEX `idx_name` (`name`)",
		},
		{
			name:     "VirtualColumns",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), full_name VARCHAR(101) AS (CONCAT(first_name, ' ', last_name)) VIRTUAL)",
			expected: "ALTER TABLE `t1` ADD COLUMN `full_name` varchar(101) NULL AFTER `last_name`",
		},
		{
			name:     "StoredColumns",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2), tax_rate DECIMAL(5,4))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2), tax_rate DECIMAL(5,4), total DECIMAL(10,2) AS (price * (1 + tax_rate)) STORED)",
			expected: "ALTER TABLE `t1` ADD COLUMN `total` decimal(10,2) NULL AFTER `tax_rate`",
		},
		{
			name:     "Timestamps1",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, updated_at TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `updated_at` timestamp NULL DEFAULT current_timestamp",
		},
		{
			name:     "Timestamps2",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT NULL)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `ts` timestamp NULL DEFAULT NULL",
		},
		// Index Modifications
		{
			name:     "ModifyIndexVisibility_MakeInvisible",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) INVISIBLE)",
			expected: "ALTER TABLE `t1` ALTER INDEX `idx_name` INVISIBLE",
		},
		{
			name:     "ModifyIndexVisibility_MakeVisible",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) INVISIBLE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			expected: "ALTER TABLE `t1` ALTER INDEX `idx_name` VISIBLE",
		},
		{
			name:     "ModifyIndexType",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) USING HASH)",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_name`, ADD INDEX `idx_name` (`name`) USING HASH",
		},
		{
			name:     "AddIndexWithComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) COMMENT 'name index')",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`) COMMENT 'name index'",
		},

		// Fulltext and Spatial Indexes
		{
			name:     "AddFulltextIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, FULLTEXT INDEX idx_content (content))",
			expected: "ALTER TABLE `t1` ADD FULLTEXT INDEX `idx_content` (`content`)",
		},
		{
			name:     "DropFulltextIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, FULLTEXT INDEX idx_content (content))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT)",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_content`",
		},
		// Constraint Modifications
		{
			name:     "ModifyCheckConstraint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 18))",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_age`, ADD CONSTRAINT `chk_age` CHECK (`age`>=18)",
		},
		{
			name:     "AddForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
		},
		{
			name:     "DropForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`",
		},
		// Table Options
		{
			name:     "ChangeEngine",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyISAM",
			expected: "ALTER TABLE `t1` ENGINE=MyISAM",
		},
		{
			name:     "ChangeCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=latin1",
			expected: "ALTER TABLE `t1` DEFAULT CHARSET=latin1",
		},
		{
			name:     "ChangeCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_unicode_ci",
			expected: "ALTER TABLE `t1` COLLATE=utf8mb4_unicode_ci",
		},
		{
			name:     "ChangeRowFormat",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			expected: "ALTER TABLE `t1` ROW_FORMAT=DYNAMIC",
		},

		{
			name:     "ChangeAutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=100",
			expected: "", // currently not supported: this is intentional for now.
		},
		// Composite Primary Key
		{
			name:     "CompositePrimaryKey",
			source:   "CREATE TABLE t1 (a INT, b INT)",
			target:   "CREATE TABLE t1 (a INT, b INT, PRIMARY KEY (a, b))",
			expected: "ALTER TABLE `t1` ADD PRIMARY KEY (`a`, `b`)",
		},
		{
			name:     "DropPrimaryKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT)",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY",
		},
		{
			name:     "DropPrimaryKeyCanonicalForm",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT NOT NULL)",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY",
		},

		// Multi-column Indexes
		{
			name:     "AddMultiColumnIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), INDEX idx_name (first_name, last_name))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`first_name`, `last_name`)",
		},
		// Column Charset/Collation
		{
			name:     "ColumnCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET utf8mb4) CHARSET utf8mb4",
			expected: "", // redundant; simplifies.
		},
		{
			name:     "ColumnCharsetDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET latin1) CHARSET utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) CHARACTER SET latin1 NULL",
		},
		{
			name:     "ColumnCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COLLATE utf8mb4_bin) CHARSET utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) COLLATE utf8mb4_bin NULL",
		},
		// Edge Cases
		{
			name:     "RemoveDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20) DEFAULT 'active')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `status` varchar(20) NULL",
		},
		{
			name:     "AddColumnFirst",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (new_col INT, id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` ADD COLUMN `new_col` int(11) NULL FIRST",
		},
		{
			name:     "ChangeColumnOrder",
			source:   "CREATE TABLE t1 (a INT, b INT, c INT)",
			target:   "CREATE TABLE t1 (b INT, c INT, a INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int(11) NULL FIRST, MODIFY COLUMN `c` int(11) NULL AFTER `b`, MODIFY COLUMN `a` int(11) NULL AFTER `c`",
		},
		// Binary/Blob Types
		{
			name:     "BinaryColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, data VARBINARY(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, data VARBINARY(200))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `data` varbinary(200) NULL",
		},
		{
			name:     "BlobColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, data BLOB)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, data LONGBLOB)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `data` longblob NULL",
		},
		// JSON and other modern types
		{
			name:     "JsonColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, data TEXT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, data JSON)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `data` json NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct1, err := ParseCreateTable(tt.source)
			require.NoError(t, err)

			ct2, err := ParseCreateTable(tt.target)
			require.NoError(t, err)

			stmts, err := ct1.Diff(ct2)
			require.NoError(t, err)

			if tt.expected == "" {
				assert.Nil(t, stmts, "expected nil for identical tables")
			} else {
				require.Len(t, stmts, 1)
				assert.Equal(t, tt.expected, stmts[0].Statement)
			}
		})
	}
}

func TestDiff_DifferentTableNames(t *testing.T) {
	ct1, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY)")
	require.NoError(t, err)

	ct2, err := ParseCreateTable("CREATE TABLE t2 (id INT PRIMARY KEY)")
	require.NoError(t, err)

	_, err = ct1.Diff(ct2)
	assert.Error(t, err, "expected error when diffing tables with different names")
}
