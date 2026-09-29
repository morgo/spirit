package statement

import (
	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/mysql"
)

func init() { registerNormalizer(utf8mb3DefaultCollationNormalizer{}) }

// utf8mb3DefaultCollationNormalizer fills in utf8mb3's default collation,
// utf8mb3_general_ci, on a column or table that declares the utf8mb3 charset
// without a COLLATE. Unlike utf8mb4's, this default does not depend on the
// server version or configuration. Verified against MySQL 8.0.43:
//
//	c NVARCHAR(10)                       (table charset utf8mb4)
//	  -> varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
//	c varchar(10) CHARACTER SET utf8mb3  (table charset utf8mb3)
//	  -> varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
//	DEFAULT CHARSET=utf8mb3
//	  -> DEFAULT CHARSET=utf8mb3         (COLLATE omitted: it is the default)
//
// The main route to this charset is NCHAR/NVARCHAR and their NATIONAL aliases,
// which always use utf8mb3 and do not accept a charset clause. Because the
// live form always writes the COLLATE out, without this rule a desired schema
// using NVARCHAR diffs against its own live table and emits a MODIFY.
//
// The table default is filled in as well, so that a column which inherits a
// utf8mb3 table default and a column which declares utf8mb3 explicitly
// resolve to the same collation and still compare equal.
//
// A column with the BINARY attribute is left to binaryAttributeNormalizer,
// which selects utf8mb3_bin for it. Filling in the default here would make a
// collation this rule invented indistinguishable from a written COLLATE,
// which that rule keeps.
type utf8mb3DefaultCollationNormalizer struct{}

func (utf8mb3DefaultCollationNormalizer) Name() string { return "utf8mb3-default-collation" }

func (utf8mb3DefaultCollationNormalizer) Normalize(ct *CreateTable) *CreateTable {
	// MySQLDefaultCollation, not GetDefaultCollation: the latter returns the
	// parser upstream's utf8_bin. The result is in the parser's legacy "utf8"
	// spelling, which is how it parses both utf8mb3_* and utf8_* names.
	def, ok := charset.MySQLDefaultCollation(charset.CharsetUTF8)
	if !ok {
		return ct
	}
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if col.Charset == nil || *col.Charset != charset.CharsetUTF8 || col.Collation != nil {
			continue
		}
		if col.Raw != nil && mysql.HasBinaryFlag(col.Raw.Tp.GetFlag()) {
			continue
		}
		collation := def
		col.Collation = &collation
	}
	if opts := ct.TableOptions; opts != nil && opts.Charset != nil && *opts.Charset == charset.CharsetUTF8 && opts.Collation == nil {
		collation := def
		opts.Collation = &collation
	}
	return ct
}
