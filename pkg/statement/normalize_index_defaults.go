package statement

import "strings"

func init() { registerNormalizer(indexDefaultsNormalizer{}) }

// indexDefaultsNormalizer drops the index options MySQL accepts and does not
// store, so a definition that spells them out matches the SHOW CREATE TABLE
// that omits them instead of re-emitting the same ALTER on every run:
//
//   - VISIBLE, on any index. It is the default and is never reported. A diff
//     against the live table used to emit ALTER INDEX ... VISIBLE each run,
//     and for PRIMARY KEY (id) VISIBLE an ALTER INDEX ... VISIBLE with an
//     empty name, which MySQL rejects. INVISIBLE is kept (on a primary key it is an
//     error, 3522, which is left for MySQL to report).
//   - USING HASH, on InnoDB. InnoDB has no hash indexes: it builds a B-tree
//     and reports no USING at all, so the option-only rebuild the diff emitted
//     changed nothing. An explicit USING BTREE is reported and is kept.
//   - KEY_BLOCK_SIZE, on InnoDB, unless the table is compressed: a
//     ROW_FORMAT=COMPRESSED or a table-level KEY_BLOCK_SIZE. The option only
//     applies to compressed tables and is dropped on creation otherwise, so
//     the diff rebuilt the index on every run, in two statements, to no
//     effect.
//
// The engine-specific rules apply when the table names InnoDB or no engine
// (the default engine on every supported server). A table on another engine
// keeps its options: MEMORY and NDB build hash indexes, and MyISAM stores a
// KEY_BLOCK_SIZE.
type indexDefaultsNormalizer struct{}

func (indexDefaultsNormalizer) Name() string { return "index-defaults" }

func (indexDefaultsNormalizer) Normalize(ct *CreateTable) *CreateTable {
	engine := ct.TableOptions.getEngine()
	innodb := engine == nil || strings.EqualFold(*engine, "InnoDB")
	rowFormat := ct.TableOptions.getRowFormat()
	compressed := (rowFormat != nil && strings.EqualFold(*rowFormat, "COMPRESSED")) ||
		ct.TableOptions.deref().KeyBlockSize != nil
	for i := range ct.Indexes {
		idx := &ct.Indexes[i]
		if idx.Invisible != nil && !*idx.Invisible {
			idx.Invisible = nil
		}
		if !innodb {
			continue
		}
		if idx.Using != nil && strings.EqualFold(*idx.Using, "HASH") {
			idx.Using = nil
		}
		if idx.KeyBlockSize != nil && !compressed {
			idx.KeyBlockSize = nil
		}
	}
	return ct
}
