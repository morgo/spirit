package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnMappingColumns(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	m := NewColumnMapping(t1, t1new, nil)
	src, _ := m.Columns()
	assert.Equal(t, "`a`, `b`, `c`", src)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	m = NewColumnMapping(t1, t1new, nil)
	src, _ = m.Columns()
	assert.Equal(t, "`a`, `c`", src)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	m = NewColumnMapping(t1, t1new, nil)
	src, _ = m.Columns()
	assert.Equal(t, "`a`, `c`", src)
}

func TestColumnMappingColumnsSlice(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	m := NewColumnMapping(t1, t1new, nil)
	cols, _ := m.ColumnsSlice()
	assert.Equal(t, []string{"a", "b", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	m = NewColumnMapping(t1, t1new, nil)
	cols, _ = m.ColumnsSlice()
	assert.Equal(t, []string{"a", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	m = NewColumnMapping(t1, t1new, nil)
	cols, _ = m.ColumnsSlice()
	assert.Equal(t, []string{"a", "c"}, cols)
}

func TestColumnMappingWithRenames(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")

	// Simple rename: a→x
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b", "c"}
	renames := map[string]string{"a": "x"}

	m := NewColumnMapping(t1, t1new, renames)
	srcStr, tgtStr := m.Columns()
	assert.Equal(t, "`a`, `b`, `c`", srcStr)
	assert.Equal(t, "`x`, `b`, `c`", tgtStr)

	srcSlice, tgtSlice := m.ColumnsSlice()
	assert.Equal(t, []string{"a", "b", "c"}, srcSlice)
	assert.Equal(t, []string{"x", "b", "c"}, tgtSlice)

	// Multiple renames: a→x, c→z
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b", "z"}
	renames = map[string]string{"a": "x", "c": "z"}

	m = NewColumnMapping(t1, t1new, renames)
	srcStr, tgtStr = m.Columns()
	assert.Equal(t, "`a`, `b`, `c`", srcStr)
	assert.Equal(t, "`x`, `b`, `z`", tgtStr)

	// No renames (nil map) - should behave like original
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}

	m = NewColumnMapping(t1, t1new, nil)
	srcStr, tgtStr = m.Columns()
	assert.Equal(t, "`a`, `b`, `c`", srcStr)
	assert.Equal(t, "`a`, `b`, `c`", tgtStr)

	// Empty renames map - should behave like original
	m = NewColumnMapping(t1, t1new, map[string]string{})
	srcStr, tgtStr = m.Columns()
	assert.Equal(t, "`a`, `b`, `c`", srcStr)
	assert.Equal(t, "`a`, `b`, `c`", tgtStr)

	// Rename with column added in new table (d is new, not in source)
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b", "c", "d"}
	renames = map[string]string{"a": "x"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	assert.Equal(t, []string{"a", "b", "c"}, srcSlice)
	assert.Equal(t, []string{"x", "b", "c"}, tgtSlice)

	// Rename with column dropped from new table (c dropped)
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b"}
	renames = map[string]string{"a": "x"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	assert.Equal(t, []string{"a", "b"}, srcSlice)
	assert.Equal(t, []string{"x", "b"}, tgtSlice)

	// Dangerous pattern: RENAME COLUMN c1 TO n1, ADD COLUMN c1 varchar(100)
	// The old name "c1" now exists in BOTH the source table AND the target table
	// (as a new column). The rename must take priority: source c1 → target n1.
	// The new c1 in the target must NOT get matched to the old c1 in the source.
	t1.NonGeneratedColumns = []string{"id", "c1"}
	t1new.NonGeneratedColumns = []string{"id", "n1", "c1"} // n1 is renamed from c1; c1 is brand new
	renames = map[string]string{"c1": "n1"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	// Source c1 must map to target n1 (via rename), NOT to target c1 (identity match).
	// The new target c1 has no source counterpart — it should get its DEFAULT value.
	assert.Equal(t, []string{"id", "c1"}, srcSlice)
	assert.Equal(t, []string{"id", "n1"}, tgtSlice)

	// Reverse dangerous pattern: RENAME COLUMN a TO c (where c already existed)
	// Source: [id, a, c], Target: [id, c] where a→c is the rename
	// source.a → target.c (rename). source.c must NOT identity-match target.c
	// because target.c is already claimed by the rename from source.a.
	t1.NonGeneratedColumns = []string{"id", "a", "c"}
	t1new.NonGeneratedColumns = []string{"id", "c"} // c is renamed from a; old c is dropped
	renames = map[string]string{"a": "c"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	// source.a → target.c (rename), source.c is excluded (target.c is claimed)
	assert.Equal(t, []string{"id", "a"}, srcSlice)
	assert.Equal(t, []string{"id", "c"}, tgtSlice)
}

func TestColumnMappingTargetNil(t *testing.T) {
	// When target is nil, source is used as target
	t1 := NewTableInfo(nil, "test", "t1")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}

	m := NewColumnMapping(t1, nil, nil)
	src, tgt := m.Columns()
	assert.Equal(t, "`a`, `b`, `c`", src)
	assert.Equal(t, "`a`, `b`, `c`", tgt)
	assert.Equal(t, t1, m.TargetTable())
}
