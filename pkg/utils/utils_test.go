package utils

import (
	"os"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestIntersectColumns(t *testing.T) {
	t1 := table.NewTableInfo(nil, "test", "t1")
	t1new := table.NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	str := IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `b`, `c`", str)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	str = IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `c`", str)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	str = IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `c`", str)
}

func TestGetIntersectingColumns(t *testing.T) {
	t1 := table.NewTableInfo(nil, "test", "t1")
	t1new := table.NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	cols := IntersectNonGeneratedColumnsAsSlice(t1, t1new)
	assert.Equal(t, []string{"a", "b", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	cols = IntersectNonGeneratedColumnsAsSlice(t1, t1new)
	assert.Equal(t, []string{"a", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	cols = IntersectNonGeneratedColumnsAsSlice(t1, t1new)
	assert.Equal(t, []string{"a", "c"}, cols)
}

func TestHashAndUnhashKey(t *testing.T) {
	// This func helps put composite keys in a map.
	key := []any{"1234", "ACDC", "12"}
	hashed := HashKey(key)
	assert.Equal(t, "1234-#-ACDC-#-12", hashed)
	unhashed := UnhashKeyToString(hashed)
	// unhashed returns as a string, not the original any
	assert.Equal(t, "('1234','ACDC','12')", unhashed)

	// This also works on single keys.
	key = []any{"1234"}
	hashed = HashKey(key)
	assert.Equal(t, "1234", hashed)
	unhashed = UnhashKeyToString(hashed)
	assert.Equal(t, "'1234'", unhashed)
}
