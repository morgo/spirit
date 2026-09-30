package testutils

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareMySQLVersions(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"8.0.28", "8.0.33", -1},
		{"8.0.33", "8.0.33", 0},
		{"8.0.45", "8.0.33", 1},
		{"8.0.100", "8.0.33", 1}, // numeric, not lexical
		{"8.4.6", "8.0.33", 1},
		{"9.7.0", "8.0.33", 1},
		{"8.0.28-log", "8.0.33", -1},
		{"8.0.33-log", "8.0.33", 0},
		{"8.0", "8.0.0", 0},
	} {
		assert.Equal(t, tc.want, compareMySQLVersions(tc.a, tc.b), "%s vs %s", tc.a, tc.b)
	}
}

func TestUniqueDatabaseName(t *testing.T) {
	// The same long test name in two packages (two processes) must not
	// collide: the pid has to survive truncation.
	long := "TestCutOverChecksUnderLockRetryPolicy/transient_error_is_retried"
	a := uniqueDatabaseName(long, 12345, 1)
	b := uniqueDatabaseName(long, 67890, 1)
	assert.NotEqual(t, a, b)
	assert.LessOrEqual(t, len(a), 64)
	assert.True(t, strings.HasSuffix(a, "_12345_1"), a)
	assert.True(t, strings.HasSuffix(b, "_67890_1"), b)

	// Within one process, the counter has to survive truncation.
	assert.NotEqual(t, uniqueDatabaseName(long, 12345, 1), uniqueDatabaseName(long, 12345, 2))

	// Two long names that differ only past the cut differ in the hash.
	prefix := "Test" + strings.Repeat("x", 80)
	c := uniqueDatabaseName(prefix+"/one", 12345, 1)
	d := uniqueDatabaseName(prefix+"/two", 12345, 1)
	assert.NotEqual(t, c, d)
	assert.Len(t, c, 64)
	assert.Len(t, d, 64)

	// The largest pid and counter still fit.
	assert.LessOrEqual(t, len(uniqueDatabaseName(long, math.MaxInt32, math.MaxUint64)), 64)

	// Short names are kept whole; characters that would need quoting are
	// replaced.
	e := uniqueDatabaseName("TestFoo/sub-test#01", 12345, 7)
	assert.Regexp(t, `^t_testfoo_sub_test_01_[0-9a-f]{8}_12345_7$`, e)
}
