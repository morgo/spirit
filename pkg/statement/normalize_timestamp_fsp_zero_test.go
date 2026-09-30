package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampFspZero(t *testing.T) {
	tests := []struct {
		column       string
		wantDefault  *string
		wantOnUpdate *string
	}{
		{"a datetime(0) DEFAULT CURRENT_TIMESTAMP(0)", new("current_timestamp"), nil},
		{"a timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)", new("current_timestamp"), new("current_timestamp")},
		{"a datetime DEFAULT NOW(0) ON UPDATE LOCALTIMESTAMP(0)", new("current_timestamp"), new("current_timestamp")},
		{"a datetime DEFAULT LOCALTIME(0)", new("current_timestamp"), nil},
		{"a datetime DEFAULT (CURRENT_TIMESTAMP(0))", new("now()"), nil},
		{"a datetime DEFAULT (NOW(0) + INTERVAL 1 DAY)", new("date_add(now(), interval 1 day)"), nil},
		{"a time DEFAULT (CURTIME(0))", new("curtime()"), nil},
		{"a time DEFAULT (CURRENT_TIME(0))", new("curtime()"), nil},
		{"a datetime DEFAULT (UTC_TIMESTAMP(0))", new("utc_timestamp()"), nil},
		{"a time DEFAULT (UTC_TIME(0))", new("utc_time()"), nil},
		{"a datetime DEFAULT (SYSDATE(0))", new("sysdate()"), nil},
		{"a bigint DEFAULT (UNIX_TIMESTAMP(NOW(0)))", new("unix_timestamp(now())"), nil},
		// A non-zero fsp is kept.
		{"a datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)", new("current_timestamp(3)"), new("current_timestamp(3)")},
		{"a datetime(3) DEFAULT (NOW(3))", new("now(3)"), nil},
		// Only the fsp functions lose a 0 argument.
		{"a int DEFAULT (ABS(0))", new("abs(0)"), nil},
		// A string default is a value, not a call.
		{"a varchar(20) DEFAULT 'now(0)'", new("now(0)"), nil},
		{"a varchar(20) DEFAULT ('now(0)')", new("now(0)"), nil},
	}
	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + tc.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantDefault, ct.Columns[0].Default)
			assert.Equal(t, tc.wantOnUpdate, ct.Columns[0].OnUpdate)
		})
	}
}

// TestTimestampFspZeroIsIdempotent runs the rule a second time over a table it
// has already normalized and checks nothing changes.
func TestTimestampFspZeroIsIdempotent(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (" +
		"a datetime DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0), " +
		"b datetime DEFAULT (NOW(0) + INTERVAL 1 DAY))")
	require.NoError(t, err)
	before := []*string{ct.Columns[0].Default, ct.Columns[0].OnUpdate, ct.Columns[1].Default}
	values := make([]string, len(before))
	for i, v := range before {
		values[i] = *v
	}
	ct = timestampFspZeroNormalizer{}.Normalize(ct)
	assert.Equal(t, values[0], *ct.Columns[0].Default)
	assert.Equal(t, values[1], *ct.Columns[0].OnUpdate)
	assert.Equal(t, values[2], *ct.Columns[1].Default)
}

// TestTimestampFspZeroConverges checks that each declared form diffs clean
// against the definition MySQL reports for it, in both directions.
func TestTimestampFspZeroConverges(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (" +
		"a datetime(0) DEFAULT CURRENT_TIMESTAMP(0), " +
		"b timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0), " +
		"c datetime DEFAULT NOW(0) ON UPDATE LOCALTIMESTAMP(0), " +
		"d datetime DEFAULT (CURRENT_TIMESTAMP(0)), " +
		"e datetime DEFAULT (NOW(0) + INTERVAL 1 DAY), " +
		"f time DEFAULT (CURTIME(0)))")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE `t` (" +
		"`a` datetime DEFAULT CURRENT_TIMESTAMP, " +
		"`b` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
		"`c` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
		"`d` datetime DEFAULT (now()), " +
		"`e` datetime DEFAULT ((now() + interval 1 day)), " +
		"`f` time DEFAULT (curtime()))")
	require.NoError(t, err)

	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)

	stmts, err = declared.Diff(live, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)
}
