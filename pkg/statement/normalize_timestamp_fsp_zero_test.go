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
// against the definition MySQL 8.0.43 reports for it, in both directions and
// under both registration orders of the normalizers.
func TestTimestampFspZeroConverges(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"literal default", "(a datetime(0) DEFAULT CURRENT_TIMESTAMP(0))",
			"(`a` datetime DEFAULT CURRENT_TIMESTAMP)"},
		{"literal default and on update", "(a timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0))",
			"(`a` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"},
		{"now and localtimestamp", "(a datetime DEFAULT NOW(0) ON UPDATE LOCALTIMESTAMP(0))",
			"(`a` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"},
		{"expression current_timestamp", "(a datetime DEFAULT (CURRENT_TIMESTAMP(0)))",
			"(`a` datetime DEFAULT (now()))"},
		{"expression localtime", "(a datetime DEFAULT (LOCALTIME(0)))",
			"(`a` datetime DEFAULT (now()))"},
		{"expression localtimestamp", "(a datetime DEFAULT (LOCALTIMESTAMP(0)))",
			"(`a` datetime DEFAULT (now()))"},
		{"expression current_time", "(a time DEFAULT (CURRENT_TIME(0)))",
			"(`a` time DEFAULT (curtime()))"},
		{"expression interval", "(a datetime DEFAULT (NOW(0) + INTERVAL 1 DAY))",
			"(`a` datetime DEFAULT ((now() + interval 1 day)))"},
		{"expression curtime", "(a time DEFAULT (CURTIME(0)))",
			"(`a` time DEFAULT (curtime()))"},
		{"zero next to non-zero fsp", "(a datetime(3) DEFAULT (IFNULL(NOW(3), NOW(0))))",
			"(`a` datetime(3) DEFAULT (ifnull(now(3),now())))"},
		{"zero next to no fsp", "(a datetime DEFAULT (COALESCE(NOW(), NOW(0))))",
			"(`a` datetime DEFAULT (coalesce(now(),now())))"},
	})
}

// TestTimestampFspZeroMixedCalls: an expression holding a zero fsp alongside
// a call with a non-zero fsp, or with none, drops only the zero one. MySQL
// 8.0.43 stores these as (ifnull(now(3),now())) and (coalesce(now(),now())).
func TestTimestampFspZeroMixedCalls(t *testing.T) {
	for column, want := range map[string]string{
		"a datetime(3) DEFAULT (IFNULL(NOW(3), NOW(0)))": "ifnull(now(3), now())",
		"a datetime DEFAULT (COALESCE(NOW(), NOW(0)))":   "coalesce(now(), now())",
	} {
		t.Run(column, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + column + ")")
			require.NoError(t, err)
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, want, *ct.Columns[0].Default)
		})
	}
}

// TestTimestampFspZeroBeforeAliases runs the rule on its own, over spellings
// functionAliasNormalizer would otherwise have renamed first, so the rule
// holds whichever order the two run in.
func TestTimestampFspZeroBeforeAliases(t *testing.T) {
	for text, want := range map[string]string{
		"localtime(0)":      "localtime()",
		"localtimestamp(0)": "localtimestamp()",
		"current_time(0)":   "current_time()",
	} {
		t.Run(text, func(t *testing.T) {
			def := text
			ct := &CreateTable{Columns: []Column{{Name: "a", Type: "datetime", Default: &def, DefaultIsExpr: true}}}
			ct = timestampFspZeroNormalizer{}.Normalize(ct)
			assert.Equal(t, want, *ct.Columns[0].Default)
		})
	}
}
