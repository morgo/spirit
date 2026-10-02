package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKey(t *testing.T) {
	keyedOnID := &table.TableInfo{KeyColumns: []string{"id"}}
	keyedOnAB := &table.TableInfo{KeyColumns: []string{"a", "b"}}
	keyedOnName := &table.TableInfo{KeyColumns: []string{"name"}}
	tests := []struct {
		name    string
		stmt    string
		table   *table.TableInfo
		scope   ScopeFlag
		wantErr string
	}{
		{
			name:  "no primary key change",
			stmt:  "ALTER TABLE t1 ADD INDEX (anothercol)",
			table: keyedOnID,
		},
		{
			name:    "dropped and not added back",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY",
			table:   keyedOnID,
			wantErr: "dropping primary key is not supported",
		},
		{
			name:    "added back on another column",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)",
			table:   keyedOnID,
			wantErr: "the primary key added back is on (anothercol), the table's is on (id)",
		},
		{
			// The plan Diff emits for a table KEY_BLOCK_SIZE change.
			name:  "added back on the same column",
			stmt:  "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (id), KEY_BLOCK_SIZE=4",
			table: keyedOnID,
		},
		{
			name:  "column names compare case-insensitively",
			stmt:  "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (ID)",
			table: keyedOnID,
		},
		{
			name:  "composite key added back in order",
			stmt:  "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)",
			table: keyedOnAB,
		},
		{
			name:    "composite key added back in another order",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (b, a)",
			table:   keyedOnAB,
			wantErr: "the primary key added back is on (b, a), the table's is on (a, b)",
		},
		{
			name:    "composite key added back on fewer columns",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (a)",
			table:   keyedOnAB,
			wantErr: "the primary key added back is on (a), the table's is on (a, b)",
		},
		{
			name:    "added back with a prefix length",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (name(10))",
			table:   keyedOnName,
			wantErr: "dropping primary key is not supported",
		},
		{
			name:    "added back twice",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (id), ADD PRIMARY KEY (id)",
			table:   keyedOnID,
			wantErr: "dropping primary key is not supported",
		},
		{
			name:    "table has no primary key",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (id)",
			table:   &table.TableInfo{},
			wantErr: "the primary key added back is on (id), the table's is on ()",
		},
		{
			// Statement scope without the table: the statement alone decides.
			name:  "no table at statement scope",
			stmt:  "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)",
			scope: ScopeStatement,
		},
		{
			name:    "no table at statement scope and not added back",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY",
			scope:   ScopeStatement,
			wantErr: "dropping primary key is not supported",
		},
		{
			name:    "no table outside statement scope",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (id)",
			scope:   ScopePreflight,
			wantErr: "the table's primary key is not available",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := Resources{
				Statement: statement.MustNew(tc.stmt)[0],
				Table:     tc.table,
				scope:     tc.scope,
			}
			err := primaryKeyCheck(t.Context(), r, slog.Default())
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestKeyBlockSizeChangePlanPassesPrimaryKeyCheck runs the plan Diff emits for
// a table KEY_BLOCK_SIZE change through Spirit's own checks, as a planning
// tool would (StatementRefusal) and as the runner does with the table.
func TestKeyBlockSizeChangePlanPassesPrimaryKeyCheck(t *testing.T) {
	liveSQL := "CREATE TABLE `t` (`id` int NOT NULL, `c` int DEFAULT NULL, PRIMARY KEY (`id`), KEY `k` (`c`)) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
	live, err := statement.ParseCreateTable(liveSQL)
	require.NoError(t, err)
	want, err := statement.ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4")
	require.NoError(t, err)
	opts := statement.NewDiffOptions()
	opts.IgnoreRowFormat = false
	stmts, err := live.Diff(want, opts)
	require.NoError(t, err)
	require.NotEmpty(t, stmts)
	require.Contains(t, stmts[0].Statement, "DROP PRIMARY KEY, ADD PRIMARY KEY (`id`)")
	for _, s := range stmts {
		reason, refused, err := StatementRefusal(t.Context(), s.Statement, liveSQL, nil)
		require.NoError(t, err, s.Statement)
		require.False(t, refused, "%s: %s", s.Statement, reason)
		require.NoError(t, primaryKeyCheck(t.Context(), Resources{Statement: s, Table: &table.TableInfo{KeyColumns: []string{"id"}}}, slog.Default()), s.Statement)
	}
}
