package check

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestIsPrefix(t *testing.T) {
	// Identical
	require.True(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "b", "c"}))

	// Append at end (safe)
	require.True(t, isPrefix([]string{"a", "b"}, []string{"a", "b", "c"}))

	// Empty old (always a prefix)
	require.True(t, isPrefix([]string{}, []string{"a", "b"}))

	// Reorder
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"c", "a", "b"}))

	// Insert in middle
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "x", "b", "c"}))

	// Remove value
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "b"}))

	// Remove from middle
	require.False(t, isPrefix([]string{"a", "b", "c"}, []string{"a", "c"}))

	// Completely different
	require.False(t, isPrefix([]string{"a", "b"}, []string{"x", "y", "z"}))
}

// TestEnumSetReorderTrailingSpaces: the reorder checks compare the members MySQL
// will store, not the members as written. MySQL strips each member's trailing
// spaces unless the column's charset is binary, where 'b ' and 'b' are
// different members, so the checks must neither refuse an append because the
// written form differs from the stored one nor pass a reorder that only the
// stripping hides.
func TestEnumSetReorderTrailingSpaces(t *testing.T) {
	type tc struct {
		name, alter, wantErr string
	}
	run := func(t *testing.T, tableName, createTable string, check func(context.Context, Resources, *slog.Logger) error, cases []tc) {
		tt := testutils.NewTestTable(t, tableName, createTable)
		tbl := table.NewTableInfo(tt.DB, "test", tableName)
		require.NoError(t, tbl.SetInfo(t.Context()))
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				// SetInfo does not load the table default, so a column that
				// inherits it is resolved through the connection.
				r := Resources{DB: tt.DB, Table: tbl, Statement: statement.MustNew(c.alter)[0]}
				err := check(t.Context(), r, slog.Default())
				if c.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, c.wantErr)
				}
			})
		}
	}

	t.Run("binary enum", func(t *testing.T) {
		run(t, "enumsp_bin", "CREATE TABLE enumsp_bin (id int PRIMARY KEY, c enum('a','b ') CHARACTER SET binary) DEFAULT CHARSET=utf8mb4", enumReorderCheck, []tc{
			{"append", "ALTER TABLE enumsp_bin MODIFY c enum('a','b ','c') CHARACTER SET binary", ""},
			{"append under COLLATE binary", "ALTER TABLE enumsp_bin MODIFY c enum('a','b ','c') COLLATE binary", ""},
			{"a new member before a kept one", "ALTER TABLE enumsp_bin MODIFY c enum('a','b','b ') CHARACTER SET binary", "unsafe ENUM value reorder"},
			{"reorder", "ALTER TABLE enumsp_bin MODIFY c enum('b ','a') CHARACTER SET binary", "unsafe ENUM value reorder"},
		})
	})
	t.Run("enum inheriting a binary table default", func(t *testing.T) {
		run(t, "enumsp_bindef", "CREATE TABLE enumsp_bindef (id int PRIMARY KEY, c enum('a','b ')) DEFAULT CHARSET=binary", enumReorderCheck, []tc{
			{"append", "ALTER TABLE enumsp_bindef MODIFY c enum('a','b ','c')", ""},
			{"reorder", "ALTER TABLE enumsp_bindef MODIFY c enum('b ','a')", "unsafe ENUM value reorder"},
		})
	})
	t.Run("utf8mb4 enum", func(t *testing.T) {
		run(t, "enumsp_utf8", "CREATE TABLE enumsp_utf8 (id int PRIMARY KEY, c enum('a','b')) DEFAULT CHARSET=utf8mb4", enumReorderCheck, []tc{
			{"append with a member written with spaces", "ALTER TABLE enumsp_utf8 MODIFY c enum('a','b  ','c')", ""},
			{"reorder with a member written with spaces", "ALTER TABLE enumsp_utf8 MODIFY c enum('b ','a')", "unsafe ENUM value reorder"},
		})
	})
	t.Run("binary set", func(t *testing.T) {
		run(t, "setsp_bin", "CREATE TABLE setsp_bin (id int PRIMARY KEY, c set('a','b ') CHARACTER SET binary) DEFAULT CHARSET=utf8mb4", setReorderCheck, []tc{
			{"append", "ALTER TABLE setsp_bin MODIFY c set('a','b ','c') CHARACTER SET binary", ""},
			{"a stripped member replaces a kept one", "ALTER TABLE setsp_bin MODIFY c set('a','b','c') CHARACTER SET binary", "unsafe SET value reorder"},
		})
	})
	t.Run("utf8mb4 set", func(t *testing.T) {
		run(t, "setsp_utf8", "CREATE TABLE setsp_utf8 (id int PRIMARY KEY, c set('a','b')) DEFAULT CHARSET=utf8mb4", setReorderCheck, []tc{
			{"append with a member written with spaces", "ALTER TABLE setsp_utf8 MODIFY c set('a ','b','c')", ""},
		})
	})
}

// TestEnumSetReorderTrailingSpacesUndetermined: without a connection or a table
// default, a member written with trailing spaces that inherits the default
// cannot be resolved, and the check must say so rather than guess.
func TestEnumSetReorderTrailingSpacesUndetermined(t *testing.T) {
	tt := testutils.NewTestTable(t, "enumsp_undet", "CREATE TABLE enumsp_undet (id int PRIMARY KEY, c enum('a','b')) DEFAULT CHARSET=utf8mb4")
	tbl := table.NewTableInfo(tt.DB, "test", "enumsp_undet")
	require.NoError(t, tbl.SetInfo(t.Context()))

	r := Resources{Table: tbl, Statement: statement.MustNew("ALTER TABLE enumsp_undet MODIFY c enum('a','b ','c')")[0]}
	err := enumReorderCheck(t.Context(), r, slog.Default())
	var classification *classificationError
	require.ErrorAs(t, err, &classification)
	require.ErrorContains(t, err, "depends on the table's default charset")

	// Written without trailing spaces, the members need no charset.
	r.Statement = statement.MustNew("ALTER TABLE enumsp_undet MODIFY c enum('a','b','c')")[0]
	require.NoError(t, enumReorderCheck(t.Context(), r, slog.Default()))
}
