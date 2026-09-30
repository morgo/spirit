package statement

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestEnumSetMemberSpaces checks the members each enum or set column is stored
// with. Each expectation is the SHOW CREATE TABLE reading of the declared
// column on MySQL 8.0.28, 8.4 and 9.7, which agree. The rules that write a
// column's charset and collation must not change the answer, so each case is
// parsed with the registry in its normal order and reversed.
func TestEnumSetMemberSpaces(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{"CREATE TABLE t (b enum('a','b '))", []string{"a", "b"}},
		{"CREATE TABLE t (b enum('a','b   ')) DEFAULT CHARSET=utf8mb4", []string{"a", "b"}},
		{"CREATE TABLE t (b enum('a','b ') BINARY) DEFAULT CHARSET=utf8mb4", []string{"a", "b"}},
		{"CREATE TABLE t (b enum('a',x'6220')) DEFAULT CHARSET=utf8mb4", []string{"a", "b"}},
		{"CREATE TABLE t (b enum('a','b ','c\t') CHARACTER SET latin1)", []string{"a", "b", "c\t"}},
		{"CREATE TABLE t (b enum('a','b ') CHARACTER SET utf8mb4) DEFAULT CHARSET=binary", []string{"a", "b"}},
		{"CREATE TABLE t (b enum('a','b ') COLLATE utf8mb4_bin) DEFAULT CHARSET=binary", []string{"a", "b"}},
		{"CREATE TABLE t (b enum('a','b ') CHARACTER SET binary) DEFAULT CHARSET=utf8mb4", []string{"a", "b "}},
		{"CREATE TABLE t (b enum('a','b  ') CHARACTER SET binary)", []string{"a", "b  "}},
		{"CREATE TABLE t (b enum('a','b ') CHARACTER SET BINARY)", []string{"a", "b "}},
		{"CREATE TABLE t (b enum('a','b ') COLLATE binary) DEFAULT CHARSET=utf8mb4", []string{"a", "b "}},
		{"CREATE TABLE t (b enum('a','b ')) DEFAULT CHARSET=binary", []string{"a", "b "}},
		{"CREATE TABLE t (b enum('a','b ')) DEFAULT COLLATE=binary", []string{"a", "b "}},
		{"CREATE TABLE t (b enum('a','b ')) DEFAULT CHARSET=binary COLLATE=binary", []string{"a", "b "}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				ct, err := ParseCreateTable(tc.sql)
				require.NoError(t, err)
				require.Equal(t, tc.want, ct.Columns[0].EnumValues)
			}
		})
	}

	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{"CREATE TABLE t (b set('a','b '))", []string{"a", "b"}},
		{"CREATE TABLE t (b set('a','b ') CHARACTER SET binary)", []string{"a", "b "}},
		{"CREATE TABLE t (b set('a','b ') COLLATE binary)", []string{"a", "b "}},
		{"CREATE TABLE t (b set('a','b ')) DEFAULT CHARSET=binary", []string{"a", "b "}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				ct, err := ParseCreateTable(tc.sql)
				require.NoError(t, err)
				require.Equal(t, tc.want, ct.Columns[0].SetValues)
			}
		})
	}
}

// TestEnumSetMemberSpacesLeavesRaw: the members are shared with the parsed
// AST, which a normalizer must not modify.
func TestEnumSetMemberSpacesLeavesRaw(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (b enum('a','b '))")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, ct.Columns[0].EnumValues)
	require.Equal(t, []string{"a", "b "}, ct.Columns[0].Raw.Tp.GetElems())
}

// TestEnumSetMemberSpacesConverge: each declared column diffs clean against
// its SHOW CREATE TABLE reading, in both directions.
func TestEnumSetMemberSpacesConverge(t *testing.T) {
	for _, tc := range []struct{ declared, live string }{
		{
			"CREATE TABLE t (b enum('a','b ') DEFAULT 'b') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"CREATE TABLE t (`b` enum('a','b') DEFAULT 'b') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			"CREATE TABLE t (b enum('a','b ') CHARACTER SET binary DEFAULT 'b ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"CREATE TABLE t (`b` enum('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'b ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			"CREATE TABLE t (b enum('a','b ') COLLATE binary DEFAULT 'b ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"CREATE TABLE t (`b` enum('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'b ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			"CREATE TABLE t (b enum('a','b ') CHARACTER SET binary DEFAULT 'b ') DEFAULT CHARSET=binary",
			"CREATE TABLE t (`b` enum('a','b ') DEFAULT 'b ') DEFAULT CHARSET=binary",
		},
		{
			"CREATE TABLE t (b enum('a','b ') DEFAULT 'b ') DEFAULT COLLATE=binary",
			"CREATE TABLE t (`b` enum('a','b ') DEFAULT 'b ') DEFAULT CHARSET=binary",
		},
		{
			"CREATE TABLE t (b set('a','b ') CHARACTER SET binary DEFAULT 'a,b ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"CREATE TABLE t (`b` set('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'a,b ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
	} {
		t.Run(tc.declared, func(t *testing.T) {
			declared, err := ParseCreateTable(tc.declared)
			require.NoError(t, err)
			live, err := ParseCreateTable(tc.live)
			require.NoError(t, err)
			stmts, err := live.Diff(declared, nil)
			require.NoError(t, err)
			require.Nil(t, stmts)
			stmts, err = declared.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts)
		})
	}
}

// TestEnumSetMemberSpacesModifyKeepsBinaryMember: a MODIFY COLUMN emitted for
// another change to a binary enum column must write the member as MySQL stores
// it. Written as enum('a','b') it would change the member, and MySQL would
// reject the DEFAULT 'b ' that no longer names one (error 1067).
func TestEnumSetMemberSpacesModifyKeepsBinaryMember(t *testing.T) {
	live, err := ParseCreateTable("CREATE TABLE t (`b` enum('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'b ') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	declared, err := ParseCreateTable("CREATE TABLE t (b enum('a','b ') CHARACTER SET binary DEFAULT 'b ' COMMENT 'x') DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "enum('a','b ')")
	require.Contains(t, stmts[0].Statement, "DEFAULT 'b '")
}

// TestAlterKeepsEnumSetMembers: the ALTER Spirit runs is restored from the
// parsed statement, so a member must reach MySQL as written. Trimmed by the
// parser, a binary member 'b ' would be created as 'b'.
func TestAlterKeepsEnumSetMembers(t *testing.T) {
	stmt := MustNew("ALTER TABLE t ADD COLUMN b enum('a','b ') COLLATE binary DEFAULT 'b ', MODIFY COLUMN c set('x ') COLLATE binary")[0]
	require.Equal(t, "ADD COLUMN `b` ENUM('a','b ') COLLATE binary DEFAULT _UTF8MB4'b ', MODIFY COLUMN `c` SET('x ') COLLATE binary", stmt.Alter)
}

// storedMembersCases are ALTERs that redeclare the enum column c, with the
// members MySQL stores for it. Each is executed by
// TestStoredEnumSetMembersMatchesMySQL, so want is MySQL's answer (8.0.28, 8.4
// and 9.7 agree) rather than a restatement of the rules under test.
var storedMembersCases = []struct {
	name   string
	create string // %s is the table name
	alter  string // %s is the table name
	want   []string
}{
	{
		name:   "inherits a utf8mb4 default",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=utf8mb4",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b ')",
		want:   []string{"a", "b"},
	},
	{
		name:   "inherits a binary default",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=binary",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b ')",
		want:   []string{"a", "b "},
	},
	{
		name:   "declares the binary charset",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=utf8mb4",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b ') CHARACTER SET binary",
		want:   []string{"a", "b "},
	},
	{
		name:   "declares the binary collation",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=utf8mb4",
		alter:  "ALTER TABLE %s CHANGE c c enum('a','b ') COLLATE binary",
		want:   []string{"a", "b "},
	},
	{
		name:   "a binary column redeclared without a charset takes the table default",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a') CHARACTER SET binary) DEFAULT CHARSET=utf8mb4",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b ')",
		want:   []string{"a", "b"},
	},
	{
		name:   "the statement changes the table default to binary",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=utf8mb4",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b '), DEFAULT CHARSET=binary",
		want:   []string{"a", "b "},
	},
	{
		name:   "CONVERT TO binary",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=utf8mb4",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b ') CHARACTER SET utf8mb4, CONVERT TO CHARACTER SET binary",
		want:   []string{"a", "b "},
	},
	{
		name:   "CONVERT TO utf8mb4 leaves a declared binary charset",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=utf8mb4",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b ') CHARACTER SET binary, CONVERT TO CHARACTER SET utf8mb4",
		want:   []string{"a", "b "},
	},
	{
		name:   "CONVERT TO utf8mb4 from a binary default",
		create: "CREATE TABLE %s (id int PRIMARY KEY, c enum('a')) DEFAULT CHARSET=binary",
		alter:  "ALTER TABLE %s MODIFY c enum('a','b '), CONVERT TO CHARACTER SET utf8mb4",
		want:   []string{"a", "b"},
	},
}

// TestStoredEnumSetMembersMatchesMySQL predicts each case's members from the
// live table default, then applies the ALTER and compares the prediction with
// the column type MySQL reports.
func TestStoredEnumSetMembersMatchesMySQL(t *testing.T) {
	for _, tc := range storedMembersCases {
		t.Run(tc.name, func(t *testing.T) {
			const name = "storedmembers"
			tt := testutils.NewTestTable(t, name, strings.ReplaceAll(tc.create, "%s", name))

			current, err := ParseCreateTable(showCreateTable(t, tt.DB, name))
			require.NoError(t, err)
			info, err := current.ToTableInfo("test")
			require.NoError(t, err)
			stmt := MustNew(strings.ReplaceAll(tc.alter, "%s", name))[0]
			alter, ok := stmt.AsAlterTable()
			require.True(t, ok)
			members, determined, err := stmt.StoredEnumSetMembers(alter.Specs[0].NewColumns[0], defaultOf(info))
			require.NoError(t, err)
			require.True(t, determined)
			require.Equal(t, tc.want, members)

			_, err = tt.DB.ExecContext(t.Context(), stmt.Statement)
			require.NoError(t, err)
			var columnType string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(),
				"SELECT column_type FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name='c'",
				name).Scan(&columnType))
			require.Equal(t, "enum('"+strings.Join(tc.want, "','")+"')", columnType,
				"the predicted members must be the ones MySQL stores")
		})
	}
}

// TestStoredEnumSetMembersUndetermined: a member that ends in a space needs the
// column's charset, and one that inherits a default the inputs do not carry
// must be reported as undetermined rather than guessed at.
func TestStoredEnumSetMembersUndetermined(t *testing.T) {
	utf8mb4 := CharsetCollation{Collation: "utf8mb4_0900_ai_ci"}
	for _, tc := range []struct {
		name           string
		alter          string
		tableDefault   CharsetCollation
		want           []string
		wantDetermined bool
	}{
		{"no member ends in a space", "ALTER TABLE t MODIFY c enum('a','b')", CharsetCollation{}, []string{"a", "b"}, true},
		{"a set member", "ALTER TABLE t MODIFY c set('a ','b')", utf8mb4, []string{"a", "b"}, true},
		{"a set member under binary", "ALTER TABLE t MODIFY c set('a ','b') CHARACTER SET binary", CharsetCollation{}, []string{"a ", "b"}, true},
		{"a declared non-binary charset", "ALTER TABLE t MODIFY c enum('a','b ') CHARACTER SET latin1", CharsetCollation{}, []string{"a", "b"}, true},
		{"the statement sets the default", "ALTER TABLE t MODIFY c enum('a','b '), DEFAULT CHARSET=binary", CharsetCollation{}, []string{"a", "b "}, true},
		{"an unknown table default", "ALTER TABLE t MODIFY c enum('a','b ')", CharsetCollation{}, nil, false},
		{"CONVERT TO the schema's default", "ALTER TABLE t MODIFY c enum('a','b ') CHARACTER SET utf8mb4, CONVERT TO CHARACTER SET DEFAULT", utf8mb4, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := MustNew(tc.alter)[0]
			alter, ok := stmt.AsAlterTable()
			require.True(t, ok)
			members, determined, err := stmt.StoredEnumSetMembers(alter.Specs[0].NewColumns[0], tc.tableDefault)
			require.NoError(t, err)
			require.Equal(t, tc.wantDetermined, determined)
			require.Equal(t, tc.want, members)
		})
	}
}
