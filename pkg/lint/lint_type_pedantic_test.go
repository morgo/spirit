package lint

import (
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// parseTables is a helper that parses one or more CREATE TABLE statements.
func parseTables(t *testing.T, sqls ...string) []*statement.CreateTable {
	t.Helper()
	out := make([]*statement.CreateTable, 0, len(sqls))
	for _, sql := range sqls {
		ct, err := statement.ParseCreateTable(sql)
		require.NoError(t, err)
		out = append(out, ct)
	}
	return out
}

func newTypePedantic(t *testing.T) *TypePedanticLinter {
	t.Helper()
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(l.DefaultConfig()))
	return l
}

func TestTypePedantic_NoViolations_ConsistentTypes(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255))`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL)`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Empty(t, violations)
}

func TestTypePedantic_SameName_TypeMismatch(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)

	sameName := filterRule(violations, "same_name")
	require.Len(t, sameName, 1, "only the minority customer_id should be flagged")
	v := sameName[0]
	require.Equal(t, "type_pedantic", v.Linter.Name())
	require.Equal(t, SeverityWarning, v.Severity)
	require.Equal(t, "returns", v.Location.Table)
	require.NotNil(t, v.Location.Column)
	require.Equal(t, "customer_id", *v.Location.Column)
	require.Contains(t, v.Message, "int(11)")
	require.Contains(t, v.Message, "bigint(20) unsigned")
	require.Equal(t, "int(11)", v.Context["current_type"])
	require.Equal(t, "bigint(20) unsigned", v.Context["expected_type"])
}

func TestTypePedantic_SameName_IgnoresIDByDefault(t *testing.T) {
	// Two tables with intentionally different `id` types — ignored by default.
	tables := parseTables(t,
		`CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE sessions (id BINARY(16) PRIMARY KEY)`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Empty(t, filterRule(violations, "same_name"))
}

func TestTypePedantic_SameName_ConfigurableIgnore(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, status VARCHAR(20))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, status VARCHAR(50))`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "true",
		"checkInferredFK":  "false",
		"ignoreColumns":    "id, status",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_SameName_DisabledViaConfig(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, val INT)`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, val BIGINT)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "false",
		"checkInferredFK":  "true",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_SameName_SignednessMismatch(t *testing.T) {
	// Two tables agree on INT UNSIGNED, one dissents with signed INT — clear majority.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, qty INT UNSIGNED NOT NULL, INDEX idx_qty (qty))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, qty INT UNSIGNED NOT NULL, INDEX idx_qty (qty))`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, qty INT NOT NULL, INDEX idx_qty (qty))`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "c", violations[0].Location.Table)
	require.Contains(t, violations[0].Message, "int(11)")
	require.Contains(t, violations[0].Message, "unsigned")
}

func TestTypePedantic_InferredFK_Mismatch(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY, name VARCHAR(100))`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT UNSIGNED NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	v := violations[0]
	require.Equal(t, SeverityError, v.Severity)
	require.Equal(t, "orders", v.Location.Table)
	require.Equal(t, "customer_id", *v.Location.Column)
	require.Contains(t, v.Message, "customers")
	require.Equal(t, "customers", v.Context["referenced_table"])
}

func TestTypePedantic_InferredFK_Match(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_PluralizedTable(t *testing.T) {
	// orders.user_id should resolve to users.id
	tables := parseTables(t,
		`CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, user_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "users", violations[0].Context["referenced_table"])
}

func TestTypePedantic_InferredFK_IESPluralization(t *testing.T) {
	// activities.id referenced by activity_id
	tables := parseTables(t,
		`CREATE TABLE activities (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE logs (id BIGINT UNSIGNED PRIMARY KEY, activity_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "activities", violations[0].Context["referenced_table"])
}

func TestTypePedantic_InferredFK_NoMatchingTable(t *testing.T) {
	// session_id but no `session` or `sessions` table - silently skip.
	tables := parseTables(t,
		`CREATE TABLE events (id BIGINT UNSIGNED PRIMARY KEY, session_id VARCHAR(64) NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_TargetMissingIDColumn(t *testing.T) {
	// Target table has a composite PK with no `id` column - skip silently.
	tables := parseTables(t,
		`CREATE TABLE products (sku VARCHAR(20) NOT NULL, region VARCHAR(2) NOT NULL, PRIMARY KEY(sku, region))`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, product_id INT NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_DisabledViaConfig(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "false",
		"checkInferredFK":  "false",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_InferredFK_ConfigurableSeverity(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "true",
		"checkInferredFK":  "true",
		"ignoreColumns":    "id",
		"fkSeverity":       "warning",
		"sameNameSeverity": "warning",
	}))
	violations := filterRule(l.Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestTypePedantic_InferredFK_FromChanges(t *testing.T) {
	// Existing customers table; the change is a new orders table.
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
	)
	stmts, err := statement.New(`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "orders", violations[0].Location.Table)
}

func TestTypePedantic_InferredFK_IgnoresEmptyBase(t *testing.T) {
	// Plain `_id` column would have base "" — should be skipped.
	tables := parseTables(t,
		"CREATE TABLE oddly_named (id BIGINT UNSIGNED PRIMARY KEY, `_id` VARCHAR(36) NOT NULL)",
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_DoesNotMatchSelf(t *testing.T) {
	// orders.order_id pointing to orders.id would be a self-reference and is skipped.
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, order_id INT NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_BothRulesEmitForSameColumn(t *testing.T) {
	// orders.customer_id is INT — flagged by both rules:
	//  - same_name: minority against two tables using BIGINT UNSIGNED
	//  - inferred_fk: doesn't match customers.id (BIGINT UNSIGNED)
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE receipts (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Len(t, filterRule(violations, "same_name"), 1)
	require.Len(t, filterRule(violations, "inferred_fk"), 1)
}

func TestTypePedantic_Configure_RejectsUnknownKey(t *testing.T) {
	l := &TypePedanticLinter{}
	err := l.Configure(map[string]string{"bogus": "value"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown config key")
}

func TestTypePedantic_Configure_RejectsBadSeverity(t *testing.T) {
	l := &TypePedanticLinter{}
	err := l.Configure(map[string]string{"fkSeverity": "panic"})
	require.Error(t, err)
}

func TestTypePedantic_Configure_RejectsBadBool(t *testing.T) {
	l := &TypePedanticLinter{}
	err := l.Configure(map[string]string{"checkSameName": "maybe"})
	require.Error(t, err)
}

func TestTypePedantic_SameName_RequireIndexed_SkipsUnindexed(t *testing.T) {
	// Neither table indexes `note` — by default this is treated as noise and skipped.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(50))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255))`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "same_name"))
}

func TestTypePedantic_SameName_RequireIndexed_FlagsWhenAnyIndexed(t *testing.T) {
	// `customer_id` is indexed in `orders` only — the unindexed `returns` copy is
	// still flagged because the join surface against the indexed copy is real.
	// Three tables so the minority is unambiguous.
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL)`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "returns", violations[0].Location.Table)
}

func TestTypePedantic_SameName_RequireIndexed_DisabledFlagsAll(t *testing.T) {
	// With requireIndexed=false the noisy non-indexed mismatch fires.
	// Three tables so we get a clear majority rather than a tie.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(50))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255))`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255))`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "true",
		"checkInferredFK":  "false",
		"requireIndexed":   "false",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	violations := filterRule(l.Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "a", violations[0].Location.Table)
}

func TestTypePedantic_SameName_RequireIndexed_CompositeIndexAnyPosition(t *testing.T) {
	// Trailing position in a composite index still counts as indexed.
	// Three tables for a clear majority.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, tenant_id BIGINT UNSIGNED, customer_id BIGINT UNSIGNED, INDEX idx_t_c (tenant_id, customer_id))`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, tenant_id BIGINT UNSIGNED, customer_id BIGINT UNSIGNED, INDEX idx_t_c (tenant_id, customer_id))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, tenant_id BIGINT UNSIGNED, customer_id INT, INDEX idx_t_c (tenant_id, customer_id))`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "b", violations[0].Location.Table)
	require.Equal(t, "customer_id", *violations[0].Location.Column)
}

func TestTypePedantic_InferredFK_UnaffectedByRequireIndexed(t *testing.T) {
	// The FK rule fires regardless of whether the FK column itself is indexed —
	// joins are the problem and the target's id column is always indexed.
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
}

func TestTypePedantic_SameName_TieEmitsInconsistentForAll(t *testing.T) {
	// One column of each type — no majority. New behavior: every occurrence is
	// reported as inconsistent so neither side is silently treated as canonical.
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, qty INT UNSIGNED NOT NULL, INDEX idx_qty (qty))`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, qty INT NOT NULL, INDEX idx_qty (qty))`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 2)
	tables_seen := map[string]bool{}
	for _, v := range violations {
		require.Contains(t, v.Message, "inconsistent across schema")
		require.Contains(t, v.Message, "types in use")
		require.NotNil(t, v.Context["conflicting_types"])
		tables_seen[v.Location.Table] = true
	}
	require.True(t, tables_seen["orders"] && tables_seen["returns"])
}

func TestTypePedantic_SameName_InlinePrimaryKeyCountsAsIndexed(t *testing.T) {
	// Inline `PRIMARY KEY` doesn't appear in t.Indexes but DOES via GetIndexes().
	// Bug fix: type mismatches against inline-PK columns must still fire.
	tables := parseTables(t,
		`CREATE TABLE customers (customer_key BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (customer_key BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE returns (customer_key INT PRIMARY KEY)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1, "inline PRIMARY KEY should register as indexed")
	require.Equal(t, "returns", violations[0].Location.Table)
}

func TestTypePedantic_SameName_InlineUniqueCountsAsIndexed(t *testing.T) {
	// Inline `UNIQUE` should also register as indexed.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(500) UNIQUE)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "c", violations[0].Location.Table)
}

func TestTypePedantic_InferredFK_PluralizationESStems(t *testing.T) {
	// address_id → addresses, process_id → processes, bus_id → buses.
	// The old code missed all of these because base ends in 's'.
	cases := []struct {
		name   string
		target string
	}{
		{"address", "addresses"},
		{"process", "processes"},
		{"bus", "buses"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tables := parseTables(t,
				fmt.Sprintf(`CREATE TABLE %s (id BIGINT UNSIGNED PRIMARY KEY)`, tc.target),
				fmt.Sprintf(`CREATE TABLE owner (id BIGINT UNSIGNED PRIMARY KEY, %s_id INT NOT NULL)`, tc.name),
			)
			violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
			require.Len(t, violations, 1)
			require.Equal(t, tc.target, violations[0].Context["referenced_table"])
		})
	}
}

func TestTypePedantic_InferredFK_IgnoreColumnsAppliesToFKRule(t *testing.T) {
	// ignoreColumns scopes to both rules now.
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"ignoreColumns": "id,customer_id",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_PostState_AlterAddColumnMismatch(t *testing.T) {
	// Existing: customers (id BIGINT UNSIGNED PK)
	// Change:   ALTER TABLE orders ADD COLUMN customer_id INT
	// Expect:   inferred_fk violation against customers.id
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders ADD COLUMN customer_id INT NOT NULL`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "orders", violations[0].Location.Table)
	require.Equal(t, "customer_id", *violations[0].Location.Column)
	require.Equal(t, "customers", violations[0].Context["referenced_table"])
}

func TestTypePedantic_PostState_AlterModifyColumnFixesViolation(t *testing.T) {
	// Existing schema has a violation; the proposed ALTER fixes it.
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders MODIFY COLUMN customer_id BIGINT UNSIGNED NOT NULL`)
	require.NoError(t, err)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk"))
}

func TestTypePedantic_PostState_AlterAddIndexEnablesSameNameCheck(t *testing.T) {
	// Without the index the same-name group is unindexed → silently skipped.
	// The ALTER adds an index → group becomes indexed → flagged.
	existing := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, ext_id BIGINT UNSIGNED NOT NULL)`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, ext_id BIGINT UNSIGNED NOT NULL)`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, ext_id INT NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, nil), "same_name"),
		"baseline: unindexed group is silent")

	stmts, err := statement.New(`ALTER TABLE orders ADD INDEX idx_ext (ext_id)`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "returns", violations[0].Location.Table)
}

func TestTypePedantic_PostState_AlterDropColumnRemovesViolation(t *testing.T) {
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders DROP COLUMN customer_id`)
	require.NoError(t, err)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk"))
}

func TestTypePedantic_PostState_AlterChangeColumnRenameAndRetype(t *testing.T) {
	// CHANGE COLUMN renames and retypes in one shot.
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, cust_id BIGINT UNSIGNED NOT NULL)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders CHANGE COLUMN cust_id customer_id INT NOT NULL`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "customer_id", *violations[0].Location.Column)
}

func TestTypePedantic_Configure_PartialDoesNotResetOtherFields(t *testing.T) {
	// Calling Configure twice with disjoint keys should compose: the second
	// call shouldn't leave state from a missing key undefined.
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{"checkSameName": "false"}))
	require.False(t, l.checkSameName)
	require.True(t, l.checkInferredFK, "checkInferredFK should default to true")
	require.True(t, l.requireIndexed, "requireIndexed should default to true")

	// Subsequent partial call resets to defaults and applies — checkSameName
	// returns to true.
	require.NoError(t, l.Configure(map[string]string{"requireIndexed": "false"}))
	require.True(t, l.checkSameName)
	require.False(t, l.requireIndexed)
}

func TestTypePedantic_RegisteredAndDescribed(t *testing.T) {
	l, err := Get("type_pedantic")
	require.NoError(t, err)
	require.NotEmpty(t, l.Description())
	require.Contains(t, l.String(), "type_pedantic")
}

// filterRule returns violations whose Context["rule"] equals the given rule name.
func filterRule(vs []Violation, rule string) []Violation {
	var out []Violation
	for _, v := range vs {
		if v.Context != nil && v.Context["rule"] == rule {
			out = append(out, v)
		}
	}
	return out
}
