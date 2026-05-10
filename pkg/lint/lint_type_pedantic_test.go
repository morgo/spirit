package lint

import (
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
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, qty INT UNSIGNED NOT NULL, INDEX idx_qty (qty))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, qty INT NOT NULL, INDEX idx_qty (qty))`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
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
	//  - same_name: doesn't match invoices.customer_id (BIGINT UNSIGNED)
	//  - inferred_fk: doesn't match customers.id (BIGINT UNSIGNED)
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
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
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "returns", violations[0].Location.Table)
}

func TestTypePedantic_SameName_RequireIndexed_DisabledFlagsAll(t *testing.T) {
	// With requireIndexed=false the noisy non-indexed mismatch fires.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(50))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255))`,
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
	require.Len(t, filterRule(l.Lint(tables, nil), "same_name"), 1)
}

func TestTypePedantic_SameName_RequireIndexed_CompositeIndexAnyPosition(t *testing.T) {
	// Trailing position in a composite index still counts as indexed.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, tenant_id BIGINT UNSIGNED, customer_id BIGINT UNSIGNED, INDEX idx_t_c (tenant_id, customer_id))`,
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
