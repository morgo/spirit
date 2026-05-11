# Linter Framework

The `lint` package provides a framework for static analysis of MySQL schema definitions and DDL statements. It enables validation and best-practice enforcement beyond the runtime checks provided by the `check` package.

## Architecture

All built-in linters are automatically registered and enabled when the `lint` package is imported. The framework uses a flat package structure:

- **Core framework files**: `lint.go`, `linter.go`, `registry.go`, `violation.go`
- **Linter implementations**: `lint_*.go` (e.g., `lint_invisible_index.go`)

## Quick Start

### Using the Linter Framework

```go
import (
    "github.com/block/spirit/pkg/lint"
)

// All built-in linters are automatically registered!
violations, err := lint.RunLinters(tables, stmts, lint.Config{})
if err != nil {
    // Handle configuration errors
}

// Check for errors
if lint.HasErrors(violations) {
    // Handle errors
}

// Filter violations by linter
flagViolations := lint.FilterByLinter(violations, "has_fk")
```

### Creating a Custom Linter

Custom linters can be 
1. added directly to the `lint` package (in new files with the `lint_` prefix, for consistency)
2. added to your own package and registered by blank import that relies on the `init()` function
3. added to your own code and registered explicitly using `lint.Register()`

```go
// lint_my_custom.go
package lint

import (
    "github.com/block/spirit/pkg/statement"
)

// Register your linter in init()
func init() {
    Register(&MyCustomLinter{})
}

// MyCustomLinter checks custom rules
type MyCustomLinter struct{}

func (l *MyCustomLinter) Name() string        { return "my_custom" }
func (l *MyCustomLinter) Description() string { return "Checks naming conventions" }
func (l *MyCustomLinter) String() string      { return l.Name() }

func (l *MyCustomLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
    var violations []Violation
    
    for _, ct := range existingTables {
        // Check table properties
        if /* condition */ {
            violations = append(violations, Violation{
                Linter:   l,
                Severity: SeverityWarning,
                Message:  "Table name issue",
                Location: &Location{
                    Table: ct.GetTableName(),
                },
            })
        }
    }
    
    return violations
}
```

### Configuring Linters

#### Enabling/Disabling Linters

```go
// Disable specific linters
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Enabled: map[string]bool{
        "invisible_index_before_drop": false,
        "primary_key":                 true,
    },
})
if err != nil {
    // Handle configuration errors
}
```

#### Configurable Linters

Some linters support additional configuration options via the `Settings` field. Linters that implement the `ConfigurableLinter` interface accept settings as `map[string]string`:

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "invisible_index_before_drop": {
            "raiseError": "false",  // Demote violations from errors to warnings (default is "true")
        },
    },
})
if err != nil {
    // Handle configuration errors (e.g., invalid settings)
}
```

Each configurable linter defines its own settings keys and values. See the individual linter documentation below for available options.

## Core Types

### Severity Levels

- **ERROR**: Will cause actual problems (data loss, inconsistency, MySQL limitations)
- **WARNING**: Best practice violations, potential issues
- **INFO**: Suggestions, style preferences

### Violation

```go
type Violation struct {
    Linter     Linter              // The linter that produced this violation
    Severity   Severity            // ERROR, WARNING, or INFO
    Message    string              // Human-readable message
    Location   *Location           // Where the violation occurred
    Suggestion *string             // Optional fix suggestion
    Context    map[string]any      // Additional context
}
```

### Location

```go
type Location struct {
    Table      string   // Table name
    Column     *string  // Column name (if applicable)
    Index      *string  // Index name (if applicable)
    Constraint *string  // Constraint name (if applicable)
}
```

## API Functions

### Registration

- `Register(l Linter)` - Register a linter (call from init())
- `Enable(name string)` - Enable a linter by name
- `Disable(name string)` - Disable a linter by name
- `List()` - Get all registered linter names
- `Get(name string)` - Get a linter by name

### Execution

- `RunLinters(createTables, alterStatements, config) ([]Violation, error)` - Run all enabled linters, returns violations and any configuration errors
- `HasErrors(violations)` - Check if any violations are errors
- `HasWarnings(violations)` - Check if any violations are warnings
- `FilterByLinter(violations, name)` - Filter by linter name

## Built-in Linters

The `lint` package includes 16 built-in linters covering schema design, data types, and safety best practices.

### allow_charset

**Severity**: Warning  
**Configurable**: Yes  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN)

Restricts which character sets are allowed for tables and columns. Helps enforce consistent encoding across your database.

**Configuration Options:**

- `charsets` (string): Comma-separated list of allowed character sets. Default: `"utf8mb4"`.

**Examples:**

```sql
-- ❌ Violation (latin1 not allowed by default)
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100) CHARACTER SET latin1
) CHARACTER SET latin1;

-- ✅ Correct
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100) CHARACTER SET utf8mb4
) CHARACTER SET utf8mb4;

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ADD COLUMN legacy VARCHAR(100) CHARACTER SET latin1;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "allow_charset": {
            "charsets": "utf8mb4,utf8mb3",  // Allow multiple charsets
        },
    },
})
```

---

### allow_engine

**Severity**: Warning  
**Configurable**: Yes  
**Checks**: CREATE TABLE, ALTER TABLE ENGINE

Restricts which storage engines are allowed. Helps ensure consistent engine usage and avoid problematic engines like MyISAM.

**Configuration Options:**

- `allowed_engines` (string): Comma-separated list of allowed engines. Default: `"innodb"`.

**Examples:**

```sql
-- ❌ Violation (MyISAM not allowed by default)
CREATE TABLE users (
  id INT PRIMARY KEY
) ENGINE=MyISAM;

-- ✅ Correct
CREATE TABLE users (
  id INT PRIMARY KEY
) ENGINE=InnoDB;

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ENGINE=MyISAM;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "allow_engine": {
            "allowed_engines": "innodb,rocksdb",  // Allow multiple engines
        },
    },
})
```

---

### auto_inc_capacity

**Severity**: Error  
**Configurable**: Yes  
**Checks**: CREATE TABLE

Ensures that AUTO_INCREMENT values are not within a dangerous percentage of the column type's maximum capacity. Approaching the capacity ceiling will eventually cause INSERTs to fail.

**Configuration Options:**

- `threshold` (string): Percentage threshold (1-100). Default: `"85"`.

**Examples:**

```sql
-- ❌ Violation (90% of INT UNSIGNED capacity)
CREATE TABLE users (
  id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT
) AUTO_INCREMENT=4000000000;

-- ✅ Correct (low value)
CREATE TABLE users (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT
) AUTO_INCREMENT=100;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "auto_inc_capacity": {
            "threshold": "90",  // Warn at 90% capacity
        },
    },
})
```

---

### has_fk

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD CONSTRAINT)

Detects foreign key constraints, which can cause performance issues and operational complexity in large-scale systems.

**Examples:**

```sql
-- ❌ Violation (foreign key detected)
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- ✅ Correct (no foreign key)
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  INDEX idx_user_id (user_id)
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE orders ADD CONSTRAINT fk_user 
  FOREIGN KEY (user_id) REFERENCES users(id);
```

---

### has_float

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN)

Detects FLOAT and DOUBLE columns, which can have precision issues. Recommends DECIMAL for exact numeric values.

**Examples:**

```sql
-- ❌ Violation (FLOAT has precision issues)
CREATE TABLE products (
  id INT PRIMARY KEY,
  price FLOAT
);

-- ✅ Correct (DECIMAL for exact values)
CREATE TABLE products (
  id INT PRIMARY KEY,
  price DECIMAL(10,2)
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE products ADD COLUMN discount DOUBLE;
```

---

### invisible_index_before_drop

**Severity**: Error (default), Warning (configurable)  
**Configurable**: Yes  
**Checks**: ALTER TABLE (DROP INDEX)

Requires indexes to be made invisible before dropping them as a safety measure. This ensures the index isn't needed before permanently removing it.

**Configuration Options:**

- `raiseError` (string): Set to `"false"` to demote violations to warnings instead of errors. Default: `"true"`.

**Examples:**

```sql
-- ❌ Violation
ALTER TABLE users DROP INDEX idx_email;

-- ✅ Correct
ALTER TABLE users ALTER INDEX idx_email INVISIBLE;
-- Wait and monitor performance
ALTER TABLE users DROP INDEX idx_email;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "invisible_index_before_drop": {
            "raiseError": "false",  // Demote violations to warnings (default is "true")
        },
    },
})
```

---

### multiple_alter_table

**Severity**: Info  
**Configurable**: No  
**Checks**: ALTER TABLE

Detects multiple ALTER TABLE statements on the same table that could be combined into one for better performance and fewer table rebuilds. This is an optimization suggestion, not a correctness issue.

**Examples:**

```sql
-- ❌ Violation (multiple ALTERs on same table)
ALTER TABLE users ADD COLUMN age INT;
ALTER TABLE users ADD INDEX idx_age (age);

-- ✅ Better (combined into one)
ALTER TABLE users 
  ADD COLUMN age INT,
  ADD INDEX idx_age (age);
```

---

### name_case

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (RENAME)

Ensures that table names are all lowercase to avoid case-sensitivity issues across different operating systems.

**Examples:**

```sql
-- ❌ Violation (mixed case)
CREATE TABLE UserAccounts (
  id INT PRIMARY KEY
);

-- ✅ Correct (lowercase)
CREATE TABLE user_accounts (
  id INT PRIMARY KEY
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE users RENAME TO UserAccounts;
```

---

### primary_key

**Severity**: Warning for existing tables, Error for new tables (CREATE TABLE in changes)  
**Configurable**: Yes  
**Checks**: CREATE TABLE

Ensures primary keys are defined and use appropriate data types (BIGINT UNSIGNED, BINARY, or VARBINARY by default). Severity is scoped by source: existing tables produce Warning so legacy schemas don't block unrelated ALTERs, while new CREATE TABLE statements in the incoming changes produce Error.

**Configuration Options:**

- `allowedTypes` (string): Comma-separated list of allowed types. Default: `"BIGINT,BINARY,VARBINARY"`.
- Supported types: `BINARY`, `VARBINARY`, `BIGINT`, `CHAR`, `VARCHAR`, `BIT`, `DECIMAL`, `ENUM`, `SET`, `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `TIME`, `TIMESTAMP`, `YEAR`, `DATE`, `DATETIME`

**Examples:**

```sql
-- ❌ Error in a new CREATE TABLE / ⚠️ Warning if the table already exists (no primary key)
CREATE TABLE users (
  id INT,
  name VARCHAR(100)
);

-- ❌ Error in a new CREATE TABLE / ⚠️ Warning if the table already exists (INT not allowed by default)
CREATE TABLE users (
  id INT PRIMARY KEY
);

-- ✅ Correct
CREATE TABLE users (
  id BIGINT UNSIGNED PRIMARY KEY
);
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "primary_key": {
            "allowedTypes": "BIGINT,INT,VARCHAR",  // Allow additional types
        },
    },
})
```

---

### type_pedantic

**Severity**: Warning (same-name rule), Error (inferred FK rule) — both configurable  
**Configurable**: Yes  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE/DROP COLUMN, ADD/DROP INDEX)

Cross-table column type consistency checks. Unlike most linters in this package, `type_pedantic` looks at the entire schema rather than a single table — it needs the full set of `existingTables` to spot inconsistencies. The linter operates on a **post-state view** of the schema: existing tables with pending CREATE TABLE and ALTER TABLE changes applied, so it's useful both for whole-schema audits (`spirit lint --source-dir`) and for ALTER-driven migration flows. Two rules are bundled:

**Rule 1 — Same-name columns must match types.** Columns sharing a name across tables (e.g. `customer_id` in both `orders` and `returns`) should use the same MySQL type, including signedness and width. When one type clearly dominates the schema, the minority occurrences are flagged against that majority. When the top counts are tied (e.g. 2 BIGINT vs 2 INT), every occurrence is flagged as "inconsistent" with the conflicting types listed — the linter doesn't silently pick a winner by alphabet. By default `id` is excluded, since `id` is intentionally typed differently across unrelated tables in many schemas — use the `primary_key` linter for PK type enforcement. By default Rule 1 also fires only when at least one table in the column-name group indexes the column (see `requireIndexed` below for the rationale).

**Rule 2 — Inferred foreign keys must match the referenced `id` type.** A column named `{table}_id` is treated as an implicit foreign key to `{table}.id`. The linter tries the literal base name and English pluralizations: `base+s`, `base+es` for sibilant- and o-stems (so `address_id → addresses`, `process_id → processes`, `bus_id → buses`, `tomato_id → tomatoes`), and `base[:-1]+ies` for y-stems (so `category_id → categories`). Mismatches default to an Error because JOINs across mismatched types force implicit casts and prevent index use. `requireIndexed` does not gate this rule — the referenced `id` is always indexed. The suggestion advises growing the smaller side rather than shrinking the larger, since the underlying problem is often an undersized PK on the target.

**Configuration Options:**

- `checkSameName` (string `"true"`/`"false"`): Enable Rule 1. Default: `"true"`.
- `checkInferredFK` (string `"true"`/`"false"`): Enable Rule 2. Default: `"true"`.
- `requireIndexed` (string `"true"`/`"false"`): Restrict Rule 1 to column-name groups where at least one occurrence is indexed (any position in any index, including inline column-level `PRIMARY KEY`/`UNIQUE`). Default: `"true"`. **Why:** the real cost of a type mismatch is on JOINs and lookups, where mismatched types force implicit casts and prevent index use. On unindexed columns the schema isn't paying that cost in the first place, so flagging them is mostly false positives — incidental name collisions on scalars like `status`, `name`, or `value` that happen to share a name across unrelated tables.
- `ignoreColumns` (string): Comma-separated column names (case-insensitive) excluded from **both** rules. Default: `"id"`.
- `fkSeverity` (string `"error"`/`"warning"`/`"info"`): Severity for Rule 2 violations. Default: `"error"`.
- `sameNameSeverity` (string `"error"`/`"warning"`/`"info"`): Severity for Rule 1 violations. Default: `"warning"`.

**Examples:**

```sql
-- Rule 1: same-name mismatch
CREATE TABLE orders   (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED);
CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED);
CREATE TABLE returns  (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT);
-- ⚠️ returns.customer_id (INT) doesn't match the majority type (BIGINT UNSIGNED)

-- Rule 2: inferred FK mismatch
CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY);
CREATE TABLE orders    (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT UNSIGNED);
-- ❌ orders.customer_id (INT UNSIGNED) doesn't match customers.id (BIGINT UNSIGNED)
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "type_pedantic": {
            "ignoreColumns":   "id,created_at,updated_at",
            "fkSeverity":      "warning", // downgrade FK rule to a warning
            "requireIndexed":  "false",   // also lint unindexed columns (noisier)
            "checkSameName":   "false",   // disable Rule 1 entirely
        },
    },
})
```

---

### reserved_words

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN, RENAME)

Checks for usage of MySQL reserved words in table and column names. Using reserved words as identifiers can cause syntax errors and requires backtick quoting. The linter uses a static list of 259 reserved words from MySQL 9.5.0.

**Examples:**

```sql
-- ❌ Violation (SELECT is reserved)
CREATE TABLE `select` (
  id INT PRIMARY KEY
);

-- ❌ Violation (ORDER is reserved)
CREATE TABLE `order` (
  id INT PRIMARY KEY,
  `where` VARCHAR(100)  -- WHERE is also reserved
);

-- ✅ Correct (non-reserved words)
CREATE TABLE orders (
  id INT PRIMARY KEY,
  location VARCHAR(100)
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ADD COLUMN `select` VARCHAR(100);
ALTER TABLE users RENAME TO `table`;
```

**Note:** While MySQL allows reserved words as identifiers when quoted with backticks, it's better practice to avoid them entirely to prevent confusion and potential issues. The reserved words list is sourced directly from MySQL 9.5.0 and includes all keywords that cannot be used as unquoted identifiers.

---

### unsafe

**Severity**: Warning  
**Configurable**: Yes  
**Checks**: ALTER TABLE, DROP TABLE, TRUNCATE TABLE, DROP DATABASE

Detects unsafe operations that can cause data loss or service disruption, such as DROP COLUMN, DROP TABLE, TRUNCATE, and DROP DATABASE.

**Configuration Options:**

- `allowUnsafe` (string): Set to `"true"` to disable this linter. Default: `"false"`.

**Examples:**

```sql
-- ❌ Violation (drops data)
ALTER TABLE users DROP COLUMN email;

-- ❌ Violation (drops table)
DROP TABLE users;

-- ❌ Violation (truncates data)
TRUNCATE TABLE users;

-- ✅ Safe operations
ALTER TABLE users ADD COLUMN email VARCHAR(255);
ALTER TABLE users ADD INDEX idx_email (email);
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "unsafe": {
            "allowUnsafe": "true",  // Disable unsafe checks (not recommended)
        },
    },
})
```

---

### zero_date

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN)

Checks for columns with zero-date default values (`0000-00-00` or `0000-00-00 00:00:00`) and NOT NULL date columns without defaults, which can cause issues with strict SQL modes.

**Examples:**

```sql
-- ❌ Violation (zero-date default)
CREATE TABLE users (
  id INT PRIMARY KEY,
  created_at DATETIME DEFAULT '0000-00-00 00:00:00'
);

-- ❌ Violation (NOT NULL without default)
CREATE TABLE users (
  id INT PRIMARY KEY,
  created_at DATETIME NOT NULL
);

-- ✅ Correct
CREATE TABLE users (
  id INT PRIMARY KEY,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ADD COLUMN legacy_date DATETIME DEFAULT '0000-00-00 00:00:00';
```

### has_timestamp

**Severity**: Warning for existing tables, Error for new tables (CREATE TABLE in changes) and for ALTER TABLE statements that add/modify TIMESTAMP columns  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN)

Detects TIMESTAMP columns, which have problematic behavior in MySQL (automatic initialization, timezone conversion, limited range to 2038). Recommends using DATETIME instead.

### redundant_indexes

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE

Detects redundant indexes where one index is a prefix of another (e.g., INDEX(a) is redundant if INDEX(a, b) exists). Also detects duplicate indexes with the same column list.

### rename_column

**Severity**: Error  
**Configurable**: No  
**Checks**: ALTER TABLE

Detects column renames via RENAME COLUMN or CHANGE COLUMN. Column renames cannot be done atomically across application pods and break ORMs that generate column names at compile time. Recommends using ADD COLUMN + DROP COLUMN instead.

---

## Linter Summary Table

| Linter | Configurable | CREATE TABLE | ALTER TABLE | Severity |
|--------|--------------|--------------|-------------|----------|
| `allow_charset` | ✅ | ✅ | ✅ | Warning |
| `allow_engine` | ✅ | ✅ | ✅ | Warning |
| `auto_inc_capacity` | ✅ | ✅ | ❌ | Error |
| `has_fk` | ❌ | ✅ | ✅ | Warning |
| `has_float` | ❌ | ✅ | ✅ | Warning |
| `has_timestamp` | ❌ | ✅ | ✅ | Warning (existing) / Error (new) |
| `invisible_index_before_drop` | ✅ | ❌ | ✅ | Error (default), Warning (configurable) |
| `multiple_alter_table` | ❌ | ❌ | ✅ | Info |
| `name_case` | ❌ | ✅ | ✅ | Warning |
| `primary_key` | ✅ | ✅ | ❌ | Warning (existing) / Error (new) |
| `redundant_indexes` | ❌ | ✅ | ❌ | Warning |
| `rename_column` | ❌ | ❌ | ✅ | Error |
| `reserved_words` | ❌ | ✅ | ✅ | Warning |
| `type_pedantic` | ✅ | ✅ | ✅ | Warning / Error |
| `unsafe` | ✅ | ❌ | ✅ | Warning |
| `zero_date` | ❌ | ✅ | ✅ | Warning |

## Example Linters

The `example` package provides demonstration linters for learning purposes:

### TableNameLengthLinter

Checks that table names don't exceed MySQL's 64 character limit.

### DuplicateColumnLinter

Detects duplicate column definitions in CREATE TABLE statements.

See `pkg/lint/example/` for reference implementations.

## Contributing

When adding new linters to the `lint` package:

1. **Create a new file** with the `lint_` prefix (e.g., `lint_my_rule.go`)
2. **Implement the `Linter` interface** with all required methods
3. **Register in `init()`** function to enable automatic registration
4. **Add comprehensive tests** in a corresponding `lint_my_rule_test.go` file
5. **Document the linter** with clear comments and examples
6. **Choose appropriate severity levels**:
   - `SeverityError` for violations that will cause actual problems
   - `SeverityWarning` for best practice violations
   - `SeverityInfo` for suggestions and style preferences
7. **Provide helpful messages** with actionable suggestions when possible
8. **Update this README** with documentation for the new linter

### File Naming Convention

- Linter implementation: `lint_<name>.go`
- Linter tests: `lint_<name>_test.go`

### Example Structure

```
pkg/lint/
├── lint.go                      # Core API
├── linter.go                    # Interface definition
├── registry.go                  # Registration system
├── violation.go                 # Violation types
├── lint_invisible_index.go      # Built-in linter
├── lint_multiple_alter.go       # Built-in linter
├── lint_primary_key_type.go     # Built-in linter
├── lint_my_new_rule.go          # Your new linter
└── lint_my_new_rule_test.go     # Your tests
```
