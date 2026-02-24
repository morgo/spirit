# Lint subcommand

The `lint` command validates an entire MySQL schema against Spirit's built-in lint rules. It checks for common issues such as foreign keys, unsafe data types, redundant indexes, and naming conventions.

Basic usage:

```bash
# Lint a live MySQL schema
spirit lint --source-dsn "user:pass@tcp(localhost:3306)/mydb"

# Lint from a directory of CREATE TABLE .sql files
spirit lint --source-dir ./schema/
```

The exit code is `0` if no errors are found, `1` if there are error-level violations, or `2` if there is a problem loading the schema.

## Configuration

- [source-dsn](#source-dsn)
- [source-dir](#source-dir)
- [ignore-tables](#ignore-tables)

### source-dsn

- Type: String
- Environment variable: `MYSQL_DSN`

A Go MySQL DSN for the existing schema to lint. Mutually exclusive with `--source-dir`.

### source-dir

- Type: String (existing directory)

Path to a directory containing `CREATE TABLE` `.sql` files representing the schema to lint. Mutually exclusive with `--source-dsn`.

### ignore-tables

- Type: String
- Default value: `""`

A regex pattern of table names to exclude from linting. For example, `--ignore-tables="^_.*"` would skip all tables whose names start with an underscore.

## Built-in Linters

| Linter | Description |
|--------|-------------|
| `allow_charset` | Checks that only allowed character sets are used |
| `allow_engine` | Restricts which storage engines are allowed |
| `auto_inc_capacity` | Ensures that an auto-inc column is not within a percentage threshold of the maximum capacity of the column type |
| `has_foreign_key` | Detects usage of FOREIGN KEY constraints in table definitions |
| `has_float` | Detects usage of FLOAT or DOUBLE data types in table definitions |
| `invisible_index_before_drop` | Requires indexes to be made invisible before dropping them as a safety measure |
| `multiple_alter_table` | Detects multiple ALTER TABLE statements on the same table that could be combined |
| `name_case` | Ensures that table names are all lowercase |
| `primary_key` | Ensures primary keys use BIGINT (preferably UNSIGNED) or BINARY/VARBINARY types |
| `redundant_indexes` | Detects redundant indexes including duplicates, prefix matches, and unnecessary PRIMARY KEY suffixes |
| `reserved_words` | Checks for usage of MySQL reserved words in table and column names |
| `unsafe` | Detects usage of unsafe operations in database schema changes |
| `zero_date` | Checks for columns with zero-date default values |

## Violation Severity

Each violation has one of three severity levels:

| Severity | Meaning | Exit code |
|----------|---------|-----------|
| **Error** | Will cause actual problems — must be fixed | `1` |
| **Warning** | Best practice violation or potential issue | `0` |
| **Info** | Suggestion or style preference | `0` |

## See Also

- [`spirit diff`](diff.md) — compare two schemas and lint the changes
- [Inline linting](migrate.md#inline-linting) — run lint checks as part of `spirit migrate`
