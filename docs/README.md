# How to use Spirit

Spirit is a single binary with subcommands for MySQL schema and data operations:

| Subcommand | Purpose | Documentation |
|------------|---------|---------------|
| **`spirit migrate`** | Online schema change tool — applies `ALTER TABLE` statements to large tables without blocking reads or writes | [migrate.md](migrate.md) |
| **`spirit move`** | Logical table mover — copies whole schemas (or a subset of tables) between different MySQL servers | [move.md](move.md) |
| **`spirit lint`** | Schema linter — validates an entire MySQL schema against built-in lint rules | [lint.md](lint.md) |
| **`spirit diff`** | Schema differ — compares two MySQL schemas and lints the changes | [diff.md](diff.md) |

## Which subcommand should I use?

- Use **`spirit migrate`** when you need to alter the schema of a table on the **same** MySQL server (e.g., add a column, add an index, change a charset).
- Use **`spirit move`** when you need to copy tables from one MySQL server to **another** (e.g., migrating to a new cluster, resharding).
- Use **`spirit lint`** to validate a MySQL schema against built-in lint rules.
- Use **`spirit diff`** to compare two MySQL schemas and lint the differences.

Both `migrate` and `move` share the same core engine: they stream binlog changes, copy rows in parallel, verify data with a checksum, and perform an atomic cutover. The `move` subcommand always uses the [buffered copy](migrate.md#buffered) algorithm, while `migrate` defaults to unbuffered `INSERT .. SELECT` (with `--buffered` available as an option).

## Building

```bash
cd cmd/spirit && go build
```

## Requirements

- **MySQL 8.0+**
- `binlog_format=ROW`
- `log_bin=ON`
- `log_slave_updates=ON`

See the individual usage docs linked above for the full list of configuration options.
