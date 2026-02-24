# How to use Spirit

Spirit provides two binaries for MySQL schema and data operations:

| Binary | Purpose | Documentation |
|--------|---------|---------------|
| **`spirit`** | Online schema change tool — applies `ALTER TABLE` statements to large tables without blocking reads or writes | [USAGE-SPIRIT.md](USAGE-SPIRIT.md) |
| **`move`** | Logical table mover — copies whole schemas (or a subset of tables) between different MySQL servers | [USAGE-MOVE.md](USAGE-MOVE.md) |

## Which binary should I use?

- Use **`spirit`** when you need to alter the schema of a table on the **same** MySQL server (e.g., add a column, add an index, change a charset).
- Use **`move`** when you need to copy tables from one MySQL server to **another** (e.g., migrating to a new cluster, resharding).

Both binaries share the same core engine: they stream binlog changes, copy rows in parallel, verify data with a checksum, and perform an atomic cutover. The `move` binary always uses the [buffered copy](USAGE-SPIRIT.md#buffered) algorithm, while `spirit` defaults to unbuffered `INSERT .. SELECT` (with `--buffered` available as an option).

## Building

```bash
# Build both binaries
cd cmd/spirit && go build
cd cmd/move && go build
```

## Requirements

- **MySQL 8.0+**
- `binlog_format=ROW`
- `log_bin=ON`
- `log_slave_updates=ON`

See the individual usage docs linked above for the full list of configuration options.
