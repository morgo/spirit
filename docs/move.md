# Move subcommand

The `move` command copies whole schemas (or a subset of tables) between different MySQL servers. It uses the buffered copy algorithm internally and streams binlog changes to keep the target in sync until cutover.

Basic usage:

```bash
spirit move --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

This will copy all tables from the source database to the target database, verify them with a checksum, and then complete.

## Configuration

- [create-sentinel](#create-sentinel)
- [defer-secondary-indexes](#defer-secondary-indexes)
- [force-enable-buffered-map](#force-enable-buffered-map)
- [source-dsn](#source-dsn)
- [target-chunk-time](#target-chunk-time)
- [target-dsn](#target-dsn)
- [threads](#threads)
- [write-threads](#write-threads)

### create-sentinel

- Type: Boolean
- Default value: `false`

When set to `true`, a sentinel table (`_spirit_sentinel`) is created on the **source** database after the table copy completes. Move will block before cutover until the sentinel table is manually dropped, giving the operator a chance to verify the copy before proceeding.

### defer-secondary-indexes

- Type: Boolean
- Default value: `false`

When set to `true`, target tables are created without secondary indexes. The indexes are restored from the source schema just before cutover. This can significantly speed up the initial data load for tables with many secondary indexes.

### force-enable-buffered-map

- Type: Boolean
- Default value: `false`

See the [migrate documentation](migrate.md#force-enable-buffered-map) for the full rationale. In short: this flag controls whether tables with non-memory-comparable primary keys (e.g. `VARCHAR` with a case-insensitive collation) use the LWW buffered-map dedup during the copy phase. The default is a FIFO queue full-time for those tables, which is the safer choice; setting this to `true` is experimental and relies on the post-cutover checksum to catch and repair any divergence.

### source-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/src`

A Go MySQL DSN for the source database. All tables in this database will be copied.

### target-chunk-time

- Type: Duration
- Default value: `5s`

The target time for each chunk of rows to be copied. See the [migrate documentation](migrate.md#target-chunk-time) for a detailed explanation of how chunk timing works.

### target-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/dest`

A Go MySQL DSN for the target database. Tables will be created here automatically from the source schema.

### threads

- Type: Integer
- Default value: `2`

How many chunks to copy in parallel from the source.

### write-threads

- Type: Integer
- Default value: `2`

How many concurrent write threads to use per target when inserting rows. This controls the fan-out parallelism of the buffered copier's write side.
