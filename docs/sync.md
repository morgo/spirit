# Sync subcommand

> **Experimental.** `spirit sync` is under active development. The flags,
> behavior, and on-disk checkpoint format may change between releases. Today it
> supports **MySQL → MySQL** only.

The `sync` command performs an initial copy of a source schema into a target
and then **continuously applies the source's change stream** to the target
until it is interrupted (Ctrl-C / SIGTERM). Unlike [`move`](move.md), it does
**not** cut over — it is meant for keeping a target continuously up to date
(e.g. seeding and then tailing a replica of a dataset).

Basic usage:

```bash
spirit sync --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

This copies all tables from the source database to the target database
(creating the target database and tables if they do not exist), then streams
changes until the process is signalled. On a clean shutdown it drains the
outstanding changes and records a checkpoint; a re-run resumes from that
checkpoint rather than re-copying from scratch.

## How it differs from `move`

| | `move` | `sync` |
|---|---|---|
| Initial copy | yes | yes |
| Continuous replication | until cutover | until interrupted (no cutover) |
| Cutover | atomic rename | none |
| Source | MySQL | MySQL (a pluggable `change.Source` allows other producers) |

The source is treated as **read-only**: `sync` needs only `SELECT` on the
source schema (plus `REPLICATION SLAVE`/`REPLICATION CLIENT` for the binlog
change feed), runs no `ANALYZE`, acquires no source locks, and performs no
cutover, so it can run against a replica.

## Requirements

- **MySQL 8.0+** on both ends
- Source: `binlog_format=ROW`, `log_bin=ON`, and `REPLICATION SLAVE` /
  `REPLICATION CLIENT` privileges for the change feed

## Configuration

- [source-dsn](#source-dsn)
- [target-dsn](#target-dsn)
- [target-chunk-time](#target-chunk-time)
- [threads](#threads)
- [write-threads](#write-threads)
- [flush-interval](#flush-interval)
- [force](#force)

### source-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/src`

A Go MySQL DSN for the source database. All tables in this database are copied
and then followed on the change stream.

### target-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/dest`

A Go MySQL DSN for the target database. The database and tables are created
automatically from the source schema if they do not already exist.

### target-chunk-time

- Type: Duration
- Default value: `5s`

The target time for each chunk of rows to be copied during the initial copy.
See the [migrate documentation](migrate.md#target-chunk-time) for how chunk
timing adapts.

### threads

- Type: Integer
- Default value: `4`

How many chunks to copy in parallel from the source during the initial copy.

### write-threads

- Type: Integer
- Default value: `4`

How many concurrent write threads to use on the target.

### flush-interval

- Type: Duration
- Default value: `30s`

How often buffered changes are applied to the target during continuous sync —
the replication-latency vs. batching trade-off.

### force

- Type: Boolean
- Default value: `false`

Drop and recreate the target database at startup **unless** a resumable
checkpoint exists. A resumable run (checkpoint present) is left intact and
resumes as normal; this only resets a target that is non-empty with no usable
checkpoint, which would otherwise trip the fresh-sync target-empty guard.
Intended for testing/iterating.
