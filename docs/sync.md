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

`sync` never writes to the source, runs no `ANALYZE`, acquires no source
locks, and performs no cutover, so it can run against a replica. The exact
source privileges depend on the change feed:

- **Built-in MySQL binlog source** (default, from `--source-dsn`): needs
  `SELECT` on the source schema, `REPLICATION SLAVE` + `REPLICATION CLIENT`
  for the binlog stream, and `RELOAD` — the binlog reader runs
  `FLUSH BINARY LOGS` to establish/advance its start position, so it is not
  a pure `SELECT`-only role even though it never modifies your data.
  See [`--gtid`](#gtid) below to switch to the experimental GTID-based feed,
  which removes the `RELOAD` / `FLUSH BINARY LOGS` requirement.
- **Injected `change.Source`** (e.g. a Vitess/PlanetScale VStream supplied by
  a programmatic caller): the feed is driven entirely by that source, so the
  built-in binlog privileges (`REPLICATION *`, `RELOAD`) do not apply — only
  `SELECT` on the source schema is required for the initial copy. `--gtid` is
  ignored when an injected source is supplied.

## Requirements

- **MySQL 8.0+** on both ends
- Source (built-in binlog feed): `binlog_format=ROW`, `log_bin=ON`, and
  `SELECT` + `REPLICATION SLAVE` + `REPLICATION CLIENT` + `RELOAD` privileges
  (`RELOAD` is required for the `FLUSH BINARY LOGS` the reader issues)

## Configuration

- [source-dsn](#source-dsn)
- [target-dsn](#target-dsn)
- [target-chunk-time](#target-chunk-time)
- [threads](#threads)
- [write-threads](#write-threads)
- [flush-interval](#flush-interval)
- [copy-only](#copy-only)
- [force](#force)
- [gtid](#gtid)

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

### copy-only

- Type: Boolean
- Default value: `false`

Run only the initial copy and then exit — no change capture, no continuous
replication. Use it for a one-shot snapshot, or when the source cannot provide
a change feed (e.g. a managed Vitess without binlog/VStream access, or a
replica lacking the `REPLICATION` privileges the binlog client needs). A
checkpoint is still written, so a re-run resumes the copy (or no-ops if it had
already completed) rather than starting over.

### force

- Type: Boolean
- Default value: `false`

Drop and recreate the target database at startup **unless** a resumable
checkpoint exists. A resumable run (checkpoint present) is left intact and
resumes as normal; this only resets a target that is non-empty with no usable
checkpoint, which would otherwise trip the fresh-sync target-empty guard.
Intended for testing/iterating.

### gtid

- Type: Boolean
- Default value: `false`

> **⚠️ Experimental.** See the full caveats and on-disk-format warning in the
> [migrate `--gtid` documentation](migrate.md#gtid).

When set to `true`, the built-in MySQL binlog source switches from the default
binlog **file + offset** coordinate to a MySQL **GTID set** coordinate. The
copy phase, applier, checkpoint contract, and continuous-stream lifecycle are
otherwise unchanged.

Sync-specific notes:

- **Ignored when an injected `Source` is supplied** (e.g. a programmatic caller
  passing a Vitess/PlanetScale VStream `change.Source`) — the flag only
  controls how Sync constructs its own MySQL binlog client.
- **No `RELOAD` / `FLUSH BINARY LOGS` requirement.** Unlike the default
  file+offset path, the GTID feed reads `@@GLOBAL.gtid_executed` to discover
  positions, so the source role can drop `RELOAD` and `FLUSH BINARY LOGS` calls
  disappear from the run. The other built-in feed privileges
  (`SELECT`, `REPLICATION SLAVE`, `REPLICATION CLIENT`) still apply.
- **Known limitation: no preflight check.** `spirit sync` does not yet have a
  preflight check system the way [`migrate`](migrate.md) and [`move`](move.md)
  do, so the GTID prerequisites below are **not** validated up-front. If the
  source server has `gtid_mode=OFF` (or `enforce_gtid_consistency=OFF`) the
  failure surfaces later as a stream-level error rather than a clear preflight
  message. Validate these settings yourself before passing `--gtid`.

**Requirements (on the source):**

- `gtid_mode = ON`
- `enforce_gtid_consistency = ON`

```bash
spirit sync --gtid \
            --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```
