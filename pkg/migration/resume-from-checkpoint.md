# Resume from Checkpoint

Spirit automatically checkpoints progress during a migration, allowing it to resume from where it left off if the process is killed or restarted. This is useful for long-running migrations on large tables, where restarting from scratch would be expensive.

As noted in the [threads](../docs/migrate.md#threads) and [target-chunk-time](../docs/migrate.md#target-chunk-time) documentation, the recommended way to adjust these settings mid-migration is to kill the Spirit process and restart it with new values. Spirit will resume from the checkpoint automatically.

## How checkpointing works

Spirit writes a checkpoint every 50 seconds to a table named `_<table>_chkpnt` in the same database as the table being migrated. If the original table name is long enough that `_<table>_chkpnt` would exceed MySQL's 64-character identifier limit, the table-name portion is deterministically truncated; the full original name is recorded in the `original_table_name` column so resume can detect collisions.

```sql
CREATE TABLE _tablename_chkpnt (
    id INT AUTO_INCREMENT PRIMARY KEY,
    copier_watermark TEXT,                                  -- where row copy left off (JSON)
    checksum_watermark TEXT,                                -- where checksum left off (JSON, if applicable)
    binlog_name VARCHAR(255),                               -- e.g., "mysql-bin.000042"
    binlog_pos INT,                                         -- e.g., 4567
    statement TEXT,                                         -- the DDL statement being executed
    original_table_name VARCHAR(64) NOT NULL DEFAULT '',    -- full untruncated table name (single-table only; '' for multi-table)
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

The checkpoint captures everything needed to resume: where the copier was, the binlog position for the replication client to start streaming from, and the original DDL statement.

## What happens on resume

When a new Runner starts (`Runner.Run()` → `setup()`), it always attempts `resumeFromCheckpoint()` first. This performs several validation steps before committing to the resume path:

1. **Check `_<table>_new` exists** — if the shadow table is gone, there's nothing to resume.
2. **Read checkpoint table** — fetch the saved watermarks, binlog position, statement, and `original_table_name`.
3. **Validate DDL statement matches** — the checkpoint must be for the same alter. In `--strict` mode, a mismatch is a hard error. In non-strict mode, Spirit discards the checkpoint and starts fresh.
4. **Validate `original_table_name` matches** (single-table mode) — guards against the rare collision where two long table names truncate to the same checkpoint table name. A mismatch causes Spirit to discard the checkpoint and start fresh.
5. **Validate binlog file still exists** — queries `SHOW BINARY LOGS` to verify the checkpoint's binlog file hasn't been purged. If it has, resume is not possible and Spirit falls back to `newMigration()`.
6. **Set up copier, checker, and replication client** — create the replication client and add subscriptions for each table.
7. **Start binlog streaming** — `replClient.Run()` begins streaming from the saved position.

If any step fails (and strict mode is not enabled), Spirit logs the reason and falls back to `newMigration()`, which starts the migration from scratch. This means resume is best-effort — Spirit will always make forward progress even if the checkpoint is unusable.

## Background: how MySQL binary logs work

MySQL binary logs (binlogs) are a sequence of files that record every data-changing operation on the server. MySQL creates a series of numbered files — `mysql-bin.000001`, `mysql-bin.000002`, etc. — rotating to a new file when the current one reaches `max_binlog_size` (default 1GB) or the server restarts. Every INSERT, UPDATE, DELETE, and DDL statement is recorded.

Spirit uses binlogs to keep the shadow table in sync while the row copy is running. A replication client streams binlog events from the saved position and replays changes onto the shadow table. The checkpoint records which binlog file and position Spirit was at, so it can pick up where it left off.

MySQL automatically deletes old binlog files based on `binlog_expire_logs_seconds` (default 30 days on MySQL 8.0, via the deprecated `expire_logs_days` on older versions). This is called **purging**. Once a file is purged, it's gone — any writes recorded in that file are no longer available. If Spirit's checkpoint references a purged file, there's a gap in the change stream and Spirit can't guarantee the shadow table is consistent.

## When resume fails

One reason resume can fail is **binlog expiry**. If the checkpoint references a binlog file that has been purged, Spirit cannot resume because changes in the gap would be lost.

Spirit detects this early by checking `SHOW BINARY LOGS` before creating any resources. If the file is missing, it returns `status.ErrBinlogNotFound` immediately, avoiding partial initialization that would need cleanup.

What happens next depends on whether strict mode is enabled:

- **Without `--strict`:** Spirit logs the reason and falls back to `newMigration()`, restarting the copy from scratch. All checkpoint progress is lost silently.
- **With `--strict`:** Spirit returns `status.ErrBinlogNotFound` to the caller. This lets automation detect the problem and alert an operator rather than silently discarding hours of copy work.

The tradeoff of falling back to `newMigration()` is that all copy progress is lost. For a large table this could mean hours of wasted work. To avoid this:

- **Keep binlog retention longer than your longest expected migration pause.** If you expect to pause migrations for up to a week, make sure `binlog_expire_logs_seconds` is set to at least 7 days. The MySQL 8.0 default is 30 days (`2592000`), which is usually sufficient.
- **Consider `--strict` mode only if you have automation that handles the errors it produces.** In strict mode, Spirit surfaces both DDL mismatches (`status.ErrMismatchedAlter`) and binlog expiry (`status.ErrBinlogNotFound`) as errors instead of restarting. This is generally not recommended for most users — the default behavior of discarding stale checkpoints and restarting is safer and simpler. See [strict](../docs/migrate.md#strict) for more details.
- **Be aware of your binlog retention window.** If Spirit is paused longer than the retention period, the checkpoint's binlog file will be purged and resume will fail. Some managed MySQL services disable retention by default.

## Strict mode

> **Note:** `--strict` is not recommended for most users. The default idempotent restart behavior (discard stale checkpoint, restart from scratch) is safer and requires no special error handling. Only use `--strict` if you have automation that can programmatically handle the specific errors it produces.

By default, Spirit treats checkpoint resume as best-effort. If the checkpoint is invalid for any reason — mismatched DDL statement, expired binlog, corrupt checkpoint data — Spirit discards it and starts a new migration. This is the recommended behavior.

With `Strict: true`, Spirit returns a hard error for two specific resume failures:

- **`status.ErrMismatchedAlter`** — the checkpoint's DDL statement doesn't match the current `--alter`. This prevents the scenario where an operator changes the alter between runs and unknowingly loses all progress.
- **`status.ErrBinlogNotFound`** — the checkpoint's binlog file has been purged from the server. This prevents silently restarting a multi-hour copy from scratch.

Both errors work with `errors.Is()`, letting callers handle each case differently. See [strict](../docs/migrate.md#strict) for more details.

Other resume failures (missing shadow table, corrupt checkpoint data) still fall through to `newMigration()` in both modes, since these typically indicate there was nothing valid to resume.

## Cross-version compatibility

Checkpoint tables are version-specific. Spirit deliberately uses `SELECT *` when reading the checkpoint, so any change to the checkpoint schema (e.g. columns added, removed, or reordered in a newer release) will cause the read to fail. This is by design — Spirit does not attempt to migrate or backfill checkpoint data across versions.

Practical implications:

- **Upgrading Spirit mid-migration** (older binary → newer binary). The newer binary's `Scan` expects a different number of columns than the on-disk checkpoint provides, so the read fails.
- **Rolling back Spirit mid-migration** (newer binary → older binary). Same failure mode in reverse.
- **Effect in both `--strict` and non-strict mode:** the read returns a generic `"could not read from table"` error wrapping the underlying `database/sql` scan error. This error is not one of the typed `status.Err…` values that strict mode promotes (`ErrMismatchedAlter`, `ErrBinlogNotFound`, `ErrCheckpointTooOld`), so Spirit logs the error and falls through to `newMigration()` regardless of strict mode. The copy restarts from scratch and all checkpoint progress is lost silently.
- **Operator implication:** do **not** rely on `--strict` alerting to catch a Spirit upgrade or rollback. Strict mode does not currently distinguish a cross-version checkpoint schema mismatch from a healthy fresh start.

Operational guidance:

- Prefer to finish in-flight migrations on the same Spirit version they were started with.
- If you must upgrade or roll back mid-migration, expect the copy to restart from zero. Plan binlog retention and operational windows accordingly.
- Treat any Spirit version change as equivalent to a `binlog_expire_logs_seconds` event for the purposes of capacity planning.

This is an intentional safety boundary: silently reading a checkpoint with a different schema risks misinterpreting fields and corrupting the migration. We prefer to lose progress loudly than to corrupt data silently.
