# Appliers

Appliers are responsible for writing rows to one or more target(s) and are utilized by the copier/subscription components.

- **SingleTargetApplier**: For standard (non-sharded) migrations to a single target database.
- **ShardedApplier**: For migrations to Vitess-style sharded databases, where rows are distributed across multiple targets based on a hash function.

Both implementations share a common interface but have different internal architectures to handle their respective use cases efficiently.

## History

The original implementation of Spirit relied on statements such as `INSERT .. SELECT` and `REPLACE INTO`, sending as much work as possible back to MySQL for processing. We now refer to this implementation as the _unbuffered_ algorithm.

The unbuffered algorithm has the advantage that there are fewer edge cases to handle that can corrupt data (accidental, charset/timezone conversions), and it does not send as much data across the network (which takes CPU cycles from both MySQL and Spirit to process). It has two downsides:

1. `INSERT .. SELECT` statements are locking, and do not use MVCC on the SELECT side.
2. It cannot be used to ship data between MySQL servers, for example in move/copy operations.

The first downside can be mitigated by using smaller chunks to yield the lock periodically, but there is no option to address the second.

Appliers were created to support an algorithm which we refer to as _buffered_, which is an implementation of [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b). Changes are extracted from the source table(s), and then sent to the applier to be loaded into an underlying target.

The _buffered_ copier is now the default for schema changes; passing `--unbuffered` opts back into the legacy unbuffered copier. The applier is used by the buffered copier and is also always used by the replication client's `bufferedMap` subscription, which writes row images directly from the binlog instead of issuing `REPLACE INTO ... SELECT` (see [issue #746](https://github.com/block/spirit/issues/746)). For move operations only the _buffered_ copier is supported.

## Why an Applier Abstraction?

Applier is an abstraction which encompasses all changes that can be applied to a target. The advantage of having an interface for this is:

1. **Complex Scenarios**: Abstract away resharding operations and other complex topologies.
2. **Future Targets**: Support non-MySQL targets or targets with different performance characteristics.

We do not intend for Spirit to support schema changes on anything other than MySQL, but it could in future be possible to use it to synchronize data between MySQL and a downstream such as PostgreSQL or Iceberg.

The applier layer provides several critical functions:

1. **Optimal Batching**: Rows are split into "chunklets" that respect both MySQL's `max_allowed_packet` limit and optimal write sizes
2. **Parallel Processing**: Multiple write workers fan-out and process chunklets concurrently for ideal use of group commit.
3. **Async Feedback**: Callers are notified via callbacks when writes complete, allowing the copier to advance its watermark.
4. **Mixed Operations**: Supports both async bulk copying (`Apply`) and synchronous operations (`DeleteKeys`, `UpsertRows`) needed by the subscription.

Without the applier layer, the copier would need to handle all of this complexity itself, making the code harder to maintain and test. The copier is agnostic to sharded migrations.

## Core Concepts

### Chunklets

A "chunklet" is an internal batching unit used by appliers. This is **different** from the "chunk" concept in `pkg/table/`, which refers to the range of rows the copier reads from the source table.

When the copier calls `Apply()` with a batch of rows (typically from one chunk), the applier splits those rows into smaller "chunklets" for writing. Each chunklet defaults to:

- **Row count**: Maximum 4,096 rows per chunklet
- **Size**: Maximum 2 MiB of estimated data per chunklet

The size limit exists because MySQL's `max_allowed_packet` is typically 64 MiB by default, and 2 MiB stays well clear of it even though the estimate is rough — `estimateRowSize` measures `fmt.Sprintf("%v", v)` while the rendered literal comes from `datum.String()`, which is larger for anything quoted or hex-encoded.

The intent is that **the byte cap is the one that normally binds**. The chunklet is the unit of nearly everything on the write path — one statement, one completion, one handoff through the single `feedbackCoordinator` — so its size sets how much per-chunklet overhead is paid per row copied. The row cap was previously 1,000, which cut first for any table narrower than about 1 KiB/row (measured: ~928 rows per chunklet against a 476–660 byte row), meaning chunklet size was decided by a row count unrelated to the cost of the statement. Sizing by bytes gives a wide table fewer rows per statement and a narrow one more, which matches the shape of the work. `Stats().RowsPerChunklet` reports which cap is actually in force — a value at the row cap means raising the byte cap alone would be a no-op, and vice versa.

One bound to respect if these are retuned: chunklets have to keep the write pool fed, so `chunks-in-flight × chunklets-per-chunk` must stay above the write worker count. With a 16 MiB target chunk and ~22 chunks in flight there is room for roughly 8x the current byte budget before large pools begin starving.

**Important**: A single row can exceed the byte budget by itself. In this edge case, the row will be placed in its own chunklet regardless of size, relying on `max_allowed_packet` being large enough. This is rare in practice.

### Async vs Sync Operations

The applier interface provides both asynchronous and synchronous methods:

**Asynchronous (used by copier)**:
- `Apply(ctx, chunk, rows, callback)`: Queues rows for writing and returns immediately. The callback is invoked when all rows have been written.

**Synchronous (used by subscription)**:
- `DeleteKeys(ctx, sourceTable, targetTable, keys, lock)`: Deletes rows by primary key and waits for completion. Emits `DELETE FROM target WHERE (pk) IN (...)`.
- `UpsertRows(ctx, mapping, rows, lock)`: Upserts rows using a `ColumnMapping` and waits for completion. Emits `REPLACE INTO target (cols) VALUES (...)` — see [REPLACE INTO semantics](#replace-into-semantics-and-eventual-consistency) below.

This distinction exists because:
- The **copier** processes large batches of rows and benefits from async processing with callbacks to advance its watermark.
- The **subscription** processes individual binlog events and needs immediate confirmation that changes have been applied before advancing the binlog position.

### REPLACE INTO semantics and eventual consistency

`UpsertRows` uses `REPLACE INTO target (cols) VALUES (...)`, not `INSERT ... ON DUPLICATE KEY UPDATE`. Per MySQL's manual:

> REPLACE works exactly like INSERT, except that if an old row in the table has the same value as a new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted before the new row is inserted.

Two consequences for callers:

1. **REPLACE may delete rows whose PKs are not in the `rows` argument.** If a new row's image collides on a unique key with some *other* row currently in the destination (the previous holder of that unique value), REPLACE deletes that other row to make room. A single REPLACE statement may therefore delete more than one row. This is what makes the multi-row VALUES list order-independent — within a batch, every row's conflicts (on PK or any unique index) are resolved before its insert runs.

2. **The destination is only eventually consistent with source mid-flush.** Between the moment REPLACE deletes a row to resolve a unique-key conflict and the moment that row's own event re-inserts it, the destination is briefly missing that row. Spirit relies on the replication client's `bufferedMap` being an up-to-date and *disjoint* representation of pending changes — every PK in the buffer holds the latest image MySQL has emitted for it — so any transiently-deleted row is guaranteed to be re-inserted as flushes progress. The destination converges back to source's current state once every event for each affected PK has been applied. The post-cutover checksum (with `FixDifferences=true`) is the backstop for any divergence that survives.

The row image is supplied inline (the binlog reader stored it on `HasChanged`); the applier never re-reads source. This avoids the binlog/visibility race fixed in [#746](https://github.com/block/spirit/issues/746) that earlier `REPLACE INTO ... SELECT` paths could lose to.

#### Why this matters for workloads that move unique values

The motivating case is a source-side transaction that legally moves a unique value between two rows:

```sql
START TRANSACTION;
UPDATE t SET slot_id = NULL WHERE id = 1;  -- was 'S'
UPDATE t SET slot_id = 'S'  WHERE id = 2;  -- was NULL
COMMIT;
```

With `INSERT ... ON DUPLICATE KEY UPDATE`, the random map iteration order in the subscription could land "activate id=2" before "deactivate id=1" in the same multi-row statement; MySQL would resolve id=2's UPDATE branch, then fail with `Error 1062 (23000): Duplicate entry 'S'` because id=1 still held the value. With `REPLACE INTO` the same batch in any order works: each REPLACE deletes the prior holder of `'S'` before inserting its own row. See [block/spirit#847](https://github.com/block/spirit/issues/847).

### Callbacks and Feedback

When the copier calls `Apply()`, it provides a callback function:

```go
callback := func(affectedRows int64, err error) {
    if err != nil {
        // Handle error
        return
    }
    // All rows have been written, advance watermark
    chunker.Feedback(affectedRows)
}
applier.Apply(ctx, chunk, rows, callback)
```

The applier tracks all pending work internally and invokes the callback only when:
1. All chunklets for that batch have been written.
2. OR an error occurs in any chunklet.

This allows the copier to continue reading and queuing more work without blocking, while still maintaining correctness by only advancing the watermark after writes complete.

### Pipeline observability (`Stats()`)

Both appliers expose a point-in-time `Stats()` snapshot (see `stats.go`): queue depth/capacity, pending work, live write workers, and rolling p50/p90 of four per-chunklet phases. This exists because the copier's chunk feedback is end-to-end — read + queue wait + write — so a saturated write side otherwise presents as a read/chunker problem. A queue pegged at capacity with queue-wait far above write time means the pipeline is write-limited; a near-empty queue means it is read-limited.

The four phases together account for a write worker's whole cycle, which is what makes the follow-up question answerable — *given* the write side is the limit, which part of it?

| Phase | What it measures | What a large value means |
| --- | --- | --- |
| **queue wait** | `Apply()` offering a chunklet to the buffer until a worker dequeues it, including send-side backpressure | Workers cannot keep up with the copier (or are blocked further down) |
| **build time** | Client-side statement construction: a datum conversion and string format per value, so it scales with rows × columns | Spirit's own CPU is the limit. No server-side signal reports this, and more write workers cannot fix it. Contained *within* write time, not additional to it |
| **write time** | Build plus the round trip to the target(s), including retry backoff | Subtract build time to get time actually at the server |
| **handoff** | Publishing the completion after the write finished | Workers are queued behind the single `feedbackCoordinator`, which invokes the chunk callback inline — so one slow callback backs up every worker at once |

The distinction matters because only *write time minus build time* is the target's write capacity. A pipeline that stops responding to added write workers looks identical in the aggregate whether the ceiling is the server, spirit's CPU, or the completion path; these four separate those cases. Note that a worker holds no connection during build or handoff, so `conns-in-use` on the runner status line sitting well below the worker count is the same observation from the other side.


## Implementation Details

For detailed information about the SingleTargetApplier and ShardedApplier implementations, see the inline code documentation in `single_target.go` and `sharded.go`.

The ShardedApplier has an important limitation: it only tracks changes by `PRIMARY KEY`, not by sharding column (vindex). This means DeleteKeys and UpsertRows must broadcast to all shards, and the vindex column must be immutable. See `sharded.go` for details.
