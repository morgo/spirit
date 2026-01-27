# Replication Client

The replication client tracks changes to tables by acting as a MySQL replica. The [go-mysql library](https://github.com/go-mysql-org/go-mysql) does most of the heavy lifting by connecting to MySQL and parsing binary log events. Spirit's role is to manage subscriptions for each table being migrated, deduplicate changes, and coordinate with the copier to avoid redundant work.

Each table tracked is represented by a `subscription`, with three main implementations:

## Subscription Implementations

### Delta Map

The delta map is the preferred subscription type and is selected for tables with memory-comparable primary keys (integers, strings, fixed-length types, etc.). It uses a hash map to track changes:

**How it works:**
- Maintains a map of `primaryKeyHash -> (isDelete, originalKey)`
- Multiple changes to the same row are automatically deduplicated (only the final state is stored)
- Flushes changes in parallel across multiple threads
- Uses `REPLACE INTO ... SELECT` to apply changes efficiently

**Advantages:**
- **Excellent deduplication**: If a row is modified 100 times, only one REPLACE operation is performed
- **Parallel flushing**: Independent keys can be written concurrently for maximum throughput
- **Memory efficient**: Only stores the latest state for each key
- **Watermark optimization**: Supports both `KeyAboveHighWatermark` and `KeyBelowLowWatermark` optimizations

**Limitations:**
- Requires memory-comparable primary keys (no VARCHAR, FLOAT etc.)
- Watermark optimization depends on the chunker implementing `KeyAboveHighWatermark` correctly (see [issue #479](https://github.com/block/spirit/issues/479))

**Example scenario:**
```
Binlog events:  INSERT(id=1), UPDATE(id=1), UPDATE(id=1), DELETE(id=2)
Delta map:      {1: REPLACE, 2: DELETE}
Applied:        REPLACE INTO ... WHERE id=1; DELETE FROM ... WHERE id=2;
```

### Delta Queue

The delta queue is a fallback for tables with non-memory-comparable primary keys. It maintains a FIFO queue of changes:

**How it works:**
- Maintains an ordered queue of `(primaryKeyHash, isDelete)` tuples
- Changes are applied sequentially in the order they were received
- Limited deduplication: only merges consecutive operations of the same type
- Uses `REPLACE INTO ... SELECT` and `DELETE` statements

**Advantages:**
- **Universal compatibility**: Works with any primary key type (VARCHAR, FLOAT, etc.)
- **Preserves order**: Maintains the exact sequence of operations

**Limitations:**
- **Single-threaded flushing**: Must process changes sequentially, limiting throughput
- **Poor deduplication**: Stores all intermediate states, only merging consecutive operations
- **Higher memory usage**: Can accumulate many operations for the same key
- **No support for optimizations**: Key above and key below watermark optimizations are not possible

**Example scenario:**
```
Binlog events:  INSERT(id=1), UPDATE(id=1), DELETE(id=2), UPDATE(id=1)
Delta queue:    [INSERT(1), UPDATE(1), DELETE(2), UPDATE(1)]
Applied:        REPLACE INTO ... WHERE id IN (1); DELETE FROM ... WHERE id=2; REPLACE INTO ... WHERE id=1;
```

**Performance note:** The delta queue is significantly worse performing than the other two implementations. It should only be used when the primary key type makes the delta map impossible. It is so bad in fact, that in the future it is worth considering disabling it and relying on the checksum to catch issues. See [issue #475](https://github.com/block/spirit/issues/475).

### Buffered Map

The buffered map is an experimental subscription type used for move operations where source and target are on different MySQL servers. It stores full row data and uses the applier interface:

**How it works:**
- Maintains a map of `primaryKeyHash -> (isDelete, fullRowData)`
- Stores complete row data in memory, not just primary keys
- Uses the applier interface to write changes (supports sharding and remote targets)
- Flushes changes in parallel using the applier's batching logic

**Advantages:**
- **Better concurrency**: Doesn't hold locks on the source table during flush (no `SELECT` needed)
- **Supports sharding**: Can distribute changes across multiple target databases via the applier
- **Flexible targets**: Can potentially target non-MySQL destinations in the future
- **Watermark optimization**: Supports both `KeyAboveHighWatermark` and `KeyBelowLowWatermark` optimizations

**Limitations:**
- **Significantly higher memory usage**: Stores full row data for each changed key
- **Higher CPU usage**: buffering changes in the spirit daemon adds CPU load to Spirit
- **More complex**: Requires coordination with the applier layer
- **Experimental**: Not enabled by default, less battle-tested than delta map

**When to use:** Primarily for move operations where `REPLACE INTO ... SELECT` cannot be used because the source and target are on different MySQL servers.

**Enabling:** Set `UseExperimentalBufferedMap: true` in the client config. This is automatically enabled for move operations.

## Features

### Watermark Optimization

The watermark optimization is a critical performance feature that prevents the replication client from doing redundant work during the copy phase.

**The Problem:**
During the initial copy phase, the copier is reading rows from the source table and writing them to the new table. Meanwhile, the replication client is also receiving binlog events for those same rows. Without optimization, we would:
1. Copy row with `id=1000` from source to target
2. Receive a binlog event for `id=1000` (from before the copy)
3. Apply the binlog change, overwriting what we just copied
4. Result: Wasted work and potential deadlocks

**The Solution:**
The copier maintains a "watermark" representing its progress. The replication client uses this watermark to filter changes:

- **High watermark**: Skip changes for rows that haven't been copied yet (they'll be picked up by the copier)
- **Low watermark**: Skip changes for rows that are currently being copied (avoid races with the copier, which may cause deadlocks/lock waits)

```go
if chunker.KeyAboveHighWatermark(key) {
    return  // Skip, copier will handle this
}

if !chunker.KeyBelowLowWatermark(key) {
    continue  // Skip, copier is actively working on this range
}
```

**Important:** The watermark optimization is disabled before the final cutover to ensure all changes are applied regardless of the copier's position.

### Checkpointing

The replication client tracks two positions:

- **Buffered position**: All events have been read from the server and stored in memory
- **Flushed position**: All events have been successfully applied to the target table

```go
// Get the safe checkpoint position
pos := client.GetBinlogApplyPosition()

// Resume from a checkpoint
client.SetFlushedPos(savedPosition)
err := client.Run(ctx)
```

Periodically, changes are flushed to advance the flushed position, which is then used as part of checkpoints. Because all replication changes are idempotent, it is understood that on recovery some changes will effectively be re-flushed, and the last ~1 minute of progress may have been lost.

### Final Cutover coordination

Before a cutover operation can run, it's important to ensure that there are no unapplied replication changes. The best practice way to do this is to first `Flush(ctx)` without a lock, and then repeat the flush with the lock held. i.e.

```go
// Ensure most changes are up to date before we need to do this again
// with a lock held (ensures lock duration is as short as possible)
err = client.Flush(ctx)

// Acquire table lock
lock, err := dbconn.LockTable(ctx, db, sourceTable)

// Flush all remaining changes under the lock
err = client.FlushUnderTableLock(ctx, lock)

// This check should be redundant, but we verify everything is applied
if !client.AllChangesFlushed() {
    return errors.New("changes still pending")
}

// Safe to cutover now
```

The `client.Flush()` will retry in a loop until the number of pending changes is considered trivial (currently <10K). It is important to handle errors correctly here, because `FlushUnderTableLock` may fail if it can't flush the pending changes fast enough. This is your cue to abandon the cutover operation for now, and try again when the server is under less load.

### Other Minor Features

- **Automatic recovery**: Handles transient errors and reconnects to the binlog stream without data loss
- **DDL detection**: Monitors for schema changes and notifies the migration coordinator. This is used to abandon any schema changes if the table was externally modified.

## See Also

- [Applier Package](../applier/README.md) - Handles writing changes to target tables
- [Table Package](../table/README.md) - Provides chunker interface for watermark optimization
- [go-mysql Library](https://github.com/go-mysql-org/go-mysql) - Binary log parsing library
