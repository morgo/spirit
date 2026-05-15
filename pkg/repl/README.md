# Replication Client

The replication client tracks changes to tables by acting as a MySQL replica. The [go-mysql library](https://github.com/go-mysql-org/go-mysql) does most of the heavy lifting by connecting to MySQL and parsing binary log events. Spirit's role is to manage subscriptions for each table being migrated, deduplicate changes, and coordinate with the copier to avoid redundant work.

Each table tracked is represented by a `subscription`. There is a single
subscription type — the **buffered map** — that stores the full row image
from the binlog and applies it through the applier. For non-memory-comparable
primary keys it falls back to a FIFO queue *internally* once the watermark
optimization is disabled, but row images are still preserved and the applier
path is still used.

## Subscription Implementation

### Background

Earlier versions of Spirit shipped two subscription types side-by-side: a `deltaMap` that stored only primary-key hashes (and re-read row state from the source via `REPLACE INTO ... SELECT` at flush time), and a `deltaQueue` that preserved binlog order for non-memory-comparable PKs. The split caused [issue #746](https://github.com/block/spirit/issues/746): MySQL's binlog-vs-visibility ordering meant that the deltaMap path could read a stale row image when its `SELECT` raced ahead of the row's commit visibility, applying the wrong final state.

The fix was to unify everything around a single subscription type — the buffered map — that captures the **full row image** from the binlog directly, so the applied state is the binlog state and the source-side `SELECT` race is gone. The deltaMap and deltaQueue types were removed entirely; the FIFO behaviour previously provided by deltaQueue now lives inside bufferedMap as an internal mode for non-memory-comparable PKs (see below).

### Buffered Map

The buffered map stores the full row image directly from the binlog and
applies it through the applier interface:

**How it works:**
- Maintains a map of `primaryKeyHash -> (isDelete, fullRowImage)`.
- Multiple changes to the same row are automatically deduplicated (only the
  final state is stored).
- Uses the applier's `UpsertRows` and `DeleteKeys` to write changes — there
  is no `SELECT FROM original` round-trip.
- Flushes changes through the applier's parallel write workers.

**Advantages:**
- **Excellent deduplication**: if a row is modified 100 times, only one upsert is performed.
- **Parallel flushing**: independent keys can be written concurrently via the applier.
- **No source-side reads at flush**: the row image is already in memory, so no contention with OLTP traffic on the source.
- **Sidesteps the binlog/visibility race**: because the row image *is* the applied state, there is no opportunity for MySQL's binlog-vs-visibility ordering to surface a stale row (see [issue #746](https://github.com/block/spirit/issues/746)).
- **Watermark optimization (when supported by the chunker)**: can skip ranges of keys using both `KeyAboveHighWatermark` and `KeyBelowLowWatermark`.
- **Cross-server compatibility**: the applier can target a different MySQL server, which is what `pkg/move` relies on.

**Limitations:**
- Requires `binlog_row_image=FULL` and an empty `binlog_row_value_options` (the applier needs the complete row image).
- Higher memory usage than a key-only map: stores full row data for each changed key.
- Watermark optimizations (`KeyAboveHighWatermark` and `KeyBelowLowWatermark`) are available on `MappedChunker` implementations (both optimistic and composite chunkers). They work correctly for numeric, binary, and temporal primary key types. For `VARCHAR`/`TEXT` columns with collations, Go's byte-order comparison may differ from MySQL's collation order; any discrepancies are caught by the checksum phase (see [issue #479](https://github.com/block/spirit/issues/479)).

**The map is optimistic.** It bets that the randomized iteration order of
`s.changes` is harmless because each row's upsert depends only on its own
PK. That bet holds for the overwhelmingly common case, but it breaks for
workloads that move a unique value between rows inside a single
transaction — see "Optimistic batching and the FIFO fallback" below.

**Example scenario:**
```
Binlog events:  INSERT(id=1, ...), UPDATE(id=1, ...), UPDATE(id=1, ...), DELETE(id=2)
Buffered map:   {1: {row: <latest image>}, 2: {isDelete}}
Applied:        UpsertRows({id=1, ...}); DeleteKeys({id=2});
```

#### FIFO fallback for non-memory-comparable primary keys

For tables with non-memory-comparable primary keys (e.g. `VARCHAR` with a
case-insensitive collation), the subscription uses LWW buffered-map dedup
during the copy phase and switches to an internal FIFO queue post-copy.
The queue still stores row images inline and applies them via the
applier — there is no `REPLACE INTO ... SELECT`, so the #746 fix and
cross-server move support ([issue #607](https://github.com/block/spirit/issues/607))
are preserved. The queue exists only to preserve binlog order:
collation-equivalent keys like `"A"` and `"a"` hash to different map slots
but resolve to the same MySQL row, so a map's non-deterministic iteration
would apply events out of order. FIFO replay through the applier preserves
binlog order; the target's own collation-aware uniqueness then collapses
the events onto the right row.

During the copy phase the chunker's own SELECT covers in-window
case-collision races, so LWW map dedup is safe and considerably faster.
When the watermark optimization is disabled at the end of the copy phase,
`SetWatermarkOptimization` drains the map inline and the subscription
switches into queue mode for the cutover/checksum window. The
post-cutover checksum (with `FixDifferences=true`) repairs any residual
divergence.

Memory-comparable PKs always use the buffered map, since map-key
equality matches MySQL row identity.

#### Optimistic batching and the FIFO fallback (#847)

The buffered map flush builds one multi-row
`INSERT ... ON DUPLICATE KEY UPDATE` per batch. MySQL processes the
`VALUES` list in array order, so the order of rows inside the batch
matters whenever two rows in the same batch can collide on a unique
key. The map's iteration is randomized, which is fine for the common
case but is **unsafe for "swap" patterns**:

```sql
-- Legal in source: deactivate one row, then activate another,
-- inside a single transaction. UNIQUE(slot_id) allows NULLs to
-- duplicate, so the invariant holds.
START TRANSACTION;
UPDATE t SET slot_id = NULL  WHERE id = 1;  -- was 'S'
UPDATE t SET slot_id = 'S'   WHERE id = 2;  -- was NULL
COMMIT;
```

The two `UPDATE` binlog events can land in the same flush batch. If
the map's random iteration places id=2 (activate) before id=1
(deactivate), MySQL processes id=2's upsert first while id=1 still
holds `'S'` in the shadow table and aborts with
`Error 1062 (23000): Duplicate entry ...`.

The subscription recovers automatically:

1. `handleFlushError` recognises the 1062, clears the pending map,
   flips an internal `forceQueueMode` flag, and asks the client to
   rewind. The flush is reported as a no-op so `flushedPos` is not
   advanced.
2. `Client.requestRewind` sets a flag and closes the current binlog
   syncer. `readStream` sees the closed syncer plus the rewind flag
   and shortcuts straight into `recreateStreamer`, skipping the
   consecutive-error backoff that exists for stream-level read
   failures.
3. `recreateStreamer` restarts the binlog reader at position 4 of
   the `flushedPos` file (the start of the binlog file containing
   the last fully-flushed position). Position 4 is required so the
   syncer picks up the file's `FormatDescriptionEvent` and
   `TableMapEvent`s — MySQL does not re-send TableMaps mid-file, so
   restarting at any later offset would leave the parser unable to
   decode subsequent `RowsEvent`s.
4. `readStream` filters out events ≤ `flushedPos` on the way in
   (`Client.eventAlreadyFlushed`). Those events were already applied
   to the destination by a prior flush; replaying them against the
   now-newer destination state would overwrite correct rows with
   stale row images and can recreate the very 1062 collision the
   rewind was meant to resolve (e.g. an old "activate" event for
   row A while the destination's current state has row B activated
   for the same unique-key bucket). Only events strictly past
   `flushedPos` enter the subscription's queue.
5. The remaining events flow into the now queue-mode subscription in
   binlog (FIFO) order. `flushQueueLocked` carries that order into
   the multi-row upsert, so the swap pair lands deactivate-first
   and no longer collides. The subscription stays in queue mode for
   the rest of the migration; we don't flip back, since one
   observed swap means the workload is likely to produce more.

Tables whose workloads never produce a swap never trip the recovery
path and keep the full dedup benefit of map mode. The cost of the
recovery is one wasted flush and a re-read of every event since the
last full flush; the destination ends up byte-identical to source
because the re-applied events are idempotent for already-applied
rows and correct for the unapplied tail.

See `TestBufferedMapSwapPairRecoversViaQueueMode` (unit) and
`TestSwapPairFullRecoveryViaQueueMode` (end-to-end) for the
regression gates.

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
if chunker.KeyAboveHighWatermark(key[0]) {
    return  // Skip, copier will handle this
}

if !chunker.KeyBelowLowWatermark(key[0]) {
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

### Memory backpressure

Each subscription approximates the bytes it is holding in memory (row image + key bytes per buffered change) and parks `HasChanged` on a per-subscription condition variable when the total reaches `DefaultSubscriptionSoftLimitBytes` (256 MiB). This keeps wide rows — LONGTEXT, BLOB, large JSON — from OOMing the migrator when the source's write rate outpaces the applier.

The cap is **soft**: the wait is checked *before* a change is added, against the buffer's current pre-add size. A row is therefore always admitted whenever `sizeBytes < softLimitBytes`, even if its own size pushes the total well past the limit; the cap only blocks *new* arrivals once the buffer is already at or over it. This is intentional — it preserves forward progress regardless of row width — but it does mean peak memory can exceed `DefaultSubscriptionSoftLimitBytes` by up to one oversized row's worth before the next caller parks.

Override via `ClientConfig.SubscriptionSoftLimitBytes`; pass a negative value to disable the cap entirely. The `times_parked_on_soft_limit` and `size_bytes` fields appear in the watermark-toggled log line, and `keys_added` / `keys_dropped_above_high` / `keys_skipped_not_below_low` provide the surrounding context.

**Limitation — binlog retention:** while parked, the binlog reader makes no progress. If the source rotates past the reader's current position (`binlog_expire_logs_seconds`) before the buffer drains, the reader will fail to resume and the migration will abort. Tune the soft limit and source retention together for sustained high-write workloads.

### Other Minor Features

- **Automatic recovery**: Handles transient errors and reconnects to the binlog stream without data loss
- **DDL detection**: Monitors for schema changes and notifies the migration coordinator. This is used to abandon any schema changes if the table was externally modified.

## See Also

- [Applier Package](../applier/README.md) - Handles writing changes to target tables
- [Table Package](../table/README.md) - Provides chunker interface for watermark optimization
- [go-mysql Library](https://github.com/go-mysql-org/go-mysql) - Binary log parsing library
