# Checksum

Checksums validate data consistency between two tables. During schema changes, this means comparing the original table with its `_new` counterpart. For move operations, checksums verify consistency between source and destination tables.

## Key Features

- **Column mapping**: The checksum uses `ColumnMapping` to determine which columns to compare between source and target tables. This handles the intersection of non-generated columns, column renames, and type casting automatically.
- **Type normalization**: A `CAST` operation converts columns to a comparable type before comparison. This enables comparisons when data types have changed and their string representations differ (e.g., `TIMESTAMP` vs. `TIMESTAMP(6)`).
- **Automatic repair**: When inconsistencies are detected, the checksum automatically repairs differences by recopying affected chunks.
- **Parallel execution**: Checksums process chunks concurrently across multiple threads for efficient handling of large tables. The worker count is resizable while a pass runs — see [Pacing and scaling](#pacing-and-scaling).
- **Consistent snapshot**: A brief table lock establishes a consistent snapshot before being released. The checksum remains immune to concurrent modifications during execution.
- **Server-side execution**: The checksum computation is pushed down to MySQL, with each chunk returning only a CRC32 value and row count to Spirit. This minimizes network overhead and is significantly more efficient than approaches that extract all data for client-side comparison.

## Why Checksums Matter

Checksums are a **defensive feature against bugs**. While Spirit is designed to correctly copy and apply data changes, subtle data corruption can occur during online operations in many ways.

Naive implementations that only compare row counts fail to catch most of these problems—validating the actual data is essential. Common issues include:

- **Trailing space handling**: Storage engines and column types may handle trailing spaces inconsistently
- **Special character mangling**: Character encoding issues can corrupt special characters during copy operations
- **Character set mishandling**: Converting between character sets (e.g., `latin1` → `utf8mb4`) can introduce subtle corruption
- **Timezone conversions**: Timestamp values may be incorrectly converted between timezones
- **Lost updates**: Race conditions or replication lag can cause updates to be missed during the copy process
- **Type conversion edge cases**: Implicit type conversions may produce unexpected results (e.g., floating point precision)
- **NULL mangling**: NULLs can be incorrectly replaced by empty strings during data operations

While we do our best to prevent such bugs, we also want to be pedantic when it comes to data integrity. In most cases we have observed that the checksum process takes about 10% of the time as the copy-rows stage, which makes it an easy cost to justify.

There are also some known cases where a checksum failure is not a bug. This includes adding a unique index on non-unique data, or a lossy data type conversion (e.g., `VARCHAR(100)` → `VARCHAR(10)` when records exist requiring more than 10 characters). Both are important cases to handle, and prevent a cutover operation from executing.

## Implementations

The checksum package contains three implementations:

1. **SingleChecker** - Compares two tables on the same MySQL server (for schema changes, or 1:1 moves)
2. **DistributedChecker** - Compares a source table against multiple distributed target databases (for sharded scenarios)
3. **ContinuousChecker** - A lock-free, *eventually-consistent* verifier that runs indefinitely against a live system where the target lags the source by a small replication delay (used by `spirit sync`, and by `spirit migrate` while waiting on a deferred cutover)

`SingleChecker` and `DistributedChecker` take a brief table lock to establish a consistent `REPEATABLE READ` snapshot; `ContinuousChecker` deliberately does not (see [Continuous checksum](#continuous-checksum) below).

All three use the same underlying checksum algorithm: **CRC32 with XOR aggregation**. This technique computes a checksum for each chunk of rows and can efficiently detect differences without comparing individual rows.

## Checksum Algorithm

The checksum is computed using (simplified version):

```sql
SELECT BIT_XOR(CRC32(CONCAT(...))) as checksum, COUNT(*) as c 
FROM table 
WHERE <chunk_range>
```

This approach:
- Computes a CRC32 hash for each row (using concatenated column values)
- Aggregates the row checksums using XOR (`BIT_XOR`)
- Provides both a checksum value and row count for verification

The actual implementation includes additional handling:
- **NULL normalization**: Uses `IFNULL()` and `ISNULL()` to ensure NULLs are consistently represented
- **Type casting**: Applies `CAST` operations to convert columns to the target table's type for comparable string representations

The CRC32 + XOR aggregate technique for table checksumming was pioneered by **pt-table-checksum** from Percona Toolkit, which established this as a reliable method for verifying data consistency in MySQL. This same approach has since been adopted by other database tools, including TiDB's data migration and verification utilities, demonstrating its effectiveness for distributed database scenarios.

## Chunk size

The checksum sizes chunks with the same dynamic sizer as the copier, but not with the same calibration, because a chunk's duration bounds different things in the two phases. A copy chunk's time is a write transaction's lifetime, so it is a latency budget. A checksum chunk's time is a read inside a snapshot that is held for the whole pass either way, and only one row crosses the wire per chunk however many rows it covers. Longer chunks are therefore nearly free, and length is what keeps a scan sequential long enough to engage InnoDB linear read-ahead and Aurora's batched prefetch.

Two things are set differently:

- **The target time** is `--checksum-target-chunk-time` (10s), not the copier's `--target-chunk-time` (500ms). See `DefaultTargetChunkTime`.
- **The starting size** is `table.MaxDynamicRowSize`, not `table.StartingChunkSize`. See `ChunkStartRows`.

The starting size is the one that mattered in practice. The sizer converges upward slowly on purpose — growth is capped at `table.MaxDynamicStepFactor` per feedback window, and a window is 10 chunks — so climbing from 1000 rows to the row cap takes roughly 130 chunks. That is more chunks than many whole tables have, which meant the checksum could spend an entire pass converging and never once use the size it had measured as correct. Starting at the cap inverts the asymmetry, and shrinking is what the sizer is good at: it shrinks with no per-step cap, and panic-shrinks on any single chunk exceeding `DynamicPanicFactor` × the target. Overshooting costs one slow chunk; undershooting cost the whole pass.

On a healthy server neither setting decides the chunk size — `table.MaxDynamicRowSize` (100k rows) does, and that is intended. The row cap is a bound that can be reasoned about: it is what bounds a repair, and what `inspectDifferences` holds in memory when a chunk mismatches. A binding *time* target, by contrast, would let chunk size follow load. The target's job is to be the safety valve for what the row cap cannot see — rows so wide, or storage so slow, that even 100k rows is too much work for one chunk. At the copier's 500ms that valve trips on ordinary tables and shrinks chunks precisely when read-ahead matters most.

Measured on a 1M-row, 6-column table (194 MB against a 128 MB buffer pool), before and after this calibration:

| | chunks | rows p50 | duration p90 | at the row cap |
| --- | --- | --- | --- | --- |
| copier's calibration | 126 | 5,062 | 30ms | 0/126 |
| checksum's calibration | 12 | 100,000 | 162ms | 10/12 |

Per-row cost at p50 is unchanged on local SSD (~1.6µs either way) — read-ahead has little left to win when the storage is this fast, and the read-ahead argument is a claim about network-attached storage that a local box cannot demonstrate. What the numbers do show is the sizing reaching its bound in two chunks instead of never, and p90 per-row cost falling with it (5.9µs → 1.6µs) as per-chunk overhead is amortised over 20× the rows.

Two costs come with the larger chunks, both bounded by the row cap. A throttle decision only takes effect between chunks (`BlockWait` is called before dispatch and chunks in flight are never abandoned), so back-off latency now tracks a chunk's duration rather than a copier's 500ms budget. And a table with fewer than `threads` × 100k rows produces fewer chunks than there are workers, so the checksum of a small table is less parallel than it was — which is affordable precisely because it is a small table.

One more consequence worth knowing: on a table with an auto-increment key, starting at the row cap means the optimistic chunker's gap-prefetch switch (`maybeSwitchToPrefetch`) now fires on the first feedback window rather than never. That is a net gain — in prefetch mode a chunk holds exactly `chunkSize` *rows* rather than `chunkSize` *key values*, so chunks stay uniform across a key space pitted with deletes — at the cost of one index-only `OFFSET` query per chunk.

## Repairing a mismatch

When a chunk fails verification and `FixDifferences` is on, the checker recopies the chunk's key range: `DELETE` then `REPLACE INTO ... SELECT` for the single-server case, or `DELETE` on every target followed by a re-apply through the applier for the distributed case.

Chunks are sized by how long they take to *read* while checksumming, which says nothing about how long they take to rewrite. Recopying a whole XL chunk to fix a handful of rows is a large write burst, a long lock hold, and a wide window in which the target range is deleted but not yet rewritten. So the repair is narrowed first (`subdivide.go`):

1. Cut the chunk's key range into `csRepairSplitParts` contiguous pieces (`table.Chunk.Split`, using `ORDER BY key LIMIT 1 OFFSET n` boundary lookups). The first and last inherit the original's bounds — inclusivity and unboundedness included — and each interior cut is an exclusive upper bound on one piece and an inclusive lower bound on the next.
2. Re-checksum each piece **inside the same snapshot transaction that observed the mismatch**, and keep only the pieces that differ.
3. Recurse on each differing piece, stopping at `csRepairMaxDepth` rounds or once a piece holds fewer than `csRepairMinSplitRows` rows — below that, recopying is cheaper than analysing.

Step 2 is sound because `BIT_XOR` is associative and the row counts are additive: over an exact partition read in one snapshot, the pieces must reproduce the whole-range result, so a mismatching range must contain a mismatching piece. Reading a piece from a *different* snapshot would break that — a piece could look clean because a concurrent write happened to fix it, and the repair would be skipped on the strength of an observation that never applied to the chunk being fixed.

That argument needs the pieces to partition the range exactly, and `Split` cannot promise it for every key type: MySQL orders `ENUM` by declaration ordinal but compares it against a string literal lexically, a case-insensitive collation orders by weight rather than by bytes, and a `NULL` key value satisfies neither `>=` nor `<`. So exactness is *checked* rather than assumed — the pieces' row counts, read in the same snapshot as the parent's, must sum to the parent's on both sides. A gap (which would let a differing row escape repair) or an overlap (which would delete and rewrite rows twice) shows up as a shortfall or an excess, and the chunk is repaired whole instead.

Everything else that makes narrowing unusable also falls back to repairing the whole range, because declining to fix a known difference is never an option: a range with too few distinct key values to cut, a failed boundary query, pieces that all verify clean, and pieces that *all* differ. The last is the systematic case — a lossy `ALTER`, a wrong column mapping, a truncated target — where narrowing has bought nothing, and one statement pair is cheaper and shorter-lived than one per piece.

A verification *failure*, by contrast, is propagated: it means the snapshot is no longer usable, so the pass should fail and be retried rather than repair on stale information. `differencesFound` is incremented before narrowing begins, so a chunk left unrepaired this way cannot be reported as a pass.

The repair's timeout (`fixChunkTimeout`) and its detachment from the caller's cancellation are owned once per chunk and shared by all its narrowed repairs. The narrowed repairs together rewrite no more than the single repair they replace, so one budget covers them — and giving each its own would multiply the span during which a fix ignores cancellation by the number of pieces.

In the distributed case the boundaries are cut from a single source shard, since offsets into one shard's rows are the only row-position information a single query can give. A chunk's key range is spread across shards by the vindex, so no shard's distribution describes the whole range and the pieces come out unevenly balanced. The shard is chosen per range rather than once per chunk: a sub-range's rows can sit almost entirely on a different shard than its parent's did, and cutting it with a shard that holds nothing in it would find no boundaries at all and silently give up on narrowing.

## Pacing and scaling

`SingleChecker` and `DistributedChecker` pace themselves against the same throttler the copier uses. Two things are separate here:

- **The hard stop** applies always. Before dispatching each chunk the checker calls `Throttler.BlockWait`, so a checksum pauses when replica lag or Aurora load says to. Chunks already in flight are never interrupted: the checksum stops *dispatching* rather than abandoning work, because an aborted chunk is wasted I/O that must be redone from the same watermark. Wire the throttler with `SetThrottler` (the `ThrottleAware` capability) — runners build the checker before their throttlers are open.
- **Scaling** is opt-in via `AutoscaleConfig`, and adjusts the live worker count during a pass. Two signals drive it:
  - The throttler's continuous **utilization** signal, applying the same zone law as the copier (see `pkg/autoscale`). Only the Aurora throttlers provide this signal, so this is where growth comes from and it is Aurora-only.
  - The **change-feed backlog**, which works everywhere. The feed flushes concurrently with the checksum, and its backlog gates cut-over — if it grows unboundedly the binlogs may be purged before a resume can replay them. If the feed is losing ground, the checksum's reads are winning a race against writes that have to finish, so a worker is shed. On stock MySQL this is the only shedding lever, and recovery is capped at the configured concurrency.

    What counts as "losing ground" is specifically a rising **post-flush residual**, not a rising backlog — and the residual is read from `change.Source.FlushResidual`, which the feed records at flush completion, rather than polled.

    Polling cannot recover this quantity. The pending count is a sawtooth: it climbs on every sample between flushes and drops when one lands, so its slope says nothing about whether the feed is coping (at 5s control tick and 30s flush interval, the rising edge alone is six samples long). Nor do window minima work, which is the subtler trap: a poll lands some offset φ after the flush and therefore reads `residual + writeRate·φ`. Because the flush interval is an exact multiple of the tick, φ is fixed for the whole pass by the arbitrary phase between two independent tickers — so on a busy table the sampling term can exceed the threshold on its own, and a *rising write rate* on a fully-draining feed produces rising apparent residuals indistinguishable from a feed falling behind.

    Reading the residual where the feed defines it removes the write rate from the signal entirely. Successive residuals are then compared across distinct flushes, with hysteresis in both directions: `csBacklogHysteresisFlushes` consecutive flushes must agree before the verdict changes. The exit condition matters as much as the entry one, because shedding is one step per flush while growth is one step per two ticks — a single favourable flush clearing the verdict would let the grows outpace the sheds and the controller would drift up while the feed fell further behind. While a verdict holds it suppresses growth as well as driving shedding.

| | hard stop | shed on backlog | grow |
| --- | --- | --- | --- |
| stock MySQL | yes | yes | no (recovers to start only) |
| Aurora + autoscaling | yes | yes | yes (utilization law) |

Concurrency is gated by a resizable `autoscale.Limiter` rather than `errgroup.SetLimit`, which may not be resized while goroutines are active.

One constraint shapes all of this: the `REPEATABLE READ` transaction pool **cannot grow** once the table lock is released. Every transaction takes its snapshot under that lock, so they all see one point in time; a transaction started later would read a newer snapshot and could compare a chunk against changes its siblings cannot see. The pool is therefore provisioned at the autoscale ceiling up front, whether or not scaling is enabled. Over-provisioning costs one connection per idle transaction and no extra history retention, since every read view pins from the same instant.

`ContinuousChecker` is not covered by any of this: it manages its own pacing through `MinPassInterval` and its retry queue, and takes no table lock or snapshot pool.

Each pass logs a `checksum chunk size distribution` line (chunk count, duration p50/p90/max, row p50/max, and how many chunks hit `table.MaxDynamicRowSize`). The row-capped count is the useful one: it is what answered the sizing question above, and with the checksum's own calibration it should now read close to all of them. A pass where it reads *low* is a pass where the time target is binding instead — meaning that table's rows are wide enough, or its storage slow enough, to be worth looking at.

## Continuous checksum

`ContinuousChecker` verifies a target that is still converging toward the source over a live replication feed, so a first-attempt mismatch is *expected* (the target simply hasn't caught up yet) rather than alarming. It runs in **passes**: each pass walks every chunk once and then drains a delayed-retry queue until empty. A mismatched chunk is re-read after a short delay and passes once the target's CRC matches a source CRC the checker has witnessed. A chunk whose source keeps changing (a "hot chunk") cycles to the back of the queue without blocking the pass.

When a chunk's source CRC is stable across the retry window but the target still disagrees, that is a **stable divergence**. How the checker reacts is governed by two config fields:

- **`Recopier`** — when set, a stable divergence is *repaired* by recopying that chunk from the source: `DELETE` the key range on the target, re-`SELECT` from the source, and re-apply through the same write path the change feed uses. `MySQLRecopier` is the production implementation used by `spirit sync`. Recopies are serialized and run under a cancellation-detached, time-bounded (10 minute) context, so a chunk is never left deleted-but-not-rewritten.
- **`DivergenceIsFatal`** — selects the policy explicitly, rather than inferring it from `Recopier` presence:
  - `true` (e.g. `spirit migrate`'s deferred-cutover check): replication keeps the new table in sync, so a confirmed stable divergence is a real bug. `Run` returns `ErrPermanentDivergence` and the caller aborts the cutover. No `Recopier` is configured.
  - `false` (e.g. `spirit sync`): the target is expected to converge, so divergences self-heal via the `Recopier`. A `Recopier` is **required** in this mode; without one, divergence is treated as fatal.

The two are decoupled: `DivergenceIsFatal: true` aborts even if a `Recopier` is supplied. Passes are paced by `MinPassInterval` so a small table is not re-checksummed back-to-back. `FirstCleanPass` exposes a channel that closes the first time a pass completes with every chunk read-verified equal and zero recopies — the signal that the target is known consistent.