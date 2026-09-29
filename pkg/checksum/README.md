# Checksum

Checksums validate data consistency between two tables. During schema changes, this means comparing the original table with its `_new` counterpart. For move operations, checksums verify consistency between source and destination tables.

## Key Features

- **Column mapping**: The checksum uses `ColumnMapping` to determine which columns to compare between source and target tables. This handles the intersection of non-generated columns, column renames, and type casting automatically.
- **Type normalization**: A `CAST` operation converts columns to a comparable type before comparison. This enables comparisons when data types have changed and their string representations differ (e.g., `TIMESTAMP` vs. `TIMESTAMP(6)`).
- **Automatic repair**: When inconsistencies are detected, the checksum automatically repairs differences by recopying affected chunks.
- **Parallel execution**: Checksums process chunks concurrently across multiple threads for efficient handling of large tables. The worker count is resizable while a pass runs — see [Pacing and scaling](#pacing-and-scaling).
- **Consistent snapshot or optimistic reads**: `SingleChecker` takes a brief table lock to establish a consistent snapshot, so it is immune to concurrent modifications. `LocklessChecker` takes no lock and re-reads a chunk until it matches (see [Lockless checksum](#lockless-checksum)).
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

The checksum package contains two implementations:

1. **SingleChecker** - Compares two tables on the same MySQL server (the default for schema changes)
2. **LocklessChecker** - An optimistic verifier using ordinary reads and retries. It compares one or more sources against one or more targets, aggregating each chunk across all of them, so it also serves moves (N sources routed onto M targets) and `spirit sync` (a target on another server). One checker serves both halves of the contract over the same pass loop: `Run` returns once a pass has verified the whole table, and `RunContinuous` keeps passing in the background. Used by `spirit move`, `spirit sync`, and experimental lockless migrations.

`SingleChecker` takes a brief table lock to establish a consistent `REPEATABLE READ` snapshot; `LocklessChecker` deliberately does not (see [Lockless checksum](#lockless-checksum) below).

### Direction: lockless replaces single

We intend to replace `SingleChecker` with `LocklessChecker`, making lockless the only checksum. Move and sync already use only lockless. Migration still defaults to `SingleChecker`, with lockless behind `--enable-experimental-lockless-checksum`, until lockless has enough production evidence to become the default for migrations as well.

The code is arranged so that removing `SingleChecker` is mostly deletion:

- Everything only `SingleChecker` uses lives in `single*.go`: the checker, its topology check and constructor, the snapshot-resume guard, the chunk-size observer, the CRC/count comparison, the continuous-snapshot loop, and `YieldTimeout` handling.
- `NewChecker` has one `CheckerConfig.Lockless` branch. Removing single means deleting those files, that branch and the `Lockless` field, plus the "Single-only" section of `CheckerConfig`.
- Everything else (`Checker`, `CheckerConfig`, the recopiers, autoscaling, row-difference logging) is shared, and is written against the `Checker` contract rather than a concrete type.

Both use **CRC32 with XOR aggregation** for chunk comparison. The lockless checker can additionally drain a bounded per-row PK/CRC32 snapshot for unresolved hot ranges.

## Comparing the two algorithms

The two checkers are not a fast one and a careful one. They prove **different
statements**, and neither statement implies the other. This section is the
argument for why, what each one costs, and where each one is blind.

### The premise: Spirit owns the target

Everything below rests on one fact about the topology, and it is easy to read
past: **Spirit is the only writer to the target table.** The `_new` table, or
the destination of a move or a sync, is created by Spirit and written only by
the copier and the change feed. No application touches it.

That asymmetry is why "lockless" is possible at all. The source cannot be held
still without a lock, but the *target* can: it only moves when Spirit flushes.
Three consequences run through the whole algorithm:

- **A target read is a read of something Spirit put there.** Between flushes
  the target literally cannot change, which is why a retry waits for a feed
  flush (`RetryFlushWait`) instead of just a delay — re-reading before one
  lands could only return the image it already saw.
- **An old source image stays a valid comparison point.** The retry rule
  "target now equals a source CRC we witnessed earlier" is sound only because
  nothing else writes the target: the target's state is a function of the
  source's history as delivered by the feed, so reaching a witnessed image
  means the pipeline carried that image faithfully. If a third party could
  write the target, matching a stale source image would prove nothing.
- **Parking the feed's reader freezes the target completely.** This is what
  [settling](#continuously-updated-hot-rows) exploits. It cannot stop writes to
  the source, so it stops them to the target instead, and compares against the
  after-image the change stream carries — which, with
  `binlog_row_image=FULL`, *is* the source's value at that position.

The third point is the one worth holding onto: for the rows where it matters,
lockless does not weaken the consistency claim, it **recovers a genuine
point-in-time comparison** — by freezing the side it is allowed to freeze
rather than the side it is not. What it gives up is not the guarantee; it is
the guarantee holding at *one instant table-wide*.

### The algorithms, side by side

```
 SingleChecker (snapshot)             LocklessChecker (optimistic)
 ───────────────────────────────      ───────────────────────────────────────
 1. flush the change feed             1. read the chunk from source and
 2. LOCK TABLES                          target, no lock, no transaction
 3. flush again, under the lock       2. CRCs equal?      -> chunk verified
 4. assert the feed is empty          3. not equal? re-read after RetryDelay
 5. open N REPEATABLE READ trx           (5s) and one feed flush
    (all see the same instant)        4. target caught up to a source image
 6. UNLOCK TABLES                        we have witnessed? -> verified
 7. read every chunk inside           5. source changed again? -> "hot":
    those transactions                   split the range and recurse, down
 8. release the transactions              to ~128 rows
    at the end of the pass           6. source stable, target still wrong?
                                         -> drain the feed and re-read;
                                            repair the chunk, or fail
                                     7. source never holds still, 10 times
                                         over? -> settle each row against
                                            the change stream itself
```

Step 7 of the lockless column is the interesting one and has its own section
([Continuously updated hot rows](#continuously-updated-hot-rows)). Everything
above it is retry and subdivision; it is what makes the algorithm terminate at
all on a row that is written continuously.

### What each one proves

**Snapshot.** Every chunk is read inside a transaction whose read view was
taken at one instant, `T0`, behind the table lock. The reads are spread over
the wall clock — hours, on a large table — but they all observe `T0`:

```
                 T0 = the lock instant
                  │
  app writes ─────┼─────────────────────────────────────────────────►
                  │
                  │   all reads below see the table as it was at T0
                  ▼
   chunk A        ├─read─┤
   chunk B        │      ├─read─┤
   chunk C        │             ├─read─┤
   chunk …        │                    ├──  …  ──┤
                  │                              │           │
                  └─────── one pass (hours) ─────┘           │
                                                 └───────────┘
                                                  not covered by
                                                  the claim
```

> **Claim:** at instant `T0`, source and target were equal — everywhere, at
> once.

The weakness is *which* instant. `T0` is where the pass **begins**, so by the
time the pass ends the claim can be many hours old, and the gap between `T0`
and cut-over is entirely uncovered.

**Lockless.** Each chunk is read whenever a worker gets to it, and re-read
until it agrees. There is no shared instant:

```
  app writes ────────────────────────────────────────────────────────►

   chunk A    ├r┤✗ ···wait··· ├r┤✓
   chunk B         ├r┤✓
   chunk C            ├r┤✗ ··· ├r┤✗ ··· ├r┤✗ ··· ├─settle─┤✓
   chunk D                 ├r┤✓
                    ▲   ▲            ▲                ▲     │      │
                    tA  tB           tD               tC    │      │
                                                             └──────┘
       every ✓ is its own instant — there is no single T      not covered
```

> **Claim:** for every chunk there exists an instant at which source and
> target were equal. Those instants differ per chunk.

The weakness is that it is a per-chunk claim, not a table-wide one. The
strength is that every one of those instants is *later* than `T0` would have
been — the last chunk is verified minutes before cut-over rather than hours
after the snapshot that vouched for it.

**Neither claim reaches cut-over.** Both diagrams end with an uncovered
window, and both windows are the same kind of gap: a divergence introduced
after a chunk was verified is caught by neither. This is the load-bearing
point about what a checksum is for — see
[Why checksums matter](#why-checksums-matter). It is a **bug detector**, not a
serialization point. Cut-over is safe because the change feed is correct; the
checksum exists to catch the case where it is not.

### The blind spot lockless has and snapshot does not

A row whose **primary key changes** can cross from one chunk into another
between the two reads. If the earlier range is read before the move and the
later one after it, a row the target lost is missing from both reads:

```
  source:   id=7 lives in chunk C  ──┬──►  id=7 lives in chunk A
  target:   id=7 lives in chunk C  ──┴──►  id=7 gone (the bug)
  ──────────────────────────────────────────────────────────────────►
                      │              │            │
   chunk A read ──────┘              │            │
     source: no id=7 yet             │            │
     target: no id=7 yet   ✓ equal   │            │
                                     │            │
                       UPDATE t SET id=7 WHERE …──┘
                       feed applies the delete half, loses the insert
                                                  │
   chunk C read ──────────────────────────────────┘
     source: id=7 moved out
     target: id=7 moved out         ✓ equal

                       → the pass is clean, and id=7 is missing
```

Chunks are walked in key order, so "the low range first, the high range later"
is the *normal* order, not a contrived one. Under a snapshot this cannot
happen: at `T0` the row is in exactly one range, and both sides of that range
are read at `T0`, so the loss mismatches there.

Three things bound it in practice, none of which eliminate it:

- The exposure per row is the interval between the two chunk reads, not the
  length of the pass. More workers narrows it.
- `RunContinuous` re-walks from the start, and the next pass reads both ranges
  after the move. The **finite** pre-cut-over gate is where a single pass has
  to carry the weight.
- A PK update is two events in the change feed, and losing exactly one of them
  is the bug class this misses — not a wholesale copy failure, which shows up
  in every chunk it touches.

### Cost and operational profile

| | `SingleChecker` (snapshot) | `LocklessChecker` (optimistic) |
| --- | --- | --- |
| Proves | one instant, whole table | one instant per chunk |
| Instant is | the *start* of the pass | spread across the pass, each as late as its chunk |
| Read isolation | `REPEATABLE READ`, pinned pool | `READ COMMITTED`, ordinary reads |
| Locks | brief `LOCK TABLES` on every table | none |
| InnoDB history list | grows for the whole pass (read views pin undo); `YieldTimeout` (24h default) exists only to bound this | no growth |
| Query stalls | every query queues behind the metadata lock, and connection pools fill head-of-line while it is held | none |
| Concurrency ceiling | fixed at construction — the pool cannot grow once the lock is released, and the ceiling lengthens the lock window in proportion | resizable mid-pass |
| Busy table | one pass, regardless of write rate | extra reads: retries, subdivision, settling |
| Blind to | nothing within `T0` | a row whose PK moves between two chunk reads |
| Cross-server / N sources | impossible — a lock and a snapshot cannot span servers | supported |
| Implementation | simple | substantially more complex |
| Used by | `spirit migrate` (default) | `spirit move`, `spirit sync`, `spirit migrate --enable-experimental-lockless-checksum` |

The two rows worth dwelling on are the ones that make snapshot unusable in
places rather than merely expensive:

- **A snapshot cannot span servers or shards.** A table lock and a
  `REPEATABLE READ` read view are per-server. `spirit move` (N sources onto M
  targets) and `spirit sync` (a target on another server) have no snapshot
  available to them at all, which is why they are lockless-only. The same is
  true of a source fronted by a Vitess vtgate.
- **The lock is brief; the stall it causes is not.** `LOCK TABLES` waits for
  in-flight statements and then blocks new ones behind a metadata lock. Every
  blocked query holds its connection, so an application's pool drains
  head-of-line, and recovery outlasts the lock itself.

### Hot rows, and the lag question

The honest concern about lockless on a continuously written table is a
feedback loop: settling a hot row parks the change-feed reader, parking raises
apply lag, more apply lag means more chunks read stale and mismatching, and
more mismatches mean more hot ranges to settle.

The shape is real, and the cost is real: settling deliberately stalls apply in
order to freeze the target (see
[the premise](#the-premise-spirit-owns-the-target)). But it is only paid for
**hot rows, and hot rows cannot be unlimited.**

That is not an assumption, it is arithmetic. A row is hot because it is
rewritten inside the retry window faster than the checksum can read it. For
a *range* to stay hot it has to change every window, and subdivision has
already cut it toward ~128 rows — so N hot ranges demand write throughput
proportional to N, on the same server, against distinct key ranges:

```
  write throughput on the source
        │
        ├──► bounds how many distinct ranges can change every ~5s
        │          = bounds the number of hot ranges
        │
        └──► is also what the change feed has to apply
                   = a write rate high enough to make most of the table
                     hot fails the migration on feed lag first, long
                     before it fails on settling
```

Both arms come from the same finite budget, which is why "most of the table is
hot" is not a reachable state. And each hot row has to be observed equal
**once** — settling is one-off work per row, not a standing tax.

What bounds it in the implementation:

- Settling is the **last** resort, not the first. A range reaches it only
  after `MaxHotAttempts` (10) observations of a moving source, and only after
  subdivision has already cut it toward ~128 rows.
- The whole escalation for a range is bounded at `settleBudget` (5 seconds),
  rows are settled one at a time, and a row that produces no event inside its
  budget is deferred rather than waited on.
- Parking terminates **faster the hotter the row is** — it ends at that row's
  next change. The rows that defeat read-and-compare are the ones this
  resolves quickest.
- Nothing is re-parked once a row has passed, so apply lag incurred by
  settling is repaid rather than compounded.

What to watch, in that order: `HotChunksSettledThisPass` rising while
`HotChunksDeferredThisPass` falls is the mechanism working. **Both** rising,
pass over pass, is the loop above, and the lever is fewer checksum workers —
which the backlog signal already pulls by itself when autoscaling is enabled
(see [Pacing and scaling](#pacing-and-scaling)).

Two guards keep a non-converging table from becoming an unbounded wait rather
than an error: the finite gate stops after `MaxPasses` (10) with
`ErrVerificationUnresolved`, and the continuous loop paces passes at
`LocklessMinPassInterval` (1 hour) so a small hot table is not re-checksummed
back to back.

### Which to use

Today: the defaults. `spirit migrate` uses `SingleChecker`;
`spirit move` and `spirit sync` use `LocklessChecker` because no snapshot is
available to them. `--enable-experimental-lockless-checksum` opts a migration
into lockless.

The intended end state is lockless everywhere and `SingleChecker` deleted
(see [Direction: lockless replaces single](#direction-lockless-replaces-single)).
What is missing is not code but evidence: the snapshot checker has years of
production migrations behind it, and lockless needs enough of the same before
it becomes the default for `migrate` too. Both are kept until then.

## Checker contract

`NewChecker` returns a `Checker`: its finite `Run` succeeds only after
verification completes. `CheckerConfig.Lockless` picks which one, and nothing
else does:

| `Lockless` | checker | compares |
|---|---|---|
| `false` (zero value) | `SingleChecker` | two tables on one server, under a REPEATABLE READ snapshot taken behind a brief table lock |
| `true` | `LocklessChecker` | N sources against M targets with optimistic READ COMMITTED reads and a delayed-retry queue, taking no locks; each chunk's CRCs are XORed and its counts summed across every server |

Selecting the checker explicitly is what lets there be one `Applier`: the write
path a repair goes through is not also the checker switch.

`SingleChecker` rejects more than one source or feed rather than using the first and
ignoring the rest; it would otherwise verify one source and report the whole
topology clean. A checksum that passes by not looking is the one failure mode
worth refusing to construct. `LocklessChecker` requires one feed per source and takes
its targets from, in order: `TargetDB` (one source only), the applier's
`GetTargets`, or the lone source itself. Several sources with no target named is
an error for the same reason. Applier targets that share a handle are read once:
a chunk read carries no key range, so each would otherwise return the whole
table's rows and double the count. Sources must be distinct handles, because
each is paired with its own feed.

Both checkers are configured from the one `CheckerConfig`. The fields common to
both (concurrency, autoscaling, throttler, metrics sink, logger,
`MaxRetries`, `Watermark`) apply whichever is selected; the rest are documented
in the section for the checker they belong to, and are ignored by the other. `YieldTimeout`
is snapshot-only — lockless reads are short by construction and hold no snapshot
to yield — and the retry, splitting and pacing fields are lockless-only.

Repair policy is `FixDifferences`, for both checkers, so a caller does not
have to know which one it picked to say whether a divergence should be healed or
should abort. The factory turns it into the `Recopier` the checker repairs
through, built over the one `Applier` both share, and the presence
of that recopier *is* the policy: with one, a confirmed divergence is repaired and
verification continues; without one, a mismatch is reported as an error
(`ErrPermanentDivergence` for lockless verification). `MaxRetries` bounds whole-run attempts for both. Migration reuses the factory
result through `Checker.RunContinuous`, which owns pacing, chunker resets, feed
flushing, and safe cancellation. `ContinuousActive` reports whether a pass is running rather than
waiting for the next interval, so callers can report throttling accurately. Snapshot passes use the same configured repair/retry policy as the
initial gate. Lockless passes retain their optimistic retry/defer behavior.

Once `RunContinuous` starts, `ResumeWatermark` stays empty, including after a
clean background pass. Copy progress is retained, but a restarted migration must
repeat initial verification. This avoids interpreting a reset background walker
or a cleared per-pass mismatch counter as resume evidence.

Cross-server callers such as datasync go through the same factory. Naming a
`TargetDB` says the copy being verified is on another server, which is what
makes the factory build a repair path that reads one server and writes the
other; it is lockless-only, because a table lock and a `REPEATABLE READ`
snapshot cannot span two servers. Such a caller typically runs the feed's
periodic flush itself for the whole process rather than per run, and says so
with `ExternalFlushLoop` — otherwise every run starts and stops it, which is
what a migration and a move want.

Callers open the chunker before construction unless supplying a nonempty
`CheckerConfig.Watermark`. In that case the factory opens it at that watermark,
for both checkers: a watermark means the prefix below it was read on both sides
and observed equal, which is the same claim whichever checker observed it.

Persist `Checker.ResumeWatermark()`, never the chunker's traversal watermark.
Snapshot checkers suppress evidence after differences. Lockless verification has
no equivalent gate and does not need one — optimistic reads mismatch routinely on
a table taking writes and almost all of those resolve on retry, so gating on the
mismatch counter would discard the watermark on essentially every real migration.
(A caller that must know whether a *separate* lockless checker ever saw the copy
wrong — move, gating its checkpoint on the sentinel-wait checker — reads
`LocklessChecker.ConfirmedDifferences()`: divergences confirmed after every feed
was drained, or settled against the stream, counted before any repair and never
reset. `DifferencesFound()` includes the lag that reconciled.)
What makes the prefix trustworthy instead is that a chunk is reported to the
chunker only once it has resolved clean, so a chunk that was repaired, deferred
as hot, or split parks the watermark below itself and a resumed run re-verifies
from there. The published answer never moves backwards: a retried attempt, and
the second pass within an attempt, both re-walk from the start of the table, and
the further-along prefix stays valid because the change feed has been keeping it
equal.

`Checker.SetThrottler` is required for every finite implementation. Tests can use
the shared `checksum.MockChecker`, whose throttler setter is a no-op and whose
mismatch count can be changed safely while a runner is active.

The optional `StatusReporter` capability exposes a structured `ChecksumStatus`.
`StatusSummary` and `StatusRow` format it, falling back to basic progress and pacing
for checkers without that capability. Runners need no concrete checker assertions.
Optimistic status distinguishes scan completion from verification completion.

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

## Chunk repair

When a chunk mismatches and `FixDifferences` is set, the chunk is *repaired* rather than the run failing immediately. Every implementation repairs the same way:

1. `DELETE` the chunk's key range on the target — this is what removes rows the source no longer has, which a pure upsert could never do.
2. `SELECT` the chunk's rows from the source into Spirit.
3. Write them back through the **applier**, the same buffered write path the copier and the binlog apply use.

Repairs are serialized (one chunk at a time) and run under a cancellation-detached, time-bounded (10 minute) context, so a chunk is never left deleted-but-not-rewritten. Rows are read and submitted in batches, so what Spirit holds is one batch plus the applier's queue — bounded by the write pipeline, not by the size of the chunk.

Going through the applier — rather than the `REPLACE INTO _new (...) SELECT ... FROM original` that `SingleChecker` used historically — matters for lock footprint, and it is what makes large checksum chunks safe to repair without splitting them first:

- `INSERT ... SELECT` is a **locking** read under `REPEATABLE READ`: it takes shared next-key locks on every source row it reads, so application `UPDATE`s to the original table blocked behind a repair for as long as the statement ran. Reading into Spirit is a plain consistent read and locks nothing.
- The write side is split into bounded statements (applier chunklets) instead of one statement whose row locks are held for its whole duration.

Two consequences of the applier being the write path:

- It writes with `INSERT IGNORE`, not `REPLACE`. Rows inside the key range were just deleted, so nothing there conflicts; a row that collides on a `UNIQUE` secondary key with a row *outside* the range is skipped instead of clobbering that row. The chunk then stays diverged, the next attempt re-flags it, and retries exhaust into a hard error — the correct outcome for a lossy `ALTER` such as adding a unique index to non-unique data. The count of skipped rows is logged.
- JSON columns are read **bare**, with no round-trip cast. The read/write pair is already text-mediated (the `SELECT` renders each document to text; the applier writes it back as a literal the target re-parses), so a repaired row lands as exactly the one-text-round-trip image the checksum's source side predicts. Casting on top would apply `parse∘render` twice, which does not converge for the doubles MySQL's JSON text parser misrounds — see `castExpr` in `pkg/table`.

The read is not synchronized with the change feed: a row deleted on the source after the repair reads it is written back if the feed has already applied that `DELETE` to the target. The chunk stays diverged and the next attempt repairs it again, converging once the churn on that key range stops. Cut-over requires a pass that finds no differences at all, so sustained delete churn on one chunk costs attempts, never a bad cut-over.

## Pacing and scaling

`SingleChecker` paces itself against the same throttler the copier uses. Two things are separate here:

- **The hard stop** is not opt-in, but it reacts only to *load*. Before dispatching each chunk the checker calls `Throttler.BlockWait`, so a checksum pauses when server load says to. Chunks already in flight are never interrupted: the checksum stops *dispatching* rather than abandoning work, because an aborted chunk is wasted I/O that must be redone from the same watermark. Wire the throttler with `Checker.SetThrottler` — runners build the checker before their throttlers are open.

  Whatever throttler a checker is given is narrowed by `loadOnlyThrottler` to the children implementing `throttler.GradualThrottler` — in practice the Aurora signals. Binary signals, meaning replica lag, are dropped, and a checker given only those runs unpaced. This is not a shortcut but a correctness point: a checksum reads inside a `REPEATABLE READ` snapshot and writes nothing to the binlog, so it cannot be the cause of replica lag and pausing it cannot reduce that lag — while the pause extends the pass, holding the snapshot open and pinning undo the purge thread cannot advance past. The lag throttler also fails closed on stale polling, so an unreachable replica would stall dispatch until the yield timeout with the snapshot still held. Load is different in kind: a checksum does add read load to the primary, so backing off on load both works and is warranted.

  The one part of a checksum that replicates is a chunk repair, and it is deliberately left unpaced — repairs are rare and small, and blocking one incurs exactly the snapshot-hold cost the narrowing exists to avoid.
- **Scaling** is opt-in via `AutoscaleConfig`, and adjusts the live worker count during a pass. Two signals drive it:
  - The throttler's continuous **utilization** signal, applying the same zone law as the copier (see `pkg/autoscale`). Only the Aurora throttlers provide this signal, so this is where growth comes from and it is Aurora-only.
  - The **change-feed backlog**, whose signal is available everywhere — unlike utilization, it needs nothing from the throttler. The feed flushes concurrently with the checksum, and its backlog gates cut-over — if it grows unboundedly the binlogs may be purged before a resume can replay them. If the feed is losing ground, the checksum's reads are winning a race against writes that have to finish, so a worker is shed. On stock MySQL this is the only shedding lever, and recovery is capped at the configured concurrency.

    Available everywhere does not mean active everywhere: shedding lives in the scaler, and the scaler is only constructed when scaling is enabled. Without the opt-in a checksum has the hard stop and nothing else — it never moves its own worker count in either direction.

    What counts as "losing ground" is specifically a rising **post-flush residual**, not a rising backlog — and the residual is read from `change.Source.FlushResidual`, which the feed records at flush completion, rather than polled.

    Polling cannot recover this quantity. The pending count is a sawtooth: it climbs on every sample between flushes and drops when one lands, so its slope says nothing about whether the feed is coping (at 5s control tick and 30s flush interval, the rising edge alone is six samples long). Nor do window minima work, which is the subtler trap: a poll lands some offset φ after the flush and therefore reads `residual + writeRate·φ`. Because the flush interval is an exact multiple of the tick, φ is fixed for the whole pass by the arbitrary phase between two independent tickers — so on a busy table the sampling term can exceed the threshold on its own, and a *rising write rate* on a fully-draining feed produces rising apparent residuals indistinguishable from a feed falling behind.

    Because the signal is keyed on the feed's flush counter, silence has to be handled explicitly rather than latched: a flush that keeps erroring returns before recording anything and the periodic flusher logs the error and carries on, so the counter can freeze while the backlog grows without bound (a flush that merely takes minutes freezes it too). After `csStaleFlushTicks` ticks with no new flush the scaler stops trusting the standing verdict and freezes *increases* — growth and recovery alike — logging once per episode. It deliberately does not shed on it: a frozen counter says the signal stopped, not which way it was heading. This also covers a lockless checker with several feeds (a move), whose aggregate counter is the minimum across feeds, so one stuck feed freezes the signal for all of them.

    Reading the residual where the feed defines it removes the write rate from the signal entirely. Successive residuals are then compared across distinct flushes, with hysteresis in both directions: `csBacklogHysteresisFlushes` consecutive flushes must agree before the verdict changes. The exit condition matters as much as the entry one, because shedding is one step per flush while growth is one step per two ticks — a single favourable flush clearing the verdict would let the grows outpace the sheds and the controller would drift up while the feed fell further behind. While a verdict holds it suppresses growth as well as driving shedding.

The opt-in is the axis that matters most, so the capability table is keyed on it rather than on the server:

| | hard stop | shed on backlog | grow |
| --- | --- | --- | --- |
| scaling disabled (the default), any server | on load only | no | no |
| scaling enabled, stock MySQL | on load only | yes | no (recovers to the configured count only) |
| scaling enabled, Aurora | on load only | yes | yes (utilization law) |

The hard stop is the one behavior that needs no opt-in — but "on load only" carries weight in every row: the load signal comes from the Aurora throttlers, so on stock MySQL there is nothing for the hard stop to react to and a checksum there is unpaced apart from the backlog lever. `AutoscaleConfig.Enabled` — `--enable-experimental-autoscaling` for `migrate` — is what builds the scaler, and the scaler is where both shedding and growth live.

Concurrency is gated by a resizable `autoscale.Limiter` rather than `errgroup.SetLimit`, which may not be resized while goroutines are active.

One constraint shapes all of this: the `REPEATABLE READ` transaction pool **cannot grow** once the table lock is released. Every transaction takes its snapshot under that lock, so they all see one point in time; a transaction started later would read a newer snapshot and could compare a chunk against changes its siblings cannot see. The pool is therefore provisioned at the autoscale ceiling up front, whether or not scaling is enabled. Over-provisioning costs one connection per idle transaction and no extra history retention, since every read view pins from the same instant. What it does cost is lock-window time: each transaction is started serially under the lock, so the ceiling lengthens that window in direct proportion. That cost is why `autoscale.ReadBounds` caps the read side at half the instance rather than all of it — for this pool a ceiling is not a hypothesis, it is spent whether or not scaling reaches it.

`LocklessChecker` uses ordinary reads rather than a pinned snapshot pool. It supports load throttling and autoscaling through its worker limiter; `MinPassInterval` and its retry queue govern pass/retry pacing.

Each pass logs a `checksum chunk size distribution` line (chunk count, duration p50/p90/max, row p50/max, and how many chunks hit `table.MaxDynamicRowSize`). The row-capped count is the useful one: the checksum aggregates server-side and returns one row per chunk, so its chunks are far cheaper than the copier's, and if most are pinned at the row ceiling then that — not the `table.ChunkerDefaultTarget` time budget — is what bounds them.

## Lockless checksum

`Run` returns only after a complete pass with no repairs and no deferred ranges
(retrying the whole run on transient failure; `RunUntilClean` is one such
attempt). `RunContinuous` keeps checking until cancelled. All of them drive the
same pass loop; how long the caller runs it does not change its correctness
criteria.

`LocklessChecker` verifies a target that is still converging toward the source over a live replication feed, so a first-attempt mismatch is *expected* (the target simply hasn't caught up yet) rather than alarming. It runs in **passes**: each pass walks every chunk once and then drains a delayed-retry queue until empty. A mismatched chunk is re-read after a short delay (`RetryDelay`, 5s) and passes once the target's CRC matches a source CRC the checker has witnessed. With a change feed, the retry also waits until every feed has completed a flush since the retry was queued (bounded by `RetryFlushWait`: two default flush intervals, or twice `--flush-interval` for `spirit sync`): the target only moves when a feed flushes, every 30s by default, so an earlier re-read could only see the image it already saw and would spend a hot chunk's attempt budget on a target that had no chance to catch up. A chunk whose source keeps changing (a "hot chunk") cycles to the back of the queue without blocking the pass.

### Continuously updated hot rows

A row that is written continuously defeats every read-and-compare strategy the
checker has: the frozen source image is stale before the first target read, so
every attempt observes a different source and the range is deferred with no
verdict — pass after pass. A genuinely diverged hot row and a merely busy one
stay indistinguishable for as long as the writes continue.

The way out is to stop reading the source. Any comparison between a `SELECT` of
the source and a read of the target is between a source image at one position
and a target state at a later one, and closing that window means stopping the
writes. But the change stream already carries the answer: with
`binlog_row_image=FULL`, an event's after-image **is** the source's value for
that row at that position. MySQL guarantees it; no read is needed, and nothing
has to hold still.

**Settling** is the terminal step built on that. Once a range has failed
`MaxHotAttempts` observations, each outstanding row is verified against the
stream's own image of it, one row at a time:

1. Ask the feed to wait for the next change to that row and park its reader
   there (`change.Source.VerifyRowAtNextChange`). The change is buffered first and the reader parks
   immediately after, so nothing past that event is admitted.
2. Drain the feed, so the target holds exactly that image. This is *not* the
   exported `Flush`, which ends in a `BlockWait` for the reader to reach the
   source's current position — the reader is parked, so that wait could never
   succeed. The parked drain applies what is buffered and stops, and reports
   whether the buffer emptied.
3. Compare the target row to the event's image, evaluating the same
   column-mapping checksum expressions used everywhere else against the image
   itself. (The image is rendered as a one-row derived table whose column types
   come from the real table, so the `CAST`s land on a column of the right type.)

A mismatch at step 3 is a real inconsistency: the feed delivered that image and
the drain applied it, so apply lag cannot explain a difference. It goes to the
ordinary repair path (or is fatal, per the policy below) instead of being
deferred again.

This terminates in the opposite direction from a lock: **the more often the row
is written, the sooner its next event arrives.** The rows that defeat every
read-and-compare strategy are exactly the ones this settles fastest, and a row
quiet enough that no event arrives inside its one-second budget is one the
ordinary poll was already converging on. A delete event is a verdict too, which
is what lets an obligation that a row be *absent* be settled — no `SELECT` can
prove a row will stay absent, because there is nothing to hold.

Nothing here takes a lock. The cost is that the stream is held for the duration
of one row's verification, which is why rows are settled one at a time, the whole
escalation is bounded at five seconds, and it is reached only after a range has
already failed `MaxHotAttempts` observations. `HotChunksSettledThisPass` counts
it; a rising value alongside a falling `HotChunksDeferredThisPass` is it working.

Three cases still defer rather than settle, and all three are honesty constraints:

- **No change arrives inside the row's budget.** Not the case this exists for.
- **The row was written again before the target could be read.** The image no
  longer matches what the drain left behind, so the comparison would be against
  a value the target was never meant to hold.
- **The parked drain could not empty the buffer** — a batch that lost to lock
  contention, a key held behind the copier's watermark. The watched change may
  be among what is left, and reporting that as a divergence would be reporting
  apply lag, the one mistake this whole path exists to avoid.

A checker with no feed at all (library callers may have none) simply leaves the
range where it was before settling existed.

**Several sources** (a move with N sources) settle each row on the feed of the
source it was read from. A key found on two sources is refused as a snapshot
(it is either a disjointness violation or a row mid-move, and a per-key image
cannot say which copy is the row), so every source row has exactly one owner,
and only that owner's stream carries its next change; only that feed is parked.
A row that only the target holds has no source, and so no owning feed, when
there are several: its obligation is to be *absent*, and only a delete event
from the source it would have come from could settle that, which cannot be
identified. It defers, and the ordinary retries carry it.

When a chunk's source CRC is stable across the retry window but the target still disagrees, that is a **stable divergence**. How the checker reacts is governed by whether it has a `Recopier`:

- **With one**, a stable divergence is *repaired* by recopying that chunk from the source: `DELETE` the key range on the target, re-`SELECT` from the source, and re-apply through the same write path the change feed uses. Migration, move's initial checksum and sync ask for this — they set `FixDifferences` — so each self-heals a divergence and gives up only when repeated passes keep re-finding one. `chunkRepairer` repairs through the applier, deleting the range on every target and reading it from every source (`spirit migrate`, `spirit move`); `mysqlRecopier` is the cross-server one (`spirit sync`). Recopies are serialized and run under a cancellation-detached, time-bounded (10 minute) context, so a chunk is never left deleted-but-not-rewritten.
- **Without one**, a stable divergence is fatal: `Run` returns `ErrPermanentDivergence` and the caller aborts. Move's continuous checksum selects this: the initial checksum already passed, so a divergence found while waiting on the sentinel is surfaced rather than repaired near cutover. A resumed move's initial checksum repairs it.

Before either policy acts, the change feed is drained and the chunk re-read, so a target that was merely behind on applying buffered changes is not mistaken for a diverged one. On a confirmed divergence the checker logs a line per differing row (mismatched, missing on the target, missing on the source), the same diagnostic the snapshot checker emits.

Passes are paced by `MinPassInterval` so a small table is not re-checksummed back-to-back; the finite gate substitutes `RetryDelay` for an unset interval rather than the continuous default, because a cut-over is waiting on the answer. `MaxPasses` bounds the finite gate: a range that never converges returns `ErrVerificationUnresolved` instead of keeping the caller in an endless re-walk with no error and no end. `FirstCleanPass` exposes a channel that closes the first time a pass completes with every chunk read-verified equal and zero recopies — the signal that the target is known consistent.