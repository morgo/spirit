# INVARIANTS.md

This file lists the rules that must hold in the Spirit codebase. [AGENTS.md](AGENTS.md) describes how the code is built, tested, and organized. This file describes what a change must not break.

A change that breaks one of these rules is a bug, even if every test passes. Several of them guard against failures that tests do not reliably catch: data loss, silent drift between runners, a lock that does not serialize. If a change needs to break a rule, update this file in the same PR and explain why in the PR description.

## Data safety

Spirit runs against financial systems, where a bug means data loss. Spirit is designed to fail safely. When in doubt:
- Return an error rather than silently continuing.
- Never skip the checksum. The initial checksum is the correctness gate for cutover: migration and move do not cut over until it passes.
- A chunk recopy (DELETE of the range, then re-insert) must stay atomic. When the sentinel is dropped, the continuous checksum lets an in-flight recopy finish (bounded by a per-chunk timeout) instead of cancelling it between the two steps.

## Unsupported features (do not implement)

- **RENAME column** — some rename operations are intentionally not supported. Renaming primary key columns and dangerous overlap patterns (e.g., `RENAME COLUMN c1 TO n1, ADD COLUMN c1 ...`) are blocked. Simple non-PK column renames are supported.
- **ALTER/DROP PRIMARY KEY** — primary key must remain unchanged
- **Lossy conversions** (e.g., shortening VARCHAR below max data length)
- **FOREIGN KEYS or TRIGGERS** on migrated tables
- **Read-replica fidelity** (<10s lag guarantees)

## Locking

Migrations serialize per-table via `dbconn.AdvisoryLock` (a `GET_LOCK` per table) — two migrations on the same table block each other, but different tables run concurrently. Atomic multi-table migrations (`--statement` with several `ALTER`s) additionally take a **schema-scoped** lock (`dbconn.WithMultiTableSchemaLock`), so only one runs per schema at a time: they all coordinate through one fixed-name `_spirit_checkpoint`/`_spirit_sentinel` and must not overlap. A second one fails fast in `Run`. Single-table migrations are unaffected. (User-facing: README "Atomic Multi-table changes".)

## Runner lifecycle parity

`pkg/migration`, `pkg/move`, and `pkg/datasync` each have a `runner.go` that drives the **same lifecycle skeleton**:

> setup (connect, resolve tables, create checkpoint table) → copy rows → post-copy (drain binlog, restore deferred indexes, ANALYZE, initial checksum) → *[migration/move: sentinel wait + atomic cutover] / [datasync: run continuously]* → close (teardown + checkpoint quiesce).

These three runners began as copy-paste forks and **drift silently** — a safety fix applied to one is easy to forget in the others, and nothing fails to compile when it's missed. The standing rule:

> **When you add or change anything in a runner's lifecycle, check whether it applies to the other two and port it — or, better, extract it into a shared package.** If a difference is genuine and can't be unified, *parameterize* it (pass a callback/config) rather than re-forking the surrounding logic.

### What is already shared (don't re-implement these)

| Concern | Shared home | Used by |
|---|---|---|
| Status + checkpoint loops (`WatchTask`) and the `State` machine | `pkg/status` (`Task` interface: `Progress`/`Status`/`DumpCheckpoint`/`Cancel`) | migration, move, datasync |
| Checkpoint table (one schema + create/drop/exists/write/read) | `pkg/checkpoint` (`Table` + `Mode`) | migration, move, datasync |
| Sentinel cutover gate (`Create`/`Exists`/`Wait`) | `pkg/sentinel` | migration, move (datasync has no cutover) |
| Lockless (optimistic) checksum, N sources × M targets | `pkg/checksum` `LocklessChecker` | move (always), datasync (always), migration (`--enable-experimental-lockless-checksum`) |
| Row copy, write layer, chunking, change feed, connections, throttling | `pkg/copier`, `pkg/applier`, `pkg/table`, `pkg/change`, `pkg/dbconn`, `pkg/throttler` | all |
| Aurora load throttling, always on when the watched server is Aurora (`throttler.AuroraSetup`: threads + commit-latency throttlers and their monitor pool; `--max-commit-latency`, default `100ms`, `0` disables — same semantics in all three) | `pkg/throttler` | migration (its source), move (every target), datasync (its target) |
| Aurora autoscaling (`--enable-experimental-autoscaling`) building blocks: instance-derived bounds (`autoscale.MinVCPUs`/`ReadBounds`/`WriteStart`/`FlushBounds`/`ClientCeiling`), write-ceiling rule (`throttler.ResolveMaxWriteThreads`, fed by `AuroraResult.RedoAware`), copy-phase read/write controller (`copier.AutoscaleConfig`), checksum controller (`checksum.AutoscaleConfig`), feed flush narrowing (`throttler.GradualOnly` → `change.ClientConfig.UnderLoad`), throttle status (`throttler.Describe`) | `pkg/autoscale`, `pkg/throttler`, `pkg/copier`, `pkg/checksum`, `pkg/change` | migration, move, datasync |

How the recently-unified pieces handle per-tool differences, as patterns to copy:

- **`sentinel.Wait`** takes the two genuinely runner-specific steps as callbacks (`RunChecksum`, `InvalidateWatermark`) — e.g. migration scopes its watermark `UPDATE` by `statement` (its checkpoint table is shared across multi-table migrations) while move blanks the whole per-move table. The poll/timeout/continuous-checksum-lifecycle orchestration is shared; only the divergent bits are injected.
- **`status.WatchTask`** is consumed via a small interface (`status.Task`); each runner keeps a `var _ status.Task = (*Runner)(nil)` assertion so a signature drift fails the build. A checkpoint-write failure is **fatal** here: it calls `Abort(status.FatalAbort(err))` when the runner implements `status.Aborter` (migration and move do, each with a `var _ status.Aborter = (*Runner)(nil)` assertion), so `Run` returns the checkpoint error instead of `context.Canceled`; otherwise it calls `Cancel()` (datasync, which records the fatal cause inside its own `DumpCheckpoint`). Don't reintroduce a loop that swallows it, and implement `Aborter` in a new runner.
- **`checksum.NewChecker`** is the one construction path for both checkers — there is no per-checker constructor. `CheckerConfig.Lockless` selects `LocklessChecker`, false selects `SingleChecker`; nothing else does (the applier is only the repair write path, and for lockless the source of the target list via `GetTargets`). `SingleChecker` is one source against one server; `LocklessChecker` is N sources against M targets, each chunk's CRCs XORed and counts summed across every server. **Lockless is intended to replace Single**: keep Single-only code in `pkg/checksum/single*.go` and new shared code off concrete checker types, so removing Single stays a deletion. Whether a confirmed stable divergence *aborts* or *self-heals* is decided by whether the factory built a `Recopier`, and there is no separate policy flag for it (the `DivergenceIsFatal` knob from block/spirit#994 is gone; it had one use site and was fully redundant with this). `FixDifferences` is what asks for one, and **migration, datasync and move's initial checksum set it**, so they repair a mismatched chunk and give up only once repeated passes keep re-finding one — with `ErrVerificationUnresolved` at `MaxPasses` for the lockless finite gate. Move's *continuous* checksum leaves it off: a divergence found during the sentinel wait returns `ErrPermanentDivergence` and aborts the move (fail loud), and the resumed move's initial checksum repairs it. Only the *shape* of the repair varies: `mysqlRecopier` when `CheckerConfig.TargetDB` names a second server (datasync), `chunkRepairer` otherwise — it deletes the range on every target and reads it from every source (migration, which is also the only one with a `ColumnMapping` to honour, and move). `TargetDB` is lockless-only because `SingleChecker` locks and snapshots exactly one server — not because a cross-server serialization point is impossible: it can be manufactured by locking every source and target, draining every feed to empty under those locks, and opening a `REPEATABLE READ` transaction per server inside that window, which is what the removed `DistributedChecker` did. Its cost scales with the topology, which is why block/spirit#1281 replaced it with lockless aggregation rather than reviving it. A checker owns the feed's periodic flush for the duration of a run unless the caller sets `ExternalFlushLoop`, which datasync does because its flush loop runs for the whole process. Both tools pace passes with `MinPassInterval`; migration's finite gate substitutes `RetryDelay` rather than `LocklessMinPassInterval`, because a cut-over is waiting on the answer.
- **`pkg/sentinel`** takes the schema from the connection (`DATABASE()` / unqualified DDL), not a passed-in schema name, so it works under Vitess. Prefer this pattern for new helpers — point the `*sql.DB` at the right schema rather than threading a schema string. `pkg/checkpoint` follows it too.
- **`pkg/checkpoint`** owns the one checkpoint-table schema and its create/drop/exists/write/read, keyed on the connection's selected schema (`DATABASE()` / unqualified, like sentinel). `Write` keeps a **single row** (`REPLACE` on `id=1` — atomic, so a crash never leaves no checkpoint, and bounded). Two `Mode`s differ only in `Create`: `Transient` (DROP+CREATE — a checkpoint for one finite run: single-table & atomic multi-table migration, move) vs `Persistent` (CREATE IF NOT EXISTS, never cleared — a continuous run: datasync, whose existence is its resume signal). Resume *policy* stays per-runner (statement match, collision, max-age, multi-source positions, datasync's three-state `Exists` routing); the package interprets no watermarks. `checkpoint.IsIncompatible` tells an unreadable cross-version checkpoint apart from a transient read error, so recovery never fires on a blip — migration falls back to a fresh run; datasync recovers under `--force` (drops the target DB).

### Not yet unified (live drift — touch with care)

- **Aurora autoscaling setup.** The controllers and bounds are shared (table above), but each runner has its own engage/derive step — migration inline in `setupCopierCheckerAndReplClient`, move in `pkg/move/runner.go` (`setupThrottling` → `setupAutoscaling`, fresh and resume), datasync in `pkg/datasync/runner.go` (`setupThrottling` → `setupAutoscaling`, fresh and resume). Move and datasync make **one** `AuroraSetup.Build` call per watched server and size autoscaling from that result (`ProbeErr`, `RedoAware`), so the signal they scale against is the one throttling them. Migration still derives its bounds from a separate `IsAurora`/`CanReadRedoAwareThreads` probe in `setupCopierCheckerAndReplClient`, before `setupThrottler` builds the throttlers. All three derive bounds once at startup, so a target instance resize needs a restart. Topology-driven differences are genuine and parameterized: move sizes from the *smallest* target, divides read/write starts and the feed flush width by the most shards on one host (the flush width also by the number of sources), splits the client write budget across targets, and scales every shard in lockstep on one composite signal (the busiest host); datasync partitions `--max-connections` between checksum reads and repair writes after reserving the feed flush. Differences that are drift, not topology — decide explicitly when you touch any of them:
  - **Non-Aurora targets.** Migration leaves the flag on so the checksum keeps its backlog-shedding veto; move and datasync turn autoscaling off entirely.
  - **Repair writes after copy.** Datasync runs `copier.StartWriteAutoscaler` for checksum repairs during continuous verification. Migration and move run no write controller after the copy, so repairs use the applier's start count.
  - **Client-ceiling warnings.** Migration logs when the host's CPU caps a derived count or a configured count exceeds `autoscale.ClientCeiling()`; move and datasync cap silently.
  - Move's reverse window always uses the configured `--write-threads`; it gets no monitor and no autoscaling.
  - Datasync's target monitor stays open until `Close`; built-in feeds use `Runner.TargetUnderLoad` for flush narrowing, and injected feeds must wire that callback themselves. Do not confuse the applier's copy/repair workers with synchronous change-feed flush concurrency.
- **`fatalError` / `Close` teardown** are similar but not identical between the three — keep the run-all-steps + `errors.Join` teardown idiom and the `>= CutOver` no-op guard in `fatalError` consistent when you touch them.

When you add a checkpoint field, add it to `pkg/checkpoint`'s schema — it is shared by all three. When you add a teardown step, a new lifecycle phase, or a safety gate, grep all three `runner.go` files and decide explicitly: port, or extract.

## Code structure

### MySQL canonical form belongs in the normalization layer

**All MySQL canonical-form handling belongs in this layer.** When the desired schema and the live `SHOW CREATE TABLE` disagree only in representation — parenthesization, display widths, inline vs table-level declarations, auto-generated names — fix it by adding a `Normalizer` rule, never by special-casing `Diff`, the parse helpers, or restore functions. Keeping every MySQL-ism in the registry is what keeps the rest of the code free of per-exception complexity: `Diff` and the parser assume canonical input and stay simple.

For the steps to add a rule, see [AGENTS.md](AGENTS.md#adding-a-normalization-rule).

### Shared helpers: look in `pkg/utils` first
Before writing a small helper (a percentile, a string/grant parser, an ENUM/SET element parser, …), check `pkg/utils/` for an existing one. If none exists and the helper has few dependencies — the standard library only, no other spirit package — add it to `pkg/utils` and export it, rather than as a private function in the package that first needs it. A private helper gets copied the second time another package needs it, and the copies then drift apart.

`pkg/utils` imports nothing from spirit, so every package can use it without an import cycle. Keep it that way. A helper that needs another spirit package belongs in that package instead:
- Identifier quoting goes in `pkg/dbconn/sqlescape` (`EscapeIdentifier`, `EscapeIdentifierList`).
- Helpers that run a query go in `pkg/dbconn`.
- MySQL error numbers come from `pkg/parser/mysql` (`ErrNoSuchTable`, …). Do not define local constants for them.

### Methods live in the file that defines their type
All methods (receivers) of a type go in the same file as the type's declaration. Do not split them into topic files such as `autoscale.go` or `helpers.go`. If a group of methods is a coherent unit with its own dependencies and wants its own file, give it its own type there (e.g. `rowSettler` in `pkg/checksum/lockless_settle.go`) instead of adding more methods to the original type. Some existing code does not follow this yet; do not add to it, and move methods back when you touch them.

### Working with the parser
All SQL parsing goes through `pkg/statement/` (built on `pkg/parser`, Spirit's fork of the TiDB parser). Do not parse SQL manually. The `Statement` type wraps parsed DDL and provides safety analysis methods.

### Database connections
Always use `pkg/dbconn` for MySQL connections. Never create raw `sql.Open()` calls in production code (test utilities are the exception). The `DBConn` type handles retries, TLS, and connection pooling.
