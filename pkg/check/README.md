# check

Package `check` holds the checks spirit's runners make before and while they change data: server configuration, privileges, and refusals of tables and statements spirit does not support.

## Phase and Runtime

Every check registers in an `init()` with two bitmasks:

- **`Phase`**: when in a run the check runs (`PhasePreRun`, `PhaseStatement`, `PhasePreflight`, `PhasePostSetup`, `PhaseResume`, `PhaseCutover`, `PhaseCutoverLocked`). The doc comment on each phase constant states what a check in it may assume and what its failure means.
- **`Runtime`**: which runner the check applies to (`Migration`, `Move`, `Sync`).

`RunChecks(ctx, r, logger, phase, exclude...)` runs every check registered for both `r.Runtime` and `phase`, in name order, and stops at the first error. A check that does not apply to a runtime is never run by it. A check that does apply must fail, rather than pass, when the runner does not supply a resource it needs.

`Checks(runtime, phase)` reports the names `RunChecks` would run. A caller that relies on a phase's contract, such as an external planner using `StatementRefusal`, can pin the set it was written against.

## Resources

`Resources` describes a run as sources and targets, so that a check shared by several runners is written once:

| Field | Migration | Move | Sync |
|---|---|---|---|
| `Sources` | the server the tables are altered on (one) | each source, with its tables | — |
| `Targets` | — | each target | — |
| `SourceTables` | — | the first source's tables | — |
| `Change` | the table, its shadow table and the statement | — | — |
| `Replicas` | `--replica-dsn` | — | — |
| `Options` | `Threads`, `ReplicaMaxLag`, `ExperimentalForeignKeys` | `MoveEverything` | — |
| `Credentials` | the connection settings for `PhasePreRun` | — | — |

The migration runner runs its checks once per change. A shared check names the source an error came from (`source 1: ...`) for a move, and not for a migration, which has one source (`Resources.sourceError`). Datasync does not run `pkg/check` yet.

## Checks by runtime

Migration's checks and the phases they run in are listed in the [migration README](../migration/README.md#checks). Move runs:

| Check | Phases |
|---|---|
| `configuration` (shared with migration) | Preflight |
| `privileges` (shared with migration; a move also needs to see every source schema object and the target triggers and events) | Preflight |
| `source_schema_objects` | Preflight, PostSetup, Resume, CutoverLocked |
| `rename_safety`, `table_compatibility` | PostSetup, Resume |
| `source_schema_consistency`, `target_state` | PostSetup |
| `target_schema_objects` | PostSetup, Resume, CutoverLocked |
| `resume_state` | Resume |

## Refusals

A check wraps its error with `refuse()` (matched by `ErrRefused`) when retrying cannot help. A cutover retries a check error that is not a refusal, and stops on a refusal.

## Adding a check

1. Create `pkg/check/<name>.go` with an `init()` that calls `register(check{name: ..., run: ..., phases: ..., runtimes: ...})`. Registering a name twice panics. A check that runs in several phases registers once, with every phase in its mask.
2. Create `pkg/check/<name>_test.go`.
3. Follow an existing check such as `tablename.go`.
