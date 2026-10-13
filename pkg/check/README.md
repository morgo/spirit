# check

Package `check` holds the checks spirit's runners make before and while they change data: server configuration, privileges, and refusals of tables and statements spirit does not support.

## Phase and Runtime

Every check registers in an `init()` with two bitmasks:

- **`Phase`**: when in a run the check runs (`PhasePreRun`, `PhaseStatement`, `PhasePreflight`, `PhasePostSetup`, `PhaseCutover`, `PhaseCutoverLocked`). The doc comment on each phase constant states what a check in it may assume and what its failure means.
- **`Runtime`**: which runner the check applies to (`Migration`, `Move`, `Sync`).

`RunChecks(ctx, r, logger, phase, exclude...)` runs every check registered for both `r.Runtime` and `phase`, in name order, and stops at the first error. A check that does not apply to a runtime is never run by it. A check that does apply must fail, rather than pass, when the runner does not supply a resource it needs.

`Checks(runtime, phase)` reports the names `RunChecks` would run. A caller that relies on a phase's contract, such as an external planner using `StatementRefusal`, can pin the set it was written against.

## Resources

`Resources` describes a run as sources and targets, so that a check shared by several runners is written once:

| Field | Migration | Move | Sync |
|---|---|---|---|
| `Sources` | the server the tables are altered on (one) | each source | — |
| `Change` | the table, its shadow table and the statement | — | — |
| `Replicas` | `--replica-dsn` | — | — |
| `Options` | settings the checks validate | — | — |
| `Credentials` | the connection settings for `PhasePreRun` | — | — |

The migration runner runs its checks once per change. Move and datasync do not run `pkg/check` yet; their checks still live in `pkg/move/check` and in `pkg/datasync`.

## Refusals

A check wraps its error with `refuse()` (matched by `ErrRefused`) when retrying cannot help. A cutover retries a check error that is not a refusal, and stops on a refusal.

## Adding a check

1. Create `pkg/check/<name>.go` with an `init()` that calls `register(check{name: ..., run: ..., phases: ..., runtimes: ...})`. Registering a name twice panics.
2. Create `pkg/check/<name>_test.go`.
3. Follow an existing check such as `tablename.go`.
