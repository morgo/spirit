# Runtime

The `runtime` package exists to reduce duplication between the three runners: `migration`, `move` and `datasync`. Each runner has a `runner.go` that drives the same lifecycle. They began as copy-paste forks, so they kept separate copies of the same plumbing. Those copies drift apart. A fix made in one runner is easy to miss in the other two, and nothing fails to compile when it is missed. (See "Keeping the runner triplet in sync" in `AGENTS.md`.)

Code moves here when it meets both conditions:

- **It is shared.** At least two of the runners have a copy, and the copies differ only in which of the runner's own fields they read.
- **It is runner plumbing.** It is not a subsystem. The copier, applier, checksum, change feed, checkpoint table and sentinel each have their own package. `runtime` only wires them into a run, so it imports them and they never import it.

A copy whose difference between runners is real is not forced into one shape. The difference is passed in instead: a callback, a field of a config struct, or an optional value. `runtime` never imports a runner package.

The name matches the standard library's `runtime` package. A file that needs both must import one of them under an alias.

## What lives here

| Type or function | What it replaced | Used by |
|---|---|---|
| `Snapshot`, `Source` | Two copies of the periodic `Status()` block, `Progress()` and its throttle status | migration, move |
| `FatalGate` | Two copies of the fatal change-feed handler (`change.ClientConfig.CancelFunc`) | migration, move |
| `Lifecycle` | Three copies of the cancel function a `Run` invocation publishes, the opening lines of `Run`, and the evidence behind `Result()` | migration, move, datasync |
| `SharedThrottler` | Two copies of the mutex-guarded throttler that setup resolves | migration, move |
| `RecordCopyCompleted` | Three identical copies of the copy aggregate reported when the copy ends | migration, move, datasync |

### `Snapshot` and `Source`

Migration and move walk the same states with the same subsystems. Their status block and `Progress` report are therefore built once, from a `Snapshot`. The runner builds a new `Snapshot` on every call. The `Snapshot` reads the state once, so every field of one report describes the same state.

`Source` is a struct of functions that read the runner's subsystems: copier, applier, checker, feeds and throttler. They are functions because setup assigns the subsystems while an API caller may already be polling. A `Snapshot` calls only the functions that the current state reports on. By the time the run is in that state, setup has assigned those subsystems. Each runner fills in `Source` inline in its `snapshot` method, so no runner declares an adapter type. `SentinelSchema` is optional. A move leaves it nil, because its sources can span several schemas.

### `FatalGate`

This is the handler a runner wires to every change feed it starts. On the first fatal condition, `Trip` does the following, once:

1. Moves the run to `status.ErrCleanup`.
2. Logs the advice for the operator.
3. Drops the checkpoint, unless the reason leaves the checkpoint resumable (`change.FatalReason.PreservesCheckpoint`).
4. Cancels the run with a `status.FatalAbort` cause.

Once the run has reached cutover, `Trip` does nothing. The runner supplies its noun, its checkpoint drop and its cancel function in a `FatalTarget`.

Datasync does not use `FatalGate`. It has no cutover, it always keeps its checkpoint, and it records the cause itself (`recordFatal`).

### `Lifecycle`

`Lifecycle` holds what a `Run` invocation shares with the methods other goroutines call while it runs:

- the function that cancels the run, used by `Cancel`, `Abort`, `Close` and the fatal handler;
- the correctness evidence the run leaves behind (`status.WorkflowResult`).

`Run` starts with:

```go
ctx, end := r.lifecycle.Begin(ctx)
defer end(&retErr)
```

`end` does three things, in this order:

1. It replaces a `context.Canceled` result with the fatal-abort cause (`status.AbortCause`).
2. It records the evidence that the error carries.
3. It cancels the context.

The evidence is recorded after the substitution, so it is the evidence of the error `Run` actually returns: a fatal-abort cause can carry `status.ErrDurableMutation` or `status.ErrOwnershipAmbiguous`, and `context.Canceled` never does. Cancelling last does not change the outcome. A context keeps the first cause it was cancelled with, so `cancel(nil)` cannot hide a fatal cause that is already set.

`Lifecycle` is a named field and is not embedded. Each runner forwards its own `Cancel`, `Abort` and `Result` to it. As a result, the runner's public API does not gain `MarkDurableMutation`, `SetTerminalOwnership` or `RecordError`. Datasync uses only the cancel side; it reports no `Result`. Tests that drive phases without calling `Run` use `SetCancel`.

### `SharedThrottler`

This holds the throttler that setup resolves partway through. `Progress` and the change feed's `UnderLoad` callback may already be reading it at that point. Datasync reads its own load signal under `progMu`, so it does not use this.

### `RecordCopyCompleted`

This reports the copy aggregate (rows and chunks) when the copy ends. A resumed run's chunker restores its row count from the checkpoint but starts its chunk count at zero. `RecordCopyCompleted` subtracts the restored rows so that the two counts cover the same invocation.

## What does not live here (yet)

These parts of the runners are still duplicated or still differ. The last two are tracked in `AGENTS.md` ("Not yet unified").

- **`DumpCheckpoint` in migration and move.** The two share a skeleton, including the rule for which states may persist checksum evidence. They differ only in the position and in the record fields.
- **`invalidateChecksumWatermark`.** It runs raw SQL that belongs in `pkg/checkpoint`.
- **`Close` teardown.** Each runner owns different resources. The order of the steps is shared, but the code is not.
- **Aurora autoscaling details** that `pkg/concurrency` does not cover.

When you find another copy, extract it here, or into the subsystem package it belongs to. Then add a row to the table above and to the shared-table section of `AGENTS.md`.
