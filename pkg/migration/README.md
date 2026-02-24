# Migration

The `migration` package is used to orchestrate schema changes to one or more tables. Each `change` is tracked in the runner, with the copier and checker largely agnostic that they may be copying multiple tables at once.

There is one replication client for all changes, and a subscription is added for each table:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    SPIRIT MIGRATION                                     │
│                                                                                         │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │                              RUNNER (Orchestrator)                               │   │
│  │                              pkg/migration/runner.go                             │   │
│  │                                                                                  │   │
│  │   Coordinates the entire migration lifecycle:                                    │   │
│  │   1. Setup (create _new table, checkpoint table)                                 │   │
│  │   2. Start replication client                                                    │   │
│  │   3. Run copier                                                                  │   │
│  │   4. Disable watermark optimization                                              │   │
│  │   5. Run checksum                                                                │   │
│  │   6. Perform cutover (RENAME TABLE)                                              │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
│         │                              │                              │                 │
│         │ owns                         │ owns                         │ owns            │
│         ▼                              ▼                              ▼                 │
│  ┌─────────────┐              ┌─────────────────┐              ┌─────────────┐          │
│  │   COPIER    │              │   REPL CLIENT   │              │   CHECKER   │          │
│  │             │              │                 │              │  (Checksum) │          │
│  └─────────────┘              └─────────────────┘              └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

The copier and [replication client](../repl/README.md) are running in parallel during step 3. The only hard requirement is that the replication client starts before the copier so we can ensure that all changes are tracked.

The copier is essentially a dirty-copy, in that each chunk does not correctly keep track of changes being made. This is reconciled via the replication client, which is able to detect any changes that have been made, and apply them to the new table consistently.

Once copying is complete, a [checksum process](../checksum/README.md) is started. This ensures that all data has safely made it to the new table, and it is safe to cutover.

## What parts of the process are locking?

To answer this question, we need to understand that there are two types of locks. Both can be an issue:

1. Data locks, aka InnoDB row level locks. These are configurable via `innodb_lock_wait_timeout`: the server default is 50s, but spirit overwrites this to 3s.
2. Metadata locks. These are configurable via `lock_wait_timeout`: the server default is 1 year(!), but spirit overwrites this to 30s (configurable by `--lock-wait-timeout`).

So when we describe Spirit as a "non-blocking schema change tool" that is a bit of a white lie. What it means is that we don't require a metadata lock for the entire 10h schema change, as built-in MySQL DDL often does. It does not mean that we do not require locks.

The following are common **data lock** issues:

* Data locks are required *on the specific rows* when copying chunks from the existing table to the new table. This is because we use the `INSERT .. SELECT` syntax which does not use MVCC (i.e. non-locking reads) when reading from the SELECT side. We mitigate this effect by offering you a configurable `target-chunk-time`. Lower values will mean that these locks are held for less time because the copies are shorter.

* Data locks are required *on the specific rows* when applying changes from the replication client. This is similar to the copier, except we use a combination of `DELETE` and `REPLACE .. SELECT`.

* The copier and replication client conflict with each other and deadlock. This only happens when the PK is a non auto-increment INT/BIGINT, see [issue #479](https://github.com/block/spirit/issues/479).

In future, we may recommend using `--enable-experimental-buffered-copy` for cases where there are hot rows, or high probability of contention with data locks. Because of its different design, it only requires data locks on the `_new` table, which effectively prevents all of the data lock contention.

The following are cases where **metadata locks** are required:

* Spirit initially attempts INSTANT/INPLACE DDL. If this is compatible, it requires an exclusive metadata lock on the table.
* Starting a checksum requires an initial exclusive metadata lock to ensure that all data is synchronized between the checksum threads.
* The cutover operation requires an exclusive metadata lock.

What causes all metadata lock issues? (hint: it's not spirit)

Any open transactions will have shared metadata locks on any of the tables that you are modifying. If you have long transactions that have not yet committed/rolled back, Spirit's exclusive lock will be queued waiting for them to finish. This then looks like a Spirit problem because any shared lock requests that arrive after Spirit's exclusive lock request will then be queued behind Spirit. So the solution is to keep your transactions as short as possible.

The _fix_ for Spirit, is that it will by default `force-kill` the specific connections that are blocking it from acquiring an exclusive lock. This can be disabled with `--skip-force-kill` if needed.

## Using Spirit `migration` as a Go package

I will assume that similar to our use-case, you are probably wrapping some sort of automation around Spirit. If this automation is written in Go, I would encourage you to use the Spirit API and not the CLI executable.

The following is a simplification of what we use ourselves:

```go
func (sm *Spirit) Execute(ctx context.Context, m *ExecutableTask) error {
	startTime := time.Now()
	runner, err := migration.NewRunner(&migration.Migration{
		Host:              m.Cluster.Host,
		Username:          m.Cluster.Username,
		Password:          &m.Cluster.Password,
		Database:          m.Cluster.DatabaseName,
		Statement:         m.Statement,
		TargetChunkTime:   m.TargetChunkTime,
		Threads:           m.Concurrency,
		LockWaitTimeout:   m.LockWaitTimeout,
		InterpolateParams: true,
	})
	if err != nil {
		return errors.Wrap(err, "failed to create spirit migration runner")
	}
	defer runner.Close()
	if m.Metrics != nil {
		runner.SetMetricsSink(m.Metrics)
	}
	sm.Lock()
	sm.progressCallback = func() string {
		return runner.Progress().Summary
	}
	sm.Unlock()
	runner.SetLogger(m.Logger)
	if err = runner.Run(ctx); err != nil {
		return errors.Wrap(err, "failed to run spirit migration")
	}
	m.Logger.Infof("spirit migration completed in %s", time.Since(startTime))
	return nil
}
```
