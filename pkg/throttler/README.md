# Throttlers

Throttlers are designed to limit the rate of changes pushed through the copier, helping maintain the health of the system during migrations.

## Design Philosophy

Throttlers were designed as an **interface from the start**, because it was always believed there would be different use cases for when a schema change should be throttled. The original vision included:

- **Replication lag throttler**: Monitors replica lag and pauses when replicas fall behind
- **HTTP service integration** (e.g., [Freno](https://github.com/github/freno) or [Doorman](https://github.com/youtube/doorman)): Allows schema changes and other background tasks to coordinate through a centralized throttling service
- **Custom implementations**: The interface allows users to implement their own throttling strategies, such as reducing migration activity during stock trading hours or at least market open

## Current State

In practice, throttlers haven't been used as extensively as originally envisioned. Because Spirit is primarily used with Aurora at Block, the replica throttler sees limited internal use. We have also found that by using Dynamic Chunking in the copier, schema changes actually self-throttle pretty well without a throttler.

However, it remains available and maintained for community use, particularly for users running traditional MySQL replication topologies. We are open to contributions to throttler improvements, such as being able to throttle on multiple replicas at once ([issue #220](https://github.com/block/spirit/issues/220)).

## Interface

All throttlers implement the `Throttler` interface:

```go
type Throttler interface {
    Open(ctx context.Context) error
    Close() error
    IsThrottled() bool
    BlockWait(ctx context.Context)
    UpdateLag(ctx context.Context) error
}
```

## Implementations

### Noop Throttler

The default throttler that performs no throttling. Used when throttling is not required.

```go
throttler := &throttler.Noop{}
```

### Mock Throttler

A throttler used internally by the test suite to help reduce race conditions when running migration tests across different types of hardware. It injects 1 second of sleep every time `BlockWait()` is called.

### Replication Throttler

Monitors replication lag on MySQL 8.0+ replicas using `performance_schema` metrics. This provides more accurate lag measurements than the traditional `SHOW SLAVE STATUS` approach.

```go
throttler, err := throttler.NewReplicationThrottler(
    replicaDB,
    120*time.Second,  // lag tolerance
    logger,
)
```

**Features:**
- Uses `performance_schema` for accurate lag calculation
- Monitors both applier latency and queue latency
- Automatically detects idle replicas to avoid false positives
- Checks lag every 5 seconds by default
- Blocks copy operations when lag exceeds tolerance (default: up to 60 seconds per check)

## Usage

Throttlers are integrated into the copier and automatically pause chunk copying when the system is under stress:

```go
copier := &copier.Unbuffered{
    Throttler: throttler,
    // ... other config
}
```

During migration, the copier calls `throttler.BlockWait(ctx)` before each chunk, pausing operations if `IsThrottled()` returns true.

## Extending

To implement a custom throttler (e.g., for Freno integration):

1. Implement the `Throttler` interface
2. Start any background monitoring in `Open()`
3. Update throttling state based on your metrics
4. Return the current state in `IsThrottled()`
5. Block appropriately in `BlockWait()` with context support

Example structure:

```go
type CustomThrottler struct {
    isThrottled atomic.Bool
    // ... your fields
}

func (t *CustomThrottler) Open(ctx context.Context) error {
    // Start monitoring
    go t.monitor(ctx)
    return nil
}

func (t *CustomThrottler) IsThrottled() bool {
    return t.isThrottled.Load()
}

func (t *CustomThrottler) BlockWait(ctx context.Context) {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    
    for t.IsThrottled() {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // Continue checking
        }
    }
}
```

## See Also

- [MySQL 8.0 Replication Lag Implementation](https://github.com/block/spirit/issues/286)
- [Freno - Throttling Service](https://github.com/github/freno)
- [Doorman - Global Distributed Client Side Rate Limiting](https://github.com/youtube/doorman)
