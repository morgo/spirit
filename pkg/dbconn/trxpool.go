package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Maybe there is a better way to do this. For the CHECKSUM algorithm we need
// not a set of DB connections, but a set of transactions which have all
// had a read-view created at a certain point in time. So we pre-create
// them in newTrxPool() under a mutex, and then have a simple Get() and Put()
// which is used by worker threads.

const (
	// keepaliveMaxInterval is the longest the keepalive will go between
	// pinging the idle transactions in the pool.
	keepaliveMaxInterval = 5 * time.Minute
	// keepaliveMinInterval floors the ping interval so a pathological
	// wait_timeout can't make the keepalive spin (or panic the ticker on a
	// non-positive interval).
	keepaliveMinInterval = time.Second
	// keepaliveRoundTimeout bounds one round of pings so a hung server can't
	// hold the pool mutex (blocking Get/Put) indefinitely.
	keepaliveRoundTimeout = 30 * time.Second
)

type TrxPool struct {
	sync.Mutex

	trxs   []*sql.Tx
	logger *slog.Logger
	// keepalive lifecycle: cancel stops the pinger, done is closed once it
	// has fully exited. Both are nil if pool creation failed before the
	// keepalive started (Close tolerates that).
	keepaliveCancel context.CancelFunc
	keepaliveDone   chan struct{}
}

// NewTrxPool creates a pool of transactions which have already
// had their read-view created in REPEATABLE READ isolation.
//
// The pool is sized for the maximum concurrency the caller may ever scale up
// to, so some transactions can sit unused for hours. An idle transaction
// still counts against the server's wait_timeout, and managed configurations
// often set it low (e.g. 600s on Aurora): without intervention the server
// silently kills the connection and a later Get() hands out a dead
// transaction that fails with "driver: bad connection". To prevent that,
// the pool runs a background keepalive that periodically pings whatever
// transactions are idle in the pool; it stops when ctx is canceled or the
// pool is closed. A nil logger discards keepalive warnings.
func NewTrxPool(ctx context.Context, db *sql.DB, count int, config *DBConfig, logger *slog.Logger) (*TrxPool, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	pool := &TrxPool{trxs: make([]*sql.Tx, 0, count), logger: logger}
	for range count {
		trx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return nil, errors.Join(err, pool.Close())
		}
		pool.trxs = append(pool.trxs, trx)
		if _, err := trx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
			return nil, errors.Join(err, pool.Close())
		}
	}
	if len(pool.trxs) > 0 {
		// Derive the ping cadence from the session wait_timeout so the
		// keepalive holds up however aggressively the server is configured.
		// Every connection comes from the same DSN, so sampling one is enough.
		var waitTimeout int
		if err := pool.trxs[0].QueryRowContext(ctx, "SELECT @@wait_timeout").Scan(&waitTimeout); err != nil {
			return nil, errors.Join(err, pool.Close())
		}
		keepaliveCtx, cancel := context.WithCancel(ctx)
		pool.keepaliveCancel = cancel
		pool.keepaliveDone = make(chan struct{})
		go pool.keepalive(keepaliveCtx, keepaliveInterval(waitTimeout))
	}
	return pool, nil
}

// keepaliveInterval returns how often to ping the idle transactions in the
// pool, given the session wait_timeout in seconds. Half the timeout leaves a
// comfortable margin for scheduling delays, capped at keepaliveMaxInterval so
// pings stay frequent even when wait_timeout is generous (an intermediate
// proxy may have a stricter idle policy than the server reports).
func keepaliveInterval(waitTimeoutSecs int) time.Duration {
	interval := time.Duration(waitTimeoutSecs) * time.Second / 2
	return min(max(interval, keepaliveMinInterval), keepaliveMaxInterval)
}

// keepalive periodically pings the transactions sitting idle in the pool so
// the server never sees their connections reach wait_timeout. Transactions
// currently checked out via Get() belong to an active worker and are
// naturally excluded.
func (p *TrxPool) keepalive(ctx context.Context, interval time.Duration) {
	defer close(p.keepaliveDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pingIdleTrxs(ctx)
		}
	}
}

func (p *TrxPool) pingIdleTrxs(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, keepaliveRoundTimeout)
	defer cancel()
	// Holding the mutex for the whole round guarantees a transaction can't be
	// handed out while a ping is in flight on it. The round is quick: one
	// trivial query per idle transaction.
	p.Lock()
	defer p.Unlock()
	for _, trx := range p.trxs {
		if _, err := trx.ExecContext(ctx, "SELECT 1"); err != nil {
			if ctx.Err() != nil {
				return // pool is shutting down, or the round timed out
			}
			// Keep going: the other transactions may still be healthy, and
			// the worst case for this one is unchanged (a worker gets it and
			// fails, exactly as it would have without the keepalive).
			p.logger.Warn("keepalive ping of idle checksum transaction failed; its connection may have been killed (check wait_timeout)",
				"error", err)
		}
	}
}

// Get gets a transaction from the pool.
func (p *TrxPool) Get() (*sql.Tx, error) {
	p.Lock()
	defer p.Unlock()
	if len(p.trxs) == 0 {
		return nil, errors.New("no transactions in pool")
	}
	trx := p.trxs[0]
	p.trxs = p.trxs[1:]
	return trx, nil
}

// Put puts a transaction back in the pool.
func (p *TrxPool) Put(trx *sql.Tx) {
	p.Lock()
	defer p.Unlock()
	p.trxs = append(p.trxs, trx)
}

// Close closes all transactions in the pool.
func (p *TrxPool) Close() error {
	// Stop the keepalive and wait for it to exit, so a ping can't race with
	// the rollbacks below. The fields are nil when pool creation failed
	// before the keepalive started.
	if p.keepaliveCancel != nil {
		p.keepaliveCancel()
		<-p.keepaliveDone
	}
	var firstErr error
	for _, trx := range p.trxs {
		if err := trx.Rollback(); err != nil {
			// sql.ErrTxDone means the transaction was already rolled back
			// (e.g. due to context cancellation). This is not a real error —
			// skip it and continue closing the remaining transactions.
			if errors.Is(err, sql.ErrTxDone) {
				continue
			}
			// A rollback over a lost connection is not a real error either:
			// when the server kills a connection (e.g. wait_timeout) it rolls
			// the transaction back and releases its locks itself, so there is
			// nothing left to clean up. Surfacing it would fail an
			// otherwise-successful caller — the checksum treats Close errors
			// as a failed pass.
			if IsConnectionLossError(err) {
				p.logger.Warn("pooled transaction's connection was already gone at rollback", "error", err)
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
