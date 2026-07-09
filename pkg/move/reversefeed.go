package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// ReverseFeed implements the data plane of the "reverse-window" move: after a
// forward cutover, it tails the (former) target's binlog and applies changes
// back to the (former) source in CHANGE-ONLY mode — no copy, no checksum — so
// the source stays current and the move can be rolled back for a bounded
// window. It is the mirror of the forward move: the former targets become the
// reverse sources and the former source becomes the reverse target.
//
// It reuses change.Source exactly as the forward move does; the only new idea
// is direction + "change-only" (SetWatermarkOptimization(false), started from a
// position captured at cutover). Because the applier is idempotent (REPLACE
// INTO / DELETE by PK), starting slightly early replays no-ops, so the cutover
// position handoff does not need to be exact.
//
// This type is deliberately decoupled from the forward Runner and its cutover
// so it can be exercised in isolation (see reversefeed_test.go): construct it
// against two already-consistent databases, drive writes on the source side,
// and assert the target converges.

// ReverseSource is one reverse source: a former move target (shard S) whose
// binlog is tailed to keep the former move source (U) current.
//
// DB's default database MUST be this source's schema. table.TableInfo.SetInfo
// resolves columns via information_schema WHERE table_schema=DATABASE() — the
// connection's default database, not the SchemaName argument — so a connection
// defaulting to the wrong schema silently loads the wrong table's definition,
// surfacing only at apply time as a fatal column-count mismatch.
type ReverseSource struct {
	DB       *sql.DB            // connection to S (default DB == its schema)
	Addr     string             // host:port for the binlog syncer
	User     string             // binlog syncer user
	Password string             // binlog syncer password
	Tables   []*table.TableInfo // S-side tables to watch, built on DB
	// Position is the opaque change.Source position to resume from (captured at
	// cutover). Empty means start from the source's current head.
	Position string
}

// ReverseFeedConfig configures a ReverseFeed.
type ReverseFeedConfig struct {
	Sources []ReverseSource
	// Target is the U side (single, unsharded). Target.DB's default database
	// MUST be U's schema (see ReverseSource.DB).
	Target applier.Target
	// TargetTables maps each watched (reverse-source) table NAME to the U-side
	// TableInfo it is written to, built on Target.DB. It is a map, not a slice,
	// because the names can differ: after a forward cutover the source tables are
	// renamed to their _old form, so a watched "t1" is written to "t1_old".
	TargetTables map[string]*table.TableInfo

	Logger        *slog.Logger
	DBConfig      *dbconn.DBConfig
	Threads       int           // applier write threads; 0 => default (4)
	FlushInterval time.Duration // 0 => change.DefaultFlushInterval
	// GTID selects the GTID-based change source (matching the forward move's
	// --enable-experimental-gtid) instead of binlog file+offset, so the reverse
	// feed uses the same coordinate scheme the operator chose for the move.
	GTID bool
}

// ReverseFeed is a running change-only reverse feed: one change.Source per
// ReverseSource, all sharing a single applier that writes to U (mirrors the
// forward move, where N sources share one applier).
type ReverseFeed struct {
	sources  []ReverseSource
	clients  []change.Source
	appl     applier.Applier
	logger   *slog.Logger
	flushInt time.Duration

	// A fatal error on any feed (schema change or stream failure) means U can no
	// longer be trusted for rollback. We capture it once and close fatalCh so
	// Run wakes immediately and the caller can degrade to complete-forward
	// rather than silently letting U drift.
	fatalOnce sync.Once
	fatalCh   chan struct{}
	fatalMu   sync.Mutex
	fatalErr  error
}

// NewReverseFeed wires the feeds and their shared applier. It does not open any
// binlog stream; call Start or Run for that.
func NewReverseFeed(cfg ReverseFeedConfig) (*ReverseFeed, error) {
	if len(cfg.Sources) == 0 {
		return nil, errors.New("reverse feed: at least one source is required")
	}
	if cfg.Target.DB == nil {
		return nil, errors.New("reverse feed: target DB must be non-nil")
	}
	if len(cfg.TargetTables) == 0 {
		return nil, errors.New("reverse feed: at least one target table is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	dbCfg := cfg.DBConfig
	if dbCfg == nil {
		dbCfg = dbconn.NewDBConfig()
	}
	flushInt := cfg.FlushInterval
	if flushInt <= 0 {
		flushInt = change.DefaultFlushInterval
	}
	threads := cfg.Threads
	if threads <= 0 {
		threads = 4
	}

	// One applier writing to U, shared across all source feeds.
	appl, err := applier.NewSingleTargetApplier(cfg.Target, &applier.ApplierConfig{
		DBConfig: dbCfg,
		Logger:   logger,
		Threads:  threads,
	})
	if err != nil {
		return nil, fmt.Errorf("reverse feed: create applier: %w", err)
	}

	rf := &ReverseFeed{
		sources:  cfg.Sources,
		appl:     appl,
		logger:   logger,
		flushInt: flushInt,
		fatalCh:  make(chan struct{}),
	}

	for si, src := range cfg.Sources {
		if src.DB == nil {
			return nil, fmt.Errorf("reverse feed: source %d DB must be non-nil", si)
		}
		if len(src.Tables) == 0 {
			return nil, fmt.Errorf("reverse feed: source %d has no tables", si)
		}
		clientCfg := &change.ClientConfig{
			Logger:     logger,
			ServerID:   change.NewServerID(),
			DBConfig:   dbCfg,
			CancelFunc: rf.onFatal,
		}
		var client change.Source
		if cfg.GTID {
			client = change.NewGTIDClient(src.DB, src.Addr, src.User, src.Password, appl, clientCfg)
		} else {
			client = change.NewBinlogClient(src.DB, src.Addr, src.User, src.Password, appl, clientCfg)
		}
		for _, sTbl := range src.Tables {
			uTbl, ok := cfg.TargetTables[sTbl.TableName]
			if !ok {
				client.Close()
				return nil, fmt.Errorf("reverse feed: source %d has no target table for %q", si, sTbl.TableName)
			}
			chunker, err := table.NewChunker(sTbl, table.ChunkerConfig{NewTable: uTbl, Logger: logger})
			if err != nil {
				client.Close()
				return nil, fmt.Errorf("reverse feed: chunker for %q: %w", sTbl.TableName, err)
			}
			if err := client.AddSubscription(sTbl, uTbl, chunker); err != nil {
				client.Close()
				return nil, fmt.Errorf("reverse feed: subscribe %q: %w", sTbl.TableName, err)
			}
		}
		rf.clients = append(rf.clients, client)
	}
	return rf, nil
}

// onFatal records the first fatal feed condition and wakes Run. It returns true
// (we always act on it: the window degrades to complete-forward).
func (rf *ReverseFeed) onFatal(reason change.FatalReason) bool {
	rf.fatalOnce.Do(func() {
		rf.fatalMu.Lock()
		rf.fatalErr = fmt.Errorf("reverse feed hit a fatal condition (%s); rollback is no longer safe", reason)
		rf.fatalMu.Unlock()
		rf.logger.Error("reverse feed fatal; source can no longer be trusted for rollback", "reason", reason.String())
		close(rf.fatalCh)
	})
	return true
}

// Err returns the first fatal feed error, if any.
func (rf *ReverseFeed) Err() error {
	rf.fatalMu.Lock()
	defer rf.fatalMu.Unlock()
	return rf.fatalErr
}

// Start opens every feed (StartFromPosition when a Position is set, else from
// current head), switches it to change-only mode, and begins periodic flush.
func (rf *ReverseFeed) Start(ctx context.Context) error {
	for i, client := range rf.clients {
		var err error
		if rf.sources[i].Position != "" {
			err = client.StartFromPosition(ctx, rf.sources[i].Position)
		} else {
			err = client.Start(ctx)
		}
		if err != nil {
			return fmt.Errorf("reverse feed: start source %d: %w", i, err)
		}
		// Change-only: apply every change regardless of copy watermark, since
		// there is no copier. This is what the forward move disables at cutover.
		if err := client.SetWatermarkOptimization(ctx, false); err != nil {
			return fmt.Errorf("reverse feed: disable watermark optimization on source %d: %w", i, err)
		}
		client.StartPeriodicFlush(ctx, rf.flushInt)
	}
	return nil
}

// Flush drains all feeds synchronously (e.g. before a health check or a reverse
// cutover, so U reflects everything written to the sources so far).
func (rf *ReverseFeed) Flush(ctx context.Context) error {
	for i, client := range rf.clients {
		if err := client.Flush(ctx); err != nil {
			return fmt.Errorf("reverse feed: flush source %d: %w", i, err)
		}
	}
	return nil
}

// Positions returns each source's current safe-to-resume position, in the same
// order as the configured sources. Intended for the caller's checkpoint so the
// window can resume in reverse mode after a restart.
func (rf *ReverseFeed) Positions() []string {
	out := make([]string, len(rf.clients))
	for i, client := range rf.clients {
		out[i] = client.Position()
	}
	return out
}

// Run opens the feeds and holds the rollback window for the given duration,
// keeping U current. It returns:
//   - nil when the window elapses normally (after a final flush);
//   - ctx.Err() if the context is cancelled;
//   - a fatal error if any feed dies (rollback is then unsafe and the caller
//     must complete-forward).
//
// Run does not Close the feeds; the caller does that after deciding the
// terminal action (complete-forward or roll back), since a reverse cutover
// needs the feeds flushed one last time first.
func (rf *ReverseFeed) Run(ctx context.Context, window time.Duration) error {
	if err := rf.Start(ctx); err != nil {
		return err
	}
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rf.fatalCh:
		return rf.Err()
	case <-timer.C:
	}
	// Window elapsed normally: final drain so U reflects everything written to
	// the sources during the window before the caller retires it.
	return rf.Flush(ctx)
}

// Close stops periodic flush and closes all feeds. Safe to call more than once.
// The applier is never Started (the subscription apply path is synchronous), so
// there is nothing to Stop on it.
func (rf *ReverseFeed) Close() {
	for _, client := range rf.clients {
		client.StopPeriodicFlush()
		client.Close()
	}
}
