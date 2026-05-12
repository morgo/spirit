// Package repl contains binary log subscription functionality.
package repl

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

const (
	binlogTrivialThreshold = 10000
	// DefaultBatchSize is the number of rows in each batched REPLACE/DELETE statement.
	// Larger is better, but we need to keep the run-time of the statement well below
	// dbconn.maximumLockTime so that it doesn't prevent copy-row tasks from failing.
	// Since on some of our Aurora tables with out-of-cache workloads only copy ~300 rows per second,
	// we probably shouldn't set this any larger than about 1K. It will also use
	// multiple-flush-threads, which should help it group commit and still be fast.
	// This is only used as an initial starting value. It will auto-scale based on the DefaultTargetBatchTime.
	DefaultBatchSize = 1000
	// minBatchSize is the minimum batch size that we will allow the targetBatchSize to be.
	minBatchSize = 5
	// DefaultTargetBatchTime is the target time for flushing REPLACE/DELETE statements.
	DefaultTargetBatchTime = time.Millisecond * 500

	// DefaultFlushInterval is the time that the client will flush all binlog changes to disk.
	// Longer values require more memory, but permit more merging.
	// I expect we will change this to 1hr-24hr in the future.
	DefaultFlushInterval = 30 * time.Second
	// DefaultSubscriptionSoftLimitBytes caps the approximate memory held
	// per subscription before HasChanged starts blocking on the buffered
	// map's condition variable. The cap is "soft": a single oversized
	// row admitted when the buffer is empty will exceed the limit, and
	// the next caller will park until that row drains. This keeps wide
	// rows (LONGTEXT / BLOB / large JSON) from OOMing the migrator
	// while still guaranteeing forward progress regardless of row width.
	// See pkg/repl/subscription_buffered.go for the accounting model.
	//
	// Operators should be aware that pausing the binlog reader for an
	// extended period risks falling past the source's binlog retention
	// (binlog_expire_logs_seconds). Tune this value, or the source's
	// retention, accordingly.
	DefaultSubscriptionSoftLimitBytes = 256 << 20
	// DefaultTimeout is how long BlockWait is supposed to wait before returning errors.
	DefaultTimeout = 30 * time.Second
	// Maximum number of consecutive errors before recreating the streamer
	maxConsecutiveErrors = 5
	// Initial backoff duration for streamer recreation
	initialBackoffDuration = time.Second
	// Maximum backoff duration
	maxBackoffDuration = time.Minute
	// Backoff multiplier
	backoffMultiplier = 2
	// Sleep time between position checks in BlockWait
	blockWaitSleep = 100 * time.Millisecond
	// Number of consecutive blockWaitSleep intervals where the buffered position
	// hasn't advanced before BlockWait flushes binary logs to nudge the syncer.
	// 3 * blockWaitSleep (~300ms) tolerates brief syncer lag (e.g. CI load) while
	// remaining negligible relative to DefaultTimeout.
	blockWaitStallThreshold = 3
)

var (
	// maxRecreateAttempts is the maximum number of streamer recreation attempts before giving up.
	// This is really a const, but set to var for testing.
	maxRecreateAttempts = 10

	// ErrChangesNotFlushed indicates that not all changes have been flushed from the replication feed.
	ErrChangesNotFlushed = errors.New("not all changes flushed")
)

type Client struct {
	sync.Mutex

	host     string
	username string
	password string

	cfg      replication.BinlogSyncerConfig
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer

	// The DB connection is used for queries like SHOW MASTER STATUS
	db       *sql.DB
	applier  applier.Applier
	dbConfig *dbconn.DBConfig

	// subscriptions is a map of tables that are actively
	// watching for changes on. The key is schemaName.tableName.
	// each subscription has its own set of changes.
	subscriptions map[string]Subscription

	// callerCancelFunc is an optional callback that is called when a DDL
	// change is detected on a subscribed table, or when a fatal stream
	// error occurs. The caller is expected to handle cancellation and
	// cleanup in this callback. It returns true if the error was acted
	// upon (i.e. the caller actually cancelled), or false if it was
	// ignored (e.g. because the migration is already past cutover).
	callerCancelFunc func() bool
	ddlFilterSchema  string
	ddlFilterTables  map[string]struct{}

	serverID    uint32         // server ID for the binlog reader
	bufferedPos mysql.Position // buffered position
	flushedPos  mysql.Position // safely written to new table

	// GTID tracking. When the source server has gtid_mode=ON at the time Run()
	// is called, we drive the syncer with GTID sets instead of file/pos so that
	// resume can survive a source failover. The file/pos fields above are still
	// updated from the binlog stream so that BlockWait and existing callers
	// keep working; on resume we prefer GTID when available.
	//
	// If gtid_mode flips OFF mid-run, we keep using the GTID set we already have;
	// the resume after restart in that case will fail loudly, which is by design.
	gtidMode     bool          // captured at Run(); does not track mid-run flips
	bufferedGTID mysql.GTIDSet // buffered GTID set (nil if !gtidMode)
	flushedGTID  mysql.GTIDSet // safely-applied GTID set (nil if !gtidMode)

	statisticsLock  sync.Mutex
	targetBatchTime time.Duration
	targetBatchSize int64 // will auto-adjust over time, use atomic to read/set
	timingHistory   []time.Duration
	concurrency     int

	binlogStatusStmt string // cached: "SHOW MASTER STATUS" or "SHOW BINARY LOG STATUS"

	// The periodic flush lock is just used for ensuring only one periodic flush runs at a time,
	// and when we disable it, no more periodic flushes will run. The actual flushing is protected
	// by a lower level lock (sync.Mutex on Client)
	periodicFlushLock    sync.Mutex
	periodicFlushEnabled bool

	cancelFunc func()
	isClosed   atomic.Bool
	logger     *slog.Logger
	streamWG   sync.WaitGroup // tracks readStream goroutine for proper cleanup

	// subscriptionSoftLimitBytes is the per-subscription byte cap passed
	// to bufferedMap.softLimitBytes on construction. Zero disables the
	// cap. See DefaultSubscriptionSoftLimitBytes.
	subscriptionSoftLimitBytes int64

	flushedBinlogs atomic.Int64 // for testing binlog flushing frequency
}

// NewClient creates a new Client instance.
// config.Applier is required!
func NewClient(db *sql.DB, host string, username, password string, appl applier.Applier, config *ClientConfig) *Client {
	if config.DBConfig == nil {
		config.DBConfig = dbconn.NewDBConfig() // default DB config
	}
	softLimit := config.SubscriptionSoftLimitBytes
	if softLimit == 0 {
		softLimit = DefaultSubscriptionSoftLimitBytes
	} else if softLimit < 0 {
		softLimit = 0 // explicit opt-out
	}
	return &Client{
		db:                         db,
		dbConfig:                   config.DBConfig,
		host:                       host,
		username:                   username,
		password:                   password,
		logger:                     config.Logger,
		targetBatchTime:            config.TargetBatchTime,
		targetBatchSize:            DefaultBatchSize, // initial starting value.
		concurrency:                config.Concurrency,
		subscriptions:              make(map[string]Subscription),
		callerCancelFunc:           config.CancelFunc,
		ddlFilterSchema:            config.DDLFilterSchema,
		ddlFilterTables:            toSet(config.DDLFilterTables),
		serverID:                   config.ServerID,
		applier:                    appl,
		subscriptionSoftLimitBytes: softLimit,
	}
}

type ClientConfig struct {
	TargetBatchTime time.Duration
	Concurrency     int
	Logger          *slog.Logger
	ServerID        uint32
	DBConfig        *dbconn.DBConfig // Database configuration including TLS settings

	// CancelFunc is an optional callback from the caller (e.g. migration or move runner).
	// It is called when a DDL change is detected on a subscribed table, or when a fatal
	// stream error occurs (such as minimal RBR detection or exhausted streamer recreation
	// attempts). The caller is expected to handle cancellation and cleanup.
	// It returns true if the error was acted upon (caller actually cancelled),
	// or false if it was ignored (e.g. because the caller is already past cutover).
	CancelFunc func() bool

	// DDLFilterSchema, when set, broadens DDL detection to cancel on any DDL change
	// in the specified schema, rather than only on exact table matches against subscriptions.
	// This is used by the move runner to detect DDL on any table in the source database.
	DDLFilterSchema string

	// DDLFilterTables, when set alongside DDLFilterSchema, narrows the schema-level
	// DDL detection to only the specified table names. This is used for partial moves
	// where only specific tables from a schema are being moved — DDL on unrelated
	// tables in the same schema should not trigger cancellation.
	// If empty (and DDLFilterSchema is set), all tables in the schema trigger cancellation.
	DDLFilterTables []string

	// SubscriptionSoftLimitBytes overrides DefaultSubscriptionSoftLimitBytes
	// for new subscriptions. Set to a negative value to disable the cap
	// entirely (HasChanged will never block on memory). Zero (the
	// zero-value default) means use DefaultSubscriptionSoftLimitBytes.
	SubscriptionSoftLimitBytes int64
}

// serverIDCounter is an atomic counter used to help ensure unique server IDs
var serverIDCounter atomic.Uint32

// NewServerID generates a unique server ID to avoid conflicts with other binlog readers.
// Uses crypto/rand combined with an atomic counter to ensure uniqueness even when called
// concurrently. Returns a value in the range 1001-4294967295 to avoid conflicts with
// typical MySQL server IDs (0-1000).
func NewServerID() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback to nanosecond-based generation if crypto/rand fails (should never happen)
		rangeSize := int64(^uint32(0) - 1000)
		return uint32(time.Now().UnixNano()%rangeSize) + 1001
	}
	// Convert bytes to uint32, mix with counter, and map to valid range
	randomPart := binary.BigEndian.Uint32(b[:])
	counterPart := serverIDCounter.Add(1)

	// XOR the random and counter parts for better distribution
	result := randomPart ^ counterPart

	// Map result into the range [1001, max uint32]
	// Use modulo to constrain to the valid range, then add 1001
	result = (result % (^uint32(0) - 1000)) + 1001

	return result
}

// NewClientDefaultConfig returns a default config for the copier.
func NewClientDefaultConfig() *ClientConfig {
	return &ClientConfig{
		Concurrency:     4,
		TargetBatchTime: DefaultTargetBatchTime,
		Logger:          slog.Default(),
		ServerID:        NewServerID(),
	}
}

// AddSubscription adds a new subscription.
// Returns an error if a subscription already exists for the given table.
func (c *Client) AddSubscription(currentTable, newTable *table.TableInfo, chunker table.MappedChunker) error {
	c.Lock()
	defer c.Unlock()

	subKey := encodeSchemaTable(currentTable.SchemaName, currentTable.TableName)
	if _, exists := c.subscriptions[subKey]; exists {
		return fmt.Errorf("subscription already exists for table %s.%s", currentTable.SchemaName, currentTable.TableName)
	}
	// If the PK is not memory comparable we still use the buffered map, but it
	// needs to know that when the watermark optimizations are disabled
	// (i.e. we've finished copying and we're about to start checksum),
	// that it needs to act like a FIFO queue. This is a requirement because of edge
	// cases caused by collations since A == a, but in our map they would
	// not compare as equal.
	var pkIsMemoryComparable = true
	if err := currentTable.PrimaryKeyIsMemoryComparable(); err != nil {
		pkIsMemoryComparable = false
	}
	sub := &bufferedMap{
		table:                currentTable,
		newTable:             newTable,
		changes:              make(map[string]bufferedChange),
		c:                    c,
		chunker:              chunker,
		applier:              c.applier,
		pkIsMemoryComparable: pkIsMemoryComparable,
		softLimitBytes:       c.subscriptionSoftLimitBytes,
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	c.subscriptions[subKey] = sub
	return nil
}

// setBufferedPos updates the in-memory position that all changes have been read
// but not necessarily flushed.
func (c *Client) setBufferedPos(pos mysql.Position) {
	c.Lock()
	defer c.Unlock()
	c.bufferedPos = pos
}

// getBufferedPos returns the buffered position under a mutex.
func (c *Client) getBufferedPos() mysql.Position {
	c.Lock()
	defer c.Unlock()
	return c.bufferedPos
}

// SetFlushedPos updates the known safe position that all changes have been flushed.
// It is used for resuming from a checkpoint.
func (c *Client) SetFlushedPos(pos mysql.Position) {
	c.Lock()
	defer c.Unlock()
	c.flushedPos = pos
}

// SetFlushedGTID restores the safe-flushed GTID set from a checkpoint. The
// parsed value is also used by Run() as the starting point for the syncer.
// Passing an empty string is a no-op and leaves the client in file/pos mode.
func (c *Client) SetFlushedGTID(s string) error {
	if s == "" {
		return nil
	}
	gset, err := mysql.ParseMysqlGTIDSet(s)
	if err != nil {
		return fmt.Errorf("could not parse checkpoint GTID set %q: %w", s, err)
	}
	c.Lock()
	defer c.Unlock()
	c.flushedGTID = gset
	c.gtidMode = true
	return nil
}

// GetBinlogApplyGTID returns the safely-applied GTID set as a string, or ""
// if the source is not in GTID mode. Used by the migration/move runners to
// persist the GTID set in checkpoints.
func (c *Client) GetBinlogApplyGTID() string {
	c.Lock()
	defer c.Unlock()
	if c.flushedGTID == nil {
		return ""
	}
	return c.flushedGTID.String()
}

// GTIDModeEnabled returns whether the client was started against a server
// with gtid_mode=ON. The value is decided at Run() and does not track mid-run
// changes to the server variable.
func (c *Client) GTIDModeEnabled() bool {
	c.Lock()
	defer c.Unlock()
	return c.gtidMode
}

func (c *Client) AllChangesFlushed() bool {
	c.Lock()
	defer c.Unlock()
	// We check if the buffered position is ahead of the flushed position.
	// We have a mutex, so we can read safely.
	if c.bufferedPos.Compare(c.flushedPos) > 0 {
		c.logger.Warn("Binlog reader info flushed-pos buffered-pos. Discrepancies could be due to modifications on other tables.", "flushed-pos", c.flushedPos, "buffered-pos", c.bufferedPos)
	}
	// We check if all subscriptions have flushed their changes.
	for _, subscription := range c.subscriptions {
		if subscription.Length() > 0 {
			return false
		}
	}
	return true
}

func (c *Client) GetBinlogApplyPosition() mysql.Position {
	c.Lock()
	defer c.Unlock()
	return c.flushedPos
}

// GetDeltaLen returns the total number of changes
// that are pending across all subscriptions.
// Acquires the client lock for thread safety.
func (c *Client) GetDeltaLen() int {
	c.Lock()
	defer c.Unlock()
	deltaLen := 0
	for _, subscription := range c.subscriptions {
		deltaLen += subscription.Length()
	}
	return deltaLen
}

func (c *Client) getCurrentBinlogPosition(ctx context.Context) (mysql.Position, error) {
	// We rotate the binary log before we start, so we can always safely just resume
	// by reopening the binary log file at Position 4. This is required to get the table map.
	// Why we need to recreate the syncer just after it is created is a mystery to me, but
	// we seem to have this issue in tests sometimes.
	if _, err := c.db.ExecContext(ctx, `FLUSH BINARY LOGS`); err != nil {
		return mysql.Position{}, fmt.Errorf("failed to flush binary logs: %w", err)
	}
	var binlogFile, fake string
	var binlogPos uint32
	// On the first call, try SHOW MASTER STATUS (works on MySQL 8.0, the most common version)
	// and fall back to SHOW BINARY LOG STATUS (MySQL 8.2+). Cache whichever succeeds
	// so subsequent calls don't waste a round-trip.
	if c.binlogStatusStmt == "" {
		err := c.db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
		if err == nil {
			c.binlogStatusStmt = "SHOW MASTER STATUS"
		} else {
			err = c.db.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
			if err == nil {
				c.binlogStatusStmt = "SHOW BINARY LOG STATUS"
			} else {
				return mysql.Position{}, err
			}
		}
	} else {
		err := c.db.QueryRowContext(ctx, c.binlogStatusStmt).Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
		if err != nil {
			return mysql.Position{}, err
		}
	}
	return mysql.Position{
		Name: binlogFile,
		Pos:  binlogPos,
	}, nil
}

// detectGTIDMode returns true if the source has gtid_mode=ON.
// ON_PERMISSIVE / OFF_PERMISSIVE / OFF all count as "not on", because in those
// modes some transactions don't carry GTIDs and resume-by-GTID is unsafe.
func (c *Client) detectGTIDMode(ctx context.Context) (bool, error) {
	var mode string
	if err := c.db.QueryRowContext(ctx, "SELECT @@global.gtid_mode").Scan(&mode); err != nil {
		return false, err
	}
	return strings.EqualFold(mode, "ON"), nil
}

// getCurrentGTIDSet returns the current value of gtid_executed parsed as a
// mysql.MysqlGTIDSet. Used as the starting point when no resume checkpoint
// is supplying one.
func (c *Client) getCurrentGTIDSet(ctx context.Context) (mysql.GTIDSet, error) {
	var s string
	if err := c.db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&s); err != nil {
		return nil, err
	}
	// gtid_executed may contain literal newlines for readability; strip them
	// so ParseMysqlGTIDSet doesn't choke on whitespace inside intervals.
	s = strings.ReplaceAll(s, "\n", "")
	return mysql.ParseMysqlGTIDSet(s)
}

// gtidIsImpossible reports whether the saved flushedGTID set is too far
// behind the source's gtid_purged for resume to be safe. The relevant check
// is "has the server purged any GTID that the saved set does NOT already
// contain?" — those are the events we would need but cannot receive.
// (Purging GTIDs that the saved set DOES contain is harmless: those
// transactions are already applied and we never have to replay them.)
//
// Returns (true, nil) when resume is unsafe, (false, nil) when it is safe,
// and a non-nil error if the validating query itself fails.
func (c *Client) gtidIsImpossible(ctx context.Context) (bool, error) {
	if c.flushedGTID == nil {
		return false, nil
	}
	// GTID_SUBSET(set1, set2) returns 1 iff every GTID in set1 is also in
	// set2. We want the server's gtid_purged to be a subset of saved —
	// otherwise the server has dropped events between saved and now.
	var isSubset int
	row := c.db.QueryRowContext(ctx,
		"SELECT GTID_SUBSET(@@global.gtid_purged, ?)", c.flushedGTID.String())
	if err := row.Scan(&isSubset); err != nil {
		return true, err
	}
	return isSubset != 1, nil
}

// Run initializes the binlog syncer and starts the binlog reader.
// It returns an error if the initialization fails.
func (c *Client) Run(ctx context.Context) (err error) {
	c.Lock()
	defer c.Unlock()

	host, portStr, err := net.SplitHostPort(c.host)
	if err != nil {
		return fmt.Errorf("failed to parse host: %w", err)
	}
	// convert portStr to a uint16
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse port: %w", err)
	}
	c.cfg = replication.BinlogSyncerConfig{
		ServerID: c.serverID,
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     c.username,
		Password: c.password,
		Logger:   c.logger,
	}

	// Apply TLS configuration using the same infrastructure as main database connections
	if c.dbConfig != nil {
		tlsConfig, err := dbconn.GetTLSConfigForBinlog(c.dbConfig, host)
		if err != nil {
			return fmt.Errorf("failed to configure TLS for binlog connection: %w", err)
		}
		c.cfg.TLSConfig = tlsConfig
	}

	// Decide once, at startup, whether the source advertises GTID mode. We
	// don't re-check this later: if the operator flips gtid_mode mid-run we
	// keep doing whatever we started with. The resume path is what enforces
	// that gtid_mode hasn't been turned OFF between runs.
	gtidEnabled, err := c.detectGTIDMode(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect gtid_mode: %w", err)
	}
	// If a checkpoint restored a GTID set via SetFlushedGTID, the server must
	// still be in GTID mode — otherwise file/pos coordinates don't map to the
	// checkpoint and silent corruption is the alternative.
	if c.flushedGTID != nil && !gtidEnabled {
		return errors.New("checkpoint contains a GTID set but gtid_mode is OFF on the source; cannot resume")
	}

	// Which coordinate are we going to drive the syncer with?
	//   - GTID: the caller supplied a GTID set (resume from a new-style
	//     checkpoint), OR there's no checkpoint at all and the server has
	//     gtid_mode=ON (fresh start on a GTID-enabled server).
	//   - file/pos: the caller supplied only a file/pos (legacy resume), or
	//     the server is not in GTID mode (fresh start on a non-GTID server).
	// The legacy-resume branch is what lets us upgrade Spirit mid-flight
	// without orphaning checkpoints that pre-date the GTID column.
	useGTID := c.flushedGTID != nil || (c.flushedPos.Name == "" && gtidEnabled)
	c.gtidMode = useGTID

	if useGTID {
		// On a fresh start we read the current gtid_executed; on resume we
		// keep whatever SetFlushedGTID populated. We still call
		// getCurrentBinlogPosition so that bufferedPos has something
		// sensible for BlockWait()/logging, even though resume uses GTID.
		if c.flushedGTID == nil {
			gset, gerr := c.getCurrentGTIDSet(ctx)
			if gerr != nil {
				return fmt.Errorf("failed to read gtid_executed: %w", gerr)
			}
			c.flushedGTID = gset
		} else if impossible, ierr := c.gtidIsImpossible(ctx); ierr != nil {
			return fmt.Errorf("failed to validate checkpoint GTID against gtid_purged: %w", ierr)
		} else if impossible {
			return errors.New("checkpoint GTID set has been purged on the source, cannot resume")
		}
		c.bufferedGTID = c.flushedGTID.Clone()
		// Also seed the file/pos for any non-resume consumers that still
		// read it (BlockWait, logging). This is best-effort.
		if pos, perr := c.getCurrentBinlogPosition(ctx); perr == nil {
			if c.flushedPos.Name == "" {
				c.flushedPos = pos
			}
			c.bufferedPos = c.flushedPos
		}
		c.syncer = replication.NewBinlogSyncer(c.cfg)
		c.streamer, err = c.syncer.StartSyncGTID(c.flushedGTID)
		if err != nil {
			return fmt.Errorf("failed to start binlog GTID streamer: %w", err)
		}
	} else {
		// File/pos path (unchanged behaviour for non-GTID servers, also
		// used when resuming from a legacy file/pos-only checkpoint even
		// if the server happens to be in GTID mode now).
		if c.flushedPos.Name == "" {
			c.flushedPos, err = c.getCurrentBinlogPosition(ctx)
			if err != nil {
				return fmt.Errorf("failed to get binlog position, check binary is enabled: %w", err)
			}
		} else if c.binlogPositionIsImpossible(ctx) {
			return errors.New("binlog position is impossible, the source may have already purged it")
		}
		c.bufferedPos = c.flushedPos
		c.syncer = replication.NewBinlogSyncer(c.cfg)
		c.streamer, err = c.syncer.StartSync(c.flushedPos)
		if err != nil {
			return fmt.Errorf("failed to start binlog streamer: %w", err)
		}
	}
	// Start the binlog reader in a go routine, using a context with cancel.
	// Write the cancel function to c.cancelFunc
	ctx, c.cancelFunc = context.WithCancel(ctx)
	c.streamWG.Add(1)
	go c.readStream(ctx)
	return nil
}

// recreateStreamer recreates the binlog streamer from the current buffered position.
// When we recreate the syncer, the parser's table map is lost. MySQL only sends
// TableMapEvents once per connection, so when we resume from a mid-stream position,
// we won't have the table metadata needed to decode RowsEvents.
//
// To handle this safely, we check if the binlog has rotated since we started:
// - Same file as initial: error
// - Different file: Safely recreate and resume from position 4.
func (c *Client) recreateStreamer() error {
	c.Lock()
	defer c.Unlock()

	c.logger.Info("recreateStreamer called",
		"buffered_position", c.bufferedPos,
		"syncer_exists", c.syncer != nil,
		"streamer_exists", c.streamer != nil)

	// Close the existing syncer completely
	// Since we can't do anything with it.
	if c.syncer != nil {
		c.syncer.Close()
	}

	c.syncer = replication.NewBinlogSyncer(c.cfg)
	var err error
	if c.gtidMode {
		// In GTID mode we can simply resume from the last safely-flushed GTID
		// set. We replay events from there forward; the subscription data
		// structures (deltaMap, bufferedMap, deltaQueue) are idempotent so
		// reprocessing is harmless. We deliberately use flushedGTID rather
		// than bufferedGTID because anything between flushed and buffered
		// is data we don't yet consider durable.
		resumeGTID := c.flushedGTID.Clone()
		c.logger.Info("Recreating streamer from GTID set",
			"gtid_set", resumeGTID.String(),
		)
		c.streamer, err = c.syncer.StartSyncGTID(resumeGTID)
	} else {
		// Still on the same binlog file we started with.
		// Safe to replay from position 4 because the subscription data structures
		// (deltaMap, bufferedMap, deltaQueue) are idempotent - reprocessing events
		// will simply overwrite previous state with the same value.
		newStartPos := mysql.Position{
			Name: c.bufferedPos.Name,
			Pos:  4, // Binlog files always start at position 4
		}
		c.logger.Info("Recreating streamer from file start",
			"file", c.bufferedPos.Name,
			"previous_position", c.bufferedPos.Pos,
			"new_start_position", newStartPos,
		)
		c.streamer, err = c.syncer.StartSync(newStartPos)
	}
	if err != nil {
		c.logger.Error("Failed to start binlog streamer in recreateStreamer",
			"error", err,
			"gtid_mode", c.gtidMode,
			"config", fmt.Sprintf("host=%s:%d user=%s", c.cfg.Host, c.cfg.Port, c.cfg.User))
		return fmt.Errorf("failed to start binlog streamer: %w", err)
	}
	return nil
}

// readStream continuously reads the binlog stream. It is usually called in a go routine.
// It will read the stream until the context is closed
// *and* it continues on any errors
func (c *Client) readStream(ctx context.Context) {
	defer c.streamWG.Done() // Signal completion when goroutine exits

	c.Lock()
	currentLogName := c.flushedPos.Name
	startPos := c.flushedPos // Copy while holding lock
	c.Unlock()

	consecutiveErrors := 0
	recreateAttempts := 0
	backoffDuration := initialBackoffDuration
	lastErrorTime := time.Time{}
	var recentErrors []string // Track recent errors for debugging

	c.logger.Debug("readStream started for binlog position", "position", startPos, "log_name", currentLogName)

	for {
		// Check if context is done before processing
		select {
		case <-ctx.Done():
			c.logger.Debug("readStream context cancelled", "error", ctx.Err())
			return // stop processing
		default:
		}

		var ev *replication.BinlogEvent
		var err error

		// If streamer is nil (such as after a failed recreation), treat it as an error
		// This will then trigger the recreation
		if c.streamer == nil {
			err = errors.New("binlog streamer is nil, cannot read events")
		} else {
			// Read the next event from the stream
			ev, err = c.streamer.GetEvent(ctx)
		}

		if err != nil {
			// We only stop processing for context cancelled errors.
			if errors.Is(err, context.Canceled) || ctx.Err() != nil || c.isClosed.Load() {
				return // stop processing
			}

			consecutiveErrors++
			currentTime := time.Now()

			// Track recent errors for debugging (keep last 20)
			errorMsg := fmt.Sprintf("[%s] %v", currentTime.Format("15:04:05.000"), err)
			recentErrors = append(recentErrors, errorMsg)
			if len(recentErrors) > 20 {
				recentErrors = recentErrors[1:]
			}

			c.logger.Error("error reading binlog stream", "consecutive_errors", consecutiveErrors, "error", err, "current_position", c.getBufferedPos())

			// If we've had too many consecutive errors, try to recreate the streamer
			if consecutiveErrors >= maxConsecutiveErrors {
				recreateAttempts++

				// Get current state information for debugging
				currentPos := c.getBufferedPos()

				c.logger.Warn("Too many consecutive errors, attempting to recreate streamer",
					"consecutive_errors", consecutiveErrors,
					"attempt", recreateAttempts,
					"max_attempts", maxRecreateAttempts,
					"current_position", currentPos,
					"backoff_duration", backoffDuration)

				// Check if we've exceeded the maximum number of recreation attempts
				if recreateAttempts >= maxRecreateAttempts {
					c.logger.Error("failed to recreate binlog streamer, giving up",
						"total_attempts", recreateAttempts,
						"current_position", currentPos,
						"start_position", startPos,
						"recent_errors", recentErrors,
						"is_closed", c.isClosed.Load())

					c.fatalError()
					return
				}

				// Apply exponential backoff
				if currentTime.Sub(lastErrorTime) < backoffDuration {
					c.logger.Info("Backing off before recreating streamer", "duration", backoffDuration)
					backoffTimer := time.NewTimer(backoffDuration)
					select {
					case <-ctx.Done():
						backoffTimer.Stop()
						return
					case <-backoffTimer.C:
					}
				}

				// Try to recreate the streamer
				if recreateErr := c.recreateStreamer(); recreateErr != nil {
					c.logger.Error("Failed to recreate streamer", "error", recreateErr)

					// Increase backoff duration for next attempt
					backoffDuration *= backoffMultiplier
					if backoffDuration > maxBackoffDuration {
						backoffDuration = maxBackoffDuration
					}
				} else {
					// Successfully recreated, reset counters
					consecutiveErrors = 0
					recreateAttempts = 0
					backoffDuration = initialBackoffDuration
				}
				lastErrorTime = currentTime
			}

			// Short sleep before retrying
			retryTimer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				retryTimer.Stop()
				return
			case <-retryTimer.C:
			}
			continue
		}

		// Reset error counters on successful read
		if consecutiveErrors > 0 {
			c.logger.Info("Binlog stream recovered after consecutive errors", "consecutive_errors", consecutiveErrors)
			consecutiveErrors = 0
			backoffDuration = initialBackoffDuration
		}

		if ev == nil {
			continue
		}
		// Handle the event.
		switch event := ev.Event.(type) {
		case *replication.RotateEvent:
			// Rotate event, update the current log name.
			currentLogName = string(event.NextLogName)
			// For RotateEvent, we must use event.Position (the position in the NEW log)
			// not ev.Header.LogPos (which is the position in the OLD log).
			// Update position immediately and skip the generic position update at the end.
			c.setBufferedPos(mysql.Position{
				Name: currentLogName,
				Pos:  uint32(event.Position),
			})
			continue
		case *replication.RowsEvent:
			// Rows event, check if there are any active subscriptions
			// for it, and pass it to the subscription.
			if err = c.processRowsEvent(ev, event); err != nil {
				c.logger.Error("fatal error processing binlog rows event", "error", err)
				c.fatalError()
				return
			}
		case *replication.QueryEvent:
			// Query event, check if it is a DDL statement,
			// in which case we need to notify the caller.
			ddlTables, err := extractTablesFromDDLStmts(string(event.Schema), string(event.Query))
			if err != nil {
				// The parser does not understand all syntax.
				// For example, it won't parse [CREATE|DROP] TRIGGER statements *or*
				// ALTER USER x IDENTIFIED WITH x RETAIN CURRENT PASSWORD
				// This behavior is copied from canal:
				// https://github.com/go-mysql-org/go-mysql/blob/ee9447d96b48783abb05ab76a12501e5f1161e47/canal/sync.go#L144C1-L150C1
				// We can't print the statement because it could contain user-data.
				// We instead rely on file + pos being useful.
				c.logger.Error("Skipping query that was unable to parse", "file", currentLogName, "pos", ev.Header.LogPos)
				continue
			}
			for _, ddlTable := range ddlTables {
				c.processDDLNotification(ddlTable.schema, ddlTable.table)
			}
		case *replication.GTIDEvent:
			// When the source is in GTID mode, record the GTID of the
			// transaction that is about to be applied so that flush()
			// can snapshot a safe-applied GTID set alongside file/pos.
			// We mutate bufferedGTID under the client lock; the set is
			// shared with flush() and must not be touched without it.
			c.Lock()
			if c.gtidMode && c.bufferedGTID != nil {
				if sid, uerr := uuid.FromBytes(event.SID); uerr == nil {
					gtidStr := sid.String() + ":" + strconv.FormatInt(event.GNO, 10)
					if uerr := c.bufferedGTID.Update(gtidStr); uerr != nil {
						c.logger.Warn("could not merge GTID into buffered set", "gtid", gtidStr, "error", uerr)
					}
				}
			}
			c.Unlock()
		case *replication.TableMapEvent,
			*replication.XIDEvent,
			*replication.FormatDescriptionEvent,
			*replication.PreviousGTIDsEvent:
			// Known stream-housekeeping events. We don't act on them here; the
			// position is still advanced via the LogPos update below. They are
			// listed explicitly (rather than handled by the default case) so the
			// default can keep logging genuinely unknown event types — a future
			// row-event variant we don't recognize could otherwise cause silent
			// data loss.
		default:
			c.logger.Debug("Received unknown event type", "type", fmt.Sprintf("%T", ev.Event))
		}
		// Update the buffered position
		// under a mutex.
		// Only update if LogPos > 0 (some events like FormatDescriptionEvent have LogPos=0)
		// and only if the position is moving forward (to avoid going backwards after rotation)
		if ev.Header.LogPos > 0 {
			newPos := mysql.Position{
				Name: currentLogName,
				Pos:  ev.Header.LogPos,
			}
			currentPos := c.getBufferedPos()
			// Only update if the new position is ahead of the current position
			if newPos.Compare(currentPos) > 0 {
				c.setBufferedPos(newPos)
			}
		}
	}
}

// processDDLNotification cancels the client if the DDL matches our filter criteria.
// By default, only exact schema.table matches against subscriptions trigger cancellation.
// If ddlFilterSchema is set, any DDL in that schema triggers cancellation instead.
// If ddlFilterTables is also set (alongside ddlFilterSchema), only DDL on those
// specific tables within the schema triggers cancellation — this is used for partial
// moves where only a subset of tables from a schema are being moved.
func (c *Client) processDDLNotification(schema, table string) {
	if c.ddlFilterSchema != "" {
		// Schema-level filtering: cancel on DDL in the specified schema.
		if schema != c.ddlFilterSchema {
			return
		}
		// If ddlFilterTables is set, further narrow to only those tables.
		if len(c.ddlFilterTables) > 0 {
			if _, ok := c.ddlFilterTables[table]; !ok {
				return
			}
		}
	} else {
		// Check if the schema.table matches any of our subscriptions.
		// This is the default behavior. Snapshot the subscription set
		// under c.Lock and inspect it after release — Tables() is a
		// pure accessor that does not need the client lock.
		c.Lock()
		subs := make([]Subscription, 0, len(c.subscriptions))
		for _, sub := range c.subscriptions {
			subs = append(subs, sub)
		}
		c.Unlock()
		matchFound := false
		for _, sub := range subs {
			for _, tsub := range sub.Tables() { // currentTable, newTable
				if tsub.SchemaName == schema && tsub.TableName == table {
					matchFound = true
					break
				}
			}
			if matchFound {
				break
			}
		}
		if !matchFound {
			return
		}
	}
	if c.fatalError() {
		c.logger.Error("table definition changed, cancelling operation", "schema", schema, "table", table)
	}
}

// processRowsEvent processes a RowsEvent. It will search all active
// subscriptions to find one that matches the event's table:
//
//   - If there is no subscription, the event will be ignored.
//   - If there is, it will call the subscription's keyHasChanged method
//     with the PK that has been changed.
//
// We hold c.Lock only for the subscription map lookup. Once we have the
// subscription pointer, we release the client lock before dispatching to
// HasChanged. This matters for backpressure: bufferedMap.HasChanged can
// block on its own condition variable when the buffer is full, and we
// must not be holding c.Lock while it parks — c.flush() and other
// callers acquire c.Lock briefly and would otherwise be unable to
// progress, which would prevent the very flush that would unblock us.
func (c *Client) processRowsEvent(ev *replication.BinlogEvent, e *replication.RowsEvent) error {
	subName := encodeSchemaTable(string(e.Table.Schema), string(e.Table.Table))
	c.Lock()
	sub, ok := c.subscriptions[subName]
	c.Unlock()
	if !ok {
		return nil // ignore event, it could be to a _new table.
	}

	// Runtime check for minimal RBR (binlog_row_image=MINIMAL).
	if isMinimalRowImage(e) {
		return fmt.Errorf("received a minimal RBR event for table %s.%s, but we require binlog_row_image=FULL on the source server", string(e.Table.Schema), string(e.Table.Table))
	}

	eventType := parseEventType(ev.Header.EventType)

	if eventType == eventTypeUpdate {
		// For update events there are always before and after images (i.e. e.Rows is always in pairs.)
		// With MINIMAL row image, the PK is only included in the before image for non-PK updates.
		// For PK updates, both before and after images will contain the PK columns since they changed.
		for i := 0; i < len(e.Rows); i += 2 {
			beforeRow := e.Rows[i]
			afterRow := e.Rows[i+1]

			// Always process the before image (guaranteed to have PK in minimal mode)
			beforeKey, err := sub.Tables()[0].PrimaryKeyValues(beforeRow)
			if err != nil {
				return err
			}
			if len(beforeKey) == 0 {
				return fmt.Errorf("no primary key found for before row: %#v", beforeRow)
			}

			// With MINIMAL row image, we need to reconstruct the after key
			// by combining the before key with any changed PK columns from the after image
			afterKey := make([]any, len(beforeKey))
			copy(afterKey, beforeKey) // Start with the before key

			// Check if any PK columns were updated by examining the after row
			isPKUpdate := false
			afterRowSlice := afterRow

			for pkIdx, pkCol := range sub.Tables()[0].KeyColumns {
				// Find the position of this PK column in the table columns
				for colIdx, col := range sub.Tables()[0].Columns {
					if col == pkCol {
						// If this column exists in the after image and is not nil, use it
						if colIdx < len(afterRowSlice) && afterRowSlice[colIdx] != nil {
							if fmt.Sprintf("%v", beforeKey[pkIdx]) != fmt.Sprintf("%v", afterRowSlice[colIdx]) {
								afterKey[pkIdx] = afterRowSlice[colIdx]
								isPKUpdate = true
							}
						}
						break
					}
				}
			}

			if isPKUpdate {
				// This is a primary key update - track both delete and insert
				sub.HasChanged(beforeKey, nil, true)      // delete old key
				sub.HasChanged(afterKey, afterRow, false) // insert new key
			} else {
				// Same PK, just a regular update
				sub.HasChanged(beforeKey, afterRow, false)
			}
		}
	} else {
		// For INSERT and DELETE events, process each row normally
		for _, row := range e.Rows {
			key, err := sub.Tables()[0].PrimaryKeyValues(row)
			if err != nil {
				return err
			}
			if len(key) == 0 {
				// In theory this is unreachable since we mandate a PK on tables
				return fmt.Errorf("no primary key found for row: %#v", row)
			}
			switch eventType { //nolint:exhaustive
			case eventTypeInsert:
				sub.HasChanged(key, row, false)
			case eventTypeDelete:
				sub.HasChanged(key, nil, true)
			default:
				c.logger.Error("unknown event type", "type", ev.Header.EventType)
			}
		}
	}
	return nil
}

// isMinimalRowImage returns true if the RowsEvent contains a minimal row image,
// i.e. some columns were skipped. This happens when binlog_row_image=MINIMAL or NOBLOB.
// With full row images, SkippedColumns entries are empty slices.
func isMinimalRowImage(e *replication.RowsEvent) bool {
	for _, skipped := range e.SkippedColumns {
		if len(skipped) > 0 {
			return true
		}
	}
	return false
}

func (c *Client) binlogPositionIsImpossible(ctx context.Context) bool {
	rows, err := c.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return true // if we can't get the logs, its already impossible
	}
	defer utils.CloseAndLog(rows)
	var logname, size, encrypted string
	for rows.Next() {
		if err := rows.Scan(&logname, &size, &encrypted); err != nil {
			return true
		}
		if logname == c.flushedPos.Name {
			return false // We just need presence of the log file for success
		}
	}
	if rows.Err() != nil {
		return true // can't determine.
	}
	return true
}

// fatalError is called from within the readStream goroutine when a truly fatal
// stream error occurs (e.g. unrecoverable stream error, minimal RBR detection,
// or a fatal rows event error). It returns true if the caller acknowledged the
// error (i.e. the cancel function was called and acted upon).
//
// IMPORTANT: This method must NOT call Close() because Close() calls
// streamWG.Wait(), which would deadlock since readStream is the caller.
func (c *Client) fatalError() bool {
	if c.callerCancelFunc != nil {
		return c.callerCancelFunc()
	}
	return false
}

func (c *Client) Close() {
	c.isClosed.Store(true)

	// Cancel the context first to signal readStream goroutine to exit
	if c.cancelFunc != nil {
		c.cancelFunc()
	}

	// Wait for the readStream goroutine to exit cleanly
	// This prevents goroutine leaks detected by goleak in tests
	// It will eventually catch the ctx cancel from calling
	// c.cancelFunc()
	c.streamWG.Wait()

	if c.syncer != nil {
		c.syncer.Close()
	}
}

// FlushUnderTableLock is a final flush under an exclusive table lock using the connection
// that holds a write lock. Because flushing generates binary log events,
// we actually want to call flush *twice*:
//   - The first time flushes the pending changes to the new table.
//   - We then ensure that we have all the binary log changes read from the server.
//   - The second time reads through the changes generated by the first flush
//     and updates the in memory applied position to match the server's position.
//     This is required to satisfy the binlog position is updated for the c.AllChangesFlushed() check.
func (c *Client) FlushUnderTableLock(ctx context.Context, lock *dbconn.TableLock) error {
	if err := c.flush(ctx, true, lock); err != nil {
		return err
	}
	// Wait for the changes flushed to be received.
	if err := c.BlockWait(ctx); err != nil {
		return err
	}
	// Do a final flush
	return c.flush(ctx, true, lock)
}

// Flush is a low level flush, that asks all of the subscriptions to flush
// Some of these will flush a delta map, others will flush a queue.
//
// Note: we yield the lock early because otherwise no new events can be sent
// to the subscriptions while we are flushing.
// This means that the actual buffered position might be slightly ahead by
// the end of the flush. That's OK, we only set the flushed position to the known
// safe buffered position taken at the start.
func (c *Client) flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	c.Lock()
	newFlushedPos := c.bufferedPos
	var newFlushedGTID mysql.GTIDSet
	if c.bufferedGTID != nil {
		// Clone so that subsequent readStream updates to bufferedGTID don't
		// retroactively mutate the snapshot we're about to commit.
		newFlushedGTID = c.bufferedGTID.Clone()
	}
	c.Unlock()
	var allChangesFlushed = true
	for _, subscription := range c.subscriptions {
		flushed, err := subscription.Flush(ctx, underLock, lock)
		if err != nil {
			return err
		}
		if !flushed {
			allChangesFlushed = false
		}
	}
	// If there is a scenario where a key couldn't be flushed because it wasn't
	// below the watermark, then we need to skip advancing the checkpoint.
	// TODO: This could lead to a starvation issue under contention where
	// the checkpoint never advances. The longterm fix for this is that we
	// would need to track the minimum binlog position that applies to a key.
	// We could then advance up to just below the lowest key that couldn't be flushed.
	// This is a little bit complicated, so for now we just accept that in some
	// high contention scenarios the binlog position in the checkpoint
	// won't advance.
	// Another potential fix is that we disable the belowLowWatermark optimization
	// for these high contention cases. But that's not a great solution either,
	// because the low watermark optimization helps a lot in these cases because
	// it reduces contention between the copier and the repl applier.
	if allChangesFlushed {
		c.Lock()
		c.flushedPos = newFlushedPos
		if newFlushedGTID != nil {
			c.flushedGTID = newFlushedGTID
		}
		c.Unlock()
	}
	return nil
}

// Flush empties the changeset in a loop until the amount of changes is considered "trivial".
// The loop is required, because changes continue to be added while the flush is occurring.
func (c *Client) Flush(ctx context.Context) error {
	for {
		// Repeat in a loop until the changeset length is trivial
		if err := c.flush(ctx, false, nil); err != nil {
			return err
		}
		// BlockWait to ensure we've read everything from the server
		// into our buffer. This can timeout, in which case we start
		// a new loop. Typically a timeout occurs when we resume from a checkpoint
		// and move from the copy phase to the apply phase, and there's
		// actually a lot to do!
		if err := c.BlockWait(ctx); err != nil {
			c.logger.Warn("error waiting for binlog reader to catch up", "error", err)
			// Check if the error is due to context cancellation
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		//  If it doesn't timeout, we ensure the deltas
		// are low, and then we can break. Otherwise we continue
		// with a new loop.
		if c.GetDeltaLen() < binlogTrivialThreshold {
			break
		}
	}
	// Flush one more time, since after BlockWait()
	// there might be more changes.
	return c.flush(ctx, false, nil)
}

// StopPeriodicFlush disables the periodic flush, also guaranteeing
// when it returns there is no current flush running
func (c *Client) StopPeriodicFlush() {
	c.periodicFlushLock.Lock()
	defer c.periodicFlushLock.Unlock()
	c.periodicFlushEnabled = false
}

// StartPeriodicFlush starts a loop that periodically flushes the binlog changeset.
// This is used by the migrator to ensure the binlog position is advanced.
func (c *Client) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	c.periodicFlushLock.Lock()
	c.periodicFlushEnabled = true
	c.periodicFlushLock.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.periodicFlushLock.Lock()
			// At some point before cutover we want to disable th periodic flush.
			// The migrator will do this by calling StopPeriodicFlush()
			if !c.periodicFlushEnabled {
				c.periodicFlushLock.Unlock()
				return
			}
			startLoop := time.Now()
			c.logger.Debug("starting periodic flush of binary log")
			// The periodic flush does not respect the throttler since we want to advance the binlog position
			// we allow this to run, and then expect that if it is under load the throttler
			// will kick in and slow down the copy-rows.
			if err := c.flush(ctx, false, nil); err != nil {
				c.logger.Error("error flushing binary log", "error", err)
			}
			c.periodicFlushLock.Unlock()
			c.logger.Info("finished periodic flush of binary log", "total-duration", time.Since(startLoop), "batch-size", atomic.LoadInt64(&c.targetBatchSize))
		}
	}
}

// BlockWait blocks until all changes are *buffered*.
// i.e. the server's current position is 1234, but our buffered position
// is only 100. We need to read all the events until we reach >= 1234.
// We do not need to guarantee that they are flushed though, so
// you need to call Flush() to do that. This call times out!
// The default timeout is 10 seconds, after which an error will be returned.
func (c *Client) BlockWait(ctx context.Context) error {
	targetPos, err := c.getCurrentBinlogPosition(ctx)
	if err != nil {
		return err
	}
	c.logger.Info("waiting to catch up to source position", "target_position", targetPos, "current_position", c.getBufferedPos())
	timer := time.NewTimer(DefaultTimeout)
	defer timer.Stop() // Ensure timer is always stopped to prevent goroutine leak

	prevPos := c.getBufferedPos()
	first := true
	stallCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting to catch up to source position: %v, current position is: %v", targetPos, c.getBufferedPos())
		default:
			currPos := c.getBufferedPos()
			if currPos.Compare(prevPos) <= 0 && !first {
				// Position hasn't advanced. Only flush after multiple consecutive
				// stalls to avoid unnecessary flushes when the binlog syncer is
				// just slightly behind (e.g., under CI load). getCurrentBinlogPosition
				// already flushes once at the start, so a brief stall is expected.
				stallCount++
				if stallCount >= blockWaitStallThreshold {
					c.logger.Debug("buffered position has not advanced, flushing binary logs")
					if err := dbconn.Exec(ctx, c.db, "FLUSH BINARY LOGS"); err != nil {
						return err // it could be context cancelled, return it
					}
					c.flushedBinlogs.Add(1)
					stallCount = 0
				}
			} else {
				stallCount = 0
			}
			prevPos = currPos
			first = false

			if c.getBufferedPos().Compare(targetPos) >= 0 {
				return nil // we are up to date!
			}

			// We are not caught up yet, so we need to wait.
			time.Sleep(blockWaitSleep)
		}
	}
}

// feedback provides feedback on the apply time of changesets.
// We use this to refine the targetBatchSize. This is a little bit
// different for feedback for the copier, because:
//
//  1. frequently the batches will not be full.
//  2. feedback is (at least currently) global to all subscriptions,
//     and does not take into account that inserting into a 2 col table
//     with 0 indexes is much faster than inserting into a 10 col table with 5 indexes.
//
// We still need to use a p90-like mechanism though,
// because the rows being changed are by definition more likely to be hotspots.
// Hotspots == Lock Contention. This is one of the exact reasons why we are
// chunking in the first place. The probability that the applier can cause
// impact on OLTP workloads is much higher than the copier.
func (c *Client) feedback(numberOfKeys int, d time.Duration) {
	c.statisticsLock.Lock()
	defer c.statisticsLock.Unlock()
	if numberOfKeys == 0 {
		return // can't calculate anything, just return
	}
	// For the p90-like mechanism rather than storing all the previous
	// durations, because the numberOfKeys is variable we instead store
	// the timePerKey. We then adjust the targetBatchSize based on this.
	// This creates some skew because small batches will have a higher
	// timePerKey, which can create a back log. Which results in a smaller
	// timePerKey. So at least the skew *should* be self-correcting. This
	// has not yet been proven though.
	timePerKey := d / time.Duration(numberOfKeys)
	c.timingHistory = append(c.timingHistory, timePerKey)

	// If we have enough feedback re-evaluate the target batch size
	// based on the p90 timePerKey.
	if len(c.timingHistory) >= 10 {
		timePerKey := table.LazyFindP90(c.timingHistory)
		newBatchSize := int64(float64(c.targetBatchTime) / float64(timePerKey))
		newBatchSize = max(newBatchSize, minBatchSize)
		atomic.StoreInt64(&c.targetBatchSize, newBatchSize)
		c.timingHistory = nil // reset
	}
}

// SetWatermarkOptimization sets both high and low watermark optimizations
// for all subscriptions. This should be disabled before checksum/cutover to
// ensure all changes are flushed regardless of watermark position.
//
// Each subscription may drain its outgoing store on the toggle (see
// bufferedMap.SetWatermarkOptimization), so this can fail with the drain
// error. If one subscription fails, subsequent subscriptions are not
// touched and the caller should treat the operation as not-yet-applied.
//
// We snapshot the subscription set under c.Lock and then release it before
// toggling, so a long-running drain on one subscription doesn't block
// processRowsEvent from finding subscriptions for unrelated tables.
func (c *Client) SetWatermarkOptimization(ctx context.Context, newVal bool) error {
	c.Lock()
	subs := make([]Subscription, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		subs = append(subs, sub)
	}
	c.Unlock()

	for _, sub := range subs {
		if err := sub.SetWatermarkOptimization(ctx, newVal); err != nil {
			return err
		}
	}
	return nil
}
