package throttler

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestThrottlerInterface(t *testing.T) {
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	db, err := sql.Open("mysql", replicaDSN)
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	//	NewReplicationThrottler will attach either MySQL 8.0 or MySQL 5.7 throttler
	loopInterval = 1 * time.Millisecond
	throttler, err := NewReplicationThrottler(db, 60*time.Second, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, throttler.Open(t.Context()))

	time.Sleep(50 * time.Millisecond)        // make sure the throttler loop can calculate.
	throttler.BlockWait(t.Context())         // wait for catch up (there's no activity)
	assert.False(t, throttler.IsThrottled()) // there's a race, but its unlikely to be throttled

	assert.NoError(t, throttler.Close())

	time.Sleep(50 * time.Millisecond) // give it time to shutdown.
}

func TestNoopThrottler(t *testing.T) {
	throttler := &Noop{}
	assert.NoError(t, throttler.Open(t.Context()))
	throttler.currentLag = 1 * time.Second
	throttler.lagTolerance = 2 * time.Second
	assert.False(t, throttler.IsThrottled())
	assert.NoError(t, throttler.UpdateLag(t.Context()))
	throttler.BlockWait(t.Context())
	throttler.lagTolerance = 100 * time.Millisecond
	assert.True(t, throttler.IsThrottled())
	assert.NoError(t, throttler.Close())
}

func TestMockThrottler(t *testing.T) {
	throttler := &Mock{}

	// Test Open and Close
	assert.NoError(t, throttler.Open(t.Context()))
	assert.NoError(t, throttler.Close())

	// Test IsThrottled always returns true
	assert.True(t, throttler.IsThrottled())

	// Test UpdateLag returns no error
	assert.NoError(t, throttler.UpdateLag(t.Context()))

	// Test BlockWait sleeps for approximately 1 second
	start := time.Now()
	throttler.BlockWait(t.Context())
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 1*time.Second)
	assert.Less(t, elapsed, 1500*time.Millisecond) // allow 500ms tolerance for CI scheduling delays

	// Test BlockWait respects context cancellation
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately
	start = time.Now()
	throttler.BlockWait(ctx)
	elapsed = time.Since(start)
	assert.Less(t, elapsed, 100*time.Millisecond) // should return almost immediately
}
