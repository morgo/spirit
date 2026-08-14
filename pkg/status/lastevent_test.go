package status

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLastEventZeroValue(t *testing.T) {
	var e LastEvent
	require.True(t, e.At().IsZero())
	require.Equal(t, "never", e.Age())
}

func TestLastEventRecord(t *testing.T) {
	var e LastEvent
	before := time.Now()
	e.Record()
	require.False(t, e.At().IsZero())
	require.False(t, e.At().Before(before))
	// Rounded to the second, so an event that just happened reads as "0s"
	// rather than a noisy sub-second duration.
	require.Equal(t, "0s", e.Age())
}

func TestLastEventAgeRounds(t *testing.T) {
	var e LastEvent
	e.Record()
	// Rewind by hand rather than sleeping.
	e.nanos.Store(time.Now().Add(-90 * time.Second).UnixNano())
	require.Equal(t, "1m30s", e.Age())
}
