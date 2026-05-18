package repl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewClientDefaultConfig pins the documented defaults so a change to
// the constructor surfaces here rather than in a downstream caller that
// silently picks up a new value. All integration tests in this package
// exercise the config indirectly through NewClient; this is the only
// place the constructor itself is asserted.
func TestNewClientDefaultConfig(t *testing.T) {
	cfg := NewClientDefaultConfig()
	require.NotNil(t, cfg)

	require.Equal(t, 4, cfg.Concurrency, "default Concurrency is 4")
	require.Equal(t, DefaultTargetBatchTime, cfg.TargetBatchTime,
		"default TargetBatchTime matches DefaultTargetBatchTime")
	require.NotNil(t, cfg.Logger, "Logger defaults to slog.Default(), not nil")
	require.GreaterOrEqual(t, cfg.ServerID, uint32(1001),
		"ServerID generated via NewServerID() must be in the safe range")

	// The zero-value fields are intentionally left zero so they pick up
	// downstream defaults at construction (e.g. SubscriptionSoftLimitBytes
	// is translated to DefaultSubscriptionSoftLimitBytes inside NewClient).
	require.Nil(t, cfg.DBConfig, "DBConfig defaults to nil (NewClient fills it)")
	require.Nil(t, cfg.CancelFunc, "CancelFunc has no default")
	require.Empty(t, cfg.DDLFilterSchema)
	require.Empty(t, cfg.DDLFilterTables)
	require.Zero(t, cfg.SubscriptionSoftLimitBytes,
		"SubscriptionSoftLimitBytes is zero so NewClient applies the default")
}

// TestNewClientDefaultConfigServerIDIsFresh pins that every call returns
// a fresh ServerID rather than a constant — a regression here would
// cause MySQL to disconnect concurrent test clients sharing an ID.
func TestNewClientDefaultConfigServerIDIsFresh(t *testing.T) {
	a := NewClientDefaultConfig()
	b := NewClientDefaultConfig()
	require.NotEqual(t, a.ServerID, b.ServerID,
		"two NewClientDefaultConfig calls must produce different ServerIDs")
}
