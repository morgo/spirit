package table

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardingProvider is a simple test implementation of ShardingMetadataProvider
type TestShardingProvider struct {
	vindexColumn string
	vindexFunc   HashFunc
	shouldError  bool
}

func NewTestShardingProvider(column string, hashFunc HashFunc) *TestShardingProvider {
	return &TestShardingProvider{
		vindexColumn: column,
		vindexFunc:   hashFunc,
		shouldError:  false,
	}
}

func (t *TestShardingProvider) GetShardingMetadata(schemaName, tableName string) (string, HashFunc, error) {
	if t.shouldError {
		return "", nil, fmt.Errorf("test error for table %s.%s", schemaName, tableName)
	}
	return t.vindexColumn, t.vindexFunc, nil
}

// TestShardingProviderInterface verifies the interface contract
func TestShardingProviderInterface(t *testing.T) {
	// Simple hash function for testing
	simpleHash := func(value any) (uint64, error) {
		switch v := value.(type) {
		case int:
			return uint64(v), nil
		case int64:
			return uint64(v), nil
		case string:
			// Simple string hash
			hash := uint64(0)
			for _, c := range v {
				hash = hash*31 + uint64(c)
			}
			return hash, nil
		default:
			return 0, fmt.Errorf("unsupported type: %T", value)
		}
	}

	t.Run("returns vindex metadata", func(t *testing.T) {
		provider := NewTestShardingProvider("user_id", simpleHash)

		column, hashFunc, err := provider.GetShardingMetadata("testdb", "users")
		require.NoError(t, err)
		assert.Equal(t, "user_id", column)
		assert.NotNil(t, hashFunc)

		// Test the hash function
		hash, err := hashFunc(123)
		require.NoError(t, err)
		assert.Equal(t, uint64(123), hash)
	})

	t.Run("returns empty for no vindex", func(t *testing.T) {
		provider := NewTestShardingProvider("", nil)

		column, hashFunc, err := provider.GetShardingMetadata("testdb", "config")
		require.NoError(t, err)
		assert.Empty(t, column)
		assert.Nil(t, hashFunc)
	})

	t.Run("returns error", func(t *testing.T) {
		provider := &TestShardingProvider{
			shouldError: true,
		}

		_, _, err := provider.GetShardingMetadata("testdb", "users")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "test error")
	})
}

// TestHashFunc verifies HashFunc behavior
func TestHashFunc(t *testing.T) {
	t.Run("handles different types", func(t *testing.T) {
		hashFunc := func(value any) (uint64, error) {
			switch v := value.(type) {
			case int:
				return uint64(v * 2), nil
			case string:
				return uint64(len(v)), nil
			default:
				return 0, fmt.Errorf("unsupported type: %T", value)
			}
		}

		// Test with int
		hash, err := hashFunc(42)
		require.NoError(t, err)
		assert.Equal(t, uint64(84), hash)

		// Test with string
		hash, err = hashFunc("hello")
		require.NoError(t, err)
		assert.Equal(t, uint64(5), hash)

		// Test with unsupported type
		_, err = hashFunc(3.14)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type")
	})
}

// MultiTableShardingProvider is a test provider that can return different
// vindex configurations for different tables
type MultiTableShardingProvider struct {
	tableConfigs map[string]struct {
		column   string
		hashFunc HashFunc
	}
}

func NewMultiTableShardingProvider() *MultiTableShardingProvider {
	return &MultiTableShardingProvider{
		tableConfigs: make(map[string]struct {
			column   string
			hashFunc HashFunc
		}),
	}
}

func (m *MultiTableShardingProvider) AddTable(tableName, column string, hashFunc HashFunc) {
	m.tableConfigs[tableName] = struct {
		column   string
		hashFunc HashFunc
	}{
		column:   column,
		hashFunc: hashFunc,
	}
}

func (m *MultiTableShardingProvider) GetShardingMetadata(schemaName, tableName string) (string, HashFunc, error) {
	config, exists := m.tableConfigs[tableName]
	if !exists {
		return "", nil, nil // No vindex for this table
	}
	return config.column, config.hashFunc, nil
}

func TestMultiTableShardingProvider(t *testing.T) {
	userHash := func(value any) (uint64, error) {
		return uint64(1), nil
	}
	orderHash := func(value any) (uint64, error) {
		return uint64(2), nil
	}

	provider := NewMultiTableShardingProvider()
	provider.AddTable("users", "user_id", userHash)
	provider.AddTable("orders", "order_id", orderHash)

	t.Run("returns correct config for users table", func(t *testing.T) {
		column, hashFunc, err := provider.GetShardingMetadata("testdb", "users")
		require.NoError(t, err)
		assert.Equal(t, "user_id", column)
		assert.NotNil(t, hashFunc)

		hash, err := hashFunc(123)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), hash)
	})

	t.Run("returns correct config for orders table", func(t *testing.T) {
		column, hashFunc, err := provider.GetShardingMetadata("testdb", "orders")
		require.NoError(t, err)
		assert.Equal(t, "order_id", column)
		assert.NotNil(t, hashFunc)

		hash, err := hashFunc(456)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), hash)
	})

	t.Run("returns empty for unconfigured table", func(t *testing.T) {
		column, hashFunc, err := provider.GetShardingMetadata("testdb", "config")
		require.NoError(t, err)
		assert.Empty(t, column)
		assert.Nil(t, hashFunc)
	})
}
