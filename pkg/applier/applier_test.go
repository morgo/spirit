package applier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEstimateRowSize(t *testing.T) {
	tests := []struct {
		name    string
		values  []any
		minSize int // minimum expected size
		maxSize int // maximum expected size (for flexibility)
	}{
		{
			name:    "empty row",
			values:  []any{},
			minSize: 2, // just parentheses
			maxSize: 2,
		},
		{
			name:    "single integer",
			values:  []any{int64(123)},
			minSize: 6, // "123" + overhead
			maxSize: 10,
		},
		{
			name:    "single string",
			values:  []any{"hello"},
			minSize: 7, // "hello" + overhead
			maxSize: 15,
		},
		{
			name:    "nil value",
			values:  []any{nil},
			minSize: 6, // "<nil>" + overhead
			maxSize: 12,
		},
		{
			name:    "mixed types",
			values:  []any{int64(42), "test", nil, true, 3.14},
			minSize: 20, // sum of all values + overhead
			maxSize: 50,
		},
		{
			name:    "large string",
			values:  []any{"this is a very long string that represents a TEXT column with lots of data"},
			minSize: 75,
			maxSize: 100,
		},
		{
			name:    "byte slice",
			values:  []any{[]byte("binary data")},
			minSize: 11,
			maxSize: 50,
		},
		{
			name:    "multiple columns",
			values:  []any{int64(1), "Alice", "alice@example.com", int64(25), true},
			minSize: 30,
			maxSize: 60,
		},
		{
			name:    "large integers",
			values:  []any{int64(9223372036854775807), int64(-9223372036854775808)},
			minSize: 38, // 19 digits each + overhead
			maxSize: 60,
		},
		{
			name:    "floating point numbers",
			values:  []any{3.14159, -2.71828, 0.0},
			minSize: 15,
			maxSize: 40,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := estimateRowSize(tt.values)
			assert.GreaterOrEqual(t, size, tt.minSize, "size should be at least minSize")
			assert.LessOrEqual(t, size, tt.maxSize, "size should not exceed maxSize")
			t.Logf("Estimated size for %s: %d bytes", tt.name, size)
		})
	}
}

func TestEstimateRowSizeRealistic(t *testing.T) {
	// Test with realistic table data
	t.Run("users table row", func(t *testing.T) {
		// id, username, email, created_at, is_active
		values := []any{
			int64(12345),
			"john_doe_2024",
			"john.doe@example.com",
			"2024-01-15 10:30:00",
			true,
		}
		size := estimateRowSize(values)
		// Should be reasonable size, not too large
		assert.Greater(t, size, 40, "should account for all fields")
		assert.Less(t, size, 150, "should not be excessively large")
		t.Logf("Users table row size: %d bytes", size)
	})

	t.Run("blog posts with TEXT column", func(t *testing.T) {
		// id, title, content (large TEXT), author_id
		largeContent := make([]byte, 10000) // 10KB of content
		for i := range largeContent {
			largeContent[i] = 'a'
		}
		values := []any{
			int64(1),
			"My Blog Post Title",
			string(largeContent),
			int64(42),
		}
		size := estimateRowSize(values)
		// Should be roughly 10KB + overhead
		assert.Greater(t, size, 10000, "should account for large content")
		assert.Less(t, size, 11000, "overhead should be reasonable")
		t.Logf("Blog post row size: %d bytes", size)
	})

	t.Run("row approaching chunkletMaxSize", func(t *testing.T) {
		// Create a row that's close to 1MB
		largeData := make([]byte, 900000) // 900KB
		for i := range largeData {
			largeData[i] = 'x'
		}
		values := []any{
			int64(1),
			string(largeData),
			"metadata",
		}
		size := estimateRowSize(values)
		// Should be close to but not exceed our threshold
		assert.Greater(t, size, 900000, "should account for large data")
		assert.Less(t, size, chunkletMaxSize, "single row should fit in a chunklet")
		t.Logf("Large row size: %d bytes (threshold: %d)", size, chunkletMaxSize)
	})
}

func TestEstimateRowSizeConsistency(t *testing.T) {
	// Test that the same input produces the same output
	values := []any{int64(123), "test", true, 3.14}

	size1 := estimateRowSize(values)
	size2 := estimateRowSize(values)
	size3 := estimateRowSize(values)

	assert.Equal(t, size1, size2, "should be consistent")
	assert.Equal(t, size2, size3, "should be consistent")
}

func TestEstimateRowSizeZeroValues(t *testing.T) {
	// Test with zero/empty values
	tests := []struct {
		name   string
		values []any
	}{
		{
			name:   "zero integer",
			values: []any{int64(0)},
		},
		{
			name:   "empty string",
			values: []any{""},
		},
		{
			name:   "zero float",
			values: []any{0.0},
		},
		{
			name:   "false boolean",
			values: []any{false},
		},
		{
			name:   "empty byte slice",
			values: []any{[]byte{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := estimateRowSize(tt.values)
			// Should have some size even for zero values
			assert.Positive(t, size, "should have non-zero size")
			t.Logf("%s size: %d bytes", tt.name, size)
		})
	}
}

func TestSplitRowsIntoChunklets(t *testing.T) {
	t.Run("empty rows", func(t *testing.T) {
		rows := []rowData{}
		chunklets := splitRowsIntoChunklets(rows)
		assert.Nil(t, chunklets, "should return nil for empty input")
	})

	t.Run("single small row", func(t *testing.T) {
		rows := []rowData{
			{values: []any{int64(1), "test"}},
		}
		chunklets := splitRowsIntoChunklets(rows)
		assert.Len(t, chunklets, 1, "should create one chunklet")
		assert.Len(t, chunklets[0], 1, "chunklet should have one row")
	})

	t.Run("rows under max count threshold", func(t *testing.T) {
		// Create 500 small rows (under chunkletMaxRows of 1000)
		rows := make([]rowData, 500)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		assert.Len(t, chunklets, 1, "should create one chunklet")
		assert.Len(t, chunklets[0], 500, "chunklet should have all 500 rows")
	})

	t.Run("rows exceeding max count threshold", func(t *testing.T) {
		// Create 2500 small rows (exceeds chunkletMaxRows of 1000)
		rows := make([]rowData, 2500)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		assert.Len(t, chunklets, 3, "should create 3 chunklets")
		assert.Len(t, chunklets[0], 1000, "first chunklet should have 1000 rows")
		assert.Len(t, chunklets[1], 1000, "second chunklet should have 1000 rows")
		assert.Len(t, chunklets[2], 500, "third chunklet should have 500 rows")
	})

	t.Run("rows exceeding max size threshold", func(t *testing.T) {
		// Create rows with large data that exceed chunkletMaxSize (1 MiB)
		// Each row is ~100KB, so 11 rows would be ~1.1MB
		largeData := make([]byte, 100000) // 100KB
		for i := range largeData {
			largeData[i] = 'x'
		}

		rows := make([]rowData, 11)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), string(largeData)}}
		}

		chunklets := splitRowsIntoChunklets(rows)
		// Should split based on size, not row count
		assert.GreaterOrEqual(t, len(chunklets), 2, "should create at least 2 chunklets due to size")

		// Verify each chunklet is under the size limit
		for i, chunklet := range chunklets {
			totalSize := 0
			for _, row := range chunklet {
				totalSize += estimateRowSize(row.values)
			}
			// Allow some overhead, but should be reasonably close to limit
			assert.LessOrEqual(t, totalSize, chunkletMaxSize+10000,
				"chunklet %d should be under size limit (with small overhead)", i)
			t.Logf("Chunklet %d: %d rows, ~%d bytes", i, len(chunklet), totalSize)
		}
	})

	t.Run("mixed row sizes", func(t *testing.T) {
		// Mix of small and large rows
		rows := make([]rowData, 100)
		for i := range rows {
			if i%10 == 0 {
				// Every 10th row is large (10KB)
				largeData := make([]byte, 10000)
				for j := range largeData {
					largeData[j] = 'y'
				}
				rows[i] = rowData{values: []any{int64(i), string(largeData)}}
			} else {
				// Small rows
				rows[i] = rowData{values: []any{int64(i), "small"}}
			}
		}

		chunklets := splitRowsIntoChunklets(rows)
		assert.NotEmpty(t, chunklets, "should create at least one chunklet")

		// Verify all rows are accounted for
		totalRows := 0
		for _, chunklet := range chunklets {
			totalRows += len(chunklet)
		}
		assert.Equal(t, 100, totalRows, "all rows should be in chunklets")
		t.Logf("Created %d chunklets for 100 mixed-size rows", len(chunklets))
	})

	t.Run("exactly at row threshold", func(t *testing.T) {
		// Create exactly chunkletMaxRows rows
		rows := make([]rowData, chunkletMaxRows)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		assert.Len(t, chunklets, 1, "should create one chunklet for exactly max rows")
		assert.Len(t, chunklets[0], chunkletMaxRows, "chunklet should have all rows")
	})

	t.Run("one row over threshold", func(t *testing.T) {
		// Create chunkletMaxRows + 1 rows
		rows := make([]rowData, chunkletMaxRows+1)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		assert.Len(t, chunklets, 2, "should create two chunklets")
		assert.Len(t, chunklets[0], chunkletMaxRows, "first chunklet should have max rows")
		assert.Len(t, chunklets[1], 1, "second chunklet should have 1 row")
	})

	t.Run("single very large row under limit", func(t *testing.T) {
		// Single row that's close to but under the size limit
		largeData := make([]byte, 900000) // 900KB
		for i := range largeData {
			largeData[i] = 'z'
		}

		rows := []rowData{
			{values: []any{int64(1), string(largeData)}},
		}

		chunklets := splitRowsIntoChunklets(rows)
		assert.Len(t, chunklets, 1, "should create one chunklet for single large row")
		assert.Len(t, chunklets[0], 1, "chunklet should have the one row")
	})

	t.Run("single row exceeding size limit", func(t *testing.T) {
		// Single row that exceeds chunkletMaxSize (1 MiB)
		// This is an edge case - the row will be placed in its own chunklet
		// and we rely on max_allowed_packet being large enough (typically 64 MiB)
		veryLargeData := make([]byte, 2*1024*1024) // 2 MiB - exceeds our 1 MiB threshold
		for i := range veryLargeData {
			veryLargeData[i] = 'x'
		}

		rows := []rowData{
			{values: []any{int64(1), string(veryLargeData)}},
		}

		chunklets := splitRowsIntoChunklets(rows)
		assert.Len(t, chunklets, 1, "should create one chunklet even though row exceeds size limit")
		assert.Len(t, chunklets[0], 1, "chunklet should have the one oversized row")

		// Verify the row size does exceed our threshold
		rowSize := estimateRowSize(rows[0].values)
		assert.Greater(t, rowSize, chunkletMaxSize, "row should exceed chunkletMaxSize")
		t.Logf("Single row size: %d bytes (exceeds threshold of %d bytes)", rowSize, chunkletMaxSize)
		t.Logf("Note: This relies on max_allowed_packet being large enough (typically 64 MiB)")
	})

	t.Run("multiple rows with one exceeding limit", func(t *testing.T) {
		// Mix of normal rows and one that exceeds the limit
		veryLargeData := make([]byte, 2*1024*1024) // 2 MiB
		for i := range veryLargeData {
			veryLargeData[i] = 'y'
		}

		rows := []rowData{
			{values: []any{int64(1), "small"}},
			{values: []any{int64(2), "small"}},
			{values: []any{int64(3), string(veryLargeData)}}, // Oversized row
			{values: []any{int64(4), "small"}},
			{values: []any{int64(5), "small"}},
		}

		chunklets := splitRowsIntoChunklets(rows)
		// Should create at least 3 chunklets: small rows before, oversized row alone, small rows after
		assert.GreaterOrEqual(t, len(chunklets), 3, "should create multiple chunklets")

		// Verify all rows are accounted for
		totalRows := 0
		for _, chunklet := range chunklets {
			totalRows += len(chunklet)
		}
		assert.Equal(t, 5, totalRows, "all rows should be in chunklets")
		t.Logf("Created %d chunklets for 5 rows (including one 2 MiB row)", len(chunklets))
	})
}
