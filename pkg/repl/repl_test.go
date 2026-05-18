package repl

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewServerIDConcurrent tests that NewServerID generates unique IDs even when called concurrently.
// This is a regression test for the issue where using time.Now().Unix() as a seed caused collisions
// when multiple clients were created within the same second. This is *only* an issue for tests,
// but the serverIDs need to be unique to prevent MySQL disconnecting the sessions.
//
// Note: Due to the birthday paradox, when generating 10,000 IDs from a ~4.3B range, there's a small
// probability (~1.16%) of collision. We allow up to 1 duplicate to make the test less flaky while
// still catching regressions where the old time-based seed caused frequent collisions.
func TestNewServerIDConcurrent(t *testing.T) {
	const numGoroutines = 100
	const idsPerGoroutine = 100
	const maxAllowedDuplicates = 1

	// Channel to collect all generated IDs
	idChan := make(chan uint32, numGoroutines*idsPerGoroutine)

	// Use sync.WaitGroup to ensure all goroutines complete
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start multiple goroutines generating IDs concurrently
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range idsPerGoroutine {
				idChan <- NewServerID()
			}
		}()
	}

	// Wait for all goroutines to complete, then close the channel
	go func() {
		wg.Wait()
		close(idChan)
	}()

	// Collect all IDs and track duplicates
	ids := make(map[uint32]int) // map ID to count
	var duplicateCount int
	var firstDuplicate uint32

	for id := range idChan {
		// Verify ID is in expected range (at least 1001)
		require.GreaterOrEqual(t, id, uint32(1001), "ServerID should be >= 1001")

		// Track duplicates - count every occurrence beyond the first
		ids[id]++
		if ids[id] > 1 {
			duplicateCount++
			if firstDuplicate == 0 {
				firstDuplicate = id
			}
		}
	}

	// Log duplicate information if any found
	if duplicateCount > 0 {
		t.Logf("Found %d duplicate ID(s). First duplicate: %d", duplicateCount, firstDuplicate)
	}

	// Allow up to maxAllowedDuplicates to account for birthday paradox
	require.LessOrEqual(t, duplicateCount, maxAllowedDuplicates,
		"Should have at most %d duplicate ID(s), but found %d", maxAllowedDuplicates, duplicateCount)

	// Verify we got close to the expected number of unique IDs
	// With 1 allowed duplicate, we should have at least 9,999 unique IDs
	minExpectedUnique := numGoroutines*idsPerGoroutine - maxAllowedDuplicates
	require.GreaterOrEqual(t, len(ids), minExpectedUnique,
		"Should have at least %d unique IDs", minExpectedUnique)
}

// TestNewServerIDRange tests that NewServerID always returns values in the expected range.
func TestNewServerIDRange(t *testing.T) {
	for range 1000 {
		id := NewServerID()
		require.GreaterOrEqual(t, id, uint32(1001), "ServerID should be >= 1001")
	}
}
