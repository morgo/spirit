// Package repl contains binary log subscription functionality.
package repl

import (
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"
	"time"
)

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
