package runtime

import (
	"sync"

	"github.com/block/spirit/pkg/throttler"
)

// SharedThrottler holds the throttler a runner resolves during setup. Setup
// assigns it partway through, while an API caller may already be polling
// Progress, which reports whether the run is paused, and a change feed's
// UnderLoad callback may already be reading it. The zero value holds nil.
type SharedThrottler struct {
	mu sync.RWMutex
	t  throttler.Throttler
}

// Set publishes the resolved throttler.
func (s *SharedThrottler) Set(t throttler.Throttler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.t = t
}

// Get returns the resolved throttler, or nil if setup has not resolved one.
func (s *SharedThrottler) Get() throttler.Throttler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.t
}
