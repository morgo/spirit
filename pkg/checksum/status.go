package checksum

import (
	"fmt"

	"github.com/block/spirit/pkg/status"
)

// StatusReporter optionally exposes algorithm-specific verification state.
// Callers depend on this capability rather than concrete checker types.
type StatusReporter interface{ ChecksumStatus() ChecksumStatus }

// ChecksumStatus separates optimistic traversal from completed verification.
// Optimistic is a point-in-time copy, not mutable checker state.
type ChecksumStatus struct {
	Progress   status.ChecksumProgress
	Optimistic *LocklessCheckerStats
}

func (s ChecksumStatus) String() string {
	if s.Optimistic == nil {
		return s.Progress.String()
	}
	stats := s.Optimistic
	phase := "scanning"
	if stats.ScanComplete {
		phase = "waiting for verification"
	}
	if !stats.FirstCleanPassAt.IsZero() {
		phase = "verified"
	}
	// settled= is shown next to deferred= because neither reads correctly
	// alone: a falling deferred count means the checker is converging only if
	// those ranges reached a verdict rather than simply stopping being hot.
	return fmt.Sprintf("lockless: %s scan≈%.1f%% passed=%d retrying=%d in-flight=%d deferred=%d settled=%d", phase, float64(stats.ProgressBasisPoints)/100, stats.ChunksPassedThisPass, stats.RetryQueueDepth, stats.InFlight, stats.HotChunksDeferredThisPass, stats.HotChunksSettledThisPass)
}

// StatusSummary formats rich status when supported, otherwise basic progress.
func StatusSummary(c Checker) string {
	if reporter, ok := c.(StatusReporter); ok {
		return reporter.ChecksumStatus().String()
	}
	return "Checksum Progress=" + c.GetProgress().String()
}

// StatusRow formats the checksum's row in a runner status block.
func StatusRow(c Checker) string {
	if reporter, ok := c.(StatusReporter); ok {
		return reporter.ChecksumStatus().String()
	}
	progress := c.GetProgress()
	return fmt.Sprintf("%6.2f%%  %d/%d%s", progress.Fraction()*100, progress.RowsChecked, progress.RowsTotal, StatusSuffix(c))
}
