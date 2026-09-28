package move

import (
	"database/sql"
)

// As in migration, reserve checksum repair/prefetch, checkpoint/flush polling,
// at least one statistics query, and a drain connection. The runtime reserve
// adds the remaining per-table statistics queries on each source pool.
const minChecksumPhaseReserve = 6

func (r *Runner) fitReadThreadsToPools() error {
	if r.move.MaxConnections <= 0 {
		return nil
	}
	reserve := minChecksumPhaseReserve + max(0, len(r.sourceTables)-1)
	// Usually each source/target owns a distinct *sql.DB, even when hosts are
	// shared. A checksum worker reads a chunk from every source and every
	// distinct target handle at once, so a handle that serves as both a source
	// and a target is held twice by each worker. Targets reusing one handle
	// count once: the checker reads each distinct target handle once.
	uses := make(map[*sql.DB]int)
	for _, source := range r.sources {
		uses[source.db]++
	}
	targetHandles := make(map[*sql.DB]bool)
	for _, target := range r.targets {
		if !targetHandles[target.DB] {
			targetHandles[target.DB] = true
			uses[target.DB]++
		}
	}
	copies := 1
	for db, n := range uses {
		if db != nil {
			copies = max(copies, n)
		}
	}
	// Preserve at least one reader, matching migration; advisory control-plane
	// queries may queue when the requested budget cannot cover all headroom.
	available := max(1, (r.move.MaxConnections-reserve)/copies)
	start := min(r.move.Threads, available)
	if start != r.move.Threads {
		r.logger.Info("fitting read threads to the connection pool", "threads", start, "max_connections", r.move.MaxConnections, "reserved", reserve)
	}
	// Autoscaling can have a ceiling above its starting count. Checksum
	// workers draw connections up to that ceiling, so fit it as well without
	// changing the fixed pool budget inherited from --max-connections.
	if r.autoscale.Enabled {
		r.autoscale.MaxReadThreads = min(max(r.move.Threads, r.autoscale.MaxReadThreads), available)
	}
	r.move.Threads = start
	return nil
}
