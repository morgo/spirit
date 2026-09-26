package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// distributedRepairer is the repair path for an N:M move: it rewrites a
// mismatched chunk onto every target from every source. It is the multi-source
// analog of chunkRepairer, and the shape of the operation is the same — DELETE
// the key range, read the rows, write them back through the applier — so the
// rationale in that type's doc applies here too: why the two steps are not one
// transaction, why they run under context.WithoutCancel with a bounded timeout,
// and why repairs are serialized.
//
// The two differences are both consequences of there being more than one
// database on each side:
//
//   - The DELETE is issued to every target, and the rows are read from every
//     source and merged, so the repair reconstructs the chunk's whole logical
//     row set rather than one shard's slice of it.
//   - The applier is the long-lived one the checker was built with, started and
//     stopped by DistributedChecker.Run around the whole pass, not a private one
//     started per repair. It is the same applier the move itself writes
//     through, which is what routes each row to the shard that owns it.
type distributedRepairer struct {
	sourceDBs []*sql.DB
	applier   applier.Applier
	dbConfig  *dbconn.DBConfig
	logger    *slog.Logger

	// recopyLock serializes Recopy calls. See chunkRepairer for rationale.
	recopyLock sync.Mutex
}

var _ Recopier = (*distributedRepairer)(nil)

// newDistributedRepairer builds the multi-source repair path. app must be the
// checker's own applier, because its lifecycle is owned by the run.
func newDistributedRepairer(sourceDBs []*sql.DB, app applier.Applier, dbConfig *dbconn.DBConfig, logger *slog.Logger) *distributedRepairer {
	if dbConfig == nil {
		dbConfig = dbconn.NewDBConfig()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &distributedRepairer{sourceDBs: sourceDBs, applier: app, dbConfig: dbConfig, logger: logger}
}

// Recopy recopies the data from the sources to the targets for a given chunk.
// The entire chunk range is first deleted from every target, then Apply
// recopies the merged source rows. This handles both missing rows and extra
// rows on the destination.
func (r *distributedRepairer) Recopy(ctx context.Context, chunk *table.Chunk) error {
	r.logger.Warn("recopying chunk via DELETE + Apply", "chunk", chunk.String())

	// We further prevent the chance of deadlocks from the recopying process by only re-copying one chunk at a time.
	// We may revisit this in future, but since conflicts are expected to be low, it should be fine for now.
	r.recopyLock.Lock()
	defer r.recopyLock.Unlock()

	// The fix is split into DELETE-from-targets and Apply-from-sources. If the
	// parent ctx is cancelled between or during these steps, the target side
	// would be left with rows DELETEd but not yet reapplied. The
	// lockless-checksum loop's cancellation on sentinel drop hits this race,
	// so we run the fix under a context that ignores the parent's
	// cancellation. The bounded timeout still protects against a hung apply.
	fixCtx, fixCancel := context.WithTimeout(context.WithoutCancel(ctx), fixChunkTimeout)
	defer fixCancel()

	// Step 1: Delete all rows in the chunk range from all targets
	// This ensures we remove any extra rows that shouldn't be there.
	// Use chunk.Table here to target the chunk's original table name consistently across targets.
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s", chunk.Table.QuotedTableName, chunk.String())

	for i, target := range r.applier.GetTargets() {
		r.logger.Debug("deleting chunk range from target", "targetID", i, "chunk", chunk.String(), "table", chunk.Table.TableName)
		_, err := dbconn.RetryableTransaction(fixCtx, target.DB, dbconn.ErrorOnDupKey, r.dbConfig, deleteStmt)
		if err != nil {
			return fmt.Errorf("failed to delete chunk from target %d: %w", i, err)
		}
	}

	// Step 2: Read all rows from ALL sources for the chunk range and merge them.
	// Use NonGeneratedColumns because the applier expects non-generated columns only.
	// This ensures the column ordinals match when the applier extracts the sharding column.
	//
	// JSON columns are deliberately read bare here — no text round-trip cast.
	// This path is already text-mediated: the SELECT renders each document to
	// text on the wire and the applier writes it back as a SQL literal that
	// the target re-parses. The repaired row therefore lands as exactly the
	// one-round-trip text image the checksum's source side predicts. Adding a
	// round-trip cast on top would apply parse∘render twice, which does not
	// converge for misparsed doubles.
	columnList := table.QuoteColumns(chunk.Table.NonGeneratedColumns)
	// Use the table name only; each source DB connection determines which database is queried.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		columnList,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)

	var rowData [][]any
	for i, srcDB := range r.sourceDBs {
		r.logger.Debug("reading chunk data for recopy", "chunk", chunk.String(), "sourceID", i, "table", chunk.Table.TableName)

		rows, err := srcDB.QueryContext(fixCtx, query)
		if err != nil {
			return fmt.Errorf("failed to query chunk data from source %d: %w", i, err)
		}

		for rows.Next() {
			values := make([]any, len(chunk.Table.NonGeneratedColumns))
			valuePtrs := make([]any, len(chunk.Table.NonGeneratedColumns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				utils.CloseAndLog(rows)
				return fmt.Errorf("failed to scan row from source %d: %w", i, err)
			}
			rowData = append(rowData, values)
		}
		if err := rows.Err(); err != nil {
			utils.CloseAndLog(rows)
			return fmt.Errorf("error iterating rows from source %d: %w", i, err)
		}
		utils.CloseAndLog(rows)
	}

	r.logger.Info("recopying chunk via applier", "chunk", chunk.String(), "rowCount", len(rowData), "sourceCount", len(r.sourceDBs))

	// Step 3: Use the applier to write the rows to all targets
	// The applier will handle distribution across shards if needed.
	//
	// The applier's worker goroutines run under context.WithoutCancel(ctx)
	// (see DistributedChecker.Run), so a parent cancellation between the
	// DELETEs above and the worker writes does not by itself cancel the
	// inserts. The remaining limitation is that workers stop when the deferred
	// Stop() at the end of Run runs — if Run returns due to a lockless-checksum
	// cancel while writes are queued, those inserts may be dropped. The
	// lockless-checksum loop's DifferencesFound() gate keeps cutover aborting
	// in that case so the broken state stays internal and is recopied on
	// resume; a tighter fix would scope a worker context to the repair window
	// only.
	if len(rowData) > 0 {
		done := make(chan error, 1)
		applyErr := r.applier.Apply(fixCtx, chunk, rowData, func(affectedRows int64, err error) {
			if err != nil {
				r.logger.Error("failed to recopy chunk via applier", "error", err)
				done <- err
			} else {
				r.logger.Debug("successfully recopied chunk via applier", "affectedRows", affectedRows)
				done <- nil
			}
		})
		if applyErr != nil {
			return fmt.Errorf("failed to initiate recopy via applier: %w", applyErr)
		}

		// Wait for the apply to complete
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("recopy via applier failed: %w", err)
			}
		case <-fixCtx.Done():
			return fixCtx.Err()
		}
	}
	r.logger.Info("successfully recopied chunk", "chunk", chunk.String(), "rowCount", len(rowData))
	return nil
}
