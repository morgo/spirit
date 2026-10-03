package runtime

import (
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/status"
)

// RecordCopyCompleted reports to tracker the copy aggregate settled during
// this Run invocation. The chunker restores its settled row count from the
// checkpoint while its chunk count starts afresh, so rowsAtResume, the row
// count it restored, is subtracted to keep the two figures on the same
// invocation. It does nothing until c has a chunker.
//
// Used by all three runners, including datasync.
func RecordCopyCompleted(tracker *status.Tracker, c copier.Copier, rowsAtResume uint64) {
	chunker := c.GetChunker()
	if chunker == nil {
		return
	}
	_, chunks, _ := chunker.Progress()
	tracker.RecordCopyCompleted(chunker.RowsCopied()-rowsAtResume, chunks)
}
