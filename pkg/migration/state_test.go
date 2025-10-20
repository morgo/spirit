package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMigrationStateString(t *testing.T) {
	assert.Equal(t, "initial", StateInitial.String())
	assert.Equal(t, "copyRows", StateCopyRows.String())
	assert.Equal(t, "waitingOnSentinelTable", StateWaitingOnSentinelTable.String())
	assert.Equal(t, "applyChangeset", StateApplyChangeset.String())
	assert.Equal(t, "checksum", StateChecksum.String())
	assert.Equal(t, "cutOver", StateCutOver.String())
	assert.Equal(t, "errCleanup", StateErrCleanup.String())
	assert.Equal(t, "analyzeTable", StateAnalyzeTable.String())
	assert.Equal(t, "close", StateClose.String())
}
