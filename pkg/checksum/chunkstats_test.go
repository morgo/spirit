package checksum

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func TestChunkObserverEmpty(t *testing.T) {
	// A pass that failed before its first chunk, or an empty table, must not
	// log a summary made of zeroes.
	var o chunkObserver
	assert.Empty(t, o.summary(time.Second))
}

func TestChunkObserverSummary(t *testing.T) {
	var o chunkObserver
	for i := 1; i <= 10; i++ {
		o.record(uint64(i*100), time.Duration(i)*10*time.Millisecond)
	}
	s := o.summary(500 * time.Millisecond)
	assert.Contains(t, s, "chunks=10")
	assert.Contains(t, s, "target=500ms")
	assert.Contains(t, s, "duration-max=100ms")
	assert.Contains(t, s, "rows-max=1000")
	// Nothing reached the dynamic sizer's ceiling, so the row cap is not what
	// is binding here.
	assert.Contains(t, s, "row-capped=0/10")
}

func TestChunkObserverCountsRowCapped(t *testing.T) {
	// The signal that matters for future sizing work: chunks pinned at the
	// dynamic sizer's row ceiling mean the sizer wanted to go bigger and was
	// not allowed to, so raising the time target alone would change nothing.
	var o chunkObserver
	o.record(table.MaxDynamicRowSize, 200*time.Millisecond)
	o.record(table.MaxDynamicRowSize+50, 210*time.Millisecond)
	o.record(500, 5*time.Millisecond)
	assert.Contains(t, o.summary(time.Second), "row-capped=2/3")
	assert.Contains(t, o.summary(time.Second), fmt.Sprintf("rows-max=%d", table.MaxDynamicRowSize+50))
}

func TestChunkObserverConcurrentRecord(t *testing.T) {
	// Every checksum worker records into the shared observer. Run with -race.
	var o chunkObserver
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 25 {
				o.record(uint64(i*j), time.Duration(j)*time.Millisecond)
			}
		})
	}
	wg.Wait()
	assert.Contains(t, o.summary(time.Second), "chunks=400")
}
