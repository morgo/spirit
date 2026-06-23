package copier

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/assert"
)

// etaEstimate reports a remaining-time estimate only once a copy rate has been
// measured and while the copy is still meaningfully in flight; the not-ready
// cases are distinguished as ETAMeasuring (no rate yet) vs ETADue (near done),
// which the GUI/summary callers turn into "TBD"/"DUE"/0.
func TestEtaEstimate(t *testing.T) {
	measured := time.Now().Add(-2 * copyETAInitialWaitTime)

	tests := []struct {
		name         string
		copiedRows   uint64
		totalRows    uint64
		pct          float64
		rowsPerSec   uint64
		startTime    time.Time
		wantState    status.ETAState
		wantDuration time.Duration
	}{
		{
			name:       "due once the copy is essentially complete",
			copiedRows: 999, totalRows: 1000, pct: 99.999, rowsPerSec: 10, startTime: measured,
			wantState: status.ETADue,
		},
		{
			name:       "measuring before a copy rate is known",
			copiedRows: 100, totalRows: 1000, pct: 10, rowsPerSec: 0, startTime: measured,
			wantState: status.ETAMeasuring,
		},
		{
			name:       "measuring during the initial wait window",
			copiedRows: 100, totalRows: 1000, pct: 10, rowsPerSec: 50, startTime: time.Now(),
			wantState: status.ETAMeasuring,
		},
		{
			name:       "ready: estimate from remaining rows and rate",
			copiedRows: 500, totalRows: 1000, pct: 50, rowsPerSec: 10, startTime: measured,
			wantState: status.ETAReady, wantDuration: 50 * time.Second,
		},
		{
			name:       "ready: floors fractional seconds",
			copiedRows: 0, totalRows: 1000, pct: 0, rowsPerSec: 3, startTime: measured,
			wantState: status.ETAReady, wantDuration: 333 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, st := etaEstimate(tt.copiedRows, tt.totalRows, tt.pct, tt.rowsPerSec, tt.startTime)
			assert.Equal(t, tt.wantState, st)
			assert.Equal(t, tt.wantDuration, d)
		})
	}
}
