package checksum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCompareChunk unit-tests the central comparison decision function. The
// key case is (equalCRC, unequalCounts): the CRCs match but the row counts
// differ, which the checksum alone would miss but compareChunk catches. This
// is the deterministic core of the defense-in-depth fix.
func TestCompareChunk(t *testing.T) {
	tests := []struct {
		name             string
		srcCRC, tgtCRC   int64
		srcCnt, tgtCnt   uint64
		wantMismatched   bool
		wantChecksumDiff bool
		wantCountDiff    bool
		wantReason       string
	}{
		{
			name:   "all equal",
			srcCRC: 12345, tgtCRC: 12345,
			srcCnt: 10, tgtCnt: 10,
			wantMismatched: false,
		},
		{
			name:   "checksum differs, counts equal",
			srcCRC: 12345, tgtCRC: 99999,
			srcCnt: 10, tgtCnt: 10,
			wantMismatched:   true,
			wantChecksumDiff: true,
			wantCountDiff:    false,
			wantReason:       "checksum mismatch",
		},
		{
			// The headline defense-in-depth case: identical CRC, different
			// counts. BIT_XOR pair-cancellation (duplicate rows) or a row
			// whose CRC32 is 0 can leave the checksum matching while the
			// count diverges.
			name:   "checksum equal, counts differ",
			srcCRC: 12345, tgtCRC: 12345,
			srcCnt: 11, tgtCnt: 10,
			wantMismatched:   true,
			wantChecksumDiff: false,
			wantCountDiff:    true,
			wantReason:       "row count mismatch (src=11, target=10)",
		},
		{
			name:   "both differ",
			srcCRC: 12345, tgtCRC: 99999,
			srcCnt: 0, tgtCnt: 2,
			wantMismatched:   true,
			wantChecksumDiff: true,
			wantCountDiff:    true,
			wantReason:       "checksum mismatch and row count mismatch (src=0, target=2)",
		},
		{
			// Zero-CRC pair-cancellation flavor: both sides report CRC 0
			// (e.g. a single row whose CRC32 is 0 on one side, none on the
			// other), so the XOR matches but the count exposes the gap.
			name:   "zero crc both sides, count differs",
			srcCRC: 0, tgtCRC: 0,
			srcCnt: 1, tgtCnt: 0,
			wantMismatched:   true,
			wantChecksumDiff: false,
			wantCountDiff:    true,
			wantReason:       "row count mismatch (src=1, target=0)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := compareChunk(tc.srcCRC, tc.tgtCRC, tc.srcCnt, tc.tgtCnt)
			require.Equal(t, tc.wantMismatched, m.mismatched())
			require.Equal(t, tc.wantChecksumDiff, m.checksumDiffers)
			require.Equal(t, tc.wantCountDiff, m.countDiffers)
			if tc.wantMismatched {
				require.Equal(t, tc.wantReason, m.reason(tc.srcCnt, tc.tgtCnt))
			}
		})
	}
}
