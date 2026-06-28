package status

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStateOrder is a safety-net tripwire for the ordinal values of the State
// iota block in state.go. The integer ORDER of these constants is load-bearing:
// code elsewhere gates safety behavior on the ordinal via >= / > comparisons
// rather than equality. A future reorder or insertion into the const block would
// silently change which states satisfy those gates and invert the safety logic.
//
// Known ordinal-comparison sites that depend on this order (non-exhaustive):
//   - pkg/migration/runner.go: `state >= Checksum` gates persisting the
//     checkpoint's checksum_watermark; `state >= CutOver` makes fatalError()
//     a no-op (errors after cutover are expected and must not cancel/invalidate).
//   - pkg/move/runner.go: `state >= Checksum` and `state >= CutOver` gate the
//     continuous-checksum loop and post-cutover error handling.
//   - pkg/status/task.go: `state > CutOver` / `state >= CutOver` gate progress
//     reporting and terminal-state handling.
//   - state.go itself documents that WaitingOnSentinelTable must sort AFTER
//     Checksum so that `state >= Checksum` stays true while the sentinel wait
//     blocks the cutover.
//
// If you INTENTIONALLY reorder or insert a State, update the expected values
// below AND audit every `>=` / `>` / `<` comparison against a State constant to
// confirm the safety gates still hold.
func TestStateOrder(t *testing.T) {
	// (a) Lock the exact integer value of EACH State constant. Any insertion or
	// reorder of the iota block shifts at least one value and fails here.
	tests := []struct {
		name  string
		state State
		want  int32
	}{
		{"Initial", Initial, 0},
		{"CopyRows", CopyRows, 1},
		{"ApplyChangeset", ApplyChangeset, 2},
		{"RestoreSecondaryIndexes", RestoreSecondaryIndexes, 3},
		{"AnalyzeTable", AnalyzeTable, 4},
		{"Checksum", Checksum, 5},
		{"PostChecksum", PostChecksum, 6},
		{"WaitingOnSentinelTable", WaitingOnSentinelTable, 7},
		{"CutOver", CutOver, 8},
		{"Close", Close, 9},
		{"ErrCleanup", ErrCleanup, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, int32(tc.state),
				"State %q has unexpected ordinal; the iota block in state.go changed. "+
					"Audit all >= / > comparisons against State constants before updating this test.",
				tc.name)
		})
	}

	// (b) Assert the key inequalities that gate safety logic explicitly, so the
	// intent survives even if someone "fixes" the literal values above to match a
	// reorder without thinking about the comparison sites.
	require.Greater(t, int32(WaitingOnSentinelTable), int32(Checksum),
		"WaitingOnSentinelTable must sort strictly after Checksum, so the `state >= Checksum` gate stays true during the sentinel wait")
	require.Greater(t, int32(CutOver), int32(Checksum),
		"CutOver must sort strictly after Checksum")
	require.Greater(t, int32(CutOver), int32(WaitingOnSentinelTable),
		"CutOver must sort strictly after WaitingOnSentinelTable (the sentinel wait precedes cutover)")
	require.Greater(t, int32(CutOver), int32(PostChecksum),
		"CutOver must sort strictly after PostChecksum")

	// (b cont.) Assert the FULL monotonic ordering of the declared states. The
	// slice is written in the intended logical order; each entry must have a
	// strictly greater ordinal than the previous one.
	monotonic := []struct {
		name  string
		state State
	}{
		{"Initial", Initial},
		{"CopyRows", CopyRows},
		{"ApplyChangeset", ApplyChangeset},
		{"RestoreSecondaryIndexes", RestoreSecondaryIndexes},
		{"AnalyzeTable", AnalyzeTable},
		{"Checksum", Checksum},
		{"PostChecksum", PostChecksum},
		{"WaitingOnSentinelTable", WaitingOnSentinelTable},
		{"CutOver", CutOver},
		{"Close", Close},
		{"ErrCleanup", ErrCleanup},
	}
	for i := 1; i < len(monotonic); i++ {
		require.Greater(t, int32(monotonic[i].state), int32(monotonic[i-1].state),
			"State %q must have a strictly greater ordinal than %q",
			monotonic[i].name, monotonic[i-1].name)
	}
}
