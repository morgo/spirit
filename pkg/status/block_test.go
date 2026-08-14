package status

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBar(t *testing.T) {
	require.Equal(t, "[·························]", Bar(0))
	require.Equal(t, "[#########################]", Bar(1))
	require.Equal(t, "[############·············]", Bar(0.5))
	// Out of range is clamped rather than producing a ragged bar.
	require.Equal(t, "[·························]", Bar(-1))
	require.Equal(t, "[#########################]", Bar(2))
	// Truncation, not rounding: only a genuinely complete fraction fills the
	// bar, because a solid bar at 99.99% reads as done.
	require.Equal(t, barWidth-1, strings.Count(Bar(0.9999), barFull))
}

func TestBlockRendering(t *testing.T) {
	b := NewBlock("migration status: state=%s total-time=%s", "copyRows", "2m6s")
	b.BarRow("copier", 0.3084, "%6.2f%%  %d/%d", 30.84, 5048712, 16370180)
	b.Row("ckpt", "20s ago  binlog.000123:41909012")

	// Labels are padded to a common width, and a row without a bar starts its
	// text where the bars start.
	require.Equal(t, strings.Join([]string{
		"migration status: state=copyRows total-time=2m6s",
		"  copier [#######··················]   30.84%  5048712/16370180",
		"  ckpt   20s ago  binlog.000123:41909012",
	}, "\n"), b.String())
}

func TestBlockDropsEmptyRows(t *testing.T) {
	// A helper that reports nothing (a nil applier, a feed that publishes no
	// stats) must not leave a dangling label behind, and neither must the
	// trailing separator of the row it would have filled.
	b := NewBlock("sync status: state=%s", "copyRows")
	b.Row("applier", "")
	b.BarRow("copier", 0.5, "")
	b.Row("binlog", "deltas=%d  %s", 0, "")
	require.Equal(t, "sync status: state=copyRows\n  binlog deltas=0", b.String())
}

func TestBlockHeaderOnly(t *testing.T) {
	require.Equal(t, "sync status: state=initial", NewBlock("sync status: state=%s", "initial").String())
}
