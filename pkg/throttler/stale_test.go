package throttler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStaleGuard_NeverSampledIsNotStale(t *testing.T) {
	var g staleGuard
	stale, entering := g.check(time.Second)
	require.False(t, stale)
	require.False(t, entering)
}

func TestStaleGuard_FreshSampleIsNotStale(t *testing.T) {
	var g staleGuard
	require.False(t, g.markFresh(), "first sample is not a recovery")
	stale, entering := g.check(time.Second)
	require.False(t, stale)
	require.False(t, entering)
}

func TestStaleGuard_EnteringReportedOncePerStalePeriod(t *testing.T) {
	var g staleGuard
	g.markFresh()
	ageLastSample(&g, 2*time.Second)

	// First check that observes staleness reports entering=true so the
	// caller logs exactly one warning...
	stale, entering := g.check(time.Second)
	require.True(t, stale)
	require.True(t, entering)

	// ...and every subsequent check during the same stale period does not
	// (the warning is rate-limited to the transition, not per-call).
	for range 3 {
		stale, entering = g.check(time.Second)
		require.True(t, stale)
		require.False(t, entering)
	}
}

func TestStaleGuard_RecoveryReportedOnce(t *testing.T) {
	var g staleGuard
	g.markFresh()
	ageLastSample(&g, 2*time.Second)
	_, entering := g.check(time.Second)
	require.True(t, entering)

	// The first fresh sample after a stale period is a recovery (logged
	// once); subsequent samples are routine.
	require.True(t, g.markFresh())
	require.False(t, g.markFresh())

	stale, _ := g.check(time.Second)
	require.False(t, stale)
}

func TestStaleGuard_NewStalePeriodWarnsAgain(t *testing.T) {
	var g staleGuard
	g.markFresh()
	ageLastSample(&g, 2*time.Second)
	_, entering := g.check(time.Second)
	require.True(t, entering)

	// Recover, then go stale a second time: the new period must produce a
	// new warning rather than staying silenced forever.
	require.True(t, g.markFresh())
	ageLastSample(&g, 2*time.Second)
	stale, entering := g.check(time.Second)
	require.True(t, stale)
	require.True(t, entering)
}
