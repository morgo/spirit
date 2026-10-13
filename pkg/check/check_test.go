package check

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withRegistry swaps in a registry holding only the given checks for the
// duration of the test, so tests of the registry itself do not see or disturb
// the real checks.
func withRegistry(t *testing.T, cs ...check) {
	t.Helper()
	lock.Lock()
	saved := checks
	checks = nil
	lock.Unlock()
	t.Cleanup(func() {
		lock.Lock()
		checks = saved
		lock.Unlock()
	})
	for _, c := range cs {
		register(c)
	}
}

func TestCheckAPI(t *testing.T) {
	var ran []string
	record := func(name string) func(context.Context, Resources, *slog.Logger) error {
		return func(context.Context, Resources, *slog.Logger) error {
			ran = append(ran, name)
			return nil
		}
	}
	withRegistry(t,
		check{name: "b_migration", run: record("b_migration"), phases: phaseTesting, runtimes: Migration},
		check{name: "a_shared", run: record("a_shared"), phases: phaseTesting, runtimes: Migration | Move},
		check{name: "c_move", run: record("c_move"), phases: phaseTesting, runtimes: Move},
		check{name: "d_other_phase", run: record("d_other_phase"), phases: PhasePreflight, runtimes: Migration},
	)

	require.NoError(t, RunChecks(t.Context(), Resources{Runtime: Migration}, slog.Default(), phaseTesting))
	assert.Equal(t, []string{"a_shared", "b_migration"}, ran, "only the migration checks of the phase run, in name order")

	ran = nil
	require.NoError(t, RunChecks(t.Context(), Resources{Runtime: Move}, slog.Default(), phaseTesting, "a_shared"))
	assert.Equal(t, []string{"c_move"}, ran, "an excluded check does not run")

	ran = nil
	require.ErrorContains(t, RunChecks(t.Context(), Resources{}, slog.Default(), phaseTesting), "Runtime is not set")
	assert.Empty(t, ran, "no check runs without a runtime")
}

func TestRegisterRefusesDuplicatesAndEmptyMasks(t *testing.T) {
	noop := func(context.Context, Resources, *slog.Logger) error { return nil }
	withRegistry(t, check{name: "mycheck", run: noop, phases: phaseTesting, runtimes: Migration})
	assert.Panics(t, func() {
		register(check{name: "mycheck", run: noop, phases: phaseTesting, runtimes: Move})
	}, "a name can only be registered once")
	assert.Panics(t, func() {
		register(check{name: "nophase", run: noop, runtimes: Migration})
	}, "a check must have a phase")
	assert.Panics(t, func() {
		register(check{name: "noruntime", run: noop, phases: phaseTesting})
	}, "a check must have a runtime")
}

// TestChecks covers reporting a phase's membership, which a caller outside
// this package pins so a check added to a phase it relies on has to be judged
// rather than picked up silently. A name only belongs in the answer if the
// check behind it really is registered for the runtime and phase asked about,
// in the order RunChecks would run it.
func TestChecks(t *testing.T) {
	assert.Empty(t, Checks(Migration, 0), "no check registers under the empty phase")
	assert.Equal(t, []string{"version"}, Checks(Migration, PhasePreRun))

	statementPhase := Checks(Migration, PhaseStatement)
	require.NotEmpty(t, statementPhase)
	assert.True(t, slices.IsSorted(statementPhase), "names must come back in the order RunChecks runs them")
	lock.Lock()
	registered := maps.Clone(checks)
	lock.Unlock()
	for _, name := range statementPhase {
		c, ok := registered[name]
		require.True(t, ok, "reported %q, which is not a registered check", name)
		assert.NotZero(t, c.phases&PhaseStatement, "reported %q, which is not in the phase asked about", name)
		assert.NotZero(t, c.runtimes&Migration, "reported %q, which is not in the runtime asked about", name)
	}
}
