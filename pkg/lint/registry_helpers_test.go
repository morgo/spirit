package lint

import (
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// initialLintRegistry holds a snapshot of the linters registered by init()
// functions, captured once on first use. Tests that wipe the registry use this
// snapshot to restore it on cleanup so they don't pollute later tests.
var (
	initialLintRegistry map[string]*linter
	initialCaptureOnce  sync.Once
)

func captureInitialLintRegistry() {
	initialCaptureOnce.Do(func() {
		lock.RLock()
		defer lock.RUnlock()
		initialLintRegistry = make(map[string]*linter, len(linters))
		for k, v := range linters {
			cp := *v
			initialLintRegistry[k] = &cp
		}
	})
}

func restoreInitialLintRegistry() {
	lock.Lock()
	defer lock.Unlock()
	linters = make(map[string]*linter, len(initialLintRegistry))
	for k, v := range initialLintRegistry {
		cp := *v
		linters[k] = &cp
	}
}

// resetForTest wipes the linter registry and registers a t.Cleanup that
// restores the init() snapshot when the test ends, so that subsequent tests in
// the same binary see the full set of linters their init() functions registered.
func resetForTest(t *testing.T) {
	t.Helper()
	captureInitialLintRegistry()
	lock.Lock()
	linters = make(map[string]*linter)
	lock.Unlock()
	t.Cleanup(restoreInitialLintRegistry)
}

// registeredLinterNames returns the names of all registered linters in sorted order.
func registeredLinterNames() []string {
	lock.RLock()
	defer lock.RUnlock()
	return slices.Sorted(maps.Keys(linters))
}

// registeredLinter returns the registered linter with the given name, failing
// the test if it is not registered.
func registeredLinter(t *testing.T, name string) Linter {
	t.Helper()
	lock.RLock()
	defer lock.RUnlock()
	l, ok := linters[name]
	require.True(t, ok, "linter %q not registered", name)
	return l.l
}
