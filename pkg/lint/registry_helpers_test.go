package lint

import (
	"sync"
	"testing"
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

// resetForTest wipes the linter registry like Reset() does, but also registers
// a t.Cleanup that restores the init() snapshot when the test ends. Use this
// instead of bare Reset() in tests so that subsequent tests in the same binary
// see the full set of linters their init() functions registered.
func resetForTest(t *testing.T) {
	t.Helper()
	captureInitialLintRegistry()
	Reset()
	t.Cleanup(restoreInitialLintRegistry)
}
