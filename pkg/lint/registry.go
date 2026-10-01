package lint

import (
	"sync"
)

// linter represents a registered linter with metadata
type linter struct {
	l       Linter
	enabled bool
}

var (
	linters map[string]*linter
	lock    sync.RWMutex
)

// Register registers a linter with the global registry.
// This should be called from init() functions in linter implementations.
// Linters are enabled by default when registered.
func Register(l Linter) {
	lock.Lock()
	defer lock.Unlock()

	if linters == nil {
		linters = make(map[string]*linter)
	}

	linters[l.Name()] = &linter{
		l:       l,
		enabled: true,
	}
}
