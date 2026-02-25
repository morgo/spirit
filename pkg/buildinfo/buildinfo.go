// Package buildinfo provides build metadata for Spirit binaries.
//
// It supports two sources of build information:
//  1. Compile-time injection via -ldflags (preferred for release binaries)
//  2. runtime/debug.ReadBuildInfo() VCS settings (automatic for dev builds from a git checkout)
//
// The Set() function should be called from main() with the ldflags-injected
// variables. Get() returns the resolved build info, preferring ldflags values
// when available and falling back to debug.ReadBuildInfo().
package buildinfo

import (
	"runtime/debug"
	"sync"
)

// Info holds the resolved build metadata.
type Info struct {
	Version  string // version (e.g. "v1.2.3"), or "dev"
	Commit   string // full git commit hash, or "unknown"
	Date     string // build date in RFC3339, or "unknown"
	Modified bool   // true if the working tree had uncommitted changes
	GoVer    string // Go version used for the build
}

var (
	// These are populated by Set() from main packages using ldflags.
	ldflagsVersion string
	ldflagsCommit  string
	ldflagsDate    string

	once   sync.Once
	cached Info
)

// Set stores the compile-time injected values from -ldflags.
// Call this once from main() before any call to Get().
//
// Example ldflags:
//
//	go build -ldflags "-X main.version=v1.2.3 -X main.commit=$(git rev-parse HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
func Set(version, commit, date string) {
	ldflagsVersion = version
	ldflagsCommit = commit
	ldflagsDate = date
}

// Get returns the resolved build info. It prefers ldflags-injected values
// and falls back to runtime/debug.ReadBuildInfo() VCS settings.
// The result is computed once and cached.
func Get() Info {
	once.Do(func() {
		cached = resolve()
	})
	return cached
}

func resolve() Info {
	info := Info{
		Version:  "dev",
		Commit:   "unknown",
		Date:     "unknown",
		Modified: false,
	}

	// First, try runtime/debug.ReadBuildInfo for VCS settings and Go version.
	// These are automatically embedded when building from a VCS working tree.
	if bi, ok := debug.ReadBuildInfo(); ok {
		info.GoVer = bi.GoVersion
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				info.Commit = s.Value
			case "vcs.time":
				info.Date = s.Value
			case "vcs.modified":
				info.Modified = s.Value == "true"
			}
		}
		// Use the module version if it looks like a real version tag
		if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			info.Version = bi.Main.Version
		}
	}

	// Override with ldflags-injected values when present.
	// These take priority because they are explicitly set by the release pipeline.
	if ldflagsVersion != "" {
		info.Version = ldflagsVersion
	}
	if ldflagsCommit != "" {
		info.Commit = ldflagsCommit
	}
	if ldflagsDate != "" {
		info.Date = ldflagsDate
	}

	return info
}
