package buildinfo

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildInfoDefaultsWithoutLdflags(t *testing.T) {
	// Reset cached state for testing
	once = sync.Once{}
	ldflagsVersion = ""
	ldflagsCommit = ""
	ldflagsDate = ""

	info := Get()
	// When built from a git checkout via `go test`, vcs.* should be populated
	require.NotEmpty(t, info.GoVer, "GoVer should always be set")
	// Version should be "dev" since we didn't inject via ldflags
	// and test binaries don't have a module version tag
	require.Equal(t, "dev", info.Version)
}

func TestBuildInfoLdflagsOverridesVCS(t *testing.T) {
	// Reset cached state for testing
	once = sync.Once{}
	ldflagsVersion = ""
	ldflagsCommit = ""
	ldflagsDate = ""

	Set("v1.2.3", "abc123def456", "2026-02-25T00:00:00Z")
	info := Get()

	require.Equal(t, "v1.2.3", info.Version)
	require.Equal(t, "abc123def456", info.Commit)
	require.Equal(t, "2026-02-25T00:00:00Z", info.Date)
	require.NotEmpty(t, info.GoVer, "GoVer from ReadBuildInfo should still be set")
}

func TestBuildInfoPartialLdflagsWithVCSFallback(t *testing.T) {
	// Reset cached state for testing
	once = sync.Once{}
	ldflagsVersion = ""
	ldflagsCommit = ""
	ldflagsDate = ""

	// Only set version via ldflags, commit/date should come from VCS
	Set("v3.0.0", "", "")
	info := Get()

	require.Equal(t, "v3.0.0", info.Version)
	// Commit should fall through to VCS (or "unknown" if not in git tree)
	require.NotEmpty(t, info.Commit)
	require.NotEmpty(t, info.GoVer)
}
