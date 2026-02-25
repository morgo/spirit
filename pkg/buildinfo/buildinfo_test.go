package buildinfo

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildInfoDefaultsWithoutLdflags(t *testing.T) {
	// Reset cached state for testing
	once = sync.Once{}
	ldflagsVersion = ""
	ldflagsCommit = ""
	ldflagsDate = ""

	info := Get()
	// When built from a git checkout via `go test`, vcs.* should be populated
	assert.NotEmpty(t, info.GoVer, "GoVer should always be set")
	// Version should be "dev" since we didn't inject via ldflags
	// and test binaries don't have a module version tag
	assert.Equal(t, "dev", info.Version)
}

func TestBuildInfoLdflagsOverridesVCS(t *testing.T) {
	// Reset cached state for testing
	once = sync.Once{}
	ldflagsVersion = ""
	ldflagsCommit = ""
	ldflagsDate = ""

	Set("v1.2.3", "abc123def456", "2026-02-25T00:00:00Z")
	info := Get()

	assert.Equal(t, "v1.2.3", info.Version)
	assert.Equal(t, "abc123def456", info.Commit)
	assert.Equal(t, "2026-02-25T00:00:00Z", info.Date)
	assert.NotEmpty(t, info.GoVer, "GoVer from ReadBuildInfo should still be set")
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

	assert.Equal(t, "v3.0.0", info.Version)
	// Commit should fall through to VCS (or "unknown" if not in git tree)
	assert.NotEmpty(t, info.Commit)
	assert.NotEmpty(t, info.GoVer)
}
