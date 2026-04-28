package check

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestConfigurationCheck(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Test with valid configuration
	r := Resources{
		Sources: []SourceResource{{DB: db}},
	}
	err = configurationCheck(context.Background(), r, slog.Default())
	assert.NoError(t, err)
}

func TestConfigurationCheckMultipleSources(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	db2, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db2)

	// Multiple valid sources should pass.
	r := Resources{
		Sources: []SourceResource{{DB: db}, {DB: db2}},
	}
	err = configurationCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)

	// A nil DB on the second source should fail and reference source 1.
	r = Resources{
		Sources: []SourceResource{{DB: db}, {DB: nil}},
	}
	err = configurationCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source 1")
}
