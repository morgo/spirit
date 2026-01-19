package check

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestConfigurationCheck(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	// Test with valid configuration
	r := Resources{
		SourceDB: db,
	}
	err = configurationCheck(context.Background(), r, slog.Default())
	assert.NoError(t, err)
}
