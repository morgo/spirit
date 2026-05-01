package check

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestReplicaHealth(t *testing.T) {
	r := Resources{
		Table:     &table.TableInfo{TableName: "test"},
		Statement: statement.MustNew("ALTER TABLE test RENAME TO newtablename")[0],
	}
	err := replicaHealth(t.Context(), r, slog.Default())
	assert.NoError(t, err) // if no replicas, it returns no error.

	// use a non-replica. this will return an error identifying which thread
	// is not running and on which host.
	nonReplica, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	r.Replicas = []*sql.DB{nonReplica}
	err = replicaHealth(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrReplicaNotHealthy)

	// use an actual replica
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	replicaDB, err := sql.Open("mysql", replicaDSN)
	assert.NoError(t, err)
	r.Replicas = []*sql.DB{replicaDB}
	err = replicaHealth(t.Context(), r, slog.Default())
	assert.NoError(t, err) // all looks good of course.

	// use a completely invalid DSN.
	// golang sql.Open lazy loads, so this is possible.
	invalidDB, err := sql.Open("mysql", "msandbox:msandbox@tcp(127.0.0.1:22)/test")
	assert.NoError(t, err)
	r.Replicas = []*sql.DB{invalidDB}
	err = replicaHealth(t.Context(), r, slog.Default())
	assert.Error(t, err) // invalid
}
