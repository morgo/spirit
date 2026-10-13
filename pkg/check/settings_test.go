package check

import (
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestSettings(t *testing.T) {
	r := Resources{
		Runtime: Migration,
		Change:  Change{Table: &table.TableInfo{TableName: "test", SchemaName: "test"}},
		Options: Options{Threads: 2, ReplicaMaxLag: time.Hour},
	}

	// 0 means "use default". This is a bit of a hack,
	// but the test suite depends on it.
	if r.Options.ReplicaMaxLag == 0 {
		r.Options.ReplicaMaxLag = time.Second * 120
	}

	err := settingsCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all looks good

	r.Options.Threads = 0
	err = settingsCheck(t.Context(), r, slog.Default())
	require.Error(t, err)

	r.Options.Threads = 65
	err = settingsCheck(t.Context(), r, slog.Default())
	require.Error(t, err)

	r.Options.Threads = 2
	err = settingsCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all looks good

	r.Options.ReplicaMaxLag = time.Second * 5
	err = settingsCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
}
