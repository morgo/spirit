package check

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

func init() {
	register(check{name: "settings", run: settingsCheck, phases: PhasePreflight, runtimes: Migration})
}

// check the settings used to initialize spirit.
func settingsCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	// Threads must be in the range of 1-64
	if r.Options.Threads < 1 || r.Options.Threads > 64 {
		return errors.New("--threads must be in the range of 1-64")
	}
	// ReplicaMaxLag must be in the range of 10s-4hr
	if r.Options.ReplicaMaxLag < 10*time.Second || r.Options.ReplicaMaxLag > time.Hour*4 {
		return errors.New("--replica-max-lag must be in the range of 10s-4hr")
	}
	return nil
}
