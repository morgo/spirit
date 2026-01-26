package utils

import (
	"context"
	"log/slog"
)

// Closer is an interface for types that have a Close() method.
// This is compatible with io.Closer and many other types in the codebase.
type Closer interface {
	Close() error
}

// ContextCloser is an interface for types that have a Close(context.Context) method.
// This is used for resources like table locks that need context for cleanup.
type ContextCloser interface {
	Close(context.Context) error
}

// CloseAndLog closes a resource and logs any error. This is useful for defer statements
// where the error cannot be meaningfully handled except by logging.
// Example: defer utils.CloseAndLog(db)
func CloseAndLog(closer Closer) {
	if closer == nil {
		return
	}
	if err := closer.Close(); err != nil {
		slog.Error("deferred close failed", "error", err)
	}
}

// CloseAndLogWithContext closes a resource that requires context and logs any error.
// This is useful for defer statements on resources like table locks.
// Example: defer utils.CloseAndLogWithContext(ctx, lock)
func CloseAndLogWithContext(ctx context.Context, closer ContextCloser) {
	if closer == nil {
		return
	}
	if err := closer.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "deferred close failed", "error", err)
	}
}
