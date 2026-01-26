package utils

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Mock closers for testing

type mockCloser struct {
	shouldFail bool
	closed     bool
}

func (m *mockCloser) Close() error {
	m.closed = true
	if m.shouldFail {
		return errors.New("mock close error")
	}
	return nil
}

type mockContextCloser struct {
	shouldFail bool
	closed     bool
}

func (m *mockContextCloser) Close(ctx context.Context) error {
	m.closed = true
	if m.shouldFail {
		return errors.New("mock context close error")
	}
	return nil
}

func TestCloseAndLog(t *testing.T) {
	t.Run("nil closer should not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			CloseAndLog(nil)
		})
	})

	t.Run("successful close", func(t *testing.T) {
		closer := &mockCloser{shouldFail: false}
		CloseAndLog(closer)
		assert.True(t, closer.closed, "Close should have been called")
	})

	t.Run("failed close logs error", func(t *testing.T) {
		closer := &mockCloser{shouldFail: true}
		// The function will log an error, but should not panic
		assert.NotPanics(t, func() {
			CloseAndLog(closer)
		})
		assert.True(t, closer.closed, "Close should have been called even though it failed")
	})
}

func TestCloseAndLogWithContext(t *testing.T) {
	ctx := context.Background()

	t.Run("nil closer should not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			CloseAndLogWithContext(ctx, nil)
		})
	})

	t.Run("successful close", func(t *testing.T) {
		closer := &mockContextCloser{shouldFail: false}
		CloseAndLogWithContext(ctx, closer)
		assert.True(t, closer.closed, "Close should have been called")
	})

	t.Run("failed close logs error", func(t *testing.T) {
		closer := &mockContextCloser{shouldFail: true}
		// The function will log an error, but should not panic
		assert.NotPanics(t, func() {
			CloseAndLogWithContext(ctx, closer)
		})
		assert.True(t, closer.closed, "Close should have been called even though it failed")
	})

	t.Run("canceled context", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		closer := &mockContextCloser{shouldFail: false}
		// Should still work even with canceled context
		assert.NotPanics(t, func() {
			CloseAndLogWithContext(canceledCtx, closer)
		})
		assert.True(t, closer.closed, "Close should have been called")
	})
}
