package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/require"
)

// fakeSleep records every backoff instead of sleeping.
type fakeSleep struct {
	sleeps []time.Duration
	// onSleep, when set, runs before the sleep returns; its error is the
	// sleep's result.
	onSleep func() error
}

func (f *fakeSleep) sleep(ctx context.Context, d time.Duration) error {
	f.sleeps = append(f.sleeps, d)
	if f.onSleep != nil {
		return f.onSleep()
	}
	return ctx.Err()
}

func retry(_ error) (CutoverVerdict, error) { return CutoverVerdictRetry, nil }

func TestRetryCutoverBackoffDoublesToTheCap(t *testing.T) {
	fs := &fakeSleep{}
	attempts := 0
	err := RetryCutover(t.Context(), CutoverRetry{Attempts: 10, Sleep: fs.sleep},
		func(_ context.Context, i int) error {
			require.Equal(t, attempts, i)
			attempts++
			return errors.New("lock wait timeout")
		}, retry, nil, nil)
	require.Error(t, err)
	require.Equal(t, 10, attempts)
	// No sleep before the first attempt nor after the last: one per gap.
	require.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond,
		10 * time.Second,
		10 * time.Second,
	}, fs.sleeps)
}

func TestRetryCutoverCustomBackoff(t *testing.T) {
	fs := &fakeSleep{}
	err := RetryCutover(t.Context(), CutoverRetry{
		Attempts: 4, InitialBackoff: time.Second, MaxBackoff: 3 * time.Second, Sleep: fs.sleep,
	}, func(context.Context, int) error { return errors.New("x") }, retry, nil, nil)
	require.Error(t, err)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}, fs.sleeps)
}

func TestRetryCutoverJoinsAttemptErrors(t *testing.T) {
	fs := &fakeSleep{}
	errs := []error{errors.New("first"), errors.New("second"), errors.New("third")}
	err := RetryCutover(t.Context(), CutoverRetry{Attempts: 3, Sleep: fs.sleep},
		func(_ context.Context, i int) error { return errs[i] }, retry, nil, nil)
	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok, "a joined error (%T)", err)
	require.Len(t, joined.Unwrap(), 3, "one error per attempt")
	for i, want := range errs {
		require.ErrorIs(t, err, want)
		require.ErrorContains(t, err, fmt.Sprintf("attempt %d: %s", i+1, want))
	}
}

func TestRetryCutoverExhaustedCallback(t *testing.T) {
	fs := &fakeSleep{}
	attemptErr := errors.New("lock wait timeout")
	var gotHistory error
	err := RetryCutover(t.Context(), CutoverRetry{Attempts: 2, Sleep: fs.sleep},
		func(context.Context, int) error { return attemptErr }, retry, nil,
		func(history []error) error {
			gotHistory = errors.Join(history...)
			return errors.Join(append(history, status.ErrOwnershipAmbiguous)...)
		})
	require.ErrorIs(t, gotHistory, attemptErr)
	require.ErrorContains(t, gotHistory, "attempt 2: lock wait timeout")
	require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)

	// exhausted can resolve the failure: the rename committed after all.
	err = RetryCutover(t.Context(), CutoverRetry{Attempts: 2, Sleep: fs.sleep},
		func(context.Context, int) error { return attemptErr }, retry, nil,
		func([]error) error { return nil })
	require.NoError(t, err)
}

func TestRetryCutoverSuccessShortCircuits(t *testing.T) {
	fs := &fakeSleep{}
	attempts := 0
	err := RetryCutover(t.Context(), CutoverRetry{Attempts: 5, Sleep: fs.sleep},
		func(context.Context, int) error {
			attempts++
			if attempts == 2 {
				return nil
			}
			return errors.New("transient")
		}, retry, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Len(t, fs.sleeps, 1)
}

func TestRetryCutoverStopVerdicts(t *testing.T) {
	fs := &fakeSleep{}
	attemptErr := errors.New("ambiguous rename")

	t.Run("succeeded", func(t *testing.T) {
		attempts := 0
		err := RetryCutover(t.Context(), CutoverRetry{Attempts: 5, Sleep: fs.sleep},
			func(context.Context, int) error { attempts++; return attemptErr },
			func(error) (CutoverVerdict, error) { return CutoverVerdictSucceeded, nil }, nil, nil)
		require.NoError(t, err)
		require.Equal(t, 1, attempts)
	})

	t.Run("failed", func(t *testing.T) {
		attempts := 0
		err := RetryCutover(t.Context(), CutoverRetry{Attempts: 5, Sleep: fs.sleep},
			func(context.Context, int) error { attempts++; return attemptErr },
			func(error) (CutoverVerdict, error) {
				return CutoverVerdictFailed, errors.Join(status.ErrDurableMutation,
					fmt.Errorf("%w: not retrying", status.ErrOwnershipAmbiguous))
			}, nil, nil)
		require.Equal(t, 1, attempts)
		require.ErrorIs(t, err, attemptErr)
		require.ErrorContains(t, err, "attempt 1: ambiguous rename")
		require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
		require.ErrorIs(t, err, status.ErrDurableMutation)
	})

	t.Run("failed without extra", func(t *testing.T) {
		err := RetryCutover(t.Context(), CutoverRetry{Attempts: 5, Sleep: fs.sleep},
			func(context.Context, int) error { return attemptErr },
			func(error) (CutoverVerdict, error) { return CutoverVerdictFailed, nil }, nil, nil)
		require.ErrorIs(t, err, attemptErr)
		require.Equal(t, "attempt 1: ambiguous rename", err.Error())
	})
}

// TestRetryCutoverStopSeesOnlyTheCurrentError checks that a marker an earlier
// attempt carried does not reach the classification of a later attempt.
func TestRetryCutoverStopSeesOnlyTheCurrentError(t *testing.T) {
	fs := &fakeSleep{}
	marker := errors.New("marker")
	var seen []error
	err := RetryCutover(t.Context(), CutoverRetry{Attempts: 3, Sleep: fs.sleep},
		func(_ context.Context, i int) error {
			if i == 0 {
				return fmt.Errorf("first: %w", marker)
			}
			return fmt.Errorf("plain %d", i)
		},
		func(err error) (CutoverVerdict, error) {
			seen = append(seen, err)
			return CutoverVerdictRetry, nil
		}, nil, nil)
	require.ErrorIs(t, err, marker, "the history keeps every attempt")
	require.Len(t, seen, 3)
	require.ErrorIs(t, seen[0], marker)
	for _, e := range seen[1:] {
		require.NotErrorIs(t, e, marker)
		require.NotContains(t, e.Error(), "attempt ")
	}
	require.Equal(t, "first: marker", seen[0].Error(), "stop gets the attempt's error unprefixed")
}

func TestRetryCutoverZeroAttemptsIsOne(t *testing.T) {
	for _, n := range []int{0, -1} {
		fs := &fakeSleep{}
		attempts := 0
		err := RetryCutover(t.Context(), CutoverRetry{Attempts: n, Sleep: fs.sleep},
			func(context.Context, int) error { attempts++; return errors.New("x") }, retry, nil, nil)
		require.Error(t, err)
		require.Equal(t, 1, attempts)
		require.Empty(t, fs.sleeps)
	}
}

func TestRetryCutoverCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fs := &fakeSleep{onSleep: func() error {
		cancel()
		return context.Canceled
	}}
	attemptErr := errors.New("lock wait timeout")
	attempts := 0
	var gotHistory error
	cancelCalls := 0
	err := RetryCutover(ctx, CutoverRetry{Attempts: 5, Sleep: fs.sleep},
		func(context.Context, int) error { attempts++; return attemptErr }, retry,
		func(history []error) error {
			cancelCalls++
			gotHistory = errors.Join(history...)
			return errors.Join(append(history, ctx.Err())...)
		}, func([]error) error {
			t.Fatal("exhausted must not run on cancel")
			return nil
		})
	require.Equal(t, 1, attempts, "no attempt after the cancel")
	require.Equal(t, 1, cancelCalls)
	require.ErrorIs(t, gotHistory, attemptErr)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "attempt 1: lock wait timeout")
}

func TestRetryCutoverCancelBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	attempts := 0
	err := RetryCutover(ctx, CutoverRetry{Attempts: 3},
		func(context.Context, int) error { attempts++; return nil }, retry, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, attempts)
}

// TestRetryCutoverDefaultCancel checks the default onCancel joins the history
// with ctx's error, using the real, ctx-aware sleep.
func TestRetryCutoverDefaultCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	attemptErr := errors.New("lock wait timeout")
	start := time.Now()
	time.AfterFunc(50*time.Millisecond, cancel)
	err := RetryCutover(ctx, CutoverRetry{Attempts: 2, InitialBackoff: time.Hour},
		func(context.Context, int) error { return attemptErr }, retry, nil, nil)
	require.Less(t, time.Since(start), time.Minute, "a cancel ends the backoff")
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, attemptErr)
}
