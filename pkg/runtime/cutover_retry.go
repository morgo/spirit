package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

const (
	// CutoverInitialBackoff is the delay before the second cutover attempt.
	// It doubles before each later attempt, up to CutoverMaxBackoff. Without a
	// backoff a contended table lock produces a tight retry loop that worsens
	// the contention; the delay gives its source time to clear.
	CutoverInitialBackoff = 100 * time.Millisecond
	// CutoverMaxBackoff caps the delay between cutover attempts.
	CutoverMaxBackoff = 10 * time.Second
)

// CutoverVerdict is a StopFunc's decision about a failed cutover attempt.
type CutoverVerdict int

const (
	// CutoverVerdictRetry runs the next attempt, after the backoff. When no attempts
	// are left, RetryCutover returns what the exhausted callback returns.
	CutoverVerdictRetry CutoverVerdict = iota
	// CutoverVerdictSucceeded ends the loop with a nil error: the runner has proven
	// that the cutover committed even though the attempt reported an error.
	CutoverVerdictSucceeded
	// CutoverVerdictFailed ends the loop without another attempt.
	CutoverVerdictFailed
)

// CutoverRetry configures RetryCutover.
type CutoverRetry struct {
	// Attempts is the number of attempts. A value below 1 is one attempt.
	Attempts int
	// InitialBackoff and MaxBackoff shape the delay between attempts. A zero
	// value takes CutoverInitialBackoff or CutoverMaxBackoff.
	InitialBackoff, MaxBackoff time.Duration
	// Logger logs each attempt and failure. Nil logs to slog.Default().
	Logger *slog.Logger
	// Sleep waits d, or returns ctx's error when ctx is done first. Nil is a
	// timer that stops early on cancel. It is a test seam.
	Sleep func(ctx context.Context, d time.Duration) error
}

// StopFunc classifies the error of the attempt that just failed. It is passed
// that attempt's error only, never the errors of earlier attempts, so a marker
// that an earlier attempt carried cannot stop a later one.
//
// For CutoverVerdictFailed, extra is appended after the attempt history in the error
// RetryCutover returns; nil appends nothing. extra is ignored for the other
// verdicts.
type StopFunc func(err error) (verdict CutoverVerdict, extra error)

// RetryCutover runs a cutover's attempt loop: it counts the attempts, sleeps
// an exponential backoff between them, records every failed attempt's error
// prefixed "attempt N:", and returns them joined, so an operator debugging a
// flapping cutover sees the full failure history rather than only the last
// try.
//
// Which failures are retried is the runner's: stop classifies each failed
// attempt. A done ctx, checked before every attempt and during the backoff,
// returns onCancel(history); nil onCancel returns the history joined with
// ctx.Err(). When the attempts run out, RetryCutover returns
// exhausted(history); nil exhausted returns the history joined. history is
// the failed attempts' prefixed errors, oldest first, in a slice the callback
// owns (empty when no attempt has failed). Join it with what the callback
// adds (errors.Join(append(history, extra...)...)), so the result unwraps
// to one error per attempt.
func RetryCutover(
	ctx context.Context,
	cfg CutoverRetry,
	attempt func(ctx context.Context, i int) error,
	stop StopFunc,
	onCancel func(history []error) error,
	exhausted func(history []error) error,
) error {
	attempts := max(1, cfg.Attempts)
	backoff := cfg.InitialBackoff
	if backoff <= 0 {
		backoff = CutoverInitialBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = CutoverMaxBackoff
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	if onCancel == nil {
		onCancel = func(history []error) error { return errors.Join(append(history, ctx.Err())...) }
	}
	if exhausted == nil {
		exhausted = func(history []error) error { return errors.Join(history...) }
	}

	var attemptErrs []error
	history := func() []error { return slices.Clone(attemptErrs) }
	for i := range attempts {
		if ctx.Err() != nil {
			return onCancel(history())
		}
		if i > 0 {
			if err := sleep(ctx, backoff); err != nil {
				return onCancel(history())
			}
			backoff = min(backoff*2, maxBackoff)
		}
		logger.Warn("Attempting final cut over operation",
			"attempt", i+1,
			"max_retries", attempts,
		)
		err := attempt(ctx, i)
		if err == nil {
			logger.Warn("final cut over operation complete")
			return nil
		}
		attemptErrs = append(attemptErrs, fmt.Errorf("attempt %d: %w", i+1, err))
		verdict, extra := stop(err)
		switch verdict {
		case CutoverVerdictSucceeded:
			return nil
		case CutoverVerdictFailed:
			return errors.Join(append(slices.Clone(attemptErrs), extra)...)
		case CutoverVerdictRetry:
		default:
			return errors.Join(append(slices.Clone(attemptErrs), fmt.Errorf("unknown cutover verdict %d", verdict))...)
		}
		if i+1 < attempts {
			logger.Warn("cutover failed", "error", err.Error(), "next_backoff", backoff)
		} else {
			logger.Warn("cutover failed", "error", err.Error())
		}
	}
	if err := exhausted(history()); err != nil {
		logger.Error("cutover failed, and retries exhausted")
		return err
	}
	return nil
}

// sleepContext waits d, or returns ctx's error as soon as ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
