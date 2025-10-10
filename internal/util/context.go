package util

import (
	"context"
	"time"
)

// WaitContext blocks for the provided duration or until the context is canceled.
// Returns the context error when canceled, otherwise returns nil once the timer fires.
func WaitContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
