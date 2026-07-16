package bench

import (
	"context"
	"time"
)

// retry runs fn up to attempts times with capped exponential backoff, aborting
// early if ctx is cancelled. It reports the total number of attempts made and the
// final error (nil on success).
func retry(ctx context.Context, attempts int, fn func() error) (tries int, err error) {
	if attempts < 1 {
		attempts = 1
	}
	backoff := 100 * time.Millisecond
	for i := 0; i < attempts; i++ {
		tries++
		if err = fn(); err == nil {
			return tries, nil
		}
		if ctx.Err() != nil {
			return tries, ctx.Err()
		}
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return tries, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
	return tries, err
}
