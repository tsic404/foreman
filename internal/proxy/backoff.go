package proxy

import "time"

// backoff implements the §4 exponential retry policy for the client-side
// control loops: 1s doubling up to a 30s ceiling, reset on success.
type backoff struct {
	min, max time.Duration
	cur      time.Duration
}

func newBackoff() *backoff {
	return &backoff{min: 1 * time.Second, max: 30 * time.Second}
}

// next returns the wait before the next attempt.
func (b *backoff) next() time.Duration {
	if b.cur < b.min {
		b.cur = b.min
		return b.cur
	}
	b.cur *= 2
	if b.cur > b.max {
		b.cur = b.max
	}
	return b.cur
}

func (b *backoff) reset() { b.cur = 0 }
