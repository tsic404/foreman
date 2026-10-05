package proxy

import (
	"errors"
	"fmt"
	"time"
)

// StatusError is a non-2xx server response. Callers branch on Code per the
// §1.1 failure table: 401 backs off, 403 is a config error (no blind retry),
// 404 with a "runtime not found" body triggers re-registration.
type StatusError struct {
	Code int
	Body []byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("server returned %d: %s", e.Code, truncate(e.Body, 200))
}

// ErrTerminalUndelivered reports a terminal forward (C9–C11) whose retry
// budget (4s/8s/16s/32s/64s) is exhausted; the caller must enqueue the
// report to PendingReports (§4: terminal callbacks are never dropped).
var ErrTerminalUndelivered = errors.New("terminal report undelivered after retry budget")

// terminalRetryWaits is the §4 retry budget for C9–C11: six attempts,
// 124s of total waiting.
var terminalRetryWaits = []time.Duration{0, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 64 * time.Second}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}
