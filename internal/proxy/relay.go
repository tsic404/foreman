package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// errRelayExhausted marks a C21 relay that used up its retry budget: the §1.1
// C21 retry rule (network errors and 5xx back off 1s→30s) ran out of window
// before the server took the result.
var errRelayExhausted = errors.New("catalog relay exhausted")

// relayCatalog answers one catalog command with the C21 retry rule: transport
// errors, 5xx, 408, 429 and 401 back off 1s→30s while the budget lasts; every
// other 4xx is an upstream rejection and is returned as-is (§1.1 C21 第 11 条).
// budget is the caller's window (an exclusive upper bound); a zero budget means
// "no local bound beyond ctx".
//
// The returned code is 0 with a non-nil error when nothing was relayed — the
// caller answers 502 to the probe daemon and records the abandoned relay.
func relayCatalog(ctx context.Context, client *Client, runtimeID, requestID string, body []byte, budget time.Time, log *slog.Logger) (int, []byte, error) {
	backoff := newBackoff()
	var scripted []time.Duration
	if client != nil {
		scripted = client.relayWaits
	}
	step := 0
	wait := func() time.Duration {
		if len(scripted) == 0 {
			return backoff.next()
		}
		delay := scripted[len(scripted)-1]
		if step < len(scripted) {
			delay = scripted[step]
		}
		step++
		return delay
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		if !budget.IsZero() && !time.Now().Before(budget) {
			return 0, nil, errRelayExhausted
		}
		code, resp, err := client.RelayModelCatalog(ctx, runtimeID, requestID, body)
		if err == nil && !retryableRelayStatus(code) {
			if code >= 400 && log != nil {
				// §1.1 C21 第 11 条: an upstream 4xx is a rejection — record it,
				// never retry it, and hand the verdict back untouched.
				log.Error("model catalog relay rejected by upstream",
					"runtime_id", runtimeID, "request_id", requestID, "status", code)
			}
			return code, resp, nil
		}
		nextWait := wait()
		if !budget.IsZero() {
			if remaining := time.Until(budget); remaining <= 0 || nextWait > remaining {
				return 0, nil, errRelayExhausted
			}
		}
		if log != nil {
			if err != nil {
				log.Warn("model catalog relay attempt failed", "runtime_id", runtimeID, "request_id", requestID, "err", err)
			} else {
				log.Warn("model catalog relay attempt rejected", "runtime_id", runtimeID, "request_id", requestID, "status", code)
			}
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-time.After(nextWait):
		}
	}
}

// retryableRelayStatus classifies a C21 answer: 5xx and the transient 4xx are
// retried (§4's shape), 401 is the credential case the failure table also backs
// off on, and every other 4xx is an upstream verdict to hand back untouched.
func retryableRelayStatus(code int) bool {
	switch code {
	case http.StatusUnauthorized, http.StatusRequestTimeout, http.StatusTooManyRequests:
		return true
	}
	return code >= 500
}
