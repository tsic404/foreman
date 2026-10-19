// TC-tech-auth-02: cross-Job and expired tokens are refused. A Job Token is
// bound to the Job that signed it, so A's token must answer 403 on B's task
// paths — including B's terminal endpoints after A's credential was reclaimed
// and only the done index remains (the late-replay branch, §2.1 rule 1).
// An expired token answers 401 before any registry lookups (§6.1).
package acceptance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/scheduler"
)

func TestTechAuth02CrossTaskAndExpiredTokens(t *testing.T) {
	h := newDeliveryHarness(t)
	taskA, taskB := "task-a", "task-b"
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskA)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskA, err)
	}
	if err := h.sched.OnClaim(t.Context(), deliveryPayload(taskB)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskB, err)
	}
	tokenA := issueJobToken(t, h, taskA)

	// Live entry: A's token on B's task paths is a scope mismatch.
	for _, path := range []string{
		"/api/daemon/tasks/" + taskB + "/start",
		"/api/daemon/tasks/" + taskB + "/complete",
	} {
		code, body := h.call("POST", path, tokenA, []byte(`{}`))
		if code != http.StatusForbidden {
			t.Errorf("A's token on %s = %d (%s), want 403", path, code, body)
		}
	}

	// Settle A and reclaim its credential: the entry disappears, the done
	// index keeps the terminal record — the exact shape of the late-replay
	// branch.
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskA+"/complete", tokenA,
		[]byte(`{"exit_code":0}`)); code != http.StatusOK {
		t.Fatalf("A's complete = %d (%s), want 200", code, body)
	}
	if err := h.sched.OnJobTerminated(t.Context(), "fm-"+taskA,
		scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
		t.Fatalf("OnJobTerminated(%s): %v", taskA, err)
	}
	if _, ok := h.reg.Get(taskA); ok {
		t.Fatalf("entry %s survived the credential reclaim", taskA)
	}
	if !h.reg.IsDone(taskA) {
		t.Fatalf("done index lost %s", taskA)
	}

	// A's own terminal endpoint keeps the idempotent 200.
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskA+"/complete", tokenA,
		[]byte(`{"exit_code":0}`)); code != http.StatusOK {
		t.Errorf("A's own terminal replay = %d (%s), want 200", code, body)
	}

	// The regression: with A's entry gone, A's token on B's terminal endpoint
	// must still be refused as a scope mismatch, not answered as a replay.
	code, body := h.call("POST", "/api/daemon/tasks/"+taskB+"/complete", tokenA,
		[]byte(`{"exit_code":0}`))
	if code != http.StatusForbidden {
		t.Errorf("A's token on B's terminal endpoint = %d (%s), want 403", code, body)
	}
	if entry, ok := h.reg.Get(taskB); !ok || entry.IsTerminal() {
		t.Errorf("B's entry = %+v/%v, want a live non-terminal entry (the refused request must not settle it)",
			entry, ok)
	}
	for _, forward := range h.server.forwarded(scheduler.EPComplete) {
		if forward.Path == taskB {
			t.Errorf("B's complete was forwarded (body %s): a refused request must not reach the server", forward.Body)
		}
	}
	if n := h.metrics.authFailureCount("scope"); n < 1 {
		t.Errorf("auth failures recorded as scope = %d, want at least 1", n)
	}

	// Expired token: refused before any registry lookup.
	expired := expiredJobToken(t, "fm-"+taskB, taskB)
	if code, body := h.call("POST", "/api/daemon/tasks/"+taskB+"/start", expired,
		[]byte(`{}`)); code != http.StatusUnauthorized {
		t.Errorf("expired token = %d (%s), want 401", code, body)
	}
}

// expiredJobToken signs a token beyond the clock-skew grace: the wire format
// is the contract's, the answer must be 401.
func expiredJobToken(t *testing.T, jobName, taskID string) string {
	t.Helper()
	payload := fmt.Sprintf(`{"j":%q,"t":%q,"w":"ws-1","e":%d}`,
		jobName, taskID, time.Now().Add(-auth.ClockSkew-time.Minute).Unix())
	segment := base64.RawURLEncoding.EncodeToString([]byte(payload))
	mac := hmac.New(sha256.New, testKey)
	mac.Write([]byte(auth.TokenPrefix + segment))
	return auth.TokenPrefix + segment + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
