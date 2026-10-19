// TC-tech-auth-01 (AC-06, AC-15): the daemon face routes by credential type.
// A missing or malformed credential is refused locally with 401 and never
// reaches the upstream; any non-Job-Token credential is forwarded verbatim —
// Foreman neither validates nor replaces it — and the upstream's answer comes
// back as given. The Job Token itself is a local-face credential and must
// never appear as an upstream credential.
package acceptance

import (
	"net/http"
	"strings"
	"testing"
)

func TestAuth01CredentialRouting(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)

	// No credential: 401 locally, upstream untouched.
	before := len(h.upstream.all())
	code, body := h.call("POST", "/api/daemon/tasks/task-1/start", "", []byte(`{}`))
	if code != http.StatusUnauthorized {
		t.Errorf("no credential = %d (%s), want 401", code, body)
	}
	if after := len(h.upstream.all()); after != before {
		t.Errorf("an unauthenticated request reached the upstream (%d new calls)", after-before)
	}

	// Malformed Job Tokens: the prefix selects the local face, the signature
	// check refuses it — still local, still 401.
	for _, malformed := range []string{"fmj_not-a-token", "fmj_eyJqIjoidCJ9.c2ln"} {
		before = len(h.upstream.all())
		code, body = h.call("POST", "/api/daemon/tasks/task-1/start", malformed, []byte(`{}`))
		if code != http.StatusUnauthorized {
			t.Errorf("malformed token %q = %d (%s), want 401", malformed, code, body)
		}
		if after := len(h.upstream.all()); after != before {
			t.Errorf("malformed token %q reached the upstream (%d new calls)", malformed, after-before)
		}
	}

	// A non-Job-Token credential is the agent's: forwarded verbatim, the
	// caller's own header and body included, and the upstream's answer is
	// replayed.
	const agentToken = "mat_agent_task_token"
	agentBody := []byte(`{"renew":true}`)
	code, body = h.call("POST", "/api/tokens/current/renew", agentToken, agentBody)
	if code != http.StatusOK {
		t.Fatalf("agent passthrough = %d (%s), want the upstream's 200", code, body)
	}
	calls := h.upstream.calls(http.MethodPost, "/api/tokens/current/renew")
	if len(calls) != 1 {
		t.Fatalf("upstream renew calls = %d, want exactly one", len(calls))
	}
	if got := calls[0].Header.Get("Authorization"); got != "Bearer "+agentToken {
		t.Errorf("upstream Authorization = %q, want the caller's own credential verbatim", got)
	}
	if string(calls[0].Body) != string(agentBody) {
		t.Errorf("upstream body = %s, want the caller's %s", calls[0].Body, agentBody)
	}
	if !strings.Contains(string(body), `"token":"mdt_test"`) {
		t.Errorf("caller body = %s, want the upstream's answer verbatim", body)
	}

	// The local credential is never used as an upstream credential.
	for _, call := range h.upstream.all() {
		if strings.HasPrefix(call.Header.Get("Authorization"), "Bearer fmj_") {
			t.Errorf("a Job Token reached the upstream on %s %s", call.Method, call.Path)
		}
	}
}
