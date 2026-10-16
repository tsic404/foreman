// TC-tech-startup-03-fail implements the failure (negative) branch of
// AC-38 / TC-tech-startup-03: the S4 zero-hit refusal and the per-runtime
// failure granularity of the declared runtime set. The positive multi-runtime
// scenarios of TC-tech-startup-03 (declaration counts, claim attribution,
// N WS connections) need a constructible N ≥ 2 cluster and are registered as
// QA_BLOCKED in the issue.
package acceptance

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// TestStartup03FailRegisterRefusedOnProviderMiss covers AC-38's refusal
// branch: the daemon's registration names no runtime for the provider its
// claim was dispatched under, so Foreman refuses the registration and settles
// the task instead of waiting out the boot deadline.
func TestStartup03FailRegisterRefusedOnProviderMiss(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	// The claim carries the provider-z runtime: this Job must run `zz`.
	payload := json.RawMessage(`{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-zz", "agent": {"model": "k3"}
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok || entry.RealRuntimeID != "rid-zz" {
		t.Fatalf("entry = %+v, want the dispatch record rid-zz", entry)
	}
	token, err := h.issuer.Issue("fm-task-1", "task-1", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// The daemon reports an `omp`-only runtime set: zero hits for `zz`.
	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-task-1",
		"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
	}`))
	if code != 409 {
		t.Fatalf("register with a foreign provider = %d (%s), want 409", code, body)
	}

	// The entry converges to terminal/failed immediately — no boot deadline
	// wait — and the compensation report reaches the server.
	settled, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the entry must still be visible (it drains)")
	}
	if settled.State != registry.StateTerminal || settled.Result != registry.ResultFailed {
		t.Fatalf("entry state = %s/%s, want terminal/failed", settled.State, settled.Result)
	}
	forwarded := h.server.forwarded(scheduler.EPFail)
	if len(forwarded) != 1 {
		t.Fatalf("C10 reports = %d, want 1", len(forwarded))
	}
	if !bytes.Contains(forwarded[0].Body, []byte(scheduler.FailureReasonDaemonNotRegistered)) {
		t.Errorf("fail body = %s, want the daemon-not-registered classification", forwarded[0].Body)
	}
}

// TestStartup03FailRegisterAcceptsMatchingProvider keeps the refusal branch
// honest: the same Job with its own provider in the declaration registers.
func TestStartup03FailRegisterAcceptsMatchingProvider(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	payload := json.RawMessage(`{
		"id": "task-2", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-zz", "agent": {"model": "k3"}
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token, err := h.issuer.Issue("fm-task-2", "task-2", "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-task-2",
		"runtimes": [
			{"name": "foreman-omp", "type": "omp", "version": "0.6.1"},
			{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}
		]
	}`))
	if code != 200 {
		t.Fatalf("register = %d (%s), want 200", code, body)
	}
	if !bytes.Contains(body, []byte(`"provider":"zz"`)) {
		t.Errorf("S4 must answer the job's own provider: %s", body)
	}
	if got := h.server.forwarded(scheduler.EPFail); len(got) != 0 {
		t.Errorf("a matching provider must not be settled as failed: %v", got)
	}
}

// TestStartup03FailHeartbeatFailureStaysPerRuntime covers AC-38's
// granularity clause: one runtime's heartbeat failing (5xx / network) only
// backs off that stream; the other runtime's stream continues and no runtime
// is ever deregistered for it.
func TestStartup03FailHeartbeatFailureStaysPerRuntime(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	h.upstream.mu.Lock()
	h.upstream.heartbeats["rid-omp"] = 503
	h.upstream.mu.Unlock()

	if _, err := h.client.Heartbeat(t.Context(), "rid-omp"); err == nil {
		t.Fatal("the failing runtime's heartbeat must surface an error (backoff input)")
	}
	if _, err := h.client.Heartbeat(t.Context(), "rid-zz"); err != nil {
		t.Fatalf("the healthy runtime's heartbeat must keep working: %v", err)
	}
	if got := h.upstream.calls("POST", "/api/daemon/deregister"); len(got) != 0 {
		t.Errorf("a heartbeat failure must never deregister any runtime: %d calls", len(got))
	}
	for _, ref := range h.client.RuntimeRefs() {
		if ref.ID == "" {
			t.Errorf("runtime %s lost its id on an unrelated failure", ref.Provider)
		}
	}
}

// TestStartup03FailRuntimeNotFoundRebuildsOnlyThatRuntime covers the second
// half of AC-38's granularity clause: a 404 `runtime not found` rebuilds only
// the affected runtime — the others keep their ids, and only that runtime's
// subscription moves.
func TestStartup03FailRuntimeNotFoundRebuildsOnlyThatRuntime(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	// The omp row dies server-side: the next heartbeat meets a 404 and the
	// rebuild answers with a fresh id for omp only.
	h.upstream.mu.Lock()
	h.upstream.heartbeats["rid-omp"] = 404
	h.upstream.registerIDs["omp"] = "rid-omp-2"
	h.upstream.mu.Unlock()

	ack, err := h.client.Heartbeat(t.Context(), "rid-omp")
	if err != nil {
		t.Fatalf("Heartbeat after a rebuild: %v", err)
	}
	if ack.RuntimeID != "rid-omp-2" {
		t.Errorf("ack runtime id = %q, want the rebuilt id", ack.RuntimeID)
	}
	ids := map[string]string{}
	for _, ref := range h.client.RuntimeRefs() {
		ids[ref.Provider] = ref.ID
	}
	if ids["omp"] != "rid-omp-2" || ids["zz"] != "rid-zz" {
		t.Fatalf("runtime set after the rebuild = %v", ids)
	}
	var changes []string
	for {
		select {
		case change := <-h.client.Changes():
			changes = append(changes, change.Provider+":"+change.OldID+"->"+change.NewID)
			continue
		default:
		}
		break
	}
	if strings.Join(changes, ",") != "omp:rid-omp->rid-omp-2" {
		t.Fatalf("runtime changes = %v, want only the affected runtime", changes)
	}
	if got := len(h.upstream.calls("POST", "/api/daemon/deregister")); got != 0 {
		t.Errorf("rebuilding one runtime must not deregister the others: %d calls", got)
	}
}
