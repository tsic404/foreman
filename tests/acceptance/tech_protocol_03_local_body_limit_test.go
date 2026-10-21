// TC-tech-protocol-03 (AC-89): the local daemon face's request-body bound is
// announced, not disguised. A body past it is refused with 413 carrying the
// bound and how much was read, the same facts land in the event log, the
// handler never runs on the truncated bytes, and S-29 — which carries agent
// payloads — is not subject to the bound.
package acceptance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

func TestTechProtocol03LocalFaceOversizeBodyIsAnnounced(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")

	// A valid S-4 body padded past the face's bound: were the handler to see
	// the truncated bytes it would register, so the entry state below is a
	// real side-effect probe rather than a malformed-body rejection.
	body := []byte(`{"daemon_id":"fm-task-1","runtimes":[{"name":"foreman-zz","type":"zz","version":"3.1.4"}],"pad":"` +
		strings.Repeat("a", forwardProbeBulkBytes) + `"}`)

	code, raw := h.call("POST", "/api/daemon/register", token, body)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize local body = %d %s, want 413", code, raw)
	}
	var payload struct {
		Error      string `json:"error"`
		LimitBytes int64  `json:"limit_bytes"`
		ReadBytes  int64  `json:"read_bytes"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("413 body %s: %v", raw, err)
	}
	if payload.LimitBytes <= 0 || payload.LimitBytes >= int64(len(body)) {
		t.Errorf("413 limit_bytes = %d, want a bound below the %d-byte probe", payload.LimitBytes, len(body))
	}
	if payload.ReadBytes != payload.LimitBytes {
		t.Errorf("413 read_bytes = %d, want the bytes read up to the bound (%d)", payload.ReadBytes, payload.LimitBytes)
	}
	if !strings.Contains(payload.Error, "bound") {
		t.Errorf("413 error = %q, want it to name the face's bound", payload.Error)
	}

	// The same facts are an event, so the refusal stays findable after the
	// caller is gone.
	events := h.handlerEvents("request body over the local face's bound")
	if len(events) != 1 {
		t.Fatalf("events = %d, want one for the refused body", len(events))
	}
	if got, want := int64(events[0]["limit_bytes"].(float64)), payload.LimitBytes; got != want {
		t.Errorf("event limit_bytes = %d, want %d", got, want)
	}
	if got, want := int64(events[0]["read_bytes"].(float64)), payload.ReadBytes; got != want {
		t.Errorf("event read_bytes = %d, want %d", got, want)
	}

	// The handler never ran: the entry still waits for its daemon and no job
	// runtime row was written.
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("entry task-1 missing")
	}
	if entry.State != registry.StateJobCreated || !entry.DaemonRegisteredAt.IsZero() {
		t.Errorf("entry after the refused body = %s (registered at %v), want the unregistered Job-created state",
			entry.State, entry.DaemonRegisteredAt)
	}
	if _, ok := h.reg.RuntimeByID(entry.JobRuntimeID); ok {
		t.Error("the refused body still registered the job runtime")
	}

	// The bound belongs to the local face, not to the passthrough: the same
	// body travels S-29 to the upstream whole, with no proxy-side 413.
	header := http.Header{
		"Authorization": {"Bearer mat_tasktoken"},
		"Content-Type":  {"application/json"},
		"User-Agent":    {"tc-protocol-03/1.0"},
	}
	client := newForwardCaller(t)
	proxiedCode, _, answer := forwardCall(t, client, h.jobsHTTP.URL+"/api/forward-probe", header, body)
	if proxiedCode != http.StatusCreated {
		t.Fatalf("S-29 upload = %d %s, want the upstream verdict", proxiedCode, answer)
	}
	observations := h.upstream.forwardObservations()
	if len(observations) != 1 {
		t.Fatalf("upstream probes = %d, want the passthrough run", len(observations))
	}
	sum := sha256.Sum256(body)
	if observations[0].BodyLen != int64(len(body)) || observations[0].BodySHA != hex.EncodeToString(sum[:]) {
		t.Errorf("upstream saw %d bytes (%s), want the caller's %d bytes (%s)",
			observations[0].BodyLen, observations[0].BodySHA, len(body), hex.EncodeToString(sum[:]))
	}
}
