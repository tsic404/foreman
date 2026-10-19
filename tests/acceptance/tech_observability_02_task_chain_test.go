// TC-tech-observability-02 implements AC-10's log faces: one task's events are
// traceable by its task id from claim to deletion, and no log line carries a
// credential. The chain half covers every path that writes the terminal line
// (forwarded report and server-already-terminal settlement); the no-token half
// drives the production logger with the dominant "err", err call shape, whose
// value is an error rather than a string.
package acceptance

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

// observability02TerminalKeys are the fields every task-face terminal line
// must carry. Checking presence (not just value) is the point: an absent key
// reads as "unknown field" to a log query, while an empty value says the
// identity is empty.
var observability02TerminalKeys = []string{"task_id", "job_name", "daemon_id", "result"}

// TestObservability02ForwardedTerminalCarriesDaemonIdentity drives a task
// through claim → register → start → terminal report and reads the line the
// forward path wrote: the daemon identity is the entry's own fm-<task_id>,
// the same value its claim, delivery and registration lines carry.
func TestObservability02ForwardedTerminalCarriesDaemonIdentity(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	if got := claimTask(t, h, "task-1", token); len(got) == 0 {
		t.Fatal("S6: empty claim answer")
	}
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(`{"exit_code":0}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	if e := waitTerminal(t, h, "task-1"); e.Result != registry.ResultCompleted {
		t.Fatalf("verdict = %s/%s, want completed", e.State, e.Result)
	}

	events := h.events("task.terminal")
	if len(events) != 1 {
		t.Fatalf("task.terminal events = %d, want 1", len(events))
	}
	event := events[0]
	assertEventKeys(t, event, observability02TerminalKeys)
	if got := event["daemon_id"]; got != "fm-task-1" {
		t.Errorf("daemon_id = %v, want the entry's identity fm-task-1", got)
	}
	if got := event["job_name"]; got != "fm-task-1" {
		t.Errorf("job_name = %v, want fm-task-1", got)
	}
}

// TestObservability02SettledTerminalKeepsTheDaemonKey drives the settlement
// path: the server already holds the task as terminal, so no report crosses
// the wire and the scheduler writes the line itself. The entry here carries
// no identity (a rebuilt entry whose Job is gone), and the key must still be
// present with the empty value — absent and empty are different forms.
func TestObservability02SettledTerminalKeepsTheDaemonKey(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.reg.Put(registry.TaskEntry{
		TaskID: "task-2", JobName: "fm-task-2", State: registry.StateJobCreated,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := h.sched.SettleTerminal(t.Context(), "task-2", string(registry.ResultCancelled)); err != nil {
		t.Fatalf("SettleTerminal: %v", err)
	}
	if e, ok := h.reg.Get("task-2"); !ok || !e.IsTerminal() || e.Result != registry.ResultCancelled {
		t.Fatalf("entry = %+v, %v; want terminal/cancelled", e, ok)
	}

	events := h.events("task.terminal")
	if len(events) != 1 {
		t.Fatalf("task.terminal events = %d, want 1", len(events))
	}
	event := events[0]
	assertEventKeys(t, event, observability02TerminalKeys)
	if got, ok := event["daemon_id"]; !ok || got != "" {
		t.Errorf("daemon_id = %v (present %v), want the empty string present", got, ok)
	}
}

// assertEventKeys checks that every required field is present in the event.
func assertEventKeys(t *testing.T, event map[string]any, keys []string) {
	t.Helper()
	for _, key := range keys {
		if _, ok := event[key]; !ok {
			t.Errorf("event %v: missing required field %q", event, key)
		}
	}
}

// TestObservability02ErrorAttrCarriesNoToken covers the no-token half of the
// TC: the production logger is driven with an error value (KindAny, not a
// string) and a struct value, each carrying a token. Neither may reach the
// line, and the error field stays the rendered text with the token replaced.
func TestObservability02ErrorAttrCarriesNoToken(t *testing.T) {
	var buf bytes.Buffer
	log := observability.NewLogger(&buf, slog.LevelInfo, "proxy")
	log.Error("forward.failed",
		"err", errors.New("upstream rejected Bearer fmj_errorleak12345"),
		"cause", struct{ Detail string }{Detail: "peer echoed mat_structleak1234"})

	out := buf.String()
	for _, token := range []string{"fmj_errorleak12345", "mat_structleak1234"} {
		if strings.Contains(out, token) {
			t.Fatalf("token %q leaked into the log line: %q", token, out)
		}
	}
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not JSON: %v (%q)", err, out)
	}
	errText, ok := line["err"].(string)
	if !ok || !strings.Contains(errText, "[redacted]") {
		t.Fatalf("err field = %v, want the redacted error text", line["err"])
	}
	cause, ok := line["cause"].(map[string]any)
	if !ok || cause["Detail"] != "peer echoed [redacted]" {
		t.Fatalf("cause = %v, want the JSON object with the token replaced", line["cause"])
	}
}
