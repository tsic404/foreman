// TC-tech-observability-02 implements AC-10's observation faces: one task's
// events are traceable by its task id from claim to deletion, no log line
// carries a credential, and the image audit fields report the deployment
// reference verbatim (reference-template window). The chain half covers every
// path that writes the terminal line (forwarded report, server-already-terminal
// settlement and the restart replay); the no-token half drives the production
// logger with the dominant "err", err call form, whose value is an error
// rather than a string; the image half reads both fields ADR-012 names.
package acceptance

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

// observability02TerminalKeys are the fields every task-face terminal line
// must carry. Checking presence (not just value) is the point: an absent key
// reads as "unknown field" to a log query, while an empty value says the
// identity is empty.
var observability02TerminalKeys = []string{"task_id", "job_name", "daemon_id", "workspace_id", "result"}

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

// TestObservability02RestartReplayCarriesTheTerminalLine (AC-10,
// TC-tech-observability-02): after a restart the rebuilt entry carries no
// verdict, so replaying its queued report is the task's terminal step. That
// step must write its own line with the identity the rebuild restored —
// without it the chain ends before 终态 on exactly the path a restart creates.
func TestObservability02RestartReplayCarriesTheTerminalLine(t *testing.T) {
	h := newDeliveryHarness(t)
	h.useRealBuilder(t)
	queueTerminalReport(t, h, []byte(`{"result":"ok"}`))

	// The pre-restart process wrote its own line before its forward failed;
	// this scenario judges the line the replay adds.
	before := len(h.events("task.terminal"))

	reg, pending, sched := restartOverCluster(t, h)
	if err := sched.Rebuild(t.Context()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got := pending.Len(); got != 1 {
		t.Fatalf("rebuilt queue depth = %d, want the one carried report", got)
	}

	h.server.setForwardCode(0)
	if remaining, err := sched.DrainPendingReports(t.Context()); err != nil || remaining != 0 {
		t.Fatalf("DrainPendingReports = %d, %v; want 0, nil", remaining, err)
	}

	events := h.events("task.terminal")
	if len(events) != before+1 {
		t.Fatalf("task.terminal events after the replay = %d, want %d", len(events), before+1)
	}
	event := events[len(events)-1]
	assertEventKeys(t, event, observability02TerminalKeys)
	for key, want := range map[string]string{
		"task_id":      "task-1",
		"job_name":     "fm-task-1",
		"daemon_id":    "fm-task-1",
		"workspace_id": "ws-1",
		"result":       string(registry.ResultCompleted),
	} {
		if got := event[key]; got != want {
			t.Errorf("%s = %v, want %q", key, got, want)
		}
	}
	if e, ok := reg.Settled("task-1"); !ok || e.Result != registry.ResultCompleted {
		t.Fatalf("settled record = %+v, %v; want the replayed completed verdict", e, ok)
	}

	// A second drain has nothing left to replay: the verdict is observed once.
	if remaining, err := sched.DrainPendingReports(t.Context()); err != nil || remaining != 0 {
		t.Fatalf("second DrainPendingReports = %d, %v; want 0, nil", remaining, err)
	}
	if got := len(h.events("task.terminal")); got != before+1 {
		t.Errorf("task.terminal events after a second drain = %d, want %d", got, before+1)
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

// TestObservability02ImageAuditMatchesTheDeploymentReference covers the image
// audit half of the TC (AC-10, ADR-012): in the reference-template window the
// template writes <job-image>, so both audit fields — job.created's image and
// /foreman/version's job_image — must report the deployment's reference
// verbatim. The value differs from the template's own text and from the
// suite's default, so a constant answer cannot pass.
func TestObservability02ImageAuditMatchesTheDeploymentReference(t *testing.T) {
	const ref = "registry.example.com/tsic404/foreman-job:audit-2026-10-24"

	h := newDeliveryHarness(t)
	h.jobImage = ref
	h.useRealBuilder(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	created := h.events("job.created")
	if len(created) != 1 {
		t.Fatalf("job.created events = %d, want 1", len(created))
	}
	if got := created[0]["image"]; got != ref {
		t.Errorf("job.created image = %v, want the deployment reference %q verbatim", got, ref)
	}

	// The field audits what the Job will run: the agent container carries the
	// same string, not the template's placeholder and not an assembled digest.
	job := h.createdJob(t, "fm-task-1")
	var agentImage string
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Name == "agent" {
			agentImage = container.Image
			break
		}
	}
	if agentImage != ref {
		t.Errorf("agent container image = %q, want %q verbatim", agentImage, ref)
	}

	// ADR-012 names both fields as the AC-10 image audit face: the version
	// endpoint reports the same deployment reference.
	ops := httptest.NewServer(observability.NewOpsHandler(observability.NewMetrics(), h.reg, nil,
		observability.OpsConfig{JobImage: ref}))
	t.Cleanup(ops.Close)
	code, body := httpGet(t, ops.URL+"/foreman/version")
	if code != http.StatusOK {
		t.Fatalf("GET /foreman/version = %d (%s), want 200", code, body)
	}
	var version map[string]string
	if err := json.Unmarshal(body, &version); err != nil {
		t.Fatalf("decode version body: %v (%s)", err, body)
	}
	if got := version["job_image"]; got != ref {
		t.Errorf("version job_image = %q, want the deployment reference %q verbatim", got, ref)
	}
}
