// TC-tech-lifecycle-02 (AC-07): the terminal report body reaches the server
// field for field and a repeated report never writes twice.
//
// The delivery body carries AC-07's three assertions: the constant-key gate,
// the known-value comparison (job runtime identity, configured workspace,
// non-empty issue id), and the conditional keys (present ⇒ correct shape;
// absent ⇒ not judged). No top-level prompt or model key the payload did not
// carry may appear.
//
// The terminal report body is opaque to Foreman: every key, value and JSON
// type the daemon sent arrives unchanged. A repeated report takes the decided
// body — the server applies one terminal write — and a replay after the
// credential reclaim is answered 200 without touching the upstream at all.
package acceptance

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// lifecycle02Payload is the claim body: constant keys, the known-value inputs,
// conditional keys with their shapes, and a key this build does not model.
const lifecycle02Payload = `{
	"id": "task-1",
	"agent_id": "agent-1",
	"workspace_id": "ws-1",
	"runtime_id": "rid-zz",
	"issue_id": "issue-1",
	"issue_identifier": "TSI-1",
	"prior_session_id": "sess-1",
	"project": {"id": "p-1", "name": "foreman"},
	"agent": {"model": "k3", "name": "omp"},
	"repos": [{"url": "https://example.invalid/r", "ref": "main"}],
	"x_future_field": {"nested": [1, 2, {"deep": true}], "null": null}
}`

// lifecycle02Decode splits a JSON object into its raw top-level fields.
func lifecycle02Decode(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode object: %v (%s)", err, raw)
	}
	return fields
}

// lifecycle02JSONKind names the JSON type of one raw value.
func lifecycle02JSONKind(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode value: %v (%s)", err, raw)
	}
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return "unknown"
}

// lifecycle02RegisterTask performs S4 and returns the job runtime identity the
// answer names — the value the task projection exposes.
func lifecycle02RegisterTask(t *testing.T, h *harness, taskID, token string) string {
	t.Helper()
	code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-`+taskID+`",
		"runtimes": [{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}]
	}`))
	if code != http.StatusOK {
		t.Fatalf("S4 = %d (%s), want 200", code, body)
	}
	var answer struct {
		Runtimes []struct {
			ID string `json:"id"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(body, &answer); err != nil || len(answer.Runtimes) != 1 {
		t.Fatalf("S4 answer = %s (%v), want one runtime", body, err)
	}
	return answer.Runtimes[0].ID
}

// lifecycle02Deliver performs one S6 claim and returns the single delivered
// task body.
func lifecycle02Deliver(t *testing.T, h *harness, taskID, token string) []byte {
	t.Helper()
	raw := claimTask(t, h, taskID, token)
	var answer struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil || len(answer.Tasks) != 1 {
		t.Fatalf("S6 answer = %s (%v), want one task", raw, err)
	}
	return answer.Tasks[0]
}

// lifecycle02ClaimAndDeliver builds one delivered task: claim, Job Token,
// daemon registration, and the S6 hand-over.
func lifecycle02ClaimAndDeliver(t *testing.T, h *harness, payload []byte, taskID string) (string, string) {
	t.Helper()
	if err := h.sched.OnClaim(t.Context(), json.RawMessage(payload)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskID, err)
	}
	token := issueJobToken(t, h, taskID)
	runtimeID := lifecycle02RegisterTask(t, h, taskID, token)
	lifecycle02Deliver(t, h, taskID, token)
	return token, runtimeID
}

// lifecycle02TerminalBodies returns every terminal body the scheduler
// forwarded for the task, across the three terminal endpoints.
func lifecycle02TerminalBodies(h *harness, taskID string) [][]byte {
	var bodies [][]byte
	for _, ep := range []scheduler.Endpoint{scheduler.EPComplete, scheduler.EPFail, scheduler.EPCancelAck} {
		for _, forward := range h.server.forwarded(ep) {
			if forward.Path == taskID {
				bodies = append(bodies, forward.Body)
			}
		}
	}
	return bodies
}

func TestLifecycle02DeliveryBodyCarriesAC07Assertions(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), json.RawMessage(lifecycle02Payload)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")
	runtimeID := lifecycle02RegisterTask(t, h, "task-1", token)
	delivered := lifecycle02Deliver(t, h, "task-1", token)

	want := lifecycle02Decode(t, []byte(lifecycle02Payload))
	got := lifecycle02Decode(t, delivered)

	// ① Constant-key gate: the payload's key set survives both ways, and every
	// key keeps its JSON type.
	if len(got) != len(want) {
		t.Errorf("delivered top-level keys = %d, want the payload's %d", len(got), len(want))
	}
	for key, wantRaw := range want {
		gotRaw, ok := got[key]
		if !ok {
			t.Errorf("delivered body lost top-level key %q", key)
			continue
		}
		if wantKind, gotKind := lifecycle02JSONKind(t, wantRaw), lifecycle02JSONKind(t, gotRaw); wantKind != gotKind {
			t.Errorf("%q type = %s, want %s", key, gotKind, wantKind)
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("delivered body invented top-level key %q", key)
		}
	}

	// ④ No top-level prompt or model key: the payload carries neither, and the
	// delivery must not hoist the agent object's model or synthesise one.
	for _, absent := range []string{"prompt", "model", "system_prompt", "thinking"} {
		if _, ok := got[absent]; ok {
			t.Errorf("delivered body carries top-level %q, which the payload does not", absent)
		}
	}

	// ② Known-value comparison: the job runtime identity, the configured
	// workspace, and the issue trigger's id. The claim's runtime_id and the
	// Job's runtime identity are two different faces: a delivered body that
	// kept the claim's value would make the rewrite indistinguishable from a
	// passthrough, so a fixture that collapses them fails here.
	var claimedRuntime string
	if err := json.Unmarshal(want["runtime_id"], &claimedRuntime); err != nil {
		t.Fatalf("payload runtime_id: %v", err)
	}
	if claimedRuntime == runtimeID {
		t.Fatalf("fixture: the job runtime identity %q equals the claim's runtime_id; the rewrite and a passthrough would be indistinguishable", runtimeID)
	}
	var deliveredRuntime string
	if err := json.Unmarshal(got["runtime_id"], &deliveredRuntime); err != nil {
		t.Fatalf("runtime_id: %v", err)
	}
	if deliveredRuntime != runtimeID {
		t.Errorf("runtime_id = %q, want the registered job runtime identity %q", deliveredRuntime, runtimeID)
	}
	if deliveredRuntime == claimedRuntime {
		t.Errorf("runtime_id = %q: the delivered body kept the claim's runtime id instead of the job runtime identity", deliveredRuntime)
	}
	if entry, ok := h.reg.Get("task-1"); !ok || entry.JobRuntimeID != deliveredRuntime {
		t.Errorf("runtime_id = %q, want the entry's job runtime identity", deliveredRuntime)
	}
	var workspaceID string
	if err := json.Unmarshal(got["workspace_id"], &workspaceID); err != nil || workspaceID != h.cfg.WorkspaceID {
		t.Errorf("workspace_id = %q (%v), want the configured %q", workspaceID, err, h.cfg.WorkspaceID)
	}
	var issueID string
	if err := json.Unmarshal(got["issue_id"], &issueID); err != nil || issueID == "" {
		t.Errorf("issue_id = %q (%v), want the non-empty issue trigger value", issueID, err)
	}

	// ③ Conditional keys, present half: presence implies the correct shape.
	var priorSession string
	if err := json.Unmarshal(got["prior_session_id"], &priorSession); err != nil || priorSession != "sess-1" {
		t.Errorf("prior_session_id = %q (%v), want sess-1", priorSession, err)
	}
	var project struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(got["project"], &project); err != nil || project.ID != "p-1" || project.Name != "foreman" {
		t.Errorf("project = %+v (%v), want the structured object", project, err)
	}
	var agent struct {
		Model string `json:"model"`
		Name  string `json:"name"`
	}
	if err := json.Unmarshal(got["agent"], &agent); err != nil || agent.Model != "k3" || agent.Name != "omp" {
		t.Errorf("agent = %+v (%v), want model k3 / name omp", agent, err)
	}
	var future map[string]json.RawMessage
	if err := json.Unmarshal(got["x_future_field"], &future); err != nil || len(future) != 2 {
		t.Errorf("x_future_field = %s (%v), want the unmodelled object preserved", got["x_future_field"], err)
	}
	// Everything except runtime_id (the one rewritten field) travels with its
	// value unchanged.
	for key, wantRaw := range want {
		if key == "runtime_id" {
			continue
		}
		var wantValue, gotValue any
		if err := json.Unmarshal(wantRaw, &wantValue); err != nil {
			t.Fatalf("decode payload %q: %v", key, err)
		}
		if err := json.Unmarshal(got[key], &gotValue); err != nil {
			t.Fatalf("decode delivered %q: %v", key, err)
		}
		if !reflect.DeepEqual(wantValue, gotValue) {
			t.Errorf("%q = %s, want the payload's %s", key, got[key], wantRaw)
		}
	}

	// ③ Conditional keys, absent half: a payload without them is delivered,
	// and the delivery does not invent them.
	const minimal = `{"id":"task-2","agent_id":"agent-2","workspace_id":"ws-1","runtime_id":"rid-zz"}`
	if err := h.sched.OnClaim(t.Context(), json.RawMessage(minimal)); err != nil {
		t.Fatalf("OnClaim(task-2): %v", err)
	}
	token2 := issueJobToken(t, h, "task-2")
	lifecycle02RegisterTask(t, h, "task-2", token2)
	delivered2 := lifecycle02Decode(t, lifecycle02Deliver(t, h, "task-2", token2))
	for _, conditional := range []string{"issue_id", "issue_identifier", "prior_session_id", "project", "agent", "repos"} {
		if _, ok := delivered2[conditional]; ok {
			t.Errorf("delivered body invented conditional key %q for a payload without it", conditional)
		}
	}
	if len(delivered2) != 4 {
		t.Errorf("delivered key set = %v, want the payload's four keys", delivered2)
	}
}

func TestLifecycle02TerminalReportBodyArrivesFieldForField(t *testing.T) {
	h := newDeliveryHarness(t)
	token, _ := lifecycle02ClaimAndDeliver(t, h, []byte(lifecycle02Payload), "task-1")
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	report := []byte(`{
		"exit_code": 0,
		"result": "completed",
		"session_id": "sess-42",
		"usage": {"input_tokens": 12, "output_tokens": 34},
		"outputs": {"summary": "done", "artifacts": ["a.txt"]},
		"x_future_field": {"nested": true, "null": null, "depth": 1.5},
		"unicode": "任务完成 ✓"
	}`)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, report); code != http.StatusOK {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}

	forwards := h.server.forwarded(scheduler.EPComplete)
	if len(forwards) != 1 {
		t.Fatalf("S13 forwards = %d, want one", len(forwards))
	}
	if !bytes.Equal(forwards[0].Body, report) {
		t.Fatalf("forwarded body = %s, want the daemon's bytes %s", forwards[0].Body, report)
	}

	// Field for field: the same key set, with each key's JSON type and value.
	want := lifecycle02Decode(t, report)
	got := lifecycle02Decode(t, forwards[0].Body)
	if len(got) != len(want) {
		t.Errorf("forwarded top-level keys = %d, want the daemon's %d", len(got), len(want))
	}
	for key, wantRaw := range want {
		gotRaw, ok := got[key]
		if !ok {
			t.Errorf("forwarded body lost top-level key %q", key)
			continue
		}
		if wantKind, gotKind := lifecycle02JSONKind(t, wantRaw), lifecycle02JSONKind(t, gotRaw); wantKind != gotKind {
			t.Errorf("%q type = %s, want %s", key, gotKind, wantKind)
		}
		var wantValue, gotValue any
		if err := json.Unmarshal(wantRaw, &wantValue); err != nil {
			t.Fatalf("decode report %q: %v", key, err)
		}
		if err := json.Unmarshal(gotRaw, &gotValue); err != nil {
			t.Fatalf("decode forwarded %q: %v", key, err)
		}
		if !reflect.DeepEqual(wantValue, gotValue) {
			t.Errorf("%q = %s, want the daemon's %s", key, gotRaw, wantRaw)
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("forwarded body invented top-level key %q", key)
		}
	}
	// The report is opaque: Foreman must not add the keys the payload face
	// forbids either.
	for _, absent := range []string{"prompt", "model", "system_prompt", "thinking"} {
		if _, ok := got[absent]; ok {
			t.Errorf("forwarded body carries top-level %q, which the daemon did not send", absent)
		}
	}

	// The first terminal body is the instance's decided body.
	if decided, ok := h.reg.DecidedReport("task-1"); !ok || !bytes.Equal(decided, report) {
		t.Errorf("decided body = %s (%v), want the daemon's report", decided, ok)
	}
	if entry := waitTerminal(t, h, "task-1"); entry.Result != registry.ResultCompleted {
		t.Fatalf("verdict = %s/%s, want completed", entry.State, entry.Result)
	}
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job was not deleted at the verdict")
	}
}

func TestLifecycle02RepeatedReportWritesOnce(t *testing.T) {
	h := newDeliveryHarness(t)
	token, _ := lifecycle02ClaimAndDeliver(t, h, []byte(lifecycle02Payload), "task-1")
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != http.StatusOK {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}

	report := []byte(`{"exit_code":0,"session_id":"sess-42","result":"completed"}`)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, report); code != http.StatusOK {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}
	if entry := waitTerminal(t, h, "task-1"); entry.Result != registry.ResultCompleted {
		t.Fatalf("verdict = %s/%s, want completed", entry.State, entry.Result)
	}
	if writes := h.server.appliedTerminalWrites("task-1"); len(writes) != 1 || !bytes.Equal(writes[0].Body, report) {
		t.Fatalf("applied terminal writes = %+v, want the daemon's one report", writes)
	}
	if n := h.metrics.taskResultCount("completed"); n != 1 {
		t.Fatalf("terminal metric count = %d, want 1", n)
	}

	// A daemon retry inside the drain window is a live report: it is forwarded
	// again, with the decided body. The server's apply rule makes it the same
	// write, so the terminal record is not written a second time. The retry
	// carries different bytes, so a forward that echoed the daemon's retry is
	// distinguishable from the decided body.
	retry := []byte(`{"exit_code":1,"session_id":"sess-retry","result":"completed","retry":true}`)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, retry); code != http.StatusOK {
		t.Fatalf("S13 retry = %d (%s), want the idempotent 200", code, body)
	}
	forwards := h.server.forwarded(scheduler.EPComplete)
	if len(forwards) != 2 {
		t.Fatalf("S13 forwards after the retry = %d, want 2", len(forwards))
	}
	if !bytes.Equal(forwards[1].Body, report) {
		t.Errorf("retry body = %s, want the decided body %s", forwards[1].Body, report)
	}
	if writes := h.server.appliedTerminalWrites("task-1"); len(writes) != 1 {
		t.Errorf("applied terminal writes after the retry = %d, want 1", len(writes))
	}
	if n := h.metrics.taskResultCount("completed"); n != 1 {
		t.Errorf("terminal metric count after the retry = %d, want 1", n)
	}
	if entry, ok := h.reg.Get("task-1"); !ok || entry.Result != registry.ResultCompleted {
		t.Errorf("verdict after the retry = %+v, want completed", entry)
	}

	// A conflicting terminal callback cannot rewrite the verdict either: the
	// decided body travels on every terminal endpoint, never the caller's. The
	// count is asserted first: an empty loop would otherwise pass vacuously if
	// the conflicting callback were dropped instead of forwarded.
	conflict := []byte(`{"error":"agent died","failure_reason":"agent_error"}`)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/fail", token, conflict); code != http.StatusOK {
		t.Fatalf("S14 after the verdict = %d (%s), want 200", code, body)
	}
	fails := h.server.forwarded(scheduler.EPFail)
	if len(fails) != 1 {
		t.Fatalf("S14 forwards = %d, want one", len(fails))
	}
	for _, forward := range fails {
		if !bytes.Equal(forward.Body, report) {
			t.Errorf("fail-endpoint forward = %s, want the decided body %s", forward.Body, report)
		}
	}
	if n := h.metrics.taskResultCount("failed"); n != 0 {
		t.Errorf("terminal metric counted %d failures, want 0", n)
	}
	if writes := h.server.appliedTerminalWrites("task-1"); len(writes) != 1 {
		t.Errorf("applied terminal writes after the conflicting report = %d, want 1", len(writes))
	}
	if entry, ok := h.reg.Get("task-1"); !ok || entry.Result != registry.ResultCompleted {
		t.Errorf("verdict after the conflicting report = %+v, want completed", entry)
	}
	if bodies := lifecycle02TerminalBodies(h, "task-1"); len(bodies) == 0 {
		t.Fatal("no terminal body reached the upstream")
	} else {
		for _, body := range bodies {
			if !bytes.Equal(body, report) {
				t.Errorf("terminal body = %s, want the one decided body %s", body, report)
			}
		}
	}

	// The credential reclaim closes the drain window. A late replay is the
	// done index's idempotent 200: no upstream call at all, so no second write.
	if err := h.sched.OnJobTerminated(t.Context(), "fm-task-1",
		scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := h.reg.Get("task-1"); ok {
		t.Fatal("the entry survived the credential reclaim")
	}
	before := len(lifecycle02TerminalBodies(h, "task-1"))
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, report); code != http.StatusOK {
		t.Fatalf("late replay = %d (%s), want the idempotent 200", code, body)
	}
	if after := len(lifecycle02TerminalBodies(h, "task-1")); after != before {
		t.Errorf("late replay reached the upstream: terminal forwards %d → %d", before, after)
	}
	if writes := h.server.appliedTerminalWrites("task-1"); len(writes) != 1 {
		t.Errorf("applied terminal writes after the late replay = %d, want 1", len(writes))
	}
}
