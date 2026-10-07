package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// fakeJobs is a JobClient recording object lifecycle.
type fakeJobs struct {
	mu            sync.Mutex
	createdJobs   []string
	createdSecret []string
	deletedJobs   []string
	deletedSecret []string
}

func (f *fakeJobs) CreateJob(_ context.Context, job *batchv1.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createdJobs = append(f.createdJobs, job.Name)
	return nil
}

func (f *fakeJobs) DeleteJob(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedJobs = append(f.deletedJobs, name)
	return nil
}

func (f *fakeJobs) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createdSecret = append(f.createdSecret, secret.Name)
	return nil
}

func (f *fakeJobs) DeleteSecret(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedSecret = append(f.deletedSecret, name)
	return nil
}

func (f *fakeJobs) ListJobs(context.Context) ([]batchv1.Job, error) { return nil, nil }
func (f *fakeJobs) WatchJobs(context.Context, scheduler.JobEventHandler) error {
	return nil
}

func (f *fakeJobs) deleted(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.deletedJobs {
		if n == name {
			return true
		}
	}
	return false
}

// fakeBuilder renders a bare Job+Secret pair.
type fakeBuilder struct{}

func (fakeBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID + "-cred"}}
	return job, secret, nil
}

var testTokenKey = bytes.Repeat([]byte{0x42}, 32)

// testRig wires the full fake-server stack against a stub upstream:
// FakeServer → scheduler → Client → stub.
type testRig struct {
	stub    *stubUpstream
	client  *Client
	reg     *registry.Registry
	sched   *scheduler.Scheduler
	issuer  *auth.Issuer
	jobs    *fakeJobs
	metrics *fakeMetrics
	server  *httptest.Server
	pending *recovery.PendingReports
	handler http.Handler
}

func newTestRig(t *testing.T) *testRig {
	t.Helper()
	upstream, stub := newStubUpstream(t)
	stubRegister(stub, "rid-real")

	rig := &testRig{stub: stub}
	stub.on("GET", "/api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, []map[string]string{{"id": "ws-1", "name": "ws-one"}})
	})
	stub.on("POST", "/api/tokens/current/renew", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]any{"renewed": true})
	})
	rig.client = NewClient(testConfig(upstream.URL), "mdt_foreman", WithTerminalRetryWaits([]time.Duration{0, 0, 0, 0, 0, 0}))
	if err := rig.client.Preflight(t.Context()); err != nil {
		t.Fatalf("Preflight: %v", err)
	}

	rig.reg = registry.New(time.Now)
	rig.jobs = &fakeJobs{}
	pending, err := recovery.NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	rig.pending = pending
	sched, err := scheduler.New(
		scheduler.Config{JobNamespace: "test-ns", MaxInflightJobs: 10, ClaimBatchMax: 32, JobBootTimeout: 300 * time.Second},
		rig.reg, rig.jobs, fakeBuilder{}, rig.client, nil,
		scheduler.WithPendingReports(pending),
	)
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	rig.sched = sched

	issuer, err := JobTokenIssuer(testTokenKey, rig.reg)
	if err != nil {
		t.Fatalf("JobTokenIssuer: %v", err)
	}
	rig.issuer = issuer
	rig.metrics = &fakeMetrics{}
	rig.handler = NewFakeServer(issuer, rig.reg, rig.sched, rig.client, nil, rig.metrics)
	rig.server = httptest.NewServer(rig.handler)
	t.Cleanup(rig.server.Close)
	return rig
}

// claimTask seeds one claimed task through the scheduler (job created).
func (r *testRig) claimTask(t *testing.T, taskID string) {
	t.Helper()
	payload := json.RawMessage(fmt.Sprintf(`{
		"id": %q, "agent_id": "agent-1", "issue_id": "issue-1",
		"issue_identifier": "TSI-1", "workspace_id": "ws-1",
		"runtime_id": "rid-real", "auth_token": "mat_tasktoken",
		"agent": {"model": "k3"}, "trigger_kind": "assign"
	}`, taskID))
	if err := r.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
}

// token issues a Job Token for the job.
func (r *testRig) token(t *testing.T, taskID string) string {
	t.Helper()
	tok, err := r.issuer.Issue("fm-"+taskID, taskID, "ws-1", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return tok
}

// call performs one J→F request and returns status + body.
func (r *testRig) call(t *testing.T, method, path, token string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, r.server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}

// registerDaemon drives S4 for the task's daemon and returns the job
// runtime id from the response.
func (r *testRig) registerDaemon(t *testing.T, taskID, token string) string {
	t.Helper()
	code, body := r.call(t, "POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-`+taskID+`",
		"runtimes": [{"name": "foreman-job", "type": "omp"}]
	}`))
	if code != 200 {
		t.Fatalf("S4 register = %d: %s", code, body)
	}
	var resp struct {
		Runtimes []struct {
			ID       string `json:"id"`
			Provider string `json:"provider"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("register response: %v", err)
	}
	if len(resp.Runtimes) != 1 {
		t.Fatalf("register must return exactly one runtime, got %d", len(resp.Runtimes))
	}
	return resp.Runtimes[0].ID
}

func TestRoutingRules(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-1")

	// Rule 3: no credential → 401, never forwarded.
	code, body := rig.call(t, "GET", "/api/daemon/workspaces", "", nil)
	if code != 401 {
		t.Errorf("no credential: %d, want 401", code)
	}
	var errBody map[string]string
	if err := json.Unmarshal(body, &errBody); err != nil || errBody["error"] == "" {
		t.Errorf("error body = %s", body)
	}

	// Rule 2: a non-fmj_ credential passes through verbatim (S29).
	rig.stub.on("GET", "/api/issues/42", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"id": "42"})
	})
	code, body = rig.call(t, "GET", "/api/issues/42", "mat_tasktoken", nil)
	if code != 200 || string(body) != `{"id":"42"}` {
		t.Errorf("passthrough: %d %s", code, body)
	}
	calls := rig.stub.calls("GET", "/api/issues/42")
	if len(calls) != 1 || calls[0].Header.Get("Authorization") != "Bearer mat_tasktoken" {
		t.Fatalf("passthrough must keep the caller credential verbatim: %+v", calls)
	}

	// Rule 1: an fmj_ token on an unknown local path → 404, not passthrough.
	code, _ = rig.call(t, "GET", "/api/daemon/unknown", rig.token(t, "t-1"), nil)
	if code != 404 {
		t.Errorf("unknown local path: %d, want 404", code)
	}
	if n := len(rig.stub.calls("GET", "/api/daemon/unknown")); n != 0 {
		t.Errorf("an fmj_ request must never pass through; upstream saw %d calls", n)
	}
}

func TestFullLifecycle(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-life")
	tok := rig.token(t, "t-life")

	// S1: single workspace.
	code, body := rig.call(t, "GET", "/api/daemon/workspaces", tok, nil)
	if code != 200 {
		t.Fatalf("S1 = %d", code)
	}
	var wss []map[string]string
	_ = json.Unmarshal(body, &wss)
	if len(wss) != 1 || wss[0]["id"] != "ws-1" || wss[0]["name"] != "ws-one" {
		t.Fatalf("S1 = %s", body)
	}

	// S4: register.
	jobRID := rig.registerDaemon(t, "t-life", tok)
	e, ok := rig.reg.Get("t-life")
	if !ok || e.State != registry.StateDaemonRegistered {
		t.Fatalf("state = %v", e.State)
	}

	// S6: first claim delivers the task; only runtime_id is rewritten.
	rig.stub.on("POST", "/api/daemon/runtimes/"+jobRID+"/tasks/t-life/prepare-lease", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})
	claimBody := []byte(`{"daemon_id":"fm-t-life","runtime_ids":["` + jobRID + `"],"max_tasks":1}`)
	code, body = rig.call(t, "POST", "/api/daemon/tasks/claim", tok, claimBody)
	if code != 200 {
		t.Fatalf("S6 = %d: %s", code, body)
	}
	var claimResp struct {
		Tasks []map[string]json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(body, &claimResp); err != nil {
		t.Fatalf("S6 body: %v", err)
	}
	if len(claimResp.Tasks) != 1 {
		t.Fatalf("S6 must deliver exactly one task, got %d", len(claimResp.Tasks))
	}
	task := claimResp.Tasks[0]
	var deliveredRuntime string
	_ = json.Unmarshal(task["runtime_id"], &deliveredRuntime)
	if deliveredRuntime != jobRID {
		t.Errorf("runtime_id = %q, want job runtime %q", deliveredRuntime, jobRID)
	}
	// Every other field is verbatim (AC-07).
	var authToken string
	_ = json.Unmarshal(task["auth_token"], &authToken)
	if authToken != "mat_tasktoken" {
		t.Errorf("auth_token = %q", authToken)
	}
	if _, ok := task["agent"]; !ok {
		t.Error("agent field lost in delivery")
	}
	if _, ok := task["trigger_kind"]; !ok {
		t.Error("trigger_kind lost in delivery")
	}

	// S6 again: empty (at-most-once delivery).
	code, body = rig.call(t, "POST", "/api/daemon/tasks/claim", tok, claimBody)
	if code != 200 || !bytes.Contains(body, []byte(`"tasks":[]`)) {
		t.Fatalf("second S6 = %d %s, want empty tasks", code, body)
	}

	// S8: start forwards C5 and marks running.
	rig.stub.on("POST", "/api/daemon/tasks/t-life/start", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"id": "t-life", "status": "running"})
	})
	code, _ = rig.call(t, "POST", "/api/daemon/tasks/t-life/start", tok, []byte(`{}`))
	if code != 200 {
		t.Fatalf("S8 = %d", code)
	}
	e, _ = rig.reg.Get("t-life")
	if e.State != registry.StateRunning || e.StartedAt.IsZero() {
		t.Fatalf("state after start = %v", e.State)
	}
	if got := len(rig.stub.calls("POST", "/api/daemon/tasks/t-life/start")); got != 1 {
		t.Fatalf("C5 calls = %d", got)
	}

	// S10/S11/S12 forward progress/messages/usage verbatim.
	for _, ep := range []string{"progress", "messages", "usage"} {
		rig.stub.on("POST", "/api/daemon/tasks/t-life/"+ep, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			writeJSONStub(w, 200, map[string]string{"status": "ok"})
		})
		payload := []byte(`{"marker":"` + ep + `"}`)
		code, _ = rig.call(t, "POST", "/api/daemon/tasks/t-life/"+ep, tok, payload)
		if code != 200 {
			t.Fatalf("S-%s = %d", ep, code)
		}
		upstream := rig.stub.calls("POST", "/api/daemon/tasks/t-life/"+ep)
		if len(upstream) != 1 || !bytes.Equal(upstream[0].Body, payload) {
			t.Fatalf("%s body not verbatim: %+v", ep, upstream)
		}
		if upstream[0].Header.Get("X-Client-Capabilities") != ClientCapabilities {
			t.Errorf("%s missing capability header", ep)
		}
	}

	// S13: complete forwards C9, then Job/Secret are deleted.
	rig.stub.on("POST", "/api/daemon/tasks/t-life/complete", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"id": "t-life", "status": "completed"})
	})
	completeBody := []byte(`{"result":"done","outputs":{"x":1}}`)
	code, body = rig.call(t, "POST", "/api/daemon/tasks/t-life/complete", tok, completeBody)
	if code != 200 {
		t.Fatalf("S13 = %d: %s", code, body)
	}
	if got := rig.stub.calls("POST", "/api/daemon/tasks/t-life/complete"); len(got) != 1 || !bytes.Equal(got[0].Body, completeBody) {
		t.Fatalf("C9 not verbatim: %+v", got)
	}
	if !rig.jobs.deleted("fm-t-life") {
		t.Error("Job not deleted after terminal report")
	}
	if _, ok := rig.reg.Get("t-life"); ok {
		t.Error("entry still live after terminal report")
	}
	if !rig.reg.IsDone("t-life") {
		t.Error("done index not updated")
	}

	// Late replay: 200 idempotent, no second C9 (proxy.md 迟到重放).
	code, _ = rig.call(t, "POST", "/api/daemon/tasks/t-life/complete", tok, completeBody)
	if code != 200 {
		t.Fatalf("late replay = %d, want 200", code)
	}
	if got := len(rig.stub.calls("POST", "/api/daemon/tasks/t-life/complete")); got != 1 {
		t.Fatalf("late replay re-forwarded: C9 calls = %d", got)
	}
	// A non-terminal endpoint on the settled task is rejected.
	code, _ = rig.call(t, "POST", "/api/daemon/tasks/t-life/progress", tok, []byte(`{}`))
	if code != 401 {
		t.Errorf("non-terminal endpoint after terminal = %d, want 401", code)
	}
}

func TestTerminalForwardFailureLandsInPendingReports(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-pend")
	tok := rig.token(t, "t-pend")
	rig.registerDaemon(t, "t-pend", tok)

	rig.stub.on("POST", "/api/daemon/tasks/t-pend/complete", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(503)
	})
	code, body := rig.call(t, "POST", "/api/daemon/tasks/t-pend/complete", tok, []byte(`{"result":"done"}`))
	if code != 502 {
		t.Fatalf("S13 with down upstream = %d, want 502: %s", code, body)
	}
	// The report landed in the durable queue (contract §4: never dropped).
	if got := rig.pending.Len(); got != 1 {
		t.Fatalf("pending reports = %d, want 1", got)
	}
	// The entry stays terminal and the Job still exists (cleanup only after
	// the report lands).
	if _, ok := rig.reg.Get("t-pend"); !ok {
		t.Error("terminal entry dropped before the report landed")
	}
	if rig.jobs.deleted("fm-t-pend") {
		t.Error("Job deleted before the terminal report landed")
	}

	// Recovery drains the queue once the upstream is healthy again.
	rig.stub.on("POST", "/api/daemon/tasks/t-pend/complete", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"id": "t-pend", "status": "completed"})
	})
	remaining, err := rig.pending.Drain(t.Context(), rig.client)
	if err != nil || remaining != 0 {
		t.Fatalf("Drain = %d, %v", remaining, err)
	}
	if got := rig.pending.Len(); got != 0 {
		t.Errorf("pending reports after drain = %d", got)
	}
}

func TestCrossJobScopeIsForbidden(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-a")
	rig.claimTask(t, "t-b")
	tokA := rig.token(t, "t-a")
	tokB := rig.token(t, "t-b")
	rig.registerDaemon(t, "t-a", tokA)
	rig.registerDaemon(t, "t-b", tokB)

	// B's token against A's task → 403.
	code, _ := rig.call(t, "POST", "/api/daemon/tasks/t-a/progress", tokB, []byte(`{}`))
	if code != 403 {
		t.Errorf("cross-job progress = %d, want 403", code)
	}
	// B's token claiming as A's daemon → 403.
	code, _ = rig.call(t, "POST", "/api/daemon/tasks/claim", tokB,
		[]byte(`{"daemon_id":"fm-t-a","runtime_ids":["x"],"max_tasks":1}`))
	if code != 403 {
		t.Errorf("cross-daemon claim = %d, want 403", code)
	}
}

func TestS5HeartbeatSemantics(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-hb")
	tok := rig.token(t, "t-hb")
	jobRID := rig.registerDaemon(t, "t-hb", tok)

	code, body := rig.call(t, "POST", "/api/daemon/heartbeat", tok,
		[]byte(`{"runtime_id":"`+jobRID+`","supports_batch_import":true}`))
	if code != 200 {
		t.Fatalf("S5 = %d: %s", code, body)
	}
	var resp map[string]any
	_ = json.Unmarshal(body, &resp)
	if resp["status"] != "ok" || resp["runtime_id"] != jobRID {
		t.Errorf("S5 body = %s", body)
	}
	for k := range resp {
		if len(k) >= 8 && k[:8] == "pending_" {
			t.Errorf("S5 must never carry pending_* fields: %s", body)
		}
	}
	rt, _ := rig.reg.RuntimeByID(jobRID)
	if rt.LastHeartbeatAt.IsZero() {
		t.Error("heartbeat clock not refreshed")
	}

	// Unknown runtime → 404 (scenario #12: daemon re-registers).
	code, _ = rig.call(t, "POST", "/api/daemon/heartbeat", tok, []byte(`{"runtime_id":"nope"}`))
	if code != 404 {
		t.Errorf("unknown runtime = %d, want 404", code)
	}

	// Another job's runtime → 403.
	rig.claimTask(t, "t-hb2")
	tok2 := rig.token(t, "t-hb2")
	rig.registerDaemon(t, "t-hb2", tok2)
	code, _ = rig.call(t, "POST", "/api/daemon/heartbeat", tok2, []byte(`{"runtime_id":"`+jobRID+`"}`))
	if code != 403 {
		t.Errorf("cross-job runtime = %d, want 403", code)
	}
}

func TestS4ResponseShape(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-reg")
	tok := rig.token(t, "t-reg")
	e, _ := rig.reg.Get("t-reg")

	code, body := rig.call(t, "POST", "/api/daemon/register", tok, []byte(`{
		"daemon_id": "fm-t-reg",
		"runtimes": [{"name": "foreman-job", "type": "omp"}, {"name": "extra", "type": "other"}]
	}`))
	if code != 200 {
		t.Fatalf("S4 = %d: %s", code, body)
	}
	var resp struct {
		Runtimes []struct {
			ID, Name, Provider, Status string
		} `json:"runtimes"`
		Repos        []map[string]string `json:"repos"`
		ReposVersion string              `json:"repos_version"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("S4 body: %v", err)
	}
	if len(resp.Runtimes) != 1 {
		t.Fatalf("S4 must return exactly one runtime, got %d", len(resp.Runtimes))
	}
	rt := resp.Runtimes[0]
	if rt.ID != e.JobRuntimeID || rt.Name != "foreman-job" || rt.Provider != "omp" || rt.Status != "online" {
		t.Errorf("runtime = %+v", rt)
	}
	if resp.ReposVersion != "abc123" || len(resp.Repos) != 1 {
		t.Errorf("repos not from the C1 cache: %+v", resp)
	}

	// A mismatched daemon_id is rejected.
	code, _ = rig.call(t, "POST", "/api/daemon/register", tok, []byte(`{"daemon_id":"fm-other","runtimes":[]}`))
	if code != 403 {
		t.Errorf("mismatched daemon_id = %d, want 403", code)
	}
}

func TestFixedEndpoints(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-fix")
	tok := rig.token(t, "t-fix")
	jobRID := rig.registerDaemon(t, "t-fix", tok)
	e, _ := rig.reg.Get("t-fix")

	// S3 runtime-profiles → 404.
	if code, _ := rig.call(t, "GET", "/api/daemon/workspaces/ws-1/runtime-profiles", tok, nil); code != 404 {
		t.Errorf("S3 = %d, want 404", code)
	}
	// S18 recover-orphans → zeros.
	code, body := rig.call(t, "POST", "/api/daemon/runtimes/"+jobRID+"/recover-orphans", tok, []byte(`{}`))
	if code != 200 || string(body) != `{"orphaned":0,"retried":0}` {
		t.Errorf("S18 = %d %s", code, body)
	}
	// S21 issues gc-check → empty.
	code, body = rig.call(t, "POST", "/api/daemon/workspaces/ws-1/issues/gc-check", tok, []byte(`{"issue_ids":["i1"]}`))
	if code != 200 || string(body) != `{"issues":[]}` {
		t.Errorf("S21 = %d %s", code, body)
	}
	// S22 legacy gc-checks → 404.
	for _, p := range []string{"/api/daemon/issues/i1/gc-check", "/api/daemon/chat-sessions/c1/gc-check", "/api/daemon/autopilot-runs/a1/gc-check"} {
		if code, _ := rig.call(t, "GET", p, tok, nil); code != 404 {
			t.Errorf("S22 %s = %d, want 404", p, code)
		}
	}
	// S27 pending_* results → 404.
	for _, p := range []string{
		"/api/daemon/runtimes/" + jobRID + "/models/m1/result",
		"/api/daemon/runtimes/" + jobRID + "/local-skills/l1/result",
		"/api/daemon/runtimes/" + jobRID + "/local-skills/import/l2/result",
		"/api/daemon/runtimes/" + jobRID + "/update/u1/result",
	} {
		if code, _ := rig.call(t, "POST", p, tok, []byte(`{}`)); code != 404 {
			t.Errorf("S27 %s = %d, want 404", p, code)
		}
	}
	// S24 renew → synthetic now+24h.
	code, body = rig.call(t, "POST", "/api/tokens/current/renew", tok, []byte(`{}`))
	if code != 200 {
		t.Fatalf("S24 = %d", code)
	}
	var renew struct {
		ExpiresAt time.Time `json:"expires_at"`
		Renewed   bool      `json:"renewed"`
	}
	if err := json.Unmarshal(body, &renew); err != nil || !renew.Renewed {
		t.Fatalf("S24 body = %s", body)
	}
	if d := time.Until(renew.ExpiresAt); d < 23*time.Hour || d > 25*time.Hour {
		t.Errorf("S24 expires_at in %s, want ≈24h", d)
	}
	// S19 deregister → ok, runtime offline.
	code, _ = rig.call(t, "POST", "/api/daemon/deregister", tok, []byte(`{"runtime_ids":["`+jobRID+`"]}`))
	if code != 200 {
		t.Errorf("S19 = %d", code)
	}
	if rt, _ := rig.reg.RuntimeByID(e.JobRuntimeID); rt.Online {
		t.Error("S19 must mark the runtime offline")
	}
	// S28 legacy workspaces == S1.
	code, body = rig.call(t, "GET", "/api/workspaces", tok, nil)
	if code != 200 || !bytes.Contains(body, []byte(`"id":"ws-1"`)) {
		t.Errorf("S28 = %d %s", code, body)
	}
}

func TestS2ReposFromRegistrationCache(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-repos")
	tok := rig.token(t, "t-repos")

	code, body := rig.call(t, "GET", "/api/daemon/workspaces/ws-1/repos", tok, nil)
	if code != 200 {
		t.Fatalf("S2 = %d", code)
	}
	var resp struct {
		WorkspaceID  string              `json:"workspace_id"`
		Repos        []map[string]string `json:"repos"`
		ReposVersion string              `json:"repos_version"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("S2 body: %v", err)
	}
	if resp.WorkspaceID != "ws-1" || resp.ReposVersion != "abc123" || len(resp.Repos) != 1 {
		t.Errorf("S2 = %s", body)
	}
	// A foreign workspace id in the path is rejected.
	if code, _ := rig.call(t, "GET", "/api/daemon/workspaces/other-ws/repos", tok, nil); code != 403 {
		t.Errorf("S2 foreign workspace = %d, want 403", code)
	}
}

func TestS20GCCheckForwarded(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-gc")
	tok := rig.token(t, "t-gc")

	rig.stub.on("GET", "/api/daemon/tasks/t-gc/gc-check", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "completed", "completed_at": "2026-10-05T00:00:00Z"})
	})
	code, body := rig.call(t, "GET", "/api/daemon/tasks/t-gc/gc-check", tok, nil)
	if code != 200 || !bytes.Contains(body, []byte(`"completed_at"`)) {
		t.Fatalf("S20 = %d %s", code, body)
	}
	// A server-side 404 passes through.
	rig.stub.on("GET", "/api/daemon/tasks/t-gc/gc-check", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":"task not found"}`))
	})
	if code, _ := rig.call(t, "GET", "/api/daemon/tasks/t-gc/gc-check", tok, nil); code != 404 {
		t.Errorf("S20 404 = %d", code)
	}
}

func TestS7PrepareLeaseForwardsC4(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-lease")
	tok := rig.token(t, "t-lease")
	jobRID := rig.registerDaemon(t, "t-lease", tok)

	rig.stub.on("POST", "/api/daemon/runtimes/rid-real/tasks/t-lease/prepare-lease", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})
	code, body := rig.call(t, "POST", "/api/daemon/runtimes/"+jobRID+"/tasks/t-lease/prepare-lease", tok, []byte(`{}`))
	if code != 200 || string(body) != `{"status":"ok"}` {
		t.Fatalf("S7 = %d %s", code, body)
	}
	// C4 ran in the foreground against the REAL runtime id.
	if got := len(rig.stub.calls("POST", "/api/daemon/runtimes/rid-real/tasks/t-lease/prepare-lease")); got != 1 {
		t.Fatalf("C4 calls = %d, want 1 (real runtime id)", got)
	}
}

func TestS7TaskVanishedSettlesLocally(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-vanish")
	tok := rig.token(t, "t-vanish")
	jobRID := rig.registerDaemon(t, "t-vanish", tok)

	rig.stub.on("POST", "/api/daemon/runtimes/rid-real/tasks/t-vanish/prepare-lease", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":"task not found"}`))
	})
	code, _ := rig.call(t, "POST", "/api/daemon/runtimes/"+jobRID+"/tasks/t-vanish/prepare-lease", tok, []byte(`{}`))
	if code != 200 {
		t.Fatalf("S7 = %d", code)
	}
	if _, ok := rig.reg.Get("t-vanish"); ok {
		t.Error("vanished task still in the registry")
	}
	if !rig.jobs.deleted("fm-t-vanish") {
		t.Error("vanished task's Job not deleted")
	}
}

func TestS23RejectsInvalidJobTokenLocally(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-hooks2")
	rig.stub.on("GET", "/api/daemon/tasks/t-hooks2/plugin-hooks", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"ok": "1"})
	})

	// A forged fmj_ token must be rejected locally (401), never forwarded.
	code, _ := rig.call(t, "GET", "/api/daemon/tasks/t-hooks2/plugin-hooks", "fmj_forged.forged", nil)
	if code != 401 {
		t.Errorf("forged fmj_ on S23 = %d, want 401", code)
	}
	if n := len(rig.stub.calls("GET", "/api/daemon/tasks/t-hooks2/plugin-hooks")); n != 0 {
		t.Errorf("forged token reached the upstream %d times", n)
	}
	if len(rig.metrics.authFails) == 0 {
		t.Error("auth failure metric not recorded")
	}
}

func TestS9Status404SettlesTaskVanished(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-s9")
	tok := rig.token(t, "t-s9")
	rig.registerDaemon(t, "t-s9", tok)

	rig.stub.on("GET", "/api/daemon/tasks/t-s9/status", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":"task not found"}`))
	})
	code, _ := rig.call(t, "GET", "/api/daemon/tasks/t-s9/status", tok, nil)
	if code != 404 {
		t.Fatalf("S9 = %d, want 404", code)
	}
	// proxy.md 转发 switch: non-terminal 404 → OnTaskVanished (Job/Secret
	// removed, mapping dropped) instead of lingering until a restart.
	if _, ok := rig.reg.Get("t-s9"); ok {
		t.Error("entry must be dropped after a C13 404")
	}
	if !rig.jobs.deleted("fm-t-s9") {
		t.Error("Job must be deleted after a C13 404")
	}
}

func TestS9StatusForwarded(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-status")
	tok := rig.token(t, "t-status")

	rig.stub.on("GET", "/api/daemon/tasks/t-status/status", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "cancelled"})
	})
	code, body := rig.call(t, "GET", "/api/daemon/tasks/t-status/status", tok, nil)
	if code != 200 || string(body) != `{"status":"cancelled"}` {
		t.Fatalf("S9 = %d %s", code, body)
	}
}

func TestS16SessionReturns204(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-session")
	tok := rig.token(t, "t-session")

	rig.stub.on("POST", "/api/daemon/tasks/t-session/session", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(204)
	})
	code, _ := rig.call(t, "POST", "/api/daemon/tasks/t-session/session", tok, []byte(`{"session":"s1"}`))
	if code != 204 {
		t.Fatalf("S16 = %d, want 204", code)
	}
	if got := len(rig.stub.calls("POST", "/api/daemon/tasks/t-session/session")); got != 1 {
		t.Fatalf("C12 calls = %d", got)
	}
}

func TestS26SkillBundlesResolve(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-skill")
	tok := rig.token(t, "t-skill")
	jobRID := rig.registerDaemon(t, "t-skill", tok)

	rig.stub.on("POST", "/api/daemon/runtimes/rid-real/tasks/t-skill/skill-bundles/resolve", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"bundle": "bytes"})
	})
	code, body := rig.call(t, "POST", "/api/daemon/runtimes/"+jobRID+"/tasks/t-skill/skill-bundles/resolve", tok, []byte(`{"skills":["s"]}`))
	if code != 200 || string(body) != `{"bundle":"bytes"}` {
		t.Fatalf("S26 = %d %s", code, body)
	}
	calls := rig.stub.calls("POST", "/api/daemon/runtimes/rid-real/tasks/t-skill/skill-bundles/resolve")
	if len(calls) != 1 {
		t.Fatalf("C19 must run against the real runtime id, calls = %d", len(calls))
	}
	if got := calls[0].Header.Get("Authorization"); got != "Bearer mdt_foreman" {
		t.Errorf("S26 must use Foreman's credential, got %q", got)
	}
}

func TestS23PassthroughKeepsCallerCredential(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-hooks")
	tok := rig.token(t, "t-hooks")

	rig.stub.on("GET", "/api/daemon/tasks/t-hooks/plugin-hooks", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":"no such endpoint"}`))
	})
	code, body := rig.call(t, "GET", "/api/daemon/tasks/t-hooks/plugin-hooks", tok, nil)
	if code != 404 || !bytes.Contains(body, []byte("no such endpoint")) {
		t.Fatalf("S23 = %d %s (upstream 404 must pass through verbatim)", code, body)
	}
	calls := rig.stub.calls("GET", "/api/daemon/tasks/t-hooks/plugin-hooks")
	if len(calls) != 1 || calls[0].Header.Get("Authorization") != "Bearer "+tok {
		t.Fatalf("S23 must forward the caller's Authorization verbatim: %+v", calls)
	}
}

func TestS29UpstreamFailureMaps502(t *testing.T) {
	rig := newTestRig(t)
	rig.stub.on("GET", "/api/issues/1", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(503)
	})
	code, body := rig.call(t, "GET", "/api/issues/1", "mat_tasktoken", nil)
	if code != 502 || !bytes.Contains(body, []byte("upstream unavailable")) {
		t.Fatalf("S29 upstream 5xx = %d %s, want 502", code, body)
	}
}

func TestExpiredAndForgedTokensRejected(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-auth")

	// Expired token → 401. The clock-skew tolerance is 5 minutes, so the
	// token is issued by a past-dated issuer (expired 30 minutes ago).
	pastIssuer, err := auth.NewIssuer(testTokenKey, jobLookup{rig.reg}, rig.reg,
		auth.WithClock(func() time.Time { return time.Now().Add(-time.Hour) }))
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	expired, err := pastIssuer.Issue("fm-t-auth", "t-auth", "ws-1", 30*time.Minute)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if code, _ := rig.call(t, "GET", "/api/daemon/workspaces", expired, nil); code != 401 {
		t.Errorf("expired token = %d, want 401", code)
	}
	// Forged signature → 401.
	payloadSeg := base64.RawURLEncoding.EncodeToString([]byte(`{"j":"fm-t-auth","t":"t-auth","w":"ws-1","e":9999999999}`))
	forged := "fmj_" + payloadSeg + "." + base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	if code, _ := rig.call(t, "GET", "/api/daemon/workspaces", forged, nil); code != 401 {
		t.Errorf("forged token = %d, want 401", code)
	}
	// The auth-failure metric records the rejection reasons.
	rig.metrics.mu.Lock()
	defer rig.metrics.mu.Unlock()
	joined := ""
	for _, rsn := range rig.metrics.authFails {
		joined += rsn + ","
	}
	if !strings.Contains(joined, "expired") || !strings.Contains(joined, "bad_signature") {
		t.Errorf("auth failure reasons = %v, want expired + bad_signature", rig.metrics.authFails)
	}
}
