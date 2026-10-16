package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
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
	// created is the apiserver timestamp stamped on every created Job; the
	// scheduler reads it as job_created_at (contract §3.1).
	created time.Time
}

func (f *fakeJobs) CreateJob(_ context.Context, job *batchv1.Job) (*batchv1.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createdJobs = append(f.createdJobs, job.Name)
	job.CreationTimestamp = metav1.NewTime(f.created)
	return job, nil
}

func (f *fakeJobs) DeleteJob(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedJobs = append(f.deletedJobs, name)
	return nil
}

func (f *fakeJobs) GetJob(context.Context, string) (*batchv1.Job, error) { return nil, nil }

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
func (f *fakeJobs) ListSecrets(context.Context) ([]corev1.Secret, error) {
	return nil, nil
}
func (f *fakeJobs) ListPods(context.Context, string) ([]corev1.Pod, error) { return nil, nil }
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

func (fakeBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fm-" + e.TaskID + "-cred"}}
	return job, secret, "sha256:" + e.TaskID, nil
}

// fakeRevoker records the credential digests the scheduler feeds to the Job
// Token authority on reclaim (ADR-017).
type fakeRevoker struct {
	mu      sync.Mutex
	revoked []string
}

func (f *fakeRevoker) Revoke(tokenHash string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked = append(f.revoked, tokenHash)
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
	revoker *fakeRevoker
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
	rig.jobs = &fakeJobs{created: time.Now()}
	rig.revoker = &fakeRevoker{}
	pending, err := recovery.NewPendingReports(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	rig.pending = pending
	sched, err := scheduler.New(
		scheduler.Config{JobNamespace: "test-ns", MaxInflightJobs: 10, ClaimBatchMax: 32, JobBootTimeout: 300 * time.Second},
		rig.reg, rig.jobs, fakeBuilder{}, rig.client, nil,
		scheduler.WithPendingReports(pending),
		scheduler.WithRevoker(rig.revoker),
	)
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	rig.sched = sched

	issuer, err := JobTokenIssuer(testTokenKey, rig.reg, auth.DefaultJobTokenTTL)
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

	// S13: complete forwards C9, then the Job is taken down — the credential
	// and the entry stay until the reclaim (ADR-017).
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
	// The verdict leaves the entry draining: it is still the authorization
	// row of its Job, and only the reclaim removes it (ADR-017).
	e, ok = rig.reg.Get("t-life")
	if !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry after terminal report = %+v, want it terminal and draining", e)
	}
	if !rig.reg.IsDone("t-life") {
		t.Error("done index not updated")
	}

	// A retry of the same terminal report inside the drain window is a live
	// Job's report: it is forwarded again and the server's terminal callback
	// is idempotent (proxy.md 迟到重放 ①).
	code, _ = rig.call(t, "POST", "/api/daemon/tasks/t-life/complete", tok, completeBody)
	if code != 200 {
		t.Fatalf("drain-window retry = %d, want 200", code)
	}
	if got := len(rig.stub.calls("POST", "/api/daemon/tasks/t-life/complete")); got != 2 {
		t.Fatalf("drain-window retry C9 calls = %d, want 2", got)
	}

	// The Job's pods are gone: the reclaim revokes the digest, deletes the
	// Secret and drops the entry, in that order (ADR-017).
	deletedBefore := len(rig.jobs.deletedSecret)
	if err := rig.sched.OnJobTerminated(t.Context(), "fm-t-life",
		scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	if _, ok := rig.reg.Get("t-life"); ok {
		t.Error("entry must be gone after the reclaim")
	}
	if got := rig.jobs.deletedSecret; len(got) != deletedBefore+1 || got[deletedBefore] != "fm-t-life-cred" {
		t.Errorf("deleted Secrets = %v, want the reclaimed credential appended", got)
	}
	if got := rig.revoker.revoked; len(got) != 1 || got[0] != "sha256:t-life" {
		t.Errorf("revoked digests = %v, want the entry's token hash", got)
	}

	// Late replay after the reclaim: the entry adjudication falls to the done
	// index, so the terminal endpoint is a 200 idempotent with no second C9
	// (proxy.md 迟到重放 ②).
	code, _ = rig.call(t, "POST", "/api/daemon/tasks/t-life/complete", tok, completeBody)
	if code != 200 {
		t.Fatalf("late replay = %d, want 200", code)
	}
	if got := len(rig.stub.calls("POST", "/api/daemon/tasks/t-life/complete")); got != 2 {
		t.Fatalf("late replay re-forwarded: C9 calls = %d", got)
	}
	// A non-terminal endpoint on the reclaimed task is rejected: the entry is
	// gone and the done index only covers terminal endpoints (auth.Verify
	// step 4).
	code, _ = rig.call(t, "POST", "/api/daemon/tasks/t-life/progress", tok, []byte(`{}`))
	if code != 401 {
		t.Errorf("non-terminal endpoint after reclaim = %d, want 401", code)
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
	var buf bytes.Buffer
	rig.handler.(*FakeServer).log = slog.New(slog.NewJSONHandler(&buf, nil)).With("component", "fake-server")
	rig.claimTask(t, "t-lease")
	tok := rig.token(t, "t-lease")
	jobRID := rig.registerDaemon(t, "t-lease", tok)

	rig.stub.on("POST", "/api/daemon/runtimes/rid-real/tasks/t-lease/prepare-lease", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})
	before, ok := rig.reg.Get("t-lease")
	if !ok {
		t.Fatal("entry missing after claim")
	}
	start := time.Now()
	code, body := rig.call(t, "POST", "/api/daemon/runtimes/"+jobRID+"/tasks/t-lease/prepare-lease", tok, []byte(`{}`))
	end := time.Now()
	if code != 200 || string(body) != `{"status":"ok"}` {
		t.Fatalf("S7 = %d %s", code, body)
	}
	// C4 ran in the foreground against the REAL runtime id.
	if got := len(rig.stub.calls("POST", "/api/daemon/runtimes/rid-real/tasks/t-lease/prepare-lease")); got != 1 {
		t.Fatalf("C4 calls = %d, want 1 (real runtime id)", got)
	}
	// S7's local renewal bookkeeping (proxy.md §请求路由): the daemon's
	// renewal restamped the server lease, so lease_keep_until (§3.1) moves to
	// the renewal moment plus the 45s TTL.
	after, ok := rig.reg.Get("t-lease")
	if !ok {
		t.Fatal("entry missing after S7")
	}
	lo, hi := start.Add(scheduler.PrepareLeaseTTL), end.Add(scheduler.PrepareLeaseTTL)
	if after.LeaseKeepUntil.Before(lo) || after.LeaseKeepUntil.After(hi) {
		t.Errorf("lease_keep_until = %v, want within [%v, %v]", after.LeaseKeepUntil, lo, hi)
	}
	if !after.LeaseKeepUntil.After(before.LeaseKeepUntil) {
		t.Errorf("S7 left lease_keep_until unadvanced: before %v, after %v",
			before.LeaseKeepUntil, after.LeaseKeepUntil)
	}
	// S7 leaves the same Info observation line the keeper does
	// (protocol-lease.md 场景 A): one record per renewal, at a level the
	// default handler admits.
	records := logRecords(t, buf.Bytes(), "lease.renewed")
	if len(records) != 1 {
		t.Fatalf("lease.renewed records = %d, want 1", len(records))
	}
	if got := records[0]["level"]; got != "INFO" {
		t.Errorf("lease.renewed level = %v, want INFO", got)
	}
	if got := records[0]["job_name"]; got != "fm-t-lease" {
		t.Errorf("lease.renewed job_name = %v, want fm-t-lease", got)
	}
	if got, want := records[0]["lease_keep_until"], projectedLeaseKeepUntil(t, after); got != want {
		t.Errorf("lease.renewed lease_keep_until = %v, projection = %v", got, want)
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
	// The verdict settles the entry locally — terminal and draining, with the
	// Job object taken down; the credential is reclaimed when the Job's pods
	// terminate or the drain bound expires (ADR-017).
	e, ok := rig.reg.Get("t-vanish")
	if !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("vanished task entry = %+v, want it terminal and draining", e)
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
	// proxy.md 转发 switch: non-terminal 404 → OnTaskVanished (the Job object
	// is taken down and the entry settles terminal, still draining until the
	// reclaim) instead of lingering until a restart.
	e, ok := rig.reg.Get("t-s9")
	if !ok || !e.IsTerminal() || !e.IsDraining() {
		t.Fatalf("entry after a C13 404 = %+v, want it terminal and draining", e)
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

// TestTerminalReportLearnsSessionNode (AC-25 学习面, ADR-015): the terminal
// reports S13/S14 teach the session index — their session_id, normalized by
// sessionKey, recorded against the node the pod watch observed for that task.
// Everything else that reaches Foreman carries no learning: S16 is not a face
// (a pi-family daemon never pins a session), a task whose placement was never
// observed has no node to record, and the learning is a decision of its own —
// a report whose forward fails is still learned, because the pending queue
// retries the report rather than losing it.
func TestTerminalReportLearnsSessionNode(t *testing.T) {
	rig := newTestRig(t)

	rig.claimTask(t, "t-learn")
	rig.reg.SetNode("t-learn", "company-02")
	rig.stub.on("POST", "/api/daemon/tasks/t-learn/complete", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "completed"})
	})
	code, body := rig.call(t, "POST", "/api/daemon/tasks/t-learn/complete", rig.token(t, "t-learn"),
		[]byte(`{"result":"done","session_id":"/state/home/.multica/pi-sessions/sess-7.jsonl"}`))
	if code != 200 {
		t.Fatalf("S13 = %d: %s", code, body)
	}
	if got := rig.reg.LastNodeForSession("sess-7"); got != "company-02" {
		t.Fatalf("learned node = %q, want company-02", got)
	}
	// The key is the session, never the issue the task was dispatched from.
	if got := rig.reg.LastNodeForSession("issue-1"); got != "" {
		t.Fatalf("issue key learned: %q", got)
	}

	// S16 carries a session id too, but it must not teach the index.
	rig.claimTask(t, "t-pin")
	rig.reg.SetNode("t-pin", "company-03")
	rig.stub.on("POST", "/api/daemon/tasks/t-pin/session", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(204)
	})
	if code, _ := rig.call(t, "POST", "/api/daemon/tasks/t-pin/session", rig.token(t, "t-pin"),
		[]byte(`{"session_id":"/state/home/.multica/pi-sessions/sess-8.jsonl"}`)); code != 204 {
		t.Fatalf("S16 = %d, want 204", code)
	}
	if got := rig.reg.LastNodeForSession("sess-8"); got != "" {
		t.Fatalf("S16 learned a session node: %q", got)
	}

	// No observed placement, no entry to record.
	rig.claimTask(t, "t-nonode")
	rig.stub.on("POST", "/api/daemon/tasks/t-nonode/complete", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "completed"})
	})
	if code, _ := rig.call(t, "POST", "/api/daemon/tasks/t-nonode/complete", rig.token(t, "t-nonode"),
		[]byte(`{"session_id":"/state/home/.multica/pi-sessions/sess-9.jsonl"}`)); code != 200 {
		t.Fatalf("S13 = %d, want 200", code)
	}
	if got := rig.reg.LastNodeForSession("sess-9"); got != "" {
		t.Fatalf("node-less task learned a session: %q", got)
	}

	// A failed forward still learns: the queue replays the report, so the
	// learning must not wait for it.
	rig.claimTask(t, "t-fail")
	rig.reg.SetNode("t-fail", "company-04")
	rig.stub.on("POST", "/api/daemon/tasks/t-fail/fail", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 500, map[string]string{"error": "boom"})
	})
	if code, _ := rig.call(t, "POST", "/api/daemon/tasks/t-fail/fail", rig.token(t, "t-fail"),
		[]byte(`{"error":"agent died","session_id":"/state/home/.multica/pi-sessions/sess-10.jsonl"}`)); code != 502 {
		t.Fatalf("S14 with a failing upstream = %d, want 502", code)
	}
	if got := rig.reg.LastNodeForSession("sess-10"); got != "company-04" {
		t.Fatalf("learned node after a failed forward = %q, want company-04", got)
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

// stubAuthority is the authority the upstream must see as Host: the target
// server's address, never the Foreman address the caller dialed (§1.2
// exclusion set).
func (r *testRig) stubAuthority() string {
	return strings.TrimPrefix(r.client.cfg.ServerURL, "http://")
}

// callWithHeaders performs one J→F request with an explicit caller header set.
// The test client mimics a bare caller: no compression negotiation and no
// User-Agent of its own, so only the caller's set reaches the fake server.
func (r *testRig) callWithHeaders(t *testing.T, method, path string, header http.Header, body []byte) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, r.server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, vv := range header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header["User-Agent"] = nil
	}
	resp, err := bareCaller.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, resp.Header, raw
}

// bareCaller is the J side of the probes: a client that adds nothing of its own.
var bareCaller = &http.Client{Transport: &http.Transport{DisableCompression: true}}

// The reported defect: a multipart upload through rule 2 (S29) must reach the
// upstream with the caller's own content type and bytes — a fixed
// application/json made every multipart call fail upstream.
func TestPassthroughKeepsMultipartContentType(t *testing.T) {
	rig := newTestRig(t)
	const upstreamErr = `{"error":"task_id upload requires a chat task"}`
	rig.stub.on("POST", "/api/upload-file", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		_, _ = w.Write([]byte(upstreamErr))
	})

	boundary := "----s29probe"
	body := multipartProbeBody(boundary)
	contentType := "multipart/form-data; boundary=" + boundary
	code, respHeader, raw := rig.callWithHeaders(t, "POST", "/api/upload-file", http.Header{
		"Authorization": {"Bearer mat_tasktoken"},
		"Content-Type":  {contentType},
	}, body)

	// The upstream sees the caller's multipart form byte-for-byte.
	got := singleCall(t, rig.stub, "POST", "/api/upload-file")
	if v := got.Header.Values("Content-Type"); !slices.Equal(v, []string{contentType}) {
		t.Errorf("content type to upstream = %v, want %v", v, contentType)
	}
	if string(got.Body) != string(body) || got.ContentLength != int64(len(body)) {
		t.Errorf("body to upstream = %d bytes (CL %d), want %d bytes", len(got.Body), got.ContentLength, len(body))
	}
	if extra := upstreamFields(got.Header); len(extra) != 2 {
		t.Errorf("upstream fields = %v, want exactly the caller's two fields", extra)
	}
	// And the upstream verdict comes back to the caller unchanged.
	if code != 400 || string(raw) != upstreamErr {
		t.Errorf("caller got %d %s, want 400 %s", code, raw, upstreamErr)
	}
	if v := respHeader.Values("Content-Type"); !slices.Equal(v, []string{"application/json"}) {
		t.Errorf("caller content type = %v, want the upstream's own", v)
	}
}

func TestPassthroughReplaysUpstreamResponseHeaders(t *testing.T) {
	rig := newTestRig(t)
	rig.stub.on("GET", "/api/attachments/7/download", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		h := w.Header()
		h.Set("Content-Type", "application/octet-stream")
		h.Set("Content-Disposition", "attachment; filename=probe.bin")
		h.Set("ETag", `"v1"`)
		h.Add("X-Probe-Resp", "a")
		h.Add("X-Probe-Resp", "b")
		h.Add("Set-Cookie", "a=1")
		h.Add("Set-Cookie", "b=2")
		w.WriteHeader(200)
		_, _ = w.Write([]byte{0x00, 0xff})
	})
	rig.stub.on("GET", "/api/no-ctype", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		// The stub must not add one either, or the assertion below would
		// measure the stub's own sniffing instead of the proxy's.
		w.Header()["Content-Type"] = nil
		w.WriteHeader(200)
		_, _ = w.Write([]byte("plain bytes"))
	})
	rig.stub.on("GET", "/api/no-date", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		// Likewise for Date: the stub sends none, so Foreman's own server must
		// not add one on the way back to the caller.
		w.Header()["Date"] = nil
		w.Header().Set("X-Probe-Note", "no-date")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("no date"))
	})

	code, header, body := rig.callWithHeaders(t, "GET", "/api/attachments/7/download",
		http.Header{"Authorization": {"Bearer mat_tasktoken"}}, nil)
	if code != 200 || !slices.Equal(body, []byte{0x00, 0xff}) {
		t.Fatalf("download = %d %v, want the upstream bytes", code, body)
	}
	for field, want := range map[string][]string{
		"Content-Type":        {"application/octet-stream"},
		"Content-Disposition": {"attachment; filename=probe.bin"},
		"ETag":                {`"v1"`},
		"X-Probe-Resp":        {"a", "b"},
		"Set-Cookie":          {"a=1", "b=2"},
	} {
		if got := header.Values(field); !slices.Equal(got, want) {
			t.Errorf("caller %s = %v, want %v", field, got, want)
		}
	}
	// Upstream gave no Content-Type on this path: the caller sees none either
	// (the local-response outlet would have stamped application/json, and the
	// server's sniffing would have invented one).
	_, header, _ = rig.callWithHeaders(t, "GET", "/api/no-ctype",
		http.Header{"Authorization": {"Bearer mat_tasktoken"}}, nil)
	if v := header.Values("Content-Type"); len(v) != 0 {
		t.Errorf("caller Content-Type = %v, want it absent", v)
	}
	// Same for a Date the upstream never sent: net/http adds one unless the
	// field is present-but-nil, so deleting that line turns this red.
	_, header, _ = rig.callWithHeaders(t, "GET", "/api/no-date",
		http.Header{"Authorization": {"Bearer mat_tasktoken"}}, nil)
	if v := header.Values("X-Probe-Note"); !slices.Equal(v, []string{"no-date"}) {
		t.Errorf("caller X-Probe-Note = %v, want the upstream's field", v)
	}
	if v := header.Values("Date"); len(v) != 0 {
		t.Errorf("caller Date = %v, want none: the upstream sent none and Foreman invents none", v)
	}
}

func TestPassthroughExclusionSetThroughFakeServer(t *testing.T) {
	rig := newTestRig(t)
	rig.stub.on("POST", "/api/daemon/tasks/whatever/status", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSONStub(w, 200, map[string]string{"status": "running"})
	})

	code, _, _ := rig.callWithHeaders(t, "POST", "/api/daemon/tasks/whatever/status", http.Header{
		"Authorization": {"Bearer mat_tasktoken"},
		"Accept":        {"application/vnd.multica+json"},
		"Connection":    {"X-Probe-Drop"},
		"TE":            {"trailers"},
		"X-Probe-Drop":  {"nope"},
		"X-Probe-Multi": {"a", "b"},
	}, []byte("probe"))
	if code != 200 {
		t.Fatalf("passthrough = %d", code)
	}
	got := singleCall(t, rig.stub, "POST", "/api/daemon/tasks/whatever/status")
	want := map[string][]string{
		"Authorization": {"Bearer mat_tasktoken"},
		"Accept":        {"application/vnd.multica+json"},
		"X-Probe-Multi": {"a", "b"},
	}
	if extra := upstreamFields(got.Header); len(extra) != len(want) {
		t.Errorf("upstream fields = %v, want exactly %v", extra, want)
	}
	for field, wantVals := range want {
		if v := got.Header.Values(field); !slices.Equal(v, wantVals) {
			t.Errorf("upstream %s = %v, want %v", field, v, wantVals)
		}
	}
	// Host is the target authority, not the Foreman address the caller dialed.
	if got.Host != rig.stubAuthority() {
		t.Errorf("upstream Host = %q, want %q", got.Host, rig.stubAuthority())
	}
}

func TestExpiredAndForgedTokensRejected(t *testing.T) {
	rig := newTestRig(t)
	rig.claimTask(t, "t-auth")

	// Expired token → 401. The clock-skew tolerance is 5 minutes, so the
	// token is issued by a past-dated issuer (expired 30 minutes ago).
	pastIssuer, err := auth.NewIssuer(testTokenKey, jobLookup{rig.reg}, rig.reg, auth.DefaultJobTokenTTL,
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
	if !strings.Contains(joined, "expired") || !strings.Contains(joined, "signature") {
		t.Errorf("auth failure reasons = %v, want expired + signature", rig.metrics.authFails)
	}
}
