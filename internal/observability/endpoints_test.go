package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/foreman/internal/registry"
)

type fakeLogReader struct {
	data      []byte
	err       error
	namespace string
	jobName   string
	tail      int64
}

func (f *fakeLogReader) ReadLogs(_ context.Context, namespace, jobName string, tail int64) ([]byte, error) {
	f.namespace, f.jobName, f.tail = namespace, jobName, tail
	return f.data, f.err
}

// A handler mounted without a metrics registry answers the readiness face
// with 503 instead of dereferencing nil at construction time.
func TestOpsHandlerWithoutMetricsAnswersNotReady(t *testing.T) {
	h := NewOpsHandler(nil, registry.New(time.Now), nil, OpsConfig{Credentials: testOpsCredentials()})
	rec := do(t, h, "/metrics")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/metrics = %d, want 503", rec.Code)
	}
}

// testJobImage is the deployment reference the handler fixture reports
// (§9.3: the version endpoint echoes the configured reference verbatim).
const testJobImage = "ghcr.io/tsic404/foreman-job@sha256:abc"

// testOpsToken is the bearer credential the unit fixtures mount on the ops
// surface (§2.2 S-25: every ops request presents one).
const testOpsToken = "ops-test-token"

// testOpsCredentials is the deployment-wide credential the fixtures use.
func testOpsCredentials() []OpsCredential {
	return []OpsCredential{{Token: testOpsToken}}
}

// newTestHandler mounts the ops surface with the fixture credential.
func newTestHandler(reg *registry.Registry, logs PodLogReader) http.Handler {
	return NewOpsHandler(NewMetrics(), reg, logs, OpsConfig{
		Version:     "v0.1.0",
		JobImage:    testJobImage,
		Credentials: testOpsCredentials(),
		StartedAt:   time.Now().Add(-time.Minute),
	})
}

func do(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	// Ops requests carry the deployment's bearer credential; nothing else is
	// admitted (a Job Token is deliberately not accepted).
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+testOpsToken)
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.NewDecoder(rec.Result().Body).Decode(v); err != nil {
		t.Fatalf("decode body: %v", err)
	}
}

func TestHealthz(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{TaskID: "t-1", State: registry.StateRunning})
	rec := do(t, newTestHandler(reg, nil), "/foreman/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]any
	decodeBody(t, rec, &body)
	if body["status"] != "ok" || body["uptime_s"].(float64) < 59 {
		t.Fatalf("healthz = %+v", body)
	}
	// The liveness answer carries no task, workspace or runtime identity
	// field: it is the same answer the Job face serves without a credential.
	for _, key := range []string{"inflight", "runtime_ids", "tasks", "workspace_id"} {
		if _, present := body[key]; present {
			t.Errorf("healthz carries %q: %+v", key, body)
		}
	}
}

// TestHealthzReflectsCriticalLoopLiveness: the endpoint feeds the liveness
// probe (§9.3), so a critical loop that stopped must turn it red instead of
// leaving a half-dead process reported healthy (§1.5: a config error is
// alarmed, not absorbed).
func TestHealthzReflectsCriticalLoopLiveness(t *testing.T) {
	health := NewHealth()
	stopClaim := health.Running("claim")
	health.Running("heartbeat")
	h := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{
		Health:      health,
		Credentials: testOpsCredentials(),
	})

	rec := do(t, h, "/foreman/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 while every critical loop runs", rec.Code)
	}

	stopClaim()
	rec = do(t, h, "/foreman/healthz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 after a critical loop stopped", rec.Code)
	}
	var body struct {
		Status            string   `json:"status"`
		StoppedComponents []string `json:"stopped_components"`
	}
	decodeBody(t, rec, &body)
	if body.Status != "failed" || len(body.StoppedComponents) != 1 || body.StoppedComponents[0] != "claim" {
		t.Fatalf("healthz = %+v, want status failed and stopped_components [claim]", body)
	}
}

func TestTasksListOmitsPayload(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{
		TaskID:          "t-1",
		JobName:         "fm-t-1",
		JobNamespace:    "multica-agents",
		DaemonID:        "fm-t-1",
		JobRuntimeID:    "jr-1",
		NodeName:        "node-a",
		State:           registry.StateRunning,
		IssueIdentifier: "TSI-1",
		Payload:         json.RawMessage(`{"auth_token":"mat_secretpayload"}`),
		PriorSessionID:  "/state/home/.multica/pi-sessions/sess-1.jsonl",
	})
	rec := do(t, newTestHandler(reg, nil), "/foreman/tasks")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var entries []map[string]any
	decodeBody(t, rec, &entries)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	e := entries[0]
	for _, key := range []string{"task_id", "job_name", "node_name", "daemon_id", "job_runtime_id", "state"} {
		if _, ok := e[key]; !ok {
			t.Fatalf("entry missing %q: %v", key, e)
		}
	}
	if _, ok := e["payload"]; ok {
		t.Fatalf("payload leaked into tasks list: %v", e)
	}
	// The soft-affinity key is payload-derived, not a trigger context: it
	// stays in memory too (§4.1).
	if _, ok := e["prior_session_id"]; ok {
		t.Fatalf("prior_session_id leaked into tasks list: %v", e)
	}
}

// TestTasksListKeepsTriggerContextKeys (§4.1 / AC-04): the four
// trigger-context keys are always in the projection — an absent context is an
// empty string, never a missing key, so consumers can assert `== ""`.
func TestTasksListKeepsTriggerContextKeys(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{
		TaskID:         "t-1",
		JobName:        "fm-t-1",
		State:          registry.StateRunning,
		AutopilotRunID: "run-1",
	})
	rec := do(t, newTestHandler(reg, nil), "/foreman/tasks")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var entries []map[string]any
	decodeBody(t, rec, &entries)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	want := map[string]any{
		"issue_id":         "",
		"issue_identifier": "",
		"autopilot_run_id": "run-1",
		"chat_session_id":  "",
	}
	for key, value := range want {
		got, ok := entries[0][key]
		if !ok {
			t.Errorf("projection dropped %q: %v", key, entries[0])
			continue
		}
		if got != value {
			t.Errorf("%s = %v, want %q", key, got, value)
		}
	}
}

func TestTasksListEmptyIsArray(t *testing.T) {
	rec := do(t, newTestHandler(registry.New(time.Now), nil), "/foreman/tasks")
	body, _ := io.ReadAll(rec.Result().Body)
	if string(body) != "[]\n" {
		t.Fatalf("empty list = %q, want []", body)
	}
}

func TestTaskGet(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{TaskID: "t-1", JobName: "fm-t-1", State: registry.StatePending})
	rec := do(t, newTestHandler(reg, nil), "/foreman/tasks/t-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var e map[string]any
	decodeBody(t, rec, &e)
	if e["task_id"] != "t-1" {
		t.Fatalf("entry = %v", e)
	}
}

func TestTaskGetUnknown(t *testing.T) {
	rec := do(t, newTestHandler(registry.New(time.Now), nil), "/foreman/tasks/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	var body map[string]string
	decodeBody(t, rec, &body)
	if body["error"] == "" {
		t.Fatalf("error shape = %v", body)
	}
}

// TestPayloadPreview (§9.3 / AC-07 ②): the preview serves the delivered body,
// including the rewritten runtime_id — and only while the entry is alive.
func TestPayloadPreview(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{
		TaskID:       "t-1",
		JobName:      "fm-t-1",
		DaemonID:     "fm-t-1",
		JobRuntimeID: "jr-1",
		State:        registry.StateDaemonRegistered,
		Payload:      json.RawMessage(`{"id":"t-1","auth_token":"mat_taskleveltoken"}`),
	})
	h := newTestHandler(reg, nil)
	// S6 rewrites runtime_id to the job runtime id and records the result on
	// the entry; the endpoint reads no other source.
	delivered := json.RawMessage(`{"id":"t-1","runtime_id":"jr-1","auth_token":"mat_taskleveltoken"}`)
	if _, ok := reg.MarkDelivering("fm-t-1", time.Now(), delivered); !ok {
		t.Fatal("MarkDelivering failed")
	}
	rec := do(t, h, "/foreman/tasks/t-1/payload-preview")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]any
	decodeBody(t, rec, &body)
	e, _ := reg.Get("t-1")
	if body["runtime_id"] != e.JobRuntimeID {
		t.Fatalf("runtime_id = %v, want job_runtime_id %q", body["runtime_id"], e.JobRuntimeID)
	}
	if body["auth_token"] != "mat_taskleveltoken" {
		t.Fatalf("preview dropped payload fields: %v", body)
	}

	// Credential reclaim deletes the entry: the window is closed.
	if err := reg.Delete("t-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	rec = do(t, h, "/foreman/tasks/t-1/payload-preview")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("reclaimed status = %d, want 404", rec.Code)
	}
	var notFound map[string]string
	decodeBody(t, rec, &notFound)
	if notFound["error"] != "task not found" {
		t.Fatalf("reclaimed body = %v", notFound)
	}
}

// TestPayloadPreviewNotDelivered (§9.3): the window opens at
// delivered_at, so an existing entry that was never delivered answers 404 with
// the contract literal — its claim-time payload is not a delivery body.
func TestPayloadPreviewNotDelivered(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{
		TaskID:       "t-1",
		JobName:      "fm-t-1",
		DaemonID:     "fm-t-1",
		JobRuntimeID: "jr-1",
		State:        registry.StateDaemonRegistered,
		Payload:      json.RawMessage(`{"id":"t-1","runtime_id":"real-runtime"}`),
	})
	// Rebuilt-after-restart entry: no payload in memory (§4.3), also
	// never delivered.
	_ = reg.Put(registry.TaskEntry{TaskID: "t-2", State: registry.StateJobCreated})
	h := newTestHandler(reg, nil)
	for _, taskID := range []string{"t-1", "t-2"} {
		rec := do(t, h, "/foreman/tasks/"+taskID+"/payload-preview")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", taskID, rec.Code)
		}
		var body map[string]string
		decodeBody(t, rec, &body)
		if body["error"] != "payload unavailable" {
			t.Fatalf("%s body = %v, want the contract literal", taskID, body)
		}
	}
}

func TestVersion(t *testing.T) {
	rec := do(t, newTestHandler(registry.New(time.Now), nil), "/foreman/version")
	var body map[string]string
	decodeBody(t, rec, &body)
	// §9.3: the configured Job image reference is reported verbatim; presence
	// alone would let an unrelated constant pass as the audit anchor.
	if body["version"] != "v0.1.0" || body["job_image"] != testJobImage {
		t.Fatalf("version = %v, want version v0.1.0 and job_image %q", body, testJobImage)
	}
}

func TestJobLogsEndpoint(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{
		TaskID: "t-1", JobName: "fm-t-1", JobNamespace: "multica-agents",
		State: registry.StateRunning,
	})
	logs := &fakeLogReader{data: []byte("line1\nline2\n")}
	rec := do(t, newTestHandler(reg, logs), "/foreman/tasks/t-1/logs")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if logs.namespace != "multica-agents" || logs.jobName != "fm-t-1" {
		t.Fatalf("reader got %s/%s", logs.namespace, logs.jobName)
	}
	if logs.tail != defaultLogTail {
		t.Fatalf("default tail = %d, want %d", logs.tail, defaultLogTail)
	}
	body, _ := io.ReadAll(rec.Result().Body)
	if string(body) != "line1\nline2\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestJobLogsTailBounds(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{TaskID: "t-1", JobName: "fm-t-1", JobNamespace: "ns", State: registry.StateRunning})
	logs := &fakeLogReader{data: []byte("x")}
	h := newTestHandler(reg, logs)

	rec := do(t, h, "/foreman/tasks/t-1/logs?tail=99999")
	if rec.Code != http.StatusOK || logs.tail != maxLogTail {
		t.Fatalf("oversize tail = %d (status %d), want clamp to %d", logs.tail, rec.Code, maxLogTail)
	}
	if rec := do(t, h, "/foreman/tasks/t-1/logs?tail=abc"); rec.Code != http.StatusBadRequest {
		t.Fatalf("non-numeric tail status = %d, want 400", rec.Code)
	}
	if rec := do(t, h, "/foreman/tasks/t-1/logs?tail=0"); rec.Code != http.StatusBadRequest {
		t.Fatalf("zero tail status = %d, want 400", rec.Code)
	}
}

func TestJobLogsUnavailable(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{TaskID: "t-1", JobName: "fm-t-1", JobNamespace: "ns", State: registry.StateRunning})

	// Pod already deleted: ErrJobLogsUnavailable maps to 404.
	logs := &fakeLogReader{err: ErrJobLogsUnavailable}
	if rec := do(t, newTestHandler(reg, logs), "/foreman/tasks/t-1/logs"); rec.Code != http.StatusNotFound {
		t.Fatalf("pod-gone status = %d, want 404", rec.Code)
	}
	// Reader not wired: optional endpoint degrades to the same 404.
	if rec := do(t, newTestHandler(reg, nil), "/foreman/tasks/t-1/logs"); rec.Code != http.StatusNotFound {
		t.Fatalf("unwired-reader status = %d, want 404", rec.Code)
	}
	// Unknown task: 404 before touching the reader.
	if rec := do(t, newTestHandler(reg, logs), "/foreman/tasks/nope/logs"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown-task status = %d, want 404", rec.Code)
	}
	// Infrastructure failure: 500, not a misleading 404.
	broken := &fakeLogReader{err: errors.New("connection refused")}
	if rec := do(t, newTestHandler(reg, broken), "/foreman/tasks/t-1/logs"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("infra-error status = %d, want 500", rec.Code)
	}
}

// TestJobLogsForbidden covers the pods/log RBAC denial path: it maps to 403
// and an error log, not the generic 500.
func TestJobLogsForbidden(t *testing.T) {
	reg := registry.New(time.Now)
	_ = reg.Put(registry.TaskEntry{TaskID: "t-1", JobName: "fm-t-1", JobNamespace: "ns", State: registry.StateRunning})
	forbidden := &fakeLogReader{err: fmt.Errorf("stream: %w",
		apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "fm-t-1-a", errors.New("denied")))}

	var logBuf bytes.Buffer
	h := NewOpsHandler(NewMetrics(), reg, forbidden, OpsConfig{
		Version: "v", Logger: slog.New(slog.NewJSONHandler(&logBuf, nil)),
		Credentials: testOpsCredentials(),
	})
	rec := do(t, h, "/foreman/tasks/t-1/logs")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("forbidden status = %d, want 403", rec.Code)
	}
	var body map[string]string
	decodeBody(t, rec, &body)
	if body["error"] == "" {
		t.Fatalf("error shape = %v", body)
	}
	if !strings.Contains(logBuf.String(), "read job logs forbidden") {
		t.Fatalf("expected error log, got %q", logBuf.String())
	}
}

// TestRegisterSourceEndpoint serves the S31 read-only projection: the startup
// discovery, the C1 declaration and the catalog slots, never a discovery
// trigger (ADR-025).
func TestRegisterSourceEndpoint(t *testing.T) {
	reg := registry.New(time.Now)
	view := map[string]any{
		"runtimes":        []map[string]string{{"name": "n-omp", "type": "omp", "version": "1.0.0"}},
		"cli_version":     "1.0.0",
		"failed_profiles": []any{},
		"declaration":     []map[string]string{{"provider": "omp", "real_runtime_id": "rt-1"}},
		"slots":           []map[string]any{{"runtime_id": "rt-1", "provider": "omp", "cached": true}},
	}
	h := NewOpsHandler(NewMetrics(), reg, nil, OpsConfig{
		Version:        "v0.1.0",
		RegisterSource: func() map[string]any { return view },
		Credentials:    testOpsCredentials(),
	})
	rec := do(t, h, "/foreman/register-source")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]any
	decodeBody(t, rec, &body)
	if body["cli_version"] != "1.0.0" {
		t.Errorf("cli_version = %v", body["cli_version"])
	}
	if len(body["declaration"].([]any)) != 1 {
		t.Errorf("declaration = %v", body["declaration"])
	}
}

// TestRegisterSourceUnavailableWithoutSeam keeps the route honest when the
// ops handler was built without a register-source view.
func TestRegisterSourceUnavailableWithoutSeam(t *testing.T) {
	h := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{Version: "v0.1.0", Credentials: testOpsCredentials()})
	if rec := do(t, h, "/foreman/register-source"); rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// TestOpsWorkspaceDimensionIsRequiredAcrossWorkspaces pins §4.1/§9.3 on a
// multi-workspace deployment: the projection, single-task, log and preview
// routes have no "all workspaces" default. A missing dimension is a bad
// request, and an explicit other-workspace id answers 404, never a cross-
// workspace entry.
func TestOpsWorkspaceDimensionIsRequiredAcrossWorkspaces(t *testing.T) {
	reg := registry.New(time.Now)
	for _, e := range []registry.TaskEntry{
		{TaskID: "task-a", JobName: "fm-task-a", WorkspaceID: "ws-a", State: registry.StateRunning},
		{TaskID: "task-b", JobName: "fm-task-b", WorkspaceID: "ws-b", State: registry.StateRunning},
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.TaskID, err)
		}
	}
	h := NewOpsHandler(NewMetrics(), reg, nil, OpsConfig{
		WorkspaceIDs: []string{"ws-a", "ws-b"},
		Credentials:  testOpsCredentials(),
	})
	get := func(path string) *httptest.ResponseRecorder {
		return do(t, h, path)
	}

	for _, path := range []string{
		"/foreman/tasks",
		"/foreman/tasks/task-a",
		"/foreman/tasks/task-a/logs",
		"/foreman/tasks/task-a/payload-preview",
	} {
		if rec := get(path); rec.Code != http.StatusBadRequest {
			t.Errorf("%s without workspace = %d, want 400", path, rec.Code)
		}
	}

	rec := get("/foreman/tasks?workspace=ws-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("list ws-a = %d: %s", rec.Code, rec.Body)
	}
	var entries []struct {
		TaskID      string `json:"task_id"`
		WorkspaceID string `json:"workspace_id"`
	}
	decodeBody(t, rec, &entries)
	if len(entries) != 1 || entries[0].TaskID != "task-a" || entries[0].WorkspaceID != "ws-a" {
		t.Fatalf("list ws-a = %+v, want exactly task-a", entries)
	}

	if rec := get("/foreman/tasks/task-b?workspace=ws-a"); rec.Code != http.StatusNotFound {
		t.Errorf("cross-workspace single task = %d, want 404", rec.Code)
	}
	// A workspace outside the caller's visible set is answered like a
	// workspace that does not exist: the query cannot widen the partition.
	if rec := get("/foreman/tasks/task-b?workspace=ws-z"); rec.Code != http.StatusNotFound {
		t.Errorf("workspace outside the grant = %d, want 404", rec.Code)
	}
}

// TestOpsSingleWorkspaceIsTheDefaultDimension keeps single-workspace
// deployments unchanged: the only workspace is the dimension when the caller
// omits it.
func TestOpsSingleWorkspaceIsTheDefaultDimension(t *testing.T) {
	reg := registry.New(time.Now)
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-a", JobName: "fm-task-a", WorkspaceID: "ws-a", State: registry.StateRunning,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	h := NewOpsHandler(NewMetrics(), reg, nil, OpsConfig{WorkspaceIDs: []string{"ws-a"}, Credentials: testOpsCredentials()})

	rec := do(t, h, "/foreman/tasks")
	if rec.Code != http.StatusOK {
		t.Fatalf("list = %d: %s", rec.Code, rec.Body)
	}
	var entries []struct {
		TaskID string `json:"task_id"`
	}
	decodeBody(t, rec, &entries)
	if len(entries) != 1 || entries[0].TaskID != "task-a" {
		t.Fatalf("list = %+v, want task-a", entries)
	}
	if rec := do(t, h, "/foreman/tasks/task-a"); rec.Code != http.StatusOK {
		t.Fatalf("single task = %d, want 200", rec.Code)
	}
}

// TestOpsSurfaceRefusesEveryRouteWithoutCredential pins the credential face:
// no route answers without an admitted credential, and a Job Token — a
// credential of the Job face — never authenticates here (§2.2 S-25).
func TestOpsSurfaceRefusesEveryRouteWithoutCredential(t *testing.T) {
	ops := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{
		Version:     "v0.1.0",
		Credentials: testOpsCredentials(),
	})
	headers := []struct {
		name   string
		header string
	}{
		{"no credential", ""},
		{"job token", "Bearer fmj_00000000000000000000000000000000"},
		{"wrong credential", "Bearer another-deployment-token"},
		{"not a bearer scheme", "Basic b3BzLXRlc3QtdG9rZW4="},
	}
	for _, pattern := range ops.RegisteredPatterns() {
		path := strings.ReplaceAll(strings.TrimPrefix(pattern, http.MethodGet+" "), "{taskID}", "t-1")
		for _, tc := range headers {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			ops.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s with %s = %d, want 401", pattern, tc.name, rec.Code)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Errorf("%s with %s carries no WWW-Authenticate", pattern, tc.name)
			}
		}
	}

	// A surface mounted without a credential refuses everything: forgetting the
	// deployment value must close the surface, never open it.
	unwired := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{})
	req := httptest.NewRequest(http.MethodGet, "/foreman/tasks", nil)
	req.Header.Set("Authorization", "Bearer "+testOpsToken)
	rec := httptest.NewRecorder()
	unwired.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("uncredentialed surface = %d, want 401", rec.Code)
	}
}

// TestOpsScopedCredentialCropsWorkspace pins the workspace dimension's source:
// the credential's grant, not the query parameter. A credential bound to ws-a
// cannot name ws-b, and the register-source view carries only its workspace.
func TestOpsScopedCredentialCropsWorkspace(t *testing.T) {
	reg := registry.New(time.Now)
	for _, e := range []registry.TaskEntry{
		{TaskID: "task-a", JobName: "fm-task-a", WorkspaceID: "ws-a", State: registry.StateRunning},
		{TaskID: "task-b", JobName: "fm-task-b", WorkspaceID: "ws-b", State: registry.StateRunning},
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.TaskID, err)
		}
	}
	view := func() map[string]any {
		return map[string]any{
			"ws-a": map[string]any{"declaration": "a"},
			"ws-b": map[string]any{"declaration": "b"},
		}
	}
	newHandler := func(workspaces []string) *OpsHandler {
		return NewOpsHandler(NewMetrics(), reg, nil, OpsConfig{
			WorkspaceIDs:   []string{"ws-a", "ws-b"},
			RegisterSource: view,
			Credentials: []OpsCredential{
				{Token: "ops-a", Workspaces: workspaces},
				{Token: testOpsToken},
			},
		})
	}
	call := func(h *OpsHandler, token, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	scoped := newHandler([]string{"ws-a"})
	// The single visible workspace is the default dimension; the other
	// workspace's id in the query is answered like one that does not exist.
	rec := call(scoped, "ops-a", "/foreman/tasks")
	if rec.Code != http.StatusOK {
		t.Fatalf("scoped list = %d: %s", rec.Code, rec.Body)
	}
	var entries []struct {
		TaskID      string `json:"task_id"`
		WorkspaceID string `json:"workspace_id"`
	}
	decodeBody(t, rec, &entries)
	if len(entries) != 1 || entries[0].TaskID != "task-a" || entries[0].WorkspaceID != "ws-a" {
		t.Fatalf("scoped list = %+v, want exactly task-a", entries)
	}
	if rec := call(scoped, "ops-a", "/foreman/tasks?workspace=ws-b"); rec.Code != http.StatusNotFound {
		t.Errorf("scoped list naming ws-b = %d, want 404", rec.Code)
	}
	if rec := call(scoped, "ops-a", "/foreman/tasks/task-b"); rec.Code != http.StatusNotFound {
		t.Errorf("scoped single task of ws-b = %d, want 404", rec.Code)
	}
	rec = call(scoped, "ops-a", "/foreman/register-source")
	if rec.Code != http.StatusOK {
		t.Fatalf("scoped register-source = %d", rec.Code)
	}
	var cropped map[string]any
	decodeBody(t, rec, &cropped)
	if len(cropped) != 1 || cropped["ws-a"] == nil {
		t.Fatalf("scoped register-source = %v, want only ws-a", cropped)
	}

	// The deployment-wide credential keeps both workspaces but still has no
	// default dimension: the caller must name one (§4.1).
	wide := newHandler(nil)
	if rec := call(wide, testOpsToken, "/foreman/tasks"); rec.Code != http.StatusBadRequest {
		t.Errorf("wide unscoped list = %d, want 400", rec.Code)
	}
	rec = call(wide, testOpsToken, "/foreman/register-source")
	if rec.Code != http.StatusOK {
		t.Fatalf("wide register-source = %d", rec.Code)
	}
	var full map[string]any
	decodeBody(t, rec, &full)
	if len(full) != 2 || full["ws-b"] == nil {
		t.Fatalf("wide register-source = %v, want both workspaces", full)
	}
}

// TestNewHealthHandlerCarriesOnlyLiveness keeps the Job face's probe input
// data-free: the verdict and its stopped components, never a task, workspace
// or runtime identity field (§2.2 S-25).
func TestNewHealthHandlerCarriesOnlyLiveness(t *testing.T) {
	health := NewHealth()
	stopClaim := health.Running("claim")
	h := NewHealthHandler(health, time.Now().Add(-time.Minute))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/foreman/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	decodeBody(t, rec, &body)
	if len(body) != 2 || body["status"] != "ok" {
		t.Fatalf("health body = %v, want only status and uptime_s", body)
	}

	stopClaim()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/foreman/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status after a loop stopped = %d, want 503", rec.Code)
	}
	var failed map[string]any
	decodeBody(t, rec, &failed)
	if failed["status"] != "failed" || failed["stopped_components"] == nil {
		t.Fatalf("failed health body = %v", failed)
	}
}

// TestLoadOpsCredentials pins the deployment value's parsing and its
// fail-fast edges: an empty or placeholder credential, a malformed pair, and a
// token or workspace bound twice all refuse startup.
func TestLoadOpsCredentials(t *testing.T) {
	env := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	credentials, err := LoadOpsCredentials(env(map[string]string{
		EnvOpsToken:           "wide-token",
		EnvOpsWorkspaceTokens: "ws-a=token-a,ws-b=token=b",
	}))
	if err != nil {
		t.Fatalf("LoadOpsCredentials: %v", err)
	}
	if len(credentials) != 3 {
		t.Fatalf("credentials = %+v, want 3", credentials)
	}
	if credentials[0].Token != "wide-token" || credentials[0].Workspaces != nil {
		t.Errorf("wide credential = %+v", credentials[0])
	}
	if len(credentials[1].Workspaces) != 1 || credentials[1].Workspaces[0] != "ws-a" || credentials[1].Token != "token-a" {
		t.Errorf("workspace credential = %+v", credentials[1])
	}
	if credentials[2].Token != "token=b" {
		t.Errorf("token containing '=' = %q", credentials[2].Token)
	}

	for name, values := range map[string]map[string]string{
		"placeholder":        {EnvOpsToken: "REPLACE_ME"},
		"malformed pair":     {EnvOpsWorkspaceTokens: "ws-a"},
		"empty token":        {EnvOpsWorkspaceTokens: "ws-a="},
		"duplicate token":    {EnvOpsToken: "shared", EnvOpsWorkspaceTokens: "ws-a=shared"},
		"duplicate ws":       {EnvOpsWorkspaceTokens: "ws-a=one,ws-a=two"},
		"placeholder scoped": {EnvOpsWorkspaceTokens: "ws-a=REPLACE_ME"},
	} {
		if _, err := LoadOpsCredentials(env(values)); err == nil {
			t.Errorf("%s: LoadOpsCredentials accepted %v", name, values)
		}
	}

	if err := ValidateOpsCredentials(credentials, []string{"ws-a"}); err == nil {
		t.Error("ValidateOpsCredentials accepted a credential bound to an unserved workspace")
	}
	if err := ValidateOpsCredentials(credentials, []string{"ws-a", "ws-b"}); err != nil {
		t.Errorf("ValidateOpsCredentials: %v", err)
	}
}

// TestLoadOpsCredentialsErrorOmitsCredential pins the rejection path's text:
// a malformed entry can be the bearer token itself (without a separator it
// stays whole), and the startup error reaches the container log, so the
// message names the entry's position and never its content.
func TestLoadOpsCredentialsErrorOmitsCredential(t *testing.T) {
	env := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	const credential = "s3cr3t-ops-credential"
	for name, value := range map[string]string{
		"no separator":    credential,
		"no workspace":    "=" + credential,
		"empty workspace": " =\t" + credential,
	} {
		_, err := LoadOpsCredentials(env(map[string]string{EnvOpsWorkspaceTokens: value}))
		if err == nil {
			t.Fatalf("%s: LoadOpsCredentials accepted %q", name, value)
		}
		if strings.Contains(err.Error(), credential) {
			t.Errorf("%s: error %q echoes the credential", name, err)
		}
	}

	_, err := LoadOpsCredentials(env(map[string]string{EnvOpsWorkspaceTokens: "ws-a=tok-a,ws-b"}))
	if err == nil {
		t.Fatal("LoadOpsCredentials accepted a pair without a token")
	}
	if !strings.Contains(err.Error(), "entry 2") {
		t.Errorf("error %q does not name the malformed entry's position", err)
	}
}

// TestOpsMetricsRequiresDeploymentCredential pins the metric set's audience:
// its series carry a per-workspace label, so a workspace-bound credential must
// not read it (§9.2 多 workspace 面). The deployment credential still does.
func TestOpsMetricsRequiresDeploymentCredential(t *testing.T) {
	h := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{
		WorkspaceIDs: []string{"ws-a", "ws-b"},
		Credentials: []OpsCredential{
			{Token: testOpsToken},
			{Token: "ops-a", Workspaces: []string{"ws-a"}},
		},
	})
	call := func(token string) int {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := call(testOpsToken); code != http.StatusOK {
		t.Errorf("deployment credential on /metrics = %d, want 200", code)
	}
	if code := call("ops-a"); code != http.StatusForbidden {
		t.Errorf("workspace credential on /metrics = %d, want 403", code)
	}
}

// TestOpsMetricsWithoutAdmittedIdentityIsForbidden keeps the metric route's
// fail-closed reading of the identity: a request that reached it without the
// credential face holds no grant at all, never the deployment-wide one.
func TestOpsMetricsWithoutAdmittedIdentityIsForbidden(t *testing.T) {
	h := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{
		WorkspaceIDs: []string{"ws-a"},
		Credentials:  testOpsCredentials(),
	})
	rec := httptest.NewRecorder()
	h.dispatch(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("metrics without an admitted identity = %d, want 403", rec.Code)
	}
}
