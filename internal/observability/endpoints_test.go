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

func newTestHandler(reg *registry.Registry, logs PodLogReader) http.Handler {
	return NewOpsHandler(NewMetrics(), reg, logs, OpsConfig{
		Version:    "v0.1.0",
		JobImage:   "ghcr.io/tsic404/foreman-job@sha256:abc",
		RuntimeIDs: func() []string { return []string{"rt-123", "rt-456"} },
		StartedAt:  time.Now().Add(-time.Minute),
	})
}

func do(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	// Ops endpoints take no Job Token (S-25); requests go out
	// without Authorization on purpose.
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
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
	var body struct {
		Status     string   `json:"status"`
		Inflight   int      `json:"inflight"`
		RuntimeIDs []string `json:"runtime_ids"`
		UptimeS    int64    `json:"uptime_s"`
	}
	decodeBody(t, rec, &body)
	if body.Status != "ok" || body.Inflight != 1 || len(body.RuntimeIDs) != 2 || body.RuntimeIDs[0] != "rt-123" || body.UptimeS < 59 {
		t.Fatalf("healthz = %+v", body)
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
	h := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{Health: health})

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
	if body["version"] != "v0.1.0" || body["job_image"] == "" {
		t.Fatalf("version = %v", body)
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
		RegisterSource: func() any { return view },
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
	h := NewOpsHandler(NewMetrics(), registry.New(time.Now), nil, OpsConfig{Version: "v0.1.0"})
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
	})
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
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
	if rec := get("/foreman/tasks/task-b?workspace=ws-z"); rec.Code != http.StatusBadRequest {
		t.Errorf("unknown workspace = %d, want 400", rec.Code)
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
	h := NewOpsHandler(NewMetrics(), reg, nil, OpsConfig{WorkspaceIDs: []string{"ws-a"}})

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
