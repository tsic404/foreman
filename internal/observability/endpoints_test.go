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
		Version:   "v0.1.0",
		JobImage:  "ghcr.io/tsic404/foreman-job@sha256:abc",
		RuntimeID: func() string { return "rt-123" },
		StartedAt: time.Now().Add(-time.Minute),
	})
}

func do(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	// Ops endpoints take no Job Token (contract §1.2 S25); requests go out
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
		Status    string `json:"status"`
		Inflight  int    `json:"inflight"`
		RuntimeID string `json:"runtime_id"`
		UptimeS   int64  `json:"uptime_s"`
	}
	decodeBody(t, rec, &body)
	if body.Status != "ok" || body.Inflight != 1 || body.RuntimeID != "rt-123" || body.UptimeS < 59 {
		t.Fatalf("healthz = %+v", body)
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

func TestPayloadPreview(t *testing.T) {
	reg := registry.New(time.Now)
	payload := json.RawMessage(`{"id":"t-1","auth_token":"mat_taskleveltoken"}`)
	_ = reg.Put(registry.TaskEntry{TaskID: "t-1", State: registry.StateDelivering, Payload: payload})
	rec := do(t, newTestHandler(reg, nil), "/foreman/tasks/t-1/payload-preview")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Result().Body)
	if string(body) != string(payload) {
		t.Fatalf("preview = %q, want verbatim payload", body)
	}
}

func TestPayloadPreviewMissing(t *testing.T) {
	reg := registry.New(time.Now)
	// Rebuilt-after-restart entry: no payload in memory (contract §3.2).
	_ = reg.Put(registry.TaskEntry{TaskID: "t-1", State: registry.StateJobCreated})
	h := newTestHandler(reg, nil)
	if rec := do(t, h, "/foreman/tasks/t-1/payload-preview"); rec.Code != http.StatusNotFound {
		t.Fatalf("empty payload status = %d, want 404", rec.Code)
	}
	if rec := do(t, h, "/foreman/tasks/nope/payload-preview"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown task status = %d, want 404", rec.Code)
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

// TestJobLogsForbidden covers observability.md §错误处理: pods/log RBAC
// denied maps to 403 and an error log, not the generic 500.
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
