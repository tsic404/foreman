package proxy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// recordingDispatcher captures what the daemon face hands the dispatcher, so
// the report routes can be checked without a real server behind them.
type recordingDispatcher struct {
	calls []reportCall
}

type reportCall struct {
	Endpoint  dispatch.Endpoint
	TaskID    string
	Workspace string
	Body      []byte
	SessionID string
}

func (r *recordingDispatcher) OnClaim(context.Context, string, json.RawMessage) error {
	return nil
}
func (r *recordingDispatcher) Deliver(context.Context, string, string, []string, int) (dispatch.Delivery, error) {
	return dispatch.Delivery{}, nil
}
func (r *recordingDispatcher) OnReport(_ context.Context, ep dispatch.Endpoint, e registry.TaskEntry, body []byte, sessionID string) (int, []byte, error) {
	r.calls = append(r.calls, reportCall{
		Endpoint: ep, TaskID: e.TaskID, Workspace: e.WorkspaceID, Body: body, SessionID: sessionID,
	})
	return http.StatusOK, []byte(`{"status":"ok"}`), nil
}
func (r *recordingDispatcher) OnTaskVanished(context.Context, string) error { return nil }
func (r *recordingDispatcher) OnBootTimeout(context.Context, string) error  { return nil }

// reportRig builds a daemon face with a recording dispatcher and one live task
// bound to the credential's daemon.
func reportRig(t *testing.T) (*recordingDispatcher, *registry.Registry, string) {
	t.Helper()
	store, err := daemonauth.Load(func(k string) string {
		switch k {
		case daemonauth.EnvDaemonTokens:
			return "worker-1=" + testToken
		case daemonauth.EnvDaemonTokenWorkspaces:
			return "worker-1=ws-1"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("creds: %v", err)
	}
	reg := registry.New()
	rec := &recordingDispatcher{}
	srv := proxy.NewFakeServer(store, reg, rec)
	httpSrv := httptest.NewServer(srv)
	t.Cleanup(httpSrv.Close)

	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-1", Online: true, RegisteredAt: reg.Now(),
	})
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-r", WorkspaceID: "ws-1", DaemonID: "worker-1",
		DaemonRuntimeID: "worker-1", RuntimeID: "srv-rt-1",
		State: registry.StateRunning,
	}); err != nil {
		t.Fatalf("put task: %v", err)
	}
	return rec, reg, httpSrv.URL
}

// TestReportRoutesReachDispatcherWithTheirOwnEndpoint is the shared-handler
// contract: eight report routes are all mounted on reportHandler(ep), so the
// endpoint value is the only thing telling them apart upstream. A mis-wired
// route would forward a complete as a progress and nobody would notice.
func TestReportRoutesReachDispatcherWithTheirOwnEndpoint(t *testing.T) {
	rec, _, url := reportRig(t)

	cases := []struct {
		path string
		ep   dispatch.Endpoint
	}{
		{"/start", dispatch.EPStart},
		{"/progress", dispatch.EPProgress},
		{"/messages", dispatch.EPMessages},
		{"/usage", dispatch.EPUsage},
		{"/complete", dispatch.EPComplete},
		{"/fail", dispatch.EPFail},
		{"/cancel-ack", dispatch.EPCancelAck},
		{"/session", dispatch.EPSession},
		{"/wait-local-directory", dispatch.EPWaitLocalDir},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			before := len(rec.calls)
			code := postWithToken(t, url, "/api/daemon/tasks/task-r"+tc.path, testToken, `{"x":1}`)
			if code != http.StatusOK {
				t.Fatalf("status = %d, want 200", code)
			}
			if len(rec.calls) != before+1 {
				t.Fatalf("dispatcher saw %d new calls, want 1", len(rec.calls)-before)
			}
			got := rec.calls[len(rec.calls)-1]
			if got.Endpoint != tc.ep {
				t.Fatalf("endpoint = %q, want %q", got.Endpoint, tc.ep)
			}
			if got.TaskID != "task-r" || got.Workspace != "ws-1" {
				t.Fatalf("task=%q workspace=%q, want task-r/ws-1", got.TaskID, got.Workspace)
			}
			if !strings.Contains(string(got.Body), `"x":1`) {
				t.Fatalf("body not passed through verbatim: %s", got.Body)
			}
		})
	}
}

// TestTerminalReportsCarrySessionID: only complete/fail teach the session
// index (ADR-015). A report route that learned a session from /progress would
// poison the affinity lookup with a half-finished run's id.
func TestTerminalReportsCarrySessionID(t *testing.T) {
	rec, _, url := reportRig(t)

	body := `{"session_id":"sess-42","status":"completed"}`
	if code := postWithToken(t, url, "/api/daemon/tasks/task-r/complete", testToken, body); code != http.StatusOK {
		t.Fatalf("complete = %d", code)
	}
	last := rec.calls[len(rec.calls)-1]
	if last.SessionID != "sess-42" {
		t.Fatalf("complete session = %q, want sess-42", last.SessionID)
	}

	// A non-terminal report must not be read for a session id.
	before := len(rec.calls)
	if code := postWithToken(t, url, "/api/daemon/tasks/task-r/progress", testToken, body); code != http.StatusOK {
		t.Fatalf("progress = %d", code)
	}
	last = rec.calls[len(rec.calls)-1]
	if len(rec.calls) != before+1 {
		t.Fatalf("progress did not reach the dispatcher")
	}
	if last.SessionID != "" {
		t.Fatalf("progress carried session %q; only complete/fail teach the index", last.SessionID)
	}
}
