package dispatch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// unboundServer records forwarded reports for the restart-tolerance path.
type unboundServer struct {
	mu   sync.Mutex
	gets []string
}

func (s *unboundServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/daemon/tasks/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.gets = append(s.gets, r.URL.Path+" "+string(body))
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"status":"ok"}`)
	})
	return mux
}

func unboundRig(t *testing.T) (*dispatch.Dispatcher, *registry.Registry, *unboundServer, *bytes.Buffer) {
	t.Helper()
	stub := &unboundServer{}
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)

	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := registry.New()
	client := proxy.NewClient(proxy.Config{
		ServerURL: srv.URL, WorkspaceID: "ws-1", DaemonID: "foreman-ws-1", Version: "test",
	}, "tok")
	disp := dispatch.New(dispatch.Config{}, reg, client, nil, dispatch.WithLogger(log))
	return disp, reg, stub, logs
}

// TestTerminalReportForUnknownTaskIsForwardedButLogged is the restart path: the
// registry is in-memory, so after a Foreman restart every binding is gone while
// daemons keep reporting. The report must still go upstream (refusing it would
// strand the task forever), but the local bookkeeping cannot record it — and
// that fact has to be visible, because a silent drop looks identical to a clean
// settle in the logs.
func TestTerminalReportForUnknownTaskIsForwardedButLogged(t *testing.T) {
	disp, _, stub, logs := unboundRig(t)

	e := registry.TaskEntry{TaskID: "gone-task", WorkspaceID: "ws-1", DaemonID: "worker-1"}
	body, _ := json.Marshal(map[string]any{"status": "completed"})
	code, _, err := disp.OnReport(context.Background(), dispatch.EPComplete, e, body, "")
	if err != nil {
		t.Fatalf("OnReport returned an error: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("code = %d, want 200 (the daemon must not retry forever)", code)
	}

	stub.mu.Lock()
	n := len(stub.gets)
	stub.mu.Unlock()
	if n != 1 {
		t.Fatalf("forwarded %d reports, want 1 (the upstream is the record)", n)
	}

	out := logs.String()
	if !strings.Contains(out, "task.unbound") {
		t.Fatalf("an unbound terminal report left no trace; want a task.unbound line, got:\n%s", out)
	}
	if !strings.Contains(out, "gone-task") {
		t.Fatalf("the unbound line does not name the task:\n%s", out)
	}
}

// TestTerminalReportForKnownTaskStaysQuiet: the ordinary path must not emit the
// unbound line, or it would be noise on every task.
func TestTerminalReportForKnownTaskStaysQuiet(t *testing.T) {
	disp, reg, _, logs := unboundRig(t)
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-1", Online: true, RegisteredAt: reg.Now(),
	})
	if err := reg.Put(registry.TaskEntry{
		TaskID: "task-1", WorkspaceID: "ws-1", DaemonID: "worker-1",
		DaemonRuntimeID: "worker-1", RuntimeID: "srv-rt-1", State: registry.StateRunning,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	e := registry.TaskEntry{TaskID: "task-1", WorkspaceID: "ws-1", DaemonID: "worker-1"}
	body, _ := json.Marshal(map[string]any{"status": "completed"})
	if _, _, err := disp.OnReport(context.Background(), dispatch.EPComplete, e, body, ""); err != nil {
		t.Fatalf("OnReport: %v", err)
	}
	if out := logs.String(); strings.Contains(out, "task.unbound") {
		t.Fatalf("the ordinary settle emitted an unbound line:\n%s", out)
	}
}
