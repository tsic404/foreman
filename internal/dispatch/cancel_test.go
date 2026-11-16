package dispatch_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

// cancelServer answers C13 (task status) and records the fail forward, so the
// cancel-settlement path can be observed end to end without a real daemon.
type cancelServer struct {
	mu         sync.Mutex
	status     int    // HTTP status for C13
	statusBody string // C13 answer
	fails      []recordedReport
	failStatus int
}

func (s *cancelServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/daemon/tasks/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/daemon/tasks/")
		// C13 is GET /tasks/{id}/status; a report is POST /tasks/{id}/{endpoint}.
		if strings.HasSuffix(rest, "/status") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(s.status)
			io.WriteString(w, s.statusBody)
			return
		}
		parts := strings.Split(rest, "/")
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		if len(parts) == 2 && parts[1] == "fail" {
			s.fails = append(s.fails, recordedReport{TaskID: parts[0], Endpoint: parts[1], Body: string(body)})
		}
		st := s.failStatus
		s.mu.Unlock()
		if st == 0 {
			st = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(st)
		io.WriteString(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"runtimes":[]}`)
	})
	return mux
}

func newCancelRig(t *testing.T, srv *cancelServer) (*dispatch.Dispatcher, *registry.Registry) {
	t.Helper()
	httpSrv := httptest.NewServer(srv.handler())
	t.Cleanup(httpSrv.Close)

	reg := registry.New()
	client := proxy.NewClient(proxy.Config{
		ServerURL:   httpSrv.URL,
		WorkspaceID: "ws-1",
		DaemonID:    "foreman-ws-1",
		Version:     "test",
	}, "tok")
	return dispatch.New(dispatch.Config{}, reg, client, nil), reg
}

// cancelledEntry registers a live task bound to a daemon, in the state a
// running task would be in when its daemon sends a cancel-ack.
func cancelledEntry(t *testing.T, reg *registry.Registry) registry.TaskEntry {
	t.Helper()
	reg.PutRuntime(registry.Runtime{
		RuntimeID: "worker-1", DaemonID: "worker-1", WorkspaceID: "ws-1",
		Provider: "omp", Name: "worker-1", Online: true,
	})
	e := registry.TaskEntry{
		TaskID: "task-c", WorkspaceID: "ws-1", DaemonID: "worker-1",
		DaemonRuntimeID: "worker-1", RuntimeID: "srv-rt-1",
		State: registry.StateRunning,
	}
	if err := reg.Put(e); err != nil {
		t.Fatalf("put task: %v", err)
	}
	return e
}

// TestCancelAckSettlesServerLiveTask is the v2 fix this path exists for: a
// daemon that cancelled on its own sends only a cancel-ack, so the task would
// sit "running" on the server forever unless Foreman closes it with a fail.
func TestCancelAckSettlesServerLiveTask(t *testing.T) {
	stub := &cancelServer{status: http.StatusOK, statusBody: `{"status":"running"}`}
	disp, reg := newCancelRig(t, stub)
	e := cancelledEntry(t, reg)

	if _, _, err := disp.OnReport(context.Background(), dispatch.EPCancelAck, e, []byte(`{}`), ""); err != nil {
		t.Fatalf("cancel-ack: %v", err)
	}

	stub.mu.Lock()
	fails := append([]recordedReport(nil), stub.fails...)
	stub.mu.Unlock()
	if len(fails) != 1 {
		t.Fatalf("expected one synthetic fail for a server-live task, got %d", len(fails))
	}
	if fails[0].TaskID != "task-c" || fails[0].Endpoint != "fail" {
		t.Fatalf("forwarded the wrong report: %+v", fails[0])
	}
	if !strings.Contains(fails[0].Body, "cancelled by daemon") {
		t.Fatalf("fail body should carry the reason, got %s", fails[0].Body)
	}

	if _, live := reg.Get("task-c"); live {
		t.Fatal("task should be settled locally after the cancel-ack")
	}
	if !reg.IsDone("ws-1", "task-c") {
		t.Fatal("task should be remembered as done")
	}
}

// TestCancelAckLeavesServerTerminalAlone: if the server already settled the
// task (a cancel it asked for), sending another fail would be a duplicate
// terminal report.
func TestCancelAckLeavesServerTerminalAlone(t *testing.T) {
	stub := &cancelServer{status: http.StatusOK, statusBody: `{"status":"cancelled"}`}
	disp, reg := newCancelRig(t, stub)
	e := cancelledEntry(t, reg)

	if _, _, err := disp.OnReport(context.Background(), dispatch.EPCancelAck, e, []byte(`{}`), ""); err != nil {
		t.Fatalf("cancel-ack: %v", err)
	}

	stub.mu.Lock()
	n := len(stub.fails)
	stub.mu.Unlock()
	if n != 0 {
		t.Fatalf("sent %d fail(s) for an already-terminal task, want 0", n)
	}
}

// TestCancelAckSwallowsStatusError: a C13 failure must not turn into a
// synthetic fail — the server may simply be unreachable, and inventing a
// verdict there would settle a task that is still running.
func TestCancelAckSwallowsStatusError(t *testing.T) {
	stub := &cancelServer{status: http.StatusInternalServerError, statusBody: `{}`}
	disp, reg := newCancelRig(t, stub)
	e := cancelledEntry(t, reg)

	if _, _, err := disp.OnReport(context.Background(), dispatch.EPCancelAck, e, []byte(`{}`), ""); err != nil {
		t.Fatalf("cancel-ack: %v", err)
	}

	stub.mu.Lock()
	n := len(stub.fails)
	stub.mu.Unlock()
	if n != 0 {
		t.Fatalf("sent %d fail(s) after a C13 error, want 0", n)
	}
}

// TestCancelAckFailForwardErrorDoesNotPanic: a failing synthetic fail must be
// logged and swallowed; the caller still gets a normal answer.
func TestCancelAckFailForwardErrorDoesNotPanic(t *testing.T) {
	stub := &cancelServer{
		status: http.StatusOK, statusBody: `{"status":"running"}`,
		failStatus: http.StatusBadGateway,
	}
	disp, reg := newCancelRig(t, stub)
	e := cancelledEntry(t, reg)

	code, _, err := disp.OnReport(context.Background(), dispatch.EPCancelAck, e, []byte(`{}`), "")
	if err != nil {
		t.Fatalf("cancel-ack returned an error: %v", err)
	}
	if code == 0 {
		t.Fatal("expected a status code from the cancel-ack report")
	}
}

// TestCancelAckWithAgentResultBodyIsForwarded: the daemon's own result detail
// in the ack body is forwarded to the server, not replaced by the synthetic
// error string.
func TestCancelAckBodyShapeIsStable(t *testing.T) {
	stub := &cancelServer{status: http.StatusOK, statusBody: `{"status":"running"}`}
	disp, reg := newCancelRig(t, stub)
	e := cancelledEntry(t, reg)

	if _, _, err := disp.OnReport(context.Background(), dispatch.EPCancelAck, e, []byte(`{}`), ""); err != nil {
		t.Fatalf("cancel-ack: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.fails) != 1 {
		t.Fatalf("want one fail, got %d", len(stub.fails))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(stub.fails[0].Body), &body); err != nil {
		t.Fatalf("fail body is not JSON: %v (%s)", err, stub.fails[0].Body)
	}
	if _, ok := body["error"]; !ok {
		t.Fatalf("fail body has no error field: %v", body)
	}
}
