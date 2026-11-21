package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

type scopedRuntimeCall struct {
	Method string
	Path   string
	Body   []byte
}

// scopedRuntimeUpstream is the minimal Multica server these tests need: one C1
// answer with two runtimes, then a verbatim record of what S8/C19 sent.
type scopedRuntimeUpstream struct {
	mu     sync.Mutex
	reqs   []scopedRuntimeCall
	routes map[string]func(w http.ResponseWriter, body []byte)
}

func newScopedRuntimeUpstream(t *testing.T) (*httptest.Server, *scopedRuntimeUpstream) {
	t.Helper()
	stub := &scopedRuntimeUpstream{routes: map[string]func(http.ResponseWriter, []byte){}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		if r.Body != nil {
			body, _ = io.ReadAll(r.Body)
		}
		stub.mu.Lock()
		stub.reqs = append(stub.reqs, scopedRuntimeCall{Method: r.Method, Path: r.URL.Path, Body: body})
		h := stub.routes[r.Method+" "+r.URL.Path]
		stub.mu.Unlock()
		if h == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		h(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, stub
}

func (s *scopedRuntimeUpstream) on(method, path string, h func(http.ResponseWriter, []byte)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routes[method+" "+path] = h
}

func (s *scopedRuntimeUpstream) calls(method, path string) []scopedRuntimeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []scopedRuntimeCall
	for _, r := range s.reqs {
		if r.Method == method && r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

func writeScopedJSON(w http.ResponseWriter, code int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

// twoRuntimeDiscovery declares two runtimes of one provider, so the identity
// pairing is (type, profile_id) and the C1 answer can bind them in order.
func twoRuntimeDiscovery() registry.Discovery {
	return registry.Discovery{
		CLIVersion: "18.0.0",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "rt-one", Type: "omp", Version: "18.0.0", ProfileID: "p1"},
			{Name: "rt-two", Type: "omp", Version: "18.0.0", ProfileID: "p2"},
		},
	}
}

// registerTwoRuntimes answers C1 with the two identities in declaration order.
func registerTwoRuntimes(stub *scopedRuntimeUpstream, ids [2]string) {
	stub.on("POST", "/api/daemon/register", func(w http.ResponseWriter, _ []byte) {
		writeScopedJSON(w, 200, map[string]any{
			"runtimes": []map[string]any{
				{"id": ids[0], "name": "rt-one", "provider": "omp", "status": "online", "profile_id": "p1"},
				{"id": ids[1], "name": "rt-two", "provider": "omp", "status": "online", "profile_id": "p2"},
			},
			"repos":         []map[string]string{},
			"repos_version": "abc123",
			"settings":      nil,
		})
	})
}

// TestFrozenRuntimeIsResolvedInsideTheTasksSourceScope pins R3a on the
// production callback path: two servers expose the same task id, this client's
// source has several runtimes, and the attempt was dispatched under the second
// one. S8 and C19 must both carry that frozen runtime id — the first declared
// runtime is another runtime of this source and is the wrong answer.
func TestFrozenRuntimeIsResolvedInsideTheTasksSourceScope(t *testing.T) {
	reg := registry.New(time.Now)
	for _, e := range []registry.TaskEntry{
		{TaskID: "task-1", SourceID: "src-a", WorkspaceID: "ws-shared", JobName: "fm-task-1-a", DaemonID: "fm-task-1-src-a", RealRuntimeID: "rid-a1", State: registry.StateRunning},
		{TaskID: "task-1", SourceID: "src-b", WorkspaceID: "ws-shared", JobName: "fm-task-1-b", DaemonID: "fm-task-1-src-b", RealRuntimeID: "rid-b2", State: registry.StateRunning},
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.SourceID, err)
		}
	}

	srv, stub := newScopedRuntimeUpstream(t)
	registerTwoRuntimes(stub, [2]string{"rid-b1", "rid-b2"})
	stub.on("POST", "/api/daemon/tasks/task-1/start", func(w http.ResponseWriter, _ []byte) {
		writeScopedJSON(w, 200, map[string]string{"status": "running"})
	})
	stub.on("POST", "/api/daemon/runtimes/rid-b2/tasks/task-1/skill-bundles/resolve", func(w http.ResponseWriter, _ []byte) {
		writeScopedJSON(w, 200, map[string]string{"bundle": "data"})
	})

	client := proxy.NewClient(proxy.Config{
		ServerURL:   srv.URL,
		SourceID:    "src-b",
		WorkspaceID: "ws-shared",
		DaemonID:    "foreman-b",
		Version:     "0.1.0",
	}, "mdt_test", taskRuntimeResolver(reg, "src-b", "ws-shared"))
	discovery := twoRuntimeDiscovery()
	if err := client.RegisterWithDiscovery(t.Context(), discovery); err != nil {
		t.Fatalf("RegisterWithDiscovery: %v", err)
	}
	if ids := client.RuntimeIDs(); len(ids) != 2 || ids[0] != "rid-b1" || ids[1] != "rid-b2" {
		t.Fatalf("RuntimeIDs = %v, want [rid-b1 rid-b2]", ids)
	}

	// S8: the daemon names the Job runtime it was handed at S6; the frozen
	// record for this attempt is rid-b2.
	body := []byte(`{"capabilities":["x"],"runtime_id":"job-runtime","dispatched_at":"2026-10-12T10:54:12.016974Z"}`)
	code, _, err := client.Forward(t.Context(), scheduler.EPStart, "task-1", body)
	if err != nil || code != 200 {
		t.Fatalf("Forward start = %d, %v", code, err)
	}
	startCalls := stub.calls("POST", "/api/daemon/tasks/task-1/start")
	if len(startCalls) != 1 {
		t.Fatalf("start calls = %d, want 1", len(startCalls))
	}
	var sent struct {
		RuntimeID string `json:"runtime_id"`
	}
	if err := json.Unmarshal(startCalls[0].Body, &sent); err != nil {
		t.Fatalf("start body: %v (%s)", err, startCalls[0].Body)
	}
	if sent.RuntimeID != "rid-b2" {
		t.Fatalf("S8 runtime_id = %q, want the attempt's frozen rid-b2 (not this source's first runtime rid-b1)", sent.RuntimeID)
	}

	// C19: the skill-bundle URL carries the same frozen runtime id.
	code, resp, err := client.ResolveSkillBundles(t.Context(), "task-1", []byte(`{"skills":["a"]}`))
	if err != nil || code != 200 {
		t.Fatalf("ResolveSkillBundles = %d, %v (%s)", code, err, resp)
	}
	if calls := stub.calls("POST", "/api/daemon/runtimes/rid-b1/tasks/task-1/skill-bundles/resolve"); len(calls) != 0 {
		t.Fatalf("C19 was sent to this source's first runtime rid-b1, not the frozen rid-b2")
	}
	if calls := stub.calls("POST", "/api/daemon/runtimes/rid-b2/tasks/task-1/skill-bundles/resolve"); len(calls) != 1 {
		t.Fatalf("C19 calls to rid-b2 = %d, want 1", len(calls))
	}
}
