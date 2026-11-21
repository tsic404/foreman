// TC-tech-multiws-02 and TC-tech-multiws-03 implement the source-partition
// face of the workspace-isolation contract: two Multica servers expose the
// same workspace id, and every runtime-face lookup, token adjudication,
// workspace-state replay and skill-cache entry stays on the server the token
// came from.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

const sourceWorkspaceID = "ws-shared"

type sourceCall struct {
	method string
	path   string
	auth   string
	body   []byte
}

// sourceStub is one Multica server: it answers C1 with its own runtime ids,
// C22 with its own settings, C13 and C19, and records every call so a
// scenario can prove which source Foreman used.
type sourceStub struct {
	id       string
	settings string
	bundle   string

	mu    sync.Mutex
	calls []sourceCall
	srv   *httptest.Server
}

func newSourceStub(t *testing.T, id, settings, bundle string) *sourceStub {
	t.Helper()
	stub := &sourceStub{id: id, settings: settings, bundle: bundle}
	stub.srv = httptest.NewServer(http.HandlerFunc(stub.serve))
	t.Cleanup(stub.srv.Close)
	return stub
}

func (s *sourceStub) serve(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(io.LimitReader(r.Body, 1<<20))
	}
	s.mu.Lock()
	s.calls = append(s.calls, sourceCall{
		method: r.Method,
		path:   r.URL.Path,
		auth:   r.Header.Get("Authorization"),
		body:   body,
	})
	s.mu.Unlock()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/daemon/register":
		var req struct {
			Runtimes []struct {
				Name      string `json:"name"`
				Type      string `json:"type"`
				ProfileID string `json:"profile_id"`
			} `json:"runtimes"`
		}
		_ = json.Unmarshal(body, &req)
		answers := make([]map[string]any, 0, len(req.Runtimes))
		for i, rt := range req.Runtimes {
			answers = append(answers, map[string]any{
				"id":         fmt.Sprintf("rid-%s-%d", s.id, i),
				"name":       rt.Name,
				"provider":   rt.Type,
				"status":     "online",
				"profile_id": nil,
			})
		}
		writeStubJSON(w, map[string]any{
			"runtimes":      answers,
			"repos":         []any{},
			"repos_version": "v-" + s.id,
			"settings":      json.RawMessage(s.settings),
		})
	case r.Method == http.MethodGet && r.URL.Path == "/api/daemon/workspaces/"+sourceWorkspaceID+"/repos":
		writeStubJSON(w, map[string]any{
			"workspace_id":  sourceWorkspaceID,
			"repos":         []any{},
			"repos_version": "v-" + s.id,
			"settings":      json.RawMessage(s.settings),
		})
	case r.Method == http.MethodGet && r.URL.Path == "/api/daemon/tasks/task-"+s.id+"/status":
		writeStubJSON(w, map[string]any{"status": "running"})
	case r.Method == http.MethodPost:
		writeStubJSON(w, map[string]any{"bundle": s.bundle})
	default:
		http.NotFound(w, r)
	}
}

func writeStubJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *sourceStub) recorded() []sourceCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sourceCall(nil), s.calls...)
}

func (s *sourceStub) count(method, path string) int {
	n := 0
	for _, call := range s.recorded() {
		if call.method == method && call.path == path {
			n++
		}
	}
	return n
}

func newSourceEntry(t *testing.T, reg *registry.Registry, taskID, sourceID string) {
	t.Helper()
	daemonID := "fm-" + taskID
	err := reg.Put(registry.TaskEntry{
		TaskID:        taskID,
		SourceID:      sourceID,
		WorkspaceID:   sourceWorkspaceID,
		JobName:       daemonID,
		DaemonID:      daemonID,
		JobRuntimeID:  "job-runtime-" + sourceID,
		RealRuntimeID: "real-runtime-" + sourceID,
		State:         registry.StatePending,
		Payload:       json.RawMessage(`{"id":"` + taskID + `"}`),
	})
	if err != nil {
		t.Fatalf("put entry %s/%s: %v", sourceID, taskID, err)
	}
}

func sourceToken(t *testing.T, issuer interface {
	IssueInSource(jobName, taskID, sourceID, workspaceID string, ttl time.Duration) (string, error)
}, taskID, sourceID string) string {
	t.Helper()
	token, err := issuer.IssueInSource("fm-"+taskID, taskID, sourceID, sourceWorkspaceID, time.Hour)
	if err != nil {
		t.Fatalf("issue token %s/%s: %v", sourceID, taskID, err)
	}
	return token
}

func TestTechMultiWS02SourceIsolation(t *testing.T) {
	stubA := newSourceStub(t, "a", `{"marker":"a"}`, "bundle-a")
	stubB := newSourceStub(t, "b", `{"marker":"b"}`, "bundle-b")

	reg := registry.New(time.Now)
	issuer, err := proxy.JobTokenIssuer(bytes.Repeat([]byte("k"), 32), reg, nil, time.Hour, nil)
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}
	newSourceEntry(t, reg, "task-a", "src-a")
	newSourceEntry(t, reg, "task-b", "src-b")

	taskRuntime := func(taskID string) string {
		e, ok := reg.Get(taskID)
		if !ok {
			return ""
		}
		return e.RealRuntimeID
	}
	clientA := proxy.NewClient(proxy.Config{
		ServerURL: stubA.srv.URL, WorkspaceID: sourceWorkspaceID, DaemonID: "foreman-a", SourceID: "src-a",
		SkillCacheFreshTTL: time.Hour, SkillCacheRetention: time.Hour,
		SkillCacheMaxEntries: 16, SkillCacheMaxBytes: 1 << 20,
	}, "mdt_a", proxy.WithTaskRuntime(taskRuntime))
	clientB := proxy.NewClient(proxy.Config{
		ServerURL: stubB.srv.URL, WorkspaceID: sourceWorkspaceID, DaemonID: "foreman-b", SourceID: "src-b",
		SkillCacheFreshTTL: time.Hour, SkillCacheRetention: time.Hour,
		SkillCacheMaxEntries: 16, SkillCacheMaxBytes: 1 << 20,
	}, "mdt_b", proxy.WithTaskRuntime(taskRuntime))
	clients, err := proxy.NewClientSet(clientA, clientB)
	if err != nil {
		t.Fatalf("client set: %v", err)
	}
	server := proxy.NewFakeServer(issuer, reg, nil, clientA, nil, nil, proxy.WithClientSet(clients))
	jobHTTP := httptest.NewServer(server)
	t.Cleanup(jobHTTP.Close)

	call := func(method, path, token string, body []byte) (int, []byte) {
		req, err := http.NewRequest(method, jobHTTP.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, raw
	}

	// Seed each source's workspace state through its own C1.
	for _, c := range []*proxy.Client{clientA, clientB} {
		if err := c.RegisterWithDiscovery(context.Background(), registry.Discovery{
			CLIVersion: "1.0.0",
			Runtimes:   []registry.DiscoveredRuntime{{Name: "foreman-job", Type: "omp", Version: "1.0.0"}},
		}); err != nil {
			t.Fatalf("C1 %s: %v", c.SourceID(), err)
		}
	}

	// S9 routes by the token's source: two servers expose the same workspace
	// id, so a workspace-only lookup would pick the wrong client.
	tokenA := sourceToken(t, issuer, "task-a", "src-a")
	tokenB := sourceToken(t, issuer, "task-b", "src-b")
	if code, body := call(http.MethodGet, "/api/daemon/tasks/task-a/status", tokenA, nil); code != http.StatusOK {
		t.Fatalf("S9 A = %d (%s)", code, body)
	}
	if got := stubA.count(http.MethodGet, "/api/daemon/tasks/task-a/status"); got != 1 {
		t.Errorf("source A status calls = %d, want 1", got)
	}
	if got := stubB.count(http.MethodGet, "/api/daemon/tasks/task-a/status"); got != 0 {
		t.Errorf("source B answered source A's task %d times", got)
	}
	if code, body := call(http.MethodGet, "/api/daemon/tasks/task-b/status", tokenB, nil); code != http.StatusOK {
		t.Fatalf("S9 B = %d (%s)", code, body)
	}
	if got := stubB.count(http.MethodGet, "/api/daemon/tasks/task-b/status"); got != 1 {
		t.Errorf("source B status calls = %d, want 1", got)
	}
	if got := stubA.count(http.MethodGet, "/api/daemon/tasks/task-b/status"); got != 0 {
		t.Errorf("source A answered source B's task %d times", got)
	}

	// S2 replays the state seeded by the token's own source. The two servers
	// use the same workspace id, so a shared carrier would answer B's settings
	// to A (V2-AC08).
	for _, tc := range []struct {
		token  string
		marker string
	}{
		{tokenA, "a"},
		{tokenB, "b"},
	} {
		code, body := call(http.MethodGet, "/api/daemon/workspaces/"+sourceWorkspaceID+"/repos", tc.token, nil)
		if code != http.StatusOK {
			t.Fatalf("S2 %s = %d (%s)", tc.marker, code, body)
		}
		var answer map[string]json.RawMessage
		if err := json.Unmarshal(body, &answer); err != nil {
			t.Fatalf("decode S2 %s: %v", tc.marker, err)
		}
		if !bytes.Contains(answer["settings"], []byte(`"marker":"`+tc.marker+`"`)) {
			t.Errorf("S2 %s settings = %s, want its own source's settings", tc.marker, answer["settings"])
		}
	}

	// S26's cache is keyed by the authorization domain as well as the payload:
	// the same resolve body on two sources must reach both upstreams.
	payload := []byte(`{"skills":["same-payload"]}`)
	for _, tc := range []struct {
		token    string
		sourceID string
		taskID   string
		jobRtID  string
		realRtID string
		wantBody string
	}{
		{tokenA, "src-a", "task-a", "job-runtime-src-a", "real-runtime-src-a", "bundle-a"},
		{tokenB, "src-b", "task-b", "job-runtime-src-b", "real-runtime-src-b", "bundle-b"},
	} {
		path := fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/skill-bundles/resolve", tc.jobRtID, tc.taskID)
		code, body := call(http.MethodPost, path, tc.token, payload)
		if code != http.StatusOK {
			t.Fatalf("S26 %s = %d (%s)", tc.sourceID, code, body)
		}
		if !bytes.Contains(body, []byte(`"bundle":"`+tc.wantBody+`"`)) {
			t.Errorf("S26 %s = %s, want its own source's bundle", tc.sourceID, body)
		}
		wantPath := fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/skill-bundles/resolve", tc.realRtID, tc.taskID)
		stub := stubA
		if tc.sourceID == "src-b" {
			stub = stubB
		}
		if got := stub.count(http.MethodPost, wantPath); got != 1 {
			t.Errorf("source %s resolve calls = %d, want 1", tc.sourceID, got)
		}
	}
	if got := stubA.count(http.MethodPost, "/api/daemon/runtimes/real-runtime-src-b/tasks/task-b/skill-bundles/resolve"); got != 0 {
		t.Errorf("source A answered source B's resolve %d times", got)
	}
	if got := stubB.count(http.MethodPost, "/api/daemon/runtimes/real-runtime-src-a/tasks/task-a/skill-bundles/resolve"); got != 0 {
		t.Errorf("source B answered source A's resolve %d times", got)
	}

	// A token whose payload names source A can never adjudicate source B's
	// Job, even when both servers expose the same workspace id.
	crossToken, err := issuer.IssueInSource("fm-task-b", "task-b", "src-a", sourceWorkspaceID, time.Hour)
	if err != nil {
		t.Fatalf("cross-source token: %v", err)
	}
	if code, body := call(http.MethodGet, "/api/daemon/tasks/task-b/status", crossToken, nil); code != http.StatusUnauthorized {
		t.Errorf("cross-source token = %d (%s), want 401", code, body)
	}
	if got := stubA.count(http.MethodGet, "/api/daemon/tasks/task-b/status"); got != 0 {
		t.Errorf("cross-source token reached source A %d times", got)
	}
}

func TestTechMultiWS02JobRebuildKeepsSource(t *testing.T) {
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fm-task-a",
			Namespace: "agents",
			Labels: map[string]string{
				registry.LabelTaskID:      "task-a",
				registry.LabelAgentID:     "agent-a",
				registry.LabelSourceID:    "src-a",
				registry.LabelWorkspaceID: sourceWorkspaceID,
			},
			Annotations: map[string]string{
				registry.AnnoDaemonID:     "fm-task-a",
				registry.AnnoJobRuntimeID: "job-runtime-src-a",
				registry.AnnoAttempt:      "1",
			},
		},
	}
	entry, err := registry.TaskEntryFromJob(job)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if entry.SourceID != "src-a" {
		t.Errorf("rebuilt source = %q, want src-a", entry.SourceID)
	}
	if got, want := entry.ScopeKey(), "src-a\x00"+sourceWorkspaceID; got != want {
		t.Errorf("rebuilt scope key = %q, want %q", got, want)
	}
}
