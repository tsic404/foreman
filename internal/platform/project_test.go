package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// recordedCall is one HTTP call the stub server observed.
type recordedCall struct {
	method string
	path   string
	query  string
	token  string
}

// projectServer answers the two anchored queries with programmable answers and
// records every call, so "no query happened" is an assertion.
type projectServer struct {
	mu          sync.Mutex
	calls       []recordedCall
	projects    []projectRow
	projectsErr int
	issueBody   string
	issueStatus int
}

func (s *projectServer) handler(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/projects", func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		if s.projectsErr != 0 {
			w.WriteHeader(s.projectsErr)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"projects": s.projects, "total": len(s.projects)})
	})
	mux.HandleFunc("GET /api/issues/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		status := s.issueStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(s.issueBody))
	})
	return mux
}

func (s *projectServer) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, recordedCall{
		method: r.Method, path: r.URL.Path, query: r.URL.RawQuery,
		token: r.Header.Get("Authorization"),
	})
}

func (s *projectServer) recorded() []recordedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedCall(nil), s.calls...)
}

func newProjectQueries(t *testing.T, server *projectServer) *ProjectQueries {
	t.Helper()
	httpServer := httptest.NewServer(server.handler(t))
	t.Cleanup(httpServer.Close)
	return NewProjectQueries(httpServer.URL, "ws-1", "mul_test-token")
}

func TestFromPayloadReadsTopLevelProjectID(t *testing.T) {
	queries := NewProjectQueries("http://127.0.0.1:1", "ws-1", "mul_x")
	cases := map[string]struct {
		payload string
		want    string
	}{
		"top-level identifier": {`{"id":"t1","project_id":"proj-1"}`, "proj-1"},
		"trimmed":              {`{"project_id":"  proj-1  "}`, "proj-1"},
		"no project":           {`{"id":"t1"}`, ""},
		"prompt context only":  {`{"project_title":"rffmpeg","project_description":"d"}`, ""},
		"malformed":            {`{`, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := queries.FromPayload(json.RawMessage(tc.payload)); got != tc.want {
				t.Errorf("FromPayload(%s) = %q, want %q", tc.payload, got, tc.want)
			}
		})
	}
}

func TestResolveNameProjectByExactTitle(t *testing.T) {
	server := &projectServer{projects: []projectRow{
		{ID: "proj-rffmpeg", Title: "rffmpeg"},
		{ID: "proj-other", Title: "other"},
	}}
	queries := newProjectQueries(t, server)

	resolution, err := queries.ResolveName(context.Background(), "ws-1", jobbuilder.PolicyKeyProject, "rffmpeg")
	if err != nil {
		t.Fatalf("ResolveName: %v", err)
	}
	if resolution.Conclusion != jobbuilder.NameResolved || resolution.Identifier != "proj-rffmpeg" {
		t.Errorf("resolution = %q/%q, want resolved/proj-rffmpeg", resolution.Conclusion, resolution.Identifier)
	}

	absent, err := queries.ResolveName(context.Background(), "ws-1", jobbuilder.PolicyKeyProject, "rffmpe")
	if err != nil {
		t.Fatalf("ResolveName(absent): %v", err)
	}
	if absent.Conclusion != jobbuilder.NameAbsent {
		t.Errorf("absent conclusion = %q, want absent (exact equality, no prefix match)", absent.Conclusion)
	}

	calls := server.recorded()
	if len(calls) != 2 {
		t.Fatalf("calls = %v, want one project list per key", calls)
	}
	for _, call := range calls {
		if call.path != "/api/projects" || call.query != "workspace_id=ws-1" {
			t.Errorf("call = %s %s?%s, want GET /api/projects?workspace_id=ws-1", call.method, call.path, call.query)
		}
		if call.token != "Bearer mul_test-token" {
			t.Errorf("Authorization = %q, want the bearer server credential", call.token)
		}
	}
}

func TestResolveNameAmbiguousTitle(t *testing.T) {
	server := &projectServer{projects: []projectRow{
		{ID: "proj-1", Title: "same"},
		{ID: "proj-2", Title: "same"},
	}}
	queries := newProjectQueries(t, server)
	resolution, err := queries.ResolveName(context.Background(), "ws-1", jobbuilder.PolicyKeyProject, "same")
	if err != nil {
		t.Fatalf("ResolveName: %v", err)
	}
	if resolution.Conclusion != jobbuilder.NameAmbiguous || resolution.Identifier != "" {
		t.Errorf("resolution = %q/%q, want ambiguous without an identifier", resolution.Conclusion, resolution.Identifier)
	}
}

func TestResolveNameAgentSectionIsUnavailableWithoutQuery(t *testing.T) {
	server := &projectServer{}
	queries := newProjectQueries(t, server)
	resolution, err := queries.ResolveName(context.Background(), "ws-1", jobbuilder.PolicyKeyAgent, "Vulcan")
	if err != nil {
		t.Fatalf("ResolveName: %v", err)
	}
	if resolution.Conclusion != jobbuilder.NameUnavailable {
		t.Errorf("agent resolution = %q, want unavailable (no anchored query)", resolution.Conclusion)
	}
	if calls := server.recorded(); len(calls) != 0 {
		t.Errorf("calls = %v, want none for the agent section", calls)
	}
}

func TestResolveNameListFailureIsAnError(t *testing.T) {
	server := &projectServer{projectsErr: http.StatusUnauthorized}
	queries := newProjectQueries(t, server)
	if _, err := queries.ResolveName(context.Background(), "ws-1", jobbuilder.PolicyKeyProject, "rffmpeg"); err == nil {
		t.Fatal("ResolveName accepted a non-200 answer")
	}
}

func TestByIssueReadsProjectID(t *testing.T) {
	cases := map[string]struct {
		status  int
		body    string
		want    string
		wantErr bool
	}{
		"project present": {body: `{"id":"issue-1","project_id":"proj-1"}`, want: "proj-1"},
		"no project":      {body: `{"id":"issue-1","project_id":null}`, want: ""},
		"missing field":   {body: `{"id":"issue-1"}`, want: ""},
		"server error":    {status: http.StatusInternalServerError, body: `{"error":"boom"}`, wantErr: true},
		"not found":       {status: http.StatusNotFound, body: `{"error":"not found"}`, wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := &projectServer{issueStatus: tc.status, issueBody: tc.body}
			queries := newProjectQueries(t, server)
			got, err := queries.ByIssue(context.Background(), "ws-1", "issue-1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("ByIssue error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("ByIssue = %q, want %q", got, tc.want)
			}
			calls := server.recorded()
			if len(calls) != 1 {
				t.Fatalf("calls = %v, want exactly one", calls)
			}
			if calls[0].path != "/api/issues/issue-1" || calls[0].query != "workspace_id=ws-1" {
				t.Errorf("call = %s %s?%s, want GET /api/issues/issue-1?workspace_id=ws-1",
					calls[0].method, calls[0].path, calls[0].query)
			}
			if !strings.HasPrefix(calls[0].token, "Bearer ") {
				t.Errorf("Authorization = %q, want a bearer credential", calls[0].token)
			}
		})
	}
}
