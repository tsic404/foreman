package observability

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// TestOpsReadFaceIsSourceQualified pins the S31/read partition: two registered
// servers expose the same workspace id, so a workspace-only query is
// ambiguous (400), a source-qualified credential reads only its own pair, and
// S31 crops by the "server/workspace" key the view actually uses.
func TestOpsReadFaceIsSourceQualified(t *testing.T) {
	reg := registry.New(time.Now)
	for _, e := range []registry.TaskEntry{
		{TaskID: "task-1", SourceID: "src-a", WorkspaceID: "ws-shared", JobName: "fm-task-1-a", State: registry.StateRunning},
		{TaskID: "task-1", SourceID: "src-b", WorkspaceID: "ws-shared", JobName: "fm-task-1-b", State: registry.StateRunning},
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.SourceID, err)
		}
	}
	wide := "ops-wide"
	scoped := "ops-src-a"
	h := NewOpsHandler(NewMetrics(), reg, nil, OpsConfig{
		Credentials: []OpsCredential{
			{Token: wide},
			{Token: scoped, Scopes: []Scope{{ServerID: "src-a", WorkspaceID: "ws-shared"}}},
		},
		Scopes: []Scope{
			{ServerID: "src-a", WorkspaceID: "ws-shared"},
			{ServerID: "src-b", WorkspaceID: "ws-shared"},
		},
		RegisterSource: func() map[string]any {
			return map[string]any{
				"src-a/ws-shared": map[string]any{"declaration": "a"},
				"src-b/ws-shared": map[string]any{"declaration": "b"},
			}
		},
	})
	call := func(token, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// The source-qualified credential reads its own pair.
	rec := call(scoped, "/foreman/tasks?source=src-a&workspace=ws-shared")
	if rec.Code != http.StatusOK {
		t.Fatalf("scoped list = %d: %s", rec.Code, rec.Body)
	}
	var rows []struct {
		TaskID   string `json:"task_id"`
		SourceID string `json:"source_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(rows) != 1 || rows[0].SourceID != "src-a" {
		t.Fatalf("scoped list = %+v, want exactly the src-a attempt", rows)
	}
	// The same credential naming the other source is outside its grant.
	if rec := call(scoped, "/foreman/tasks?source=src-b&workspace=ws-shared"); rec.Code != http.StatusNotFound {
		t.Fatalf("other source with the src-a credential = %d, want 404", rec.Code)
	}

	// A bare workspace id is ambiguous across the two served sources.
	if rec := call(wide, "/foreman/tasks?workspace=ws-shared"); rec.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous bare workspace = %d, want 400", rec.Code)
	}
	// The wide credential resolves each attempt when it names the source.
	rec = call(wide, "/foreman/tasks/task-1?source=src-b&workspace=ws-shared")
	if rec.Code != http.StatusOK {
		t.Fatalf("src-b detail = %d: %s", rec.Code, rec.Body)
	}
	var detail struct {
		TaskID   string `json:"task_id"`
		SourceID string `json:"source_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail.SourceID != "src-b" {
		t.Fatalf("detail source = %q, want src-b", detail.SourceID)
	}

	// S31 is cropped by the same pair, not by the bare workspace.
	rec = call(scoped, "/foreman/register-source")
	if rec.Code != http.StatusOK {
		t.Fatalf("register-source = %d: %s", rec.Code, rec.Body)
	}
	var cropped map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &cropped); err != nil {
		t.Fatalf("decode register-source: %v", err)
	}
	if len(cropped) != 1 || cropped["src-a/ws-shared"] == nil {
		t.Fatalf("register-source = %v, want only src-a/ws-shared", cropped)
	}
}

// TestLegacyUnqualifiedGrantStaysUnique pins R4: a grant that names only a
// workspace was written for the deployment's single source. It keeps that
// meaning while exactly one served source holds the id; once a second source
// exposes the same id the grant is ambiguous and must yield nothing for either
// source. The request's `source` query selects what to read — it can never
// repair the widened grant, so naming src-a with the old credential is still
// a not-found.
func TestLegacyUnqualifiedGrantStaysUnique(t *testing.T) {
	reg := registry.New(time.Now)
	for _, e := range []registry.TaskEntry{
		{TaskID: "task-1", SourceID: "src-a", WorkspaceID: "ws-shared", JobName: "fm-task-1-a", State: registry.StateRunning},
		{TaskID: "task-1", SourceID: "src-b", WorkspaceID: "ws-shared", JobName: "fm-task-1-b", State: registry.StateRunning},
	} {
		if err := reg.Put(e); err != nil {
			t.Fatalf("Put %s: %v", e.SourceID, err)
		}
	}

	const legacy, explicit = "ops-legacy", "ops-src-b"
	handlerFor := func(served []Scope) *OpsHandler {
		return NewOpsHandler(NewMetrics(), reg, nil, OpsConfig{
			Credentials: []OpsCredential{
				{Token: legacy, Scopes: []Scope{{WorkspaceID: "ws-shared"}}},
				{Token: explicit, Scopes: []Scope{{ServerID: "src-b", WorkspaceID: "ws-shared"}}},
			},
			Scopes: served,
			RegisterSource: func() map[string]any {
				return map[string]any{
					"src-a/ws-shared": map[string]any{"declaration": "a"},
					"src-b/ws-shared": map[string]any{"declaration": "b"},
				}
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

	// The deployment serves one source: the legacy grant still reads it.
	single := handlerFor([]Scope{{ServerID: "src-a", WorkspaceID: "ws-shared"}})
	rec := call(single, legacy, "/foreman/tasks?workspace=ws-shared")
	if rec.Code != http.StatusOK {
		t.Fatalf("single-source legacy list = %d: %s", rec.Code, rec.Body)
	}
	var rows []struct {
		SourceID string `json:"source_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(rows) != 1 || rows[0].SourceID != "src-a" {
		t.Fatalf("single-source legacy list = %+v, want exactly the src-a attempt", rows)
	}

	// A second source now exposes the same workspace id. The old grant names
	// no source, so it grants neither — even when the request names src-a.
	multi := handlerFor([]Scope{
		{ServerID: "src-a", WorkspaceID: "ws-shared"},
		{ServerID: "src-b", WorkspaceID: "ws-shared"},
	})
	// The source-named read is the discriminating case: if the old grant were
	// widened to both sources, naming src-a would return its row — the request
	// would have repaired the grant. It must be a not-found instead.
	for _, path := range []string{
		"/foreman/tasks?source=src-a&workspace=ws-shared",
		"/foreman/tasks/task-1?source=src-a&workspace=ws-shared",
		"/foreman/tasks?workspace=ws-shared",
	} {
		if rec := call(multi, legacy, path); rec.Code != http.StatusNotFound {
			t.Fatalf("ambiguous legacy grant %s = %d, want 404: %s", path, rec.Code, rec.Body)
		}
	}
	rec = call(multi, legacy, "/foreman/register-source")
	if rec.Code != http.StatusOK {
		t.Fatalf("register-source = %d: %s", rec.Code, rec.Body)
	}
	var cropped map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &cropped); err != nil {
		t.Fatalf("decode register-source: %v", err)
	}
	if len(cropped) != 0 {
		t.Fatalf("ambiguous legacy grant saw %v, want no source at all", cropped)
	}

	// An explicit source grant keeps its own positive and negative cases.
	rec = call(multi, explicit, "/foreman/tasks?source=src-b&workspace=ws-shared")
	if rec.Code != http.StatusOK {
		t.Fatalf("explicit grant list = %d: %s", rec.Code, rec.Body)
	}
	rows = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode explicit list: %v", err)
	}
	if len(rows) != 1 || rows[0].SourceID != "src-b" {
		t.Fatalf("explicit grant list = %+v, want exactly the src-b attempt", rows)
	}
	if rec := call(multi, explicit, "/foreman/tasks?source=src-a&workspace=ws-shared"); rec.Code != http.StatusNotFound {
		t.Fatalf("explicit grant reading src-a = %d, want 404", rec.Code)
	}
}
