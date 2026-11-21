package rules

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tsic404/foreman/internal/catalog"
)

func testAdmin(t *testing.T, service *Service, admit bool) *AdminHandler {
	t.Helper()
	handler, err := NewAdminHandler(AdminConfig{
		Service: service,
		Grant: func(*http.Request) (catalog.Grant, bool) {
			return catalog.Grant{}, admit
		},
		Actor: func(*http.Request) string { return "test" },
	})
	if err != nil {
		t.Fatalf("NewAdminHandler: %v", err)
	}
	return handler
}

func request(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode request: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, reader))
	return recorder
}

func TestAdminPublishRefusesTheConflictAndPreviewStaysReadOnly(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1), envView("build-b", 1), envView("build-c", 1))
	service := newTestService(t, envs, testDirectory())
	handler := testAdmin(t, service, true)

	if got := request(t, handler, http.MethodPut, "/admin/rules/draft", draftRequest{
		BaseRevision: 0, Rules: []Rule{combinedRule(), agentRule(), projectRule()},
	}).Code; got != http.StatusOK {
		t.Fatalf("draft status = %d, want 200", got)
	}
	if got := request(t, handler, http.MethodPost, "/admin/rules/publish", publishRequest{BaseRevision: 0}).Code; got != http.StatusOK {
		t.Fatalf("publish status = %d, want 200", got)
	}

	recorder := request(t, handler, http.MethodPost, "/admin/rules/preview", Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("preview status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var resolution Resolution
	if err := json.Unmarshal(recorder.Body.Bytes(), &resolution); err != nil {
		t.Fatalf("decode preview: %v", err)
	}
	if resolution.Outcome != OutcomeReady || resolution.Environment == nil || resolution.Environment.ID != "build-a" {
		t.Fatalf("preview = %+v, want ready on build-a", resolution)
	}
	if envs.acquires != 0 {
		t.Fatalf("preview reserved %d slots", envs.acquires)
	}

	if got := request(t, handler, http.MethodPut, "/admin/rules/draft", draftRequest{
		BaseRevision: 1, Rules: []Rule{agentRule(), projectRule()},
	}).Code; got != http.StatusOK {
		t.Fatalf("conflicted draft status = %d, want 200 (a draft may hold findings)", got)
	}
	recorder = request(t, handler, http.MethodPost, "/admin/rules/publish", publishRequest{BaseRevision: 1})
	if recorder.Code != http.StatusConflict {
		t.Fatalf("publish status = %d, want 409: %s", recorder.Code, recorder.Body.String())
	}
	var refusal struct {
		Error    string    `json:"error"`
		Findings []Finding `json:"findings"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &refusal); err != nil {
		t.Fatalf("decode refusal: %v", err)
	}
	if len(refusal.Findings) == 0 || refusal.Findings[0].Code != "rule_conflict" {
		t.Fatalf("refusal findings = %+v, want rule_conflict", refusal.Findings)
	}
}

func TestAdminFailsClosedWithoutAGrant(t *testing.T) {
	service := newTestService(t, newFakeEnvironments(envView("build-a", 1)), testDirectory())
	handler := testAdmin(t, service, false)
	if got := request(t, handler, http.MethodGet, "/admin/rules", nil).Code; got != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", got)
	}
	if got := request(t, handler, http.MethodPost, "/admin/readiness", Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}).Code; got != http.StatusUnauthorized {
		t.Fatalf("readiness status = %d, want 401", got)
	}
}

func TestAdminReadinessAnswersTheFiveStateReportWithoutReserving(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	service := newTestService(t, envs, testDirectory())
	handler := testAdmin(t, service, true)
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	recorder := request(t, handler, http.MethodPost, "/admin/readiness", Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("readiness status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var report ReadinessReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode readiness: %v", err)
	}
	if report.Outcome != OutcomeReady || report.RuleID != "a5" || len(report.Checks) != 10 {
		t.Fatalf("readiness = %+v", report.Resolution)
	}
	if envs.acquires != 0 || envs.releases != 0 {
		t.Fatalf("readiness reserved/released slots: %d/%d", envs.acquires, envs.releases)
	}
}
