// Package platform holds Foreman's read-only queries of the Multica server's
// user-facing API — today the project-identity lookups ADR-048 requires: a
// policy project name resolved to its platform project identifier once at
// startup, and an issue's project identifier read once per claim that needs
// it. The wire facts (paths, auth, response fields) are anchored in
// docs/upstream-anchors.md; this package invents no endpoint.
package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// ProjectQueries is the production adapter for the platform's two project
// identity queries (ADR-048 decisions 2/4): the policy name → project
// identifier resolution at startup and the issue → project identifier lookup
// at claim time. It implements jobbuilder.PolicyNameResolver and the
// scheduler's ProjectIdentity seam, and main wires the one value into both.
type ProjectQueries struct {
	baseURL     string
	workspaceID string
	token       string
	client      *http.Client
}

// NewProjectQueries builds the adapter over the same server credential and
// workspace the rest of Foreman runs with (MULTICA_SERVER_URL /
// MULTICA_WORKSPACE_ID / MULTICA_TOKEN).
func NewProjectQueries(baseURL, workspaceID, token string) *ProjectQueries {
	return &ProjectQueries{
		baseURL:     strings.TrimRight(baseURL, "/"),
		workspaceID: workspaceID,
		token:       token,
		// No client-level timeout: every call carries the caller's own
		// bounded context (the startup resolution window / the claim →
		// Job budget), which is the bound the contract names.
		client: &http.Client{},
	}
}

// FromPayload reads the claim payload's top-level project_id. It never reads
// project_title / project_description: those are prompt context, not the
// identity (ADR-048 decision 1).
func (q *ProjectQueries) FromPayload(task json.RawMessage) string {
	var head struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(task, &head); err != nil {
		return ""
	}
	return strings.TrimSpace(head.ProjectID)
}

// ResolveName answers the startup name resolution (ADR-048 decision 2). The
// project section resolves against the workspace's project list by exact title
// — the platform offers no name filter, and its human-readable project name is
// the title. The agent section has no anchored workspace-wide query (the
// agents list is visibility-filtered, so its absence answer is not the
// platform's authority), so agent keys read the documented unavailable
// degradation.
func (q *ProjectQueries) ResolveName(ctx context.Context, kind, name string) (jobbuilder.NameResolution, error) {
	if kind != jobbuilder.PolicyKeyProject {
		return jobbuilder.NameResolution{Conclusion: jobbuilder.NameUnavailable}, nil
	}
	projects, err := q.listProjects(ctx)
	if err != nil {
		return jobbuilder.NameResolution{}, err
	}
	var matches []string
	for _, project := range projects {
		if project.Title == name {
			matches = append(matches, project.ID)
		}
	}
	switch len(matches) {
	case 0:
		return jobbuilder.NameResolution{Conclusion: jobbuilder.NameAbsent}, nil
	case 1:
		return jobbuilder.NameResolution{Conclusion: jobbuilder.NameResolved, Identifier: matches[0]}, nil
	default:
		return jobbuilder.NameResolution{Conclusion: jobbuilder.NameAmbiguous}, nil
	}
}

// ByIssue reads one issue's project identifier (ADR-048 decision 4). A
// successful answer without a project — the field is null — is the empty
// string with no error: "no project" is a legal answer, not a failure. Any
// other non-2xx answer is an error, which the caller converges on
// project.lookup_failed.
func (q *ProjectQueries) ByIssue(ctx context.Context, issueID string) (string, error) {
	var issue struct {
		ProjectID *string `json:"project_id"`
	}
	query := url.Values{"workspace_id": {q.workspaceID}}
	if err := q.get(ctx, "/api/issues/"+url.PathEscape(issueID), query, &issue); err != nil {
		return "", err
	}
	if issue.ProjectID == nil {
		return "", nil
	}
	return strings.TrimSpace(*issue.ProjectID), nil
}

// projectRow is the answered project row's identity faces:
// docs/upstream-anchors.md anchors id + title.
type projectRow struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// listProjects is GET /api/projects?workspace_id=...: every project of the
// workspace, with no server-side name filter.
func (q *ProjectQueries) listProjects(ctx context.Context) ([]projectRow, error) {
	var answer struct {
		Projects []projectRow `json:"projects"`
	}
	query := url.Values{"workspace_id": {q.workspaceID}}
	if err := q.get(ctx, "/api/projects", query, &answer); err != nil {
		return nil, err
	}
	return answer.Projects, nil
}

// maxAnswerBytes bounds one read of a query answer; the queries answer small
// JSON documents, and a bound keeps a wrong or hostile answer from being
// buffered whole.
const maxAnswerBytes = 1 << 20

// get performs one authenticated read-only GET and decodes a JSON answer.
func (q *ProjectQueries) get(ctx context.Context, path string, query url.Values, out any) error {
	target := q.baseURL + path
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build %s request: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+q.token)
	req.Header.Set("Accept", "application/json")
	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBytes))
	if err != nil {
		return fmt.Errorf("GET %s: read answer: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: server answered %d: %s", path, resp.StatusCode, bodySnippet(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("GET %s: decode answer: %w", path, err)
	}
	return nil
}

// bodySnippet clips an error answer for a log line; the answer is server text,
// never a credential.
func bodySnippet(body []byte) string {
	const max = 160
	text := strings.TrimSpace(string(body))
	if len(text) > max {
		return text[:max] + "…"
	}
	return text
}

var _ jobbuilder.PolicyNameResolver = (*ProjectQueries)(nil)
