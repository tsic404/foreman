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
	"github.com/tsic404/foreman/internal/scope"
)

// ProjectQueries is the production adapter for the platform's two project
// identity queries (ADR-048 decisions 2/4): the policy name → project
// identifier resolution at startup and the issue → project identifier lookup
// at claim time. It implements jobbuilder.PolicyNameResolver and the
// scheduler's ProjectIdentity seam, and main wires the one value into both.
type ProjectQueries struct {
	baseURL string
	// credentials maps workspace id → that workspace's server credential
	// (§4.1: the lookup runs with the Job's workspace credential).
	credentials map[string]string
	// serverURLs overrides baseURL per source+workspace key.
	serverURLs map[string]string
	client     *http.Client
}

// NewProjectQueries builds the single-workspace adapter over the same server
// credential and workspace the rest of Foreman runs with (MULTICA_SERVER_URL /
// MULTICA_WORKSPACE_ID / MULTICA_TOKEN).
func NewProjectQueries(baseURL, workspaceID, token string) *ProjectQueries {
	return NewProjectQueriesFor(baseURL, map[string]string{workspaceID: token})
}

// NewProjectQueriesFor builds the adapter over one credential per configured
// workspace.
func NewProjectQueriesFor(baseURL string, credentials map[string]string) *ProjectQueries {
	return NewProjectQueriesForSources(baseURL, credentials, nil)
}

// NewProjectQueriesForSources builds the adapter with one credential and one
// upstream URL per source+workspace key (scope.Key). A source is the stable
// Multica server id; an empty source keeps the legacy workspace-only key.
func NewProjectQueriesForSources(baseURL string, credentials, serverURLs map[string]string) *ProjectQueries {
	return &ProjectQueries{
		baseURL:     strings.TrimRight(baseURL, "/"),
		credentials: credentials,
		serverURLs:  serverURLs,
		// No client-level timeout: every call carries the caller's own
		// bounded context (the startup resolution window / the claim →
		// Job budget), which is the bound the contract names.
		client: &http.Client{},
	}
}

// credentialFor resolves one source and workspace pair to its base URL,
// credential and workspace id. A raw workspace id without a source is the
// legacy flat form; when it matches several sources the answer is ambiguous
// and refuses rather than borrowing one server's credential.
func (q *ProjectQueries) credentialFor(sourceID, workspaceID string) (baseURL, token, ws string, ok bool) {
	key := scope.Key(sourceID, workspaceID)
	if token, found := q.credentials[key]; found {
		return q.baseURLFor(key), token, workspaceID, true
	}
	if sourceID != "" {
		return "", "", "", false
	}
	var (
		matchedKey   string
		matchedToken string
		matchedWS    string
		found        bool
	)
	for candidate, candidateToken := range q.credentials {
		_, candidateWS := scope.Split(candidate)
		if workspaceID != "" && candidateWS != workspaceID {
			continue
		}
		if found {
			// A bare workspace id that names several sources is not an
			// identity: fail closed instead of picking one.
			return "", "", "", false
		}
		found = true
		matchedKey, matchedToken, matchedWS = candidate, candidateToken, candidateWS
	}
	if !found {
		return "", "", "", false
	}
	return q.baseURLFor(matchedKey), matchedToken, matchedWS, true
}

// baseURLFor returns the upstream URL registered for a scope key, falling
// back to the deployment-level URL.
func (q *ProjectQueries) baseURLFor(key string) string {
	if url := strings.TrimRight(q.serverURLs[key], "/"); url != "" {
		return url
	}
	return q.baseURL
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
func (q *ProjectQueries) ResolveName(ctx context.Context, workspaceID, kind, name string) (jobbuilder.NameResolution, error) {
	return q.ResolveNameInSource(ctx, "", workspaceID, kind, name)
}

// ResolveNameInSource is the source-aware name resolution. A source-carrying
// workspace never falls back to another server's credential.
func (q *ProjectQueries) ResolveNameInSource(ctx context.Context, sourceID, workspaceID, kind, name string) (jobbuilder.NameResolution, error) {
	if kind != jobbuilder.PolicyKeyProject {
		return jobbuilder.NameResolution{Conclusion: jobbuilder.NameUnavailable}, nil
	}
	base, token, ws, ok := q.credentialFor(sourceID, workspaceID)
	if !ok {
		return jobbuilder.NameResolution{Conclusion: jobbuilder.NameUnavailable}, fmt.Errorf("no credential for workspace %q", workspaceID)
	}
	projects, err := q.listProjects(ctx, base, ws, token)
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
func (q *ProjectQueries) ByIssue(ctx context.Context, workspaceID, issueID string) (string, error) {
	return q.ByIssueInSource(ctx, "", workspaceID, issueID)
}

// ByIssueInSource is the source-aware issue project lookup.
func (q *ProjectQueries) ByIssueInSource(ctx context.Context, sourceID, workspaceID, issueID string) (string, error) {
	base, token, ws, ok := q.credentialFor(sourceID, workspaceID)
	if !ok {
		return "", fmt.Errorf("no credential for workspace %q", workspaceID)
	}
	var issue struct {
		ProjectID *string `json:"project_id"`
	}
	query := url.Values{"workspace_id": {ws}}
	if err := q.get(ctx, base, token, "/api/issues/"+url.PathEscape(issueID), query, &issue); err != nil {
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
func (q *ProjectQueries) listProjects(ctx context.Context, baseURL, workspaceID, token string) ([]projectRow, error) {
	var answer struct {
		Projects []projectRow `json:"projects"`
	}
	query := url.Values{"workspace_id": {workspaceID}}
	if err := q.get(ctx, baseURL, token, "/api/projects", query, &answer); err != nil {
		return nil, err
	}
	return answer.Projects, nil
}

// maxAnswerBytes bounds one read of a query answer; the queries answer small
// JSON documents, and a bound keeps a wrong or hostile answer from being
// buffered whole.
const maxAnswerBytes = 1 << 20

// get performs one authenticated read-only GET and decodes a JSON answer.
func (q *ProjectQueries) get(ctx context.Context, baseURL, token, path string, query url.Values, out any) error {
	target := baseURL + path
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build %s request: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
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
