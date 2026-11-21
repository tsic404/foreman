package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// The five upstream routes are anchored in docs/upstream-anchors.md: the
// daemon workspace list, the workspace-scoped projects / agents / runtimes
// lists, the single-agent read and the agent PUT that sets runtime_id. Foreman
// invents no endpoint: a capability the upstream lacks disables the dependent
// action instead of being emulated.

// UpstreamErrorKind classifies one failed upstream call.
type UpstreamErrorKind string

const (
	UpstreamUnauthorized UpstreamErrorKind = "unauthorized"
	UpstreamNotFound     UpstreamErrorKind = "not_found"
	UpstreamConflict     UpstreamErrorKind = "conflict"
	UpstreamRejected     UpstreamErrorKind = "rejected"
	UpstreamTransient    UpstreamErrorKind = "transient"
	UpstreamInvalid      UpstreamErrorKind = "invalid_answer"
)

// UpstreamError is one failed upstream call. Its message never carries the
// credential or a request URL.
type UpstreamError struct {
	Op      string
	Status  int
	Kind    UpstreamErrorKind
	Message string
}

func (e *UpstreamError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%s: upstream answered %d (%s)", e.Op, e.Status, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Op, e.Message)
}

// IsUnauthorized reports whether err is the upstream's revocation answer.
func IsUnauthorized(err error) bool {
	var upstream *UpstreamError
	return errors.As(err, &upstream) && upstream.Kind == UpstreamUnauthorized
}

// IsNotFound reports whether err is the upstream's "does not exist" answer.
func IsNotFound(err error) bool {
	var upstream *UpstreamError
	return errors.As(err, &upstream) && upstream.Kind == UpstreamNotFound
}

// IsTransient reports whether err may be retried without changing the request.
func IsTransient(err error) bool {
	var upstream *UpstreamError
	return errors.As(err, &upstream) && upstream.Kind == UpstreamTransient
}

// IsConflict reports whether err is the upstream's conditional-write refusal:
// the value changed between the read and the write, so nothing was applied.
func IsConflict(err error) bool {
	var upstream *UpstreamError
	return errors.As(err, &upstream) && upstream.Kind == UpstreamConflict
}

// Credentials resolves a source's credential reference to its value. The value
// is process memory only: it never enters a catalog row, a response or a log.
type Credentials interface {
	Token(ref string) (string, error)
}

// FileCredentials resolves a reference to a file under one directory, so the
// deployment mounts each source credential as its own Secret key and the
// catalog only ever holds the reference. The reference must stay a bare file
// name: a path would leave the directory.
type FileCredentials struct {
	Dir string
}

// Token reads one credential file.
func (f FileCredentials) Token(ref string) (string, error) {
	if !credentialRefPattern.MatchString(ref) {
		return "", errors.New("credential reference is not a bare name")
	}
	path := filepath.Join(f.Dir, ref)
	raw, err := os.ReadFile(path)
	if err != nil {
		// The error names the reference, never a value; the path itself is a
		// deployment path and is safe to omit.
		return "", fmt.Errorf("credential %q is not readable", ref)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("credential %q is empty", ref)
	}
	return token, nil
}

// Upstream is the directory and binding face of one Multica server. The HTTP
// implementation is the production adapter; tests drive the same interface with
// a stub that can fail on demand.
type Upstream interface {
	// ListWorkspaces returns every workspace the source credential can act for.
	ListWorkspaces(ctx context.Context, src Source) ([]Workspace, error)
	// WorkspaceCatalog reads one workspace's projects, agents, runtimes and
	// bindings.
	WorkspaceCatalog(ctx context.Context, src Source, workspaceID string) (WorkspaceCatalog, error)
	// ReadAgent reads one agent's current binding inside its workspace. The
	// workspace identifier is not optional: the upstream's agent routes are
	// workspace-scoped and answer 400 without it.
	ReadAgent(ctx context.Context, src Source, workspaceID, agentID string) (Agent, bool, error)
	// SetAgentRuntime submits only this agent's runtime association, inside
	// its workspace, conditional on the binding still being expectedRuntimeID.
	// A conditional answer is the only evidence a competing edit was not
	// overwritten; an adapter that cannot express the condition must report
	// BindingWriteSupported false instead of sending an unconditional body.
	SetAgentRuntime(ctx context.Context, src Source, workspaceID, agentID, expectedRuntimeID, runtimeID string) error
	// BindingWriteSupported reports whether this adapter can submit a binding
	// change that is conditional on the value it read. The pinned Multica API
	// has no version/If-Match field on the agent update, so the HTTP adapter
	// answers false and the management surface stays read-only with the reason.
	BindingWriteSupported(src Source) (bool, string)
}

// maxAnswerBytes bounds one upstream answer.
const maxAnswerBytes = 4 << 20

// HTTPUpstream is the production adapter over the anchored routes.
type HTTPUpstream struct {
	creds  Credentials
	client *http.Client
}

// NewHTTPUpstream builds the adapter. The client carries no timeout of its own:
// every call runs under the caller's bounded context.
func NewHTTPUpstream(creds Credentials) *HTTPUpstream {
	return &HTTPUpstream{creds: creds, client: &http.Client{}}
}

type workspaceRow struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type projectRow struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type projectsAnswer struct {
	Projects []projectRow `json:"projects"`
	Total    *int         `json:"total"`
}

type agentRow struct {
	ID           string `json:"id"`
	WorkspaceID  string `json:"workspace_id"`
	Name         string `json:"name"`
	RuntimeID    string `json:"runtime_id"`
	RuntimeBound *bool  `json:"runtime_bound"`
}

type runtimeRow struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspace_id"`
	Name        string `json:"name"`
	Provider    string `json:"provider"`
	Status      string `json:"status"`
}

// ListWorkspaces reads GET /api/daemon/workspaces.
func (u *HTTPUpstream) ListWorkspaces(ctx context.Context, src Source) ([]Workspace, error) {
	var rows []workspaceRow
	if err := u.get(ctx, src, "/api/daemon/workspaces", nil, &rows); err != nil {
		return nil, err
	}
	out := make([]Workspace, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.ID) == "" {
			return nil, &UpstreamError{Op: "list workspaces", Kind: UpstreamInvalid, Message: "a workspace row has no id"}
		}
		out = append(out, Workspace{ID: row.ID, Name: row.Name})
	}
	return out, nil
}

// WorkspaceCatalog reads the three directory lists of one workspace. The
// project answer carries a total; a mismatch means the page was truncated and
// the caller must keep its previous generation.
func (u *HTTPUpstream) WorkspaceCatalog(ctx context.Context, src Source, workspaceID string) (WorkspaceCatalog, error) {
	query := url.Values{"workspace_id": {workspaceID}}
	var projects projectsAnswer
	if err := u.get(ctx, src, "/api/projects", query, &projects); err != nil {
		return WorkspaceCatalog{}, err
	}
	if projects.Total != nil && *projects.Total != len(projects.Projects) {
		return WorkspaceCatalog{}, &UpstreamError{
			Op: "list projects", Kind: UpstreamInvalid,
			Message: fmt.Sprintf("answer carries %d of %d projects; the page is incomplete", len(projects.Projects), *projects.Total),
		}
	}
	var agents []agentRow
	if err := u.get(ctx, src, "/api/agents", query, &agents); err != nil {
		return WorkspaceCatalog{}, err
	}
	var runtimes []runtimeRow
	if err := u.get(ctx, src, "/api/runtimes", query, &runtimes); err != nil {
		return WorkspaceCatalog{}, err
	}
	catalog := WorkspaceCatalog{
		Projects: make([]Project, 0, len(projects.Projects)),
		Agents:   make([]Agent, 0, len(agents)),
		Runtimes: make([]Runtime, 0, len(runtimes)),
	}
	for _, row := range projects.Projects {
		if strings.TrimSpace(row.ID) == "" {
			return WorkspaceCatalog{}, &UpstreamError{Op: "list projects", Kind: UpstreamInvalid, Message: "a project row has no id"}
		}
		catalog.Projects = append(catalog.Projects, Project{WorkspaceID: workspaceID, ID: row.ID, Title: row.Title})
	}
	for _, row := range agents {
		if strings.TrimSpace(row.ID) == "" {
			return WorkspaceCatalog{}, &UpstreamError{Op: "list agents", Kind: UpstreamInvalid, Message: "an agent row has no id"}
		}
		if row.WorkspaceID != "" && row.WorkspaceID != workspaceID {
			return WorkspaceCatalog{}, &UpstreamError{
				Op: "list agents", Kind: UpstreamInvalid,
				Message: "an agent row belongs to another workspace",
			}
		}
		bound := row.RuntimeID != ""
		if row.RuntimeBound != nil {
			bound = *row.RuntimeBound
		}
		catalog.Agents = append(catalog.Agents, Agent{
			WorkspaceID: workspaceID, ID: row.ID, Name: row.Name,
			RuntimeID: row.RuntimeID, RuntimeBound: bound,
		})
	}
	for _, row := range runtimes {
		if strings.TrimSpace(row.ID) == "" {
			return WorkspaceCatalog{}, &UpstreamError{Op: "list runtimes", Kind: UpstreamInvalid, Message: "a runtime row has no id"}
		}
		if row.WorkspaceID != "" && row.WorkspaceID != workspaceID {
			return WorkspaceCatalog{}, &UpstreamError{
				Op: "list runtimes", Kind: UpstreamInvalid,
				Message: "a runtime row belongs to another workspace",
			}
		}
		catalog.Runtimes = append(catalog.Runtimes, Runtime{
			WorkspaceID: workspaceID, ID: row.ID, Name: row.Name,
			Provider: row.Provider, Status: row.Status,
		})
	}
	return catalog, nil
}

// ReadAgent reads GET /api/agents/{id}: the read-back the binding contract
// treats as the only success evidence. The workspace rides in X-Workspace-ID
// (and ?workspace_id=) because the route is workspace-scoped.
func (u *HTTPUpstream) ReadAgent(ctx context.Context, src Source, workspaceID, agentID string) (Agent, bool, error) {
	var row agentRow
	err := u.get(ctx, src, "/api/agents/"+url.PathEscape(agentID), workspaceQuery(workspaceID), &row)
	if err != nil {
		if IsNotFound(err) {
			return Agent{}, false, nil
		}
		return Agent{}, false, err
	}
	if row.ID != "" && row.ID != agentID {
		return Agent{}, false, &UpstreamError{Op: "get agent", Kind: UpstreamInvalid, Message: "the answer names another agent"}
	}
	if row.WorkspaceID != "" && row.WorkspaceID != workspaceID {
		return Agent{}, false, &UpstreamError{Op: "get agent", Kind: UpstreamInvalid, Message: "the answer names another workspace"}
	}
	bound := row.RuntimeID != ""
	if row.RuntimeBound != nil {
		bound = *row.RuntimeBound
	}
	return Agent{WorkspaceID: row.WorkspaceID, ID: row.ID, Name: row.Name, RuntimeID: row.RuntimeID, RuntimeBound: bound}, true, nil
}

// SetAgentRuntime writes PUT /api/agents/{id} with the runtime association
// alone. The upstream validates the runtime against the agent's workspace; a
// refusal is the rejected class, never a silent no-op. The pinned API has no
// conditional field, so this adapter never reports the write as supported:
// BindingWriteSupported keeps the caller from sending it at all.
func (u *HTTPUpstream) SetAgentRuntime(ctx context.Context, src Source, workspaceID, agentID, expectedRuntimeID, runtimeID string) error {
	body, err := json.Marshal(map[string]string{"runtime_id": runtimeID})
	if err != nil {
		return fmt.Errorf("encode binding: %w", err)
	}
	return u.do(ctx, src, http.MethodPut, "/api/agents/"+url.PathEscape(agentID), workspaceQuery(workspaceID), body, nil)
}

// BindingWriteSupported answers the pinned API's capability, truthfully: its
// agent update is unconditional, so a write cannot exclude a competing edit.
func (u *HTTPUpstream) BindingWriteSupported(Source) (bool, string) {
	return false, "the pinned Multica API updates an agent without a version or If-Match precondition, so Foreman cannot prove it is not overwriting a competing edit"
}

// workspaceQuery carries the workspace identifier on every agent-scoped call.
// The header is what the upstream middleware prefers; the query keeps the
// request reproducible from a log line that never carries a credential.
func workspaceQuery(workspaceID string) url.Values {
	return url.Values{"workspace_id": {workspaceID}}
}

// get performs one authenticated read and decodes its answer.
func (u *HTTPUpstream) get(ctx context.Context, src Source, path string, query url.Values, out any) error {
	return u.do(ctx, src, http.MethodGet, path, query, nil, out)
}

func (u *HTTPUpstream) do(ctx context.Context, src Source, method, path string, query url.Values, body []byte, out any) error {
	token, err := u.creds.Token(src.CredentialRef)
	if err != nil {
		return &UpstreamError{Op: method + " " + path, Kind: UpstreamUnauthorized, Message: "source credential is not resolvable"}
	}
	target := strings.TrimRight(src.URL, "/") + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return &UpstreamError{Op: method + " " + path, Kind: UpstreamTransient, Message: "build request"}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if workspaceID := query.Get("workspace_id"); workspaceID != "" {
		// The workspace-scoped routes resolve their identity from this header
		// first; the query parameter beside it keeps a log line readable.
		req.Header.Set("X-Workspace-ID", workspaceID)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := u.client.Do(req)
	if err != nil {
		// The transport error text may name the address; it never carries the
		// credential because the credential travels in a header.
		return &UpstreamError{Op: method + " " + path, Kind: UpstreamTransient, Message: "request failed"}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBytes))
	if err != nil {
		return &UpstreamError{Op: method + " " + path, Kind: UpstreamTransient, Message: "read answer failed"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &UpstreamError{
			Op: method + " " + path, Status: resp.StatusCode,
			Kind:    classifyStatus(resp.StatusCode),
			Message: answerSnippet(raw),
		}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &UpstreamError{Op: method + " " + path, Kind: UpstreamInvalid, Message: "answer is not the expected JSON"}
	}
	return nil
}

// classifyStatus maps one upstream status onto the error class.
func classifyStatus(status int) UpstreamErrorKind {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return UpstreamUnauthorized
	case status == http.StatusNotFound:
		return UpstreamNotFound
	case status == http.StatusConflict || status == http.StatusPreconditionFailed:
		return UpstreamConflict
	case status >= 500 || status == http.StatusTooManyRequests || status == http.StatusRequestTimeout:
		return UpstreamTransient
	default:
		return UpstreamRejected
	}
}

// answerSnippet clips one upstream answer for an error line. It is server text,
// never a credential Foreman sent.
func answerSnippet(raw []byte) string {
	const max = 160
	text := strings.TrimSpace(string(raw))
	if len(text) > max {
		return text[:max] + "…"
	}
	return text
}

var _ Upstream = (*HTTPUpstream)(nil)
