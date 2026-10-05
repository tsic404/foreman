package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// Client is the fake client: every F→S call of §1.1 (C1–C19). It injects
// the daemon identity headers and Foreman's server credential, caches the
// registered real runtime, and owns the terminal-forward retry budget.
type Client struct {
	cfg     Config
	token   string
	ctrl    *http.Client // 30s control plane (§4)
	lease   *http.Client // 10s prepare-lease (§4)
	metrics Metrics
	log     *slog.Logger

	mu           sync.RWMutex
	runtimeID    string
	repos        json.RawMessage
	reposVersion string
	workspace    string // workspace display name from C17

	// runtimeLost records that a 404 "runtime not found" proved the runtime
	// row deleted (§1.1 failure table); it survives a failed re-register so
	// the next successful Register is still treated as a rebuild (C15 +
	// runtime-set signal).
	runtimeLost bool

	terminalWaits []time.Duration // §4 terminal retry budget

	runtimeSetCh chan struct{} // cap 1: real runtime replaced → WS reconnect
}

// Option customizes a Client.
type Option func(*Client)

// WithMetrics wires the metrics seam.
func WithMetrics(m Metrics) Option {
	return func(c *Client) { c.metrics = orNoopMetrics(m) }
}

// WithLogger overrides the logger (tests).
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.log = l.With("component", "proxy-client") }
}

// WithTerminalRetryWaits overrides the §4 terminal retry budget (tests).
func WithTerminalRetryWaits(waits []time.Duration) Option {
	return func(c *Client) { c.terminalWaits = waits }
}

// NewClient builds the fake client. token is Foreman's server credential
// (mdt_/mul_, auth.LoadServerToken); it is never forwarded to Jobs (F3).
func NewClient(cfg Config, token string, opts ...Option) *Client {
	c := &Client{
		cfg:     cfg,
		token:   token,
		ctrl:    &http.Client{Timeout: controlTimeout},
		lease:   &http.Client{Timeout: leaseTimeout},
		metrics: noopMetrics{},
		log:     slog.Default().With("component", "proxy-client"),

		terminalWaits: terminalRetryWaits,
		runtimeSetCh:  make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// RuntimeSet returns the channel that receives a signal whenever the real
// runtime id is replaced (404 → re-register); WSSubscriber reconnects on it.
func (c *Client) RuntimeSet() <-chan struct{} { return c.runtimeSetCh }

// RuntimeID returns the cached real runtime id ("" before C1).
func (c *Client) RuntimeID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.runtimeID
}

// Registration returns the cached C1 response fields the fake server hands
// to Job daemons (S2/S4): repos verbatim plus repos_version.
func (c *Client) Registration() (repos json.RawMessage, reposVersion string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.repos, c.reposVersion
}

// Workspace returns the workspace id and the display name cached by C17
// (falling back to the id until the preflight runs).
func (c *Client) Workspace() (id, name string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.workspace == "" {
		return c.cfg.WorkspaceID, c.cfg.WorkspaceID
	}
	return c.cfg.WorkspaceID, c.workspace
}

// ---- C1 register / ensureRuntime ----

// registerRequest is the C1 body (§1.1; field names strict, type not
// provider, legacy_daemon_ids empty).
type registerRequest struct {
	WorkspaceID     string           `json:"workspace_id"`
	DaemonID        string           `json:"daemon_id"`
	LegacyDaemonIDs []string         `json:"legacy_daemon_ids"`
	DeviceName      string           `json:"device_name"`
	CLIVersion      string           `json:"cli_version"`
	LaunchedBy      string           `json:"launched_by"`
	Runtimes        []runtimePayload `json:"runtimes"`
	FailedProfiles  []string         `json:"failed_profiles"`
}

type runtimePayload struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	Status    string `json:"status"`
	ProfileID string `json:"profile_id"`
}

type registerResponse struct {
	Runtimes []struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		Provider string `json:"provider"`
		Status   string `json:"status"`
	} `json:"runtimes"`
	Repos        json.RawMessage `json:"repos"`
	ReposVersion string          `json:"repos_version"`
	Settings     json.RawMessage `json:"settings"`
}

// Register performs C1 and caches the real runtime id and repos payload.
// Safe to call again after a 404 "runtime not found" (ensureRuntime).
func (c *Client) Register(ctx context.Context) error {
	body := registerRequest{
		WorkspaceID:     c.cfg.WorkspaceID,
		DaemonID:        c.cfg.DaemonID,
		LegacyDaemonIDs: []string{},
		DeviceName:      "foreman",
		CLIVersion:      c.cfg.Version,
		LaunchedBy:      "",
		Runtimes: []runtimePayload{{
			Name:      c.cfg.RuntimeName,
			Type:      c.cfg.RuntimeType,
			Version:   c.cfg.RuntimeVersion,
			Status:    "online",
			ProfileID: "",
		}},
		FailedProfiles: []string{},
	}
	var resp registerResponse
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/register", mustMarshal(body))
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("register: %w", &StatusError{Code: code, Body: raw})
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("register: decode response: %w", err)
	}
	if len(resp.Runtimes) == 0 || resp.Runtimes[0].ID == "" {
		return fmt.Errorf("register: response carries no runtime id")
	}
	c.mu.Lock()
	replaced := c.runtimeLost || (c.runtimeID != "" && c.runtimeID != resp.Runtimes[0].ID)
	c.runtimeID = resp.Runtimes[0].ID
	c.runtimeLost = false
	c.repos = resp.Repos
	c.reposVersion = resp.ReposVersion
	c.mu.Unlock()
	if replaced {
		// The runtime row was rebuilt (§1.1 failure table): recover orphans
		// (C15) and move the WS subscription to the new runtime.
		c.log.Warn("runtime.rebuilt", "runtime_id", resp.Runtimes[0].ID)
		c.recoverOrphans(ctx, resp.Runtimes[0].ID)
		select {
		case c.runtimeSetCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// recoverOrphans is C15, called only after a real-runtime rebuild (§1.1).
func (c *Client) recoverOrphans(ctx context.Context, runtimeID string) {
	var resp struct {
		Orphaned int `json:"orphaned"`
		Retried  int `json:"retried"`
	}
	path := fmt.Sprintf("/api/daemon/runtimes/%s/recover-orphans", url.PathEscape(runtimeID))
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, path, []byte("{}"))
	if err == nil && code >= 200 && code < 300 {
		err = json.Unmarshal(raw, &resp)
	}
	if err != nil {
		c.log.Warn("recover-orphans failed", "runtime_id", runtimeID, "err", err)
		return
	}
	if resp.Orphaned > 0 {
		c.log.Warn("recover-orphans settled orphans", "orphaned", resp.Orphaned, "retried", resp.Retried)
	}
}

// ensureRuntime returns the cached real runtime id, registering (C1) when
// empty. A 404 "runtime not found" from any call clears the cache via
// noteRuntimeGone so the next ensureRuntime re-registers.
func (c *Client) ensureRuntime(ctx context.Context) (string, error) {
	if rid := c.RuntimeID(); rid != "" {
		return rid, nil
	}
	if err := c.Register(ctx); err != nil {
		return "", err
	}
	return c.RuntimeID(), nil
}

// noteRuntimeGone clears the cached id and marks the rebuild pending: the
// next successful Register — whenever it lands — is a runtime replacement
// (C15 + runtime-set signal), even if earlier re-register attempts failed.
func (c *Client) noteRuntimeGone() {
	c.mu.Lock()
	c.runtimeID = ""
	c.runtimeLost = true
	c.mu.Unlock()
}

// Preflight runs the startup checks: C1 register, C17 workspace validation
// (caches the workspace name for S1), C18 runtime-profiles (404 tolerated),
// C16 token renew (non-fatal). Callers retry with the §4 backoff.
func (c *Client) Preflight(ctx context.Context) error {
	if err := c.Register(ctx); err != nil {
		return err
	}
	if err := c.Workspaces(ctx); err != nil {
		return fmt.Errorf("workspace check: %w", err)
	}
	if err := c.RuntimeProfiles(ctx); err != nil {
		return err
	}
	if err := c.RenewToken(ctx); err != nil {
		c.log.Warn("token renew failed (non-fatal)", "err", err)
	}
	return nil
}

// ---- C2 heartbeat ----

// Heartbeat is C2 for the cached real runtime. A non-2xx answer is a
// StatusError — the loop's §4 backoff depends on it (a silently swallowed
// 401 would let the runtime die unnoticed).
func (c *Client) Heartbeat(ctx context.Context) error {
	code, raw, err := c.withRuntime(ctx, c.ctrl, func(rid string) (string, []byte) {
		body, _ := json.Marshal(map[string]any{"runtime_id": rid, "supports_batch_import": true})
		return "/api/daemon/heartbeat", body
	})
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("heartbeat: %w", &StatusError{Code: code, Body: raw})
	}
	c.log.Debug("daemon.heartbeat", "runtime_id", c.RuntimeID(), "status", "ok")
	return nil
}

// ---- C3 claim ----

// ClaimTasks is C3: claims up to maxTasks on the real runtime and returns
// the raw task payloads (opaque; proxy never deserializes them, §数据结构).
func (c *Client) ClaimTasks(ctx context.Context, maxTasks int) ([]json.RawMessage, error) {
	if maxTasks < 1 {
		return nil, nil
	}
	code, raw, err := c.withRuntime(ctx, c.ctrl, func(rid string) (string, []byte) {
		body, _ := json.Marshal(map[string]any{
			"daemon_id":   c.cfg.DaemonID,
			"runtime_ids": []string{rid},
			"max_tasks":   maxTasks,
		})
		return "/api/daemon/tasks/claim", body
	})
	if err == nil && code >= 200 && code < 300 {
		var resp struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		if err = json.Unmarshal(raw, &resp); err == nil {
			return resp.Tasks, nil
		}
	}
	if err == nil {
		err = &StatusError{Code: code, Body: raw}
	}
	se := new(StatusError)
	codeLabel := "transport"
	if errors.As(err, &se) {
		codeLabel = fmt.Sprintf("%d", se.Code)
	}
	c.metrics.ClaimError(codeLabel)
	return nil, fmt.Errorf("claim: %w", err)
}

// ---- C4 prepare-lease ----

// PrepareLease is C4. The §1.1 C4 semantics are normalized to ready:
// 200 → ready=true; 400 → ready=false (not an error: the task left the
// renewable pre-start state); 404 → scheduler.ErrTaskNotFound.
func (c *Client) PrepareLease(ctx context.Context, taskID string) (ready bool, err error) {
	code, _, err := c.withRuntime(ctx, c.lease, func(rid string) (string, []byte) {
		return fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/prepare-lease",
			url.PathEscape(rid), url.PathEscape(taskID)), []byte("{}")
	})
	if err != nil {
		return false, err
	}
	switch {
	case code >= 200 && code < 300:
		return true, nil
	case code == 400:
		return false, nil
	case code == 404:
		return false, scheduler.ErrTaskNotFound
	default:
		return false, &StatusError{Code: code}
	}
}

// ---- C5–C12 lifecycle forward ----

// endpointPath maps a lifecycle endpoint to its §1.1 path suffix.
func endpointPath(ep scheduler.Endpoint, taskID string) string {
	t := url.PathEscape(taskID)
	switch ep {
	case scheduler.EPStart:
		return "/api/daemon/tasks/" + t + "/start"
	case scheduler.EPProgress:
		return "/api/daemon/tasks/" + t + "/progress"
	case scheduler.EPMessages:
		return "/api/daemon/tasks/" + t + "/messages"
	case scheduler.EPUsage:
		return "/api/daemon/tasks/" + t + "/usage"
	case scheduler.EPComplete:
		return "/api/daemon/tasks/" + t + "/complete"
	case scheduler.EPFail:
		return "/api/daemon/tasks/" + t + "/fail"
	case scheduler.EPCancelAck:
		return "/api/daemon/tasks/" + t + "/cancel-ack"
	case scheduler.EPSession:
		return "/api/daemon/tasks/" + t + "/session"
	case scheduler.EPWaitLocalDir:
		return "/api/daemon/tasks/" + t + "/wait-local-directory"
	}
	return ""
}

// Forward posts a lifecycle callback (C5–C12) with Foreman's server
// credential and returns the upstream status and body. Terminal endpoints
// carry the §4 retry budget (4s/8s/16s/32s/64s; 5xx/408/429/network only);
// exhaustion returns ErrTerminalUndelivered for the PendingReports queue.
func (c *Client) Forward(ctx context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error) {
	path := endpointPath(ep, taskID)
	if path == "" {
		return 0, nil, fmt.Errorf("unknown endpoint %q", ep)
	}
	if !ep.IsTerminal() {
		return c.forwardOnce(ctx, ep, path, body)
	}
	for attempt, wait := range c.terminalWaits {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-time.After(wait):
			}
		}
		code, resp, err := c.forwardOnce(ctx, ep, path, body)
		if err == nil && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests {
			return code, resp, nil
		}
		if err != nil {
			c.log.Warn("terminal forward attempt failed", "task_id", taskID, "endpoint", string(ep), "attempt", attempt+1, "err", err)
		} else {
			c.log.Warn("terminal forward attempt rejected", "task_id", taskID, "endpoint", string(ep), "attempt", attempt+1, "status", code)
		}
	}
	return 0, nil, fmt.Errorf("%w: %s %s", ErrTerminalUndelivered, ep, taskID)
}

func (c *Client) forwardOnce(ctx context.Context, ep scheduler.Endpoint, path string, body []byte) (int, []byte, error) {
	start := time.Now()
	code, resp, err := c.doOnce(ctx, c.ctrl, http.MethodPost, path, body)
	c.metrics.Forward(string(ep), code, time.Since(start))
	return code, resp, err
}

// ---- S29 passthrough ----

// Passthrough forwards an agent-API call to the real server verbatim
// (§1.2 rule 2): method, raw path (EscapedPath, keeping %2F-style
// segments), raw query (no decode/re-encode), body and Authorization
// unchanged; Foreman never injects its own credential. The response is
// returned as-is; mapping upstream 5xx/network failures to 502 is the
// caller's job.
func (c *Client) Passthrough(ctx context.Context, method, rawPath, rawQuery string, body []byte, authHeader string) (int, []byte, error) {
	u := c.cfg.ServerURL + rawPath
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", authHeader)
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	resp, err := c.ctrl.Do(req)
	if err != nil {
		c.metrics.Forward(EndpointAgentAPI, 0, time.Since(start))
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	c.metrics.Forward(EndpointAgentAPI, resp.StatusCode, time.Since(start))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, data, nil
}

// ---- C13 status / S20 gc-check ----

// TaskStatus is C13; a server-side 404 maps to scheduler.ErrTaskNotFound.
func (c *Client) TaskStatus(ctx context.Context, taskID string) (string, error) {
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodGet, "/api/daemon/tasks/"+url.PathEscape(taskID)+"/status", nil)
	if err != nil {
		return "", err
	}
	if code == 404 {
		return "", scheduler.ErrTaskNotFound
	}
	if code < 200 || code >= 300 {
		return "", &StatusError{Code: code, Body: raw}
	}
	var resp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("decode task status: %w", err)
	}
	return resp.Status, nil
}

// GCCheck forwards the S20 gc-check query to the server verbatim (GC is
// disabled Job-side, but the endpoint stays truthful if enabled, §1.2 S20).
func (c *Client) GCCheck(ctx context.Context, taskID string) (int, []byte, error) {
	return c.doOnce(ctx, c.ctrl, http.MethodGet, "/api/daemon/tasks/"+url.PathEscape(taskID)+"/gc-check", nil)
}

// ---- C19 skill bundles ----

// ResolveSkillBundles is C19: forwards the Job daemon's resolve body with
// the {rid} rewritten to the real runtime id and Foreman's credential (S26).
func (c *Client) ResolveSkillBundles(ctx context.Context, taskID string, body []byte) (int, []byte, error) {
	return c.withRuntime(ctx, c.ctrl, func(rid string) (string, []byte) {
		return fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/skill-bundles/resolve",
			url.PathEscape(rid), url.PathEscape(taskID)), body
	})
}

// ---- C16/C17/C18 ----

// RenewToken is C16 (startup preflight + periodic; non-fatal on failure).
func (c *Client) RenewToken(ctx context.Context) error {
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/tokens/current/renew", []byte("{}"))
	if err != nil {
		return fmt.Errorf("renew token: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("renew token: %w", &StatusError{Code: code, Body: raw})
	}
	return nil
}

// Workspaces is C17: validates the configured workspace is reachable and
// caches its display name for S1.
func (c *Client) Workspaces(ctx context.Context) error {
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodGet, "/api/daemon/workspaces", nil)
	if err != nil {
		return fmt.Errorf("list workspaces: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("list workspaces: %w", &StatusError{Code: code, Body: raw})
	}
	var list []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("list workspaces: decode: %w", err)
	}
	for _, w := range list {
		if w.ID == c.cfg.WorkspaceID {
			c.mu.Lock()
			c.workspace = w.Name
			c.mu.Unlock()
			return nil
		}
	}
	return fmt.Errorf("workspace %s not visible to the server token", c.cfg.WorkspaceID)
}

// RuntimeProfiles is C18; a 404 is tolerated (no custom profiles, §1.1).
func (c *Client) RuntimeProfiles(ctx context.Context) error {
	path := fmt.Sprintf("/api/daemon/workspaces/%s/runtime-profiles", url.PathEscape(c.cfg.WorkspaceID))
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if code == 404 {
		return nil
	}
	if code < 200 || code >= 300 {
		return &StatusError{Code: code, Body: raw}
	}
	return nil
}

// ---- C14 deregister ----

// Deregister is C14 (graceful shutdown; failure-handling scenario #8b).
func (c *Client) Deregister(ctx context.Context) error {
	rid := c.RuntimeID()
	if rid == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]any{"runtime_ids": []string{rid}})
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/deregister", body)
	if err != nil {
		return fmt.Errorf("deregister: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("deregister: %w", &StatusError{Code: code, Body: raw})
	}
	return nil
}

// ---- transport ----

// withRuntime runs one rid-dependent call: it resolves the real runtime id
// (registering when needed), and on a 404 "runtime not found" re-registers
// once and retries against the fresh id (§1.1 failure table).
func (c *Client) withRuntime(ctx context.Context, hc *http.Client, req func(rid string) (path string, body []byte)) (int, []byte, error) {
	rid, err := c.ensureRuntime(ctx)
	if err != nil {
		return 0, nil, err
	}
	path, body := req(rid)
	code, resp, err := c.doOnce(ctx, hc, http.MethodPost, path, body)
	if err != nil {
		return 0, nil, err
	}
	if code == 404 && containsRuntimeNotFound(resp) {
		c.log.Warn("runtime not found; re-registering", "path", path)
		// Register must see the stale id to detect the replacement; the
		// cache is only cleared when re-registration itself fails.
		if rerr := c.Register(ctx); rerr != nil {
			c.noteRuntimeGone()
			return 0, nil, fmt.Errorf("re-register after runtime loss: %w", rerr)
		}
		path, body = req(c.RuntimeID())
		return c.doOnce(ctx, hc, http.MethodPost, path, body)
	}
	return code, resp, nil
}

// doOnce is the transport core: one attempt with the daemon identity
// headers; the response body is returned verbatim.
func (c *Client) doOnce(ctx context.Context, hc *http.Client, method, path string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.ServerURL+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	c.setHeaders(req, body != nil)
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, data, nil
}

// setHeaders injects the daemon identity (§1.1): the capability string is
// verbatim and must never gain or lose an entry.
func (c *Client) setHeaders(req *http.Request, hasBody bool) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Client-Platform", "daemon")
	req.Header.Set("X-Client-Version", c.cfg.Version)
	req.Header.Set("X-Client-OS", "linux")
	req.Header.Set("X-Client-Capabilities", ClientCapabilities)
	if hasBody {
		req.Header.Set("Content-Type", "application/json")
	}
}

func mustMarshal(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic("proxy: marshal static request body: " + err.Error())
	}
	return raw
}

func containsRuntimeNotFound(body []byte) bool {
	return bytes.Contains(bytes.ToLower(body), []byte("runtime not found"))
}
