// Package proxy implements v2's two protocol faces:
//
//   - the fake client: Foreman registers itself to the multica server as one
//     daemon per workspace, claims tasks in batches, renews leases, forwards
//     lifecycle callbacks, and renews its token;
//   - the fake server: Foreman answers the long-lived multica daemons that
//     register to it (S1–S32), authenticates them with an operator-issued
//     bearer credential, and hands each the task it was assigned.
//
// v2 dropped everything that existed only because a task used to be a
// disposable pod: the startup/registration-discovery probe, the catalog-probe
// Job, the WS control channel (§2 of the v1 contract) and the Job Token
// minting per task.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Environment variable names read by the proxy module.
const (
	EnvServerURL            = "MULTICA_SERVER_URL"
	EnvWorkspaceID          = "MULTICA_WORKSPACE_ID"
	EnvDaemonID             = "FOREMAN_DAEMON_ID"
	EnvClaimInterval        = "FOREMAN_CLAIM_INTERVAL"
	EnvClaimIntervalIdle    = "FOREMAN_CLAIM_INTERVAL_IDLE"
	EnvLeaseRefreshInterval = "FOREMAN_LEASE_REFRESH_INTERVAL"
)

// Defaults applied when the environment variable is unset.
const (
	DefaultDaemonID          = "foreman"
	DefaultClaimInterval     = 5 * time.Second
	DefaultClaimIntervalIdle = 30 * time.Second
	// DefaultLeaseRefreshInterval is the daemon-identity heartbeat cadence
	// (C-2, POST /api/daemon/heartbeat), not the task lease renewal cadence —
	// despite the historical variable name. Task leases are renewed by C-4
	// from the dispatcher's convergence round (FOREMAN_RECONCILE_INTERVAL) and
	// by the daemon itself via S-7.
	DefaultLeaseRefreshInterval = 15 * time.Second

	// MaxLeaseRefreshInterval caps the heartbeat cadence. The upstream daemon
	// heartbeats every 15s; staying near that keeps Foreman's view of liveness
	// as fresh as the daemon's own. (The offline sweep is a separate window,
	// FOREMAN_HEARTBEAT_TIMEOUT, default 60s — this cap is about freshness,
	// not about avoiding false offline verdicts.)
	MaxLeaseRefreshInterval = 20 * time.Second

	// ClientCapabilities is the X-Client-Capabilities header value (§1.1).
	ClientCapabilities = "skill-bundles-v1,coalesced-comments-v1,rpc-v1"

	controlTimeout = 30 * time.Second
	leaseTimeout   = 10 * time.Second
)

// Config is the fake client's configuration: one (credential, daemon identity,
// workspace) tuple.
type Config struct {
	ServerURL   string
	WorkspaceID string
	DaemonID    string
	Version     string

	ClaimInterval        time.Duration
	ClaimIntervalIdle    time.Duration
	LeaseRefreshInterval time.Duration
}

// LoadConfig reads the client configuration from env.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = func(k string) string { return "" }
	}
	cfg := Config{
		DaemonID:             DefaultDaemonID,
		Version:              "dev",
		ClaimInterval:        DefaultClaimInterval,
		ClaimIntervalIdle:    DefaultClaimIntervalIdle,
		LeaseRefreshInterval: DefaultLeaseRefreshInterval,
	}
	cfg.ServerURL = strings.TrimRight(strings.TrimSpace(getenv(EnvServerURL)), "/")
	if cfg.ServerURL == "" {
		return Config{}, fmt.Errorf("%s is required", EnvServerURL)
	}
	parsed, err := url.Parse(cfg.ServerURL)
	if err != nil {
		return Config{}, fmt.Errorf("%s is not a valid URL: %v", EnvServerURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Config{}, fmt.Errorf("%s must use http or https, got %q", EnvServerURL, parsed.Scheme)
	}
	cfg.WorkspaceID = strings.TrimSpace(getenv(EnvWorkspaceID))
	if v := strings.TrimSpace(getenv(EnvDaemonID)); v != "" {
		cfg.DaemonID = v
	}
	if cfg.ClaimInterval, err = envDuration(getenv, EnvClaimInterval, cfg.ClaimInterval); err != nil {
		return Config{}, err
	}
	if cfg.ClaimIntervalIdle, err = envDuration(getenv, EnvClaimIntervalIdle, cfg.ClaimIntervalIdle); err != nil {
		return Config{}, err
	}
	if cfg.LeaseRefreshInterval, err = envDuration(getenv, EnvLeaseRefreshInterval, cfg.LeaseRefreshInterval); err != nil {
		return Config{}, err
	}
	if cfg.LeaseRefreshInterval > MaxLeaseRefreshInterval {
		return Config{}, fmt.Errorf("%s (%s) must be <= %s",
			EnvLeaseRefreshInterval, cfg.LeaseRefreshInterval, MaxLeaseRefreshInterval)
	}
	return cfg, nil
}

func envDuration(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration: %q", key, raw)
	}
	return d, nil
}

// ---- status errors ----

// StatusError is a non-2xx server response.
type StatusError struct {
	Code int
	Body []byte
}

// Error includes the upstream body. A bare code turns every refusal into the
// same line, so an operator cannot tell a stale registration from a runtime
// mismatch or a policy denial — the server's message is the only place that
// difference is spelled out.
func (e *StatusError) Error() string {
	if body := strings.TrimSpace(string(e.Body)); body != "" {
		return fmt.Sprintf("server returned %d: %s", e.Code, truncateForLog(e.Body))
	}
	return fmt.Sprintf("server returned %d", e.Code)
}

// ErrTerminalUndelivered reports a terminal forward whose retry budget ran out.
var ErrTerminalUndelivered = errors.New("terminal report undelivered after retry budget")

// ---- fake client ----

// RuntimeRef is one runtime the server assigned to Foreman's registration.
type RuntimeRef struct {
	ID        string
	Provider  string
	ProfileID string
	Name      string
	// RuntimeKey is the daemon-side runtime id this ref was bound from.
	RuntimeKey string
}

// Key is the identity key (provider \x00 profile id).

type Client struct {
	cfg     Config
	token   string
	ctrl    *http.Client
	lease   *http.Client
	metrics Metrics
	log     *slog.Logger

	// runtimes is the declared set. v2 declares one runtime per daemon that
	// registered to Foreman; the server binds each to a real runtime id.
	runtimes []RuntimeRef
	// declared is the declaration derived from the registered daemons; it is
	// sent at the next C1. dirty marks a declaration that differs from what the
	// server last accepted, so the claim loop's preflight only re-runs C1 when
	// a daemon registered or deregistered — never every round.
	declared []DeclaredRuntime
	dirty    bool
}

// DeclaredRuntime is one runtime Foreman reports at C1, derived from the
// daemons currently registered to it.
type DeclaredRuntime struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	ProfileID string `json:"profile_id"`
	// RuntimeKey is the daemon's own runtime id for this declaration (unique
	// per daemon runtime). It never travels in the C1 payload — the server
	// assigns the real id — but it keys the daemon→real-id mapping so a
	// multi-runtime daemon resolves each task to the right server runtime.
	RuntimeKey string `json:"-"`
	// DeviceName is the daemon's own device name, forwarded verbatim so the
	// server builds the runtime's display name from the daemon's identity
	// rather than from anything Foreman invents.
	DeviceName string `json:"-"`
}

// Option customizes a Client.
type Option func(*Client)

// WithMetrics wires the metrics seam.
func WithMetrics(m Metrics) Option {
	return func(c *Client) { c.metrics = orNoopMetrics(m) }
}

// WithLogger overrides the logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.log = l.With("component", "proxy-client") }
}

// NewClient builds the fake client. token is Foreman's server credential.
func NewClient(cfg Config, token string, opts ...Option) *Client {
	c := &Client{
		cfg:     cfg,
		token:   token,
		ctrl:    &http.Client{Timeout: controlTimeout},
		lease:   &http.Client{Timeout: leaseTimeout},
		metrics: noopMetrics{},
		log:     slog.Default().With("component", "proxy-client"),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// WorkspaceID returns the workspace this client serves.
func (c *Client) WorkspaceID() string { return c.cfg.WorkspaceID }

// RuntimeIDs returns the real runtime ids the server assigned.
func (c *Client) RuntimeIDs() []string {
	ids := make([]string, 0, len(c.runtimes))
	for _, r := range c.runtimes {
		if r.ID != "" {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

// RuntimeRefs returns the declared runtimes and their ids.
func (c *Client) RuntimeRefs() []RuntimeRef { return append([]RuntimeRef(nil), c.runtimes...) }

// RealRuntimeID maps a daemon-side runtime key to the server's runtime id.
// It answers "" when the declaration has not been bound yet (C1 still in
// flight), so a caller can fall back instead of sending a key the server
// never registered.
func (c *Client) RealRuntimeID(runtimeKey string) string {
	if runtimeKey == "" {
		return ""
	}
	for _, ref := range c.RuntimeRefs() {
		if ref.RuntimeKey == runtimeKey && ref.ID != "" {
			return ref.ID
		}
	}
	return ""
}

// SetDeclared updates the runtime set Foreman will declare at the next C1 and
// marks the declaration dirty, so the claim loop re-runs the registration.
func (c *Client) SetDeclared(d []DeclaredRuntime) {
	c.declared = append([]DeclaredRuntime(nil), d...)
	c.dirty = true
}

// errNoDeclaredRuntime is the no-daemon-yet signal Register returns. It is not
// an error of configuration or transport: a daemon has simply not registered to
// Foreman yet, so there is nothing to declare. The claim loop treats it as a
// deferral, not a failure.
var errNoDeclaredRuntime = errors.New("register: no daemon registered to declare")

// NeedsRegistration reports whether the declared set is dirty (a daemon
// registered or deregistered since the last C1) or no runtime has ever been
// registered. The claim loop's preflight reads it to skip a no-op round.
func (c *Client) NeedsRegistration() bool { return c.dirty || len(c.runtimes) == 0 }

// Register performs C1 with the current declaration. A server that answers
// binds each declared identity to a real runtime id.
func (c *Client) Register(ctx context.Context) error {
	if len(c.declared) == 0 {
		return errNoDeclaredRuntime
	}
	payloads := make([]runtimePayload, 0, len(c.declared))
	for _, d := range c.declared {
		if d.Type == "" {
			continue
		}
		payloads = append(payloads, runtimePayload{
			Name: d.Name, Type: d.Type, Version: d.Version,
			Status: "online", ProfileID: d.ProfileID,
			RuntimeKey: d.RuntimeKey,
		})
	}
	if len(payloads) == 0 {
		return errors.New("register: declared set has no usable runtime")
	}
	// The device name is the daemon's own, forwarded verbatim: the server
	// builds a runtime's display name from device_name + provider, so Foreman
	// must never substitute a name of its own. With several daemons behind one
	// Foreman identity the first non-empty device name wins, which keeps the
	// server-side label stable across re-registrations.
	deviceName := ""
	for _, d := range c.declared {
		if d.DeviceName != "" {
			deviceName = d.DeviceName
			break
		}
	}
	body := registerRequest{
		WorkspaceID:     c.cfg.WorkspaceID,
		DaemonID:        c.cfg.DaemonID,
		LegacyDaemonIDs: []string{},
		DeviceName:      deviceName,
		CLIVersion:      c.cfg.Version,
		Runtimes:        payloads,
	}
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/register", mustMarshal(body))
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("register: %w", &StatusError{Code: code, Body: raw})
	}
	var resp registerResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("register: decode: %w", err)
	}
	c.runtimes = bindRuntimes(payloads, resp.Runtimes)
	c.dirty = false
	return nil
}

// Heartbeat is C2: it keeps one real runtime online on the server. Without it
// the server's runtime row lapses to offline and dispatch is refused
// ("agent runtime is offline at dispatch time"). The body names the runtime id
// the server assigned at C1.
func (c *Client) Heartbeat(ctx context.Context, runtimeID string) error {
	body, err := json.Marshal(map[string]any{"runtime_id": runtimeID, "supports_batch_import": true})
	if err != nil {
		return err
	}
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/heartbeat", body)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("heartbeat: %w", &StatusError{Code: code, Body: raw})
	}
	return nil
}

// ResolveSkillBundles is C19: forwards the daemon's skill-bundle resolve body
// to the server (S26) under the runtime the task is actually bound to. The
// path is runtime-scoped upstream, so using "the first registered runtime"
// would address a runtime that never claimed the task and earn a 404.
func (c *Client) ResolveSkillBundles(ctx context.Context, runtimeID, taskID string, body []byte) (int, []byte, error) {
	if runtimeID == "" {
		return 0, nil, errors.New("skill bundles: task has no bound runtime")
	}
	path := fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/skill-bundles/resolve",
		url.PathEscape(runtimeID), url.PathEscape(taskID))
	return c.doOnce(ctx, c.ctrl, http.MethodPost, path, body)
}

// WorkspaceRepos is C22: the workspace's repos/settings snapshot the daemon
// asks for at S-2. The raw map keeps key presence faithful — an absent key
// stays absent from the daemon-facing answer (§1.7).
func (c *Client) WorkspaceRepos(ctx context.Context) (int, []byte, error) {
	path := "/api/daemon/workspaces/" + url.PathEscape(c.cfg.WorkspaceID) + "/repos"
	return c.doOnce(ctx, c.ctrl, http.MethodGet, path, nil)
}

// RuntimeProfiles is C18: the workspace's custom runtime profiles the daemon
// asks for at S-3. A 404 means the workspace has no profile set (the daemon
// continues with its built-in runtimes), a failure means unavailable.
func (c *Client) RuntimeProfiles(ctx context.Context) (int, []byte, error) {
	path := "/api/daemon/workspaces/" + url.PathEscape(c.cfg.WorkspaceID) + "/runtime-profiles"
	return c.doOnce(ctx, c.ctrl, http.MethodGet, path, nil)
}

// ContextReceipt is C23 (v0.9.0): forwards the daemon's task-context
// acknowledgment to the server, verbatim. The upstream route is task-scoped
// under the daemon credential, not runtime-scoped.
func (c *Client) ContextReceipt(ctx context.Context, taskID string, body []byte) (int, []byte, error) {
	path := "/api/daemon/tasks/" + url.PathEscape(taskID) + "/context-receipt"
	return c.doOnce(ctx, c.ctrl, http.MethodPost, path, body)
}

// ClaimTasks is C3 over the declared runtime set.
func (c *Client) ClaimTasks(ctx context.Context, maxTasks int) ([]json.RawMessage, error) {
	if maxTasks < 1 {
		return nil, nil
	}
	ids := c.RuntimeIDs()
	if len(ids) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(map[string]any{
		"daemon_id":   c.cfg.DaemonID,
		"runtime_ids": ids,
		"max_tasks":   maxTasks,
	})
	if err != nil {
		return nil, err
	}
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/tasks/claim", body)
	if err != nil {
		c.metrics.ClaimError("transport")
		return nil, fmt.Errorf("claim: %w", err)
	}
	if code < 200 || code >= 300 {
		c.metrics.ClaimError(fmt.Sprintf("%d", code))
		return nil, fmt.Errorf("claim: %w", &StatusError{Code: code, Body: raw})
	}
	var resp struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		c.metrics.ClaimError("decode")
		return nil, fmt.Errorf("claim: decode: %w", err)
	}
	return resp.Tasks, nil
}

// PrepareLease is C4. 200 → ready, 400 → not ready, 404 → ErrTaskNotFound.
func (c *Client) PrepareLease(ctx context.Context, runtimeID, taskID string) (bool, error) {
	path := fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/prepare-lease",
		url.PathEscape(runtimeID), url.PathEscape(taskID))
	code, raw, err := c.doOnce(ctx, c.lease, http.MethodPost, path, []byte("{}"))
	if err != nil {
		return false, err
	}
	switch {
	case code >= 200 && code < 300:
		return true, nil
	case code == http.StatusBadRequest:
		return false, nil
	case code == http.StatusNotFound:
		return false, ErrTaskNotFound
	default:
		return false, &StatusError{Code: code, Body: raw}
	}
}

// TaskStatus is C13.
func (c *Client) TaskStatus(ctx context.Context, taskID string) (string, error) {
	path := "/api/daemon/tasks/" + url.PathEscape(taskID) + "/status"
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	if code == http.StatusNotFound {
		return "", ErrTaskNotFound
	}
	if code < 200 || code >= 300 {
		return "", &StatusError{Code: code, Body: raw}
	}
	var resp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("task status: decode: %w", err)
	}
	return resp.Status, nil
}

// ForwardInWorkspace posts one lifecycle callback (C5–C12). Terminal endpoints
// carry the retry budget — except cancel-ack.
//
// Cancel-ack is the daemon's own stop notification, sent while its HTTP
// request is open. Retrying it inside that request would hold the daemon for up
// to the full budget (~2 minutes); the daemon re-sends the ack on its own and
// the server's lease expiry settles the task regardless, so one bounded
// attempt is the right cost here.
func (c *Client) ForwardInWorkspace(ctx context.Context, ep Endpoint, taskID, workspaceID string, body []byte) (int, []byte, error) {
	if workspaceID != "" && workspaceID != c.cfg.WorkspaceID {
		return 0, nil, fmt.Errorf("workspace %s is not served by this client", workspaceID)
	}
	path := endpointPath(ep, taskID)
	if path == "" {
		return 0, nil, fmt.Errorf("unknown endpoint %q", ep)
	}
	if !ep.IsTerminal() || ep == EPCancelAck {
		return c.forwardOnce(ctx, ep, path, body)
	}
	for attempt, wait := range terminalRetryWaits {
		if attempt > 0 {
			// The ladder is shared by every terminal forward, so a server blip
			// makes all of them line up on the same instant. Spread each step
			// the way the claim backoff does.
			wait = jitterWait(wait)
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
		// Without this line the whole budget can burn silently: the final
		// error carries no status or body, so a complete/fail that never
		// landed would leave only a metrics counter behind.
		c.log.Warn("terminal forward attempt failed",
			"endpoint", string(ep), "task_id", taskID, "attempt", attempt+1,
			"status", code, "err", err)
	}
	return 0, nil, fmt.Errorf("%w: %s %s", ErrTerminalUndelivered, ep, taskID)
}

func (c *Client) forwardOnce(ctx context.Context, ep Endpoint, path string, body []byte) (int, []byte, error) {
	start := time.Now()
	code, resp, err := c.doOnce(ctx, c.ctrl, http.MethodPost, path, body)
	c.metrics.Forward(string(ep), code, time.Since(start))
	return code, resp, err
}

// RenewToken is C16 (non-fatal at the call site).
func (c *Client) RenewToken(ctx context.Context) error {
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/tokens/current/renew", []byte("{}"))
	if err != nil {
		return err
	}
	if code < 200 || code >= 300 {
		return &StatusError{Code: code, Body: raw}
	}
	return nil
}

// Deregister is C14 (graceful shutdown).
func (c *Client) Deregister(ctx context.Context) error {
	ids := c.RuntimeIDs()
	if len(ids) == 0 {
		return nil
	}
	body, err := json.Marshal(map[string]any{"runtime_ids": ids})
	if err != nil {
		return err
	}
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodPost, "/api/daemon/deregister", body)
	if err != nil {
		return fmt.Errorf("deregister: %w", err)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("deregister: %w", &StatusError{Code: code, Body: raw})
	}
	return nil
}

// doOnce performs one request with the daemon identity headers and Foreman's
// server credential.
func (c *Client) doOnce(ctx context.Context, hc *http.Client, method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.ServerURL+path, bodyReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Client-Capabilities", ClientCapabilities)
	req.Header.Set("X-Client-Version", c.cfg.Version)
	req.Header.Set("X-Daemon-Id", c.cfg.DaemonID)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := hc.Do(req)
	if err != nil {
		// The C face is where every server interaction lives; without a line
		// here a transport failure is invisible unless the caller happens to
		// log it, which several do not.
		c.log.Debug("server request failed", "method", method, "path", path, "err", err)
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := readLimited(resp.Body)
	if err != nil {
		c.log.Debug("server response unreadable", "method", method, "path", path,
			"status", resp.StatusCode, "err", err)
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode >= 500 {
		c.log.Warn("server error", "method", method, "path", path,
			"status", resp.StatusCode, "body", truncateForLog(raw))
	}
	return resp.StatusCode, raw, nil
}

// truncateForLog bounds an upstream error body so one bad response cannot
// flood the log.
func truncateForLog(raw []byte) string {
	const max = 512
	if len(raw) <= max {
		return string(raw)
	}
	return string(raw[:max]) + "…"
}

// ---- wire types ----

type registerRequest struct {
	WorkspaceID     string            `json:"workspace_id"`
	DaemonID        string            `json:"daemon_id"`
	LegacyDaemonIDs []string          `json:"legacy_daemon_ids"`
	DeviceName      string            `json:"device_name"`
	CLIVersion      string            `json:"cli_version"`
	Runtimes        []runtimePayload  `json:"runtimes"`
	FailedProfiles  []json.RawMessage `json:"failed_profiles"`
}

type runtimePayload struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	Status    string `json:"status"`
	ProfileID string `json:"profile_id"`
	// RuntimeKey is the daemon-side id; it keys the bind map and never hits
	// the wire (the register body is built from the json-tagged fields).
	RuntimeKey string `json:"-"`
}

type registerResponse struct {
	Runtimes []runtimeAnswer `json:"runtimes"`
}

type runtimeAnswer struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Provider  string  `json:"provider"`
	Status    string  `json:"status"`
	ProfileID *string `json:"profile_id"`
}

// bindRuntimes matches every declared runtime to exactly one answered row.
// The daemon's C1 answer carries one row per declared runtime in the same
// order Foreman sent them, so the answer row is the primary join: consuming
// each row once keeps two daemons that declare the same identity (same
// provider and profile) from sharing one server runtime. Identity matching is
// the fallback for a server that reorders its answer.
func bindRuntimes(declared []runtimePayload, answers []runtimeAnswer) []RuntimeRef {
	used := make([]bool, len(answers))
	refs := make([]RuntimeRef, 0, len(declared))
	pickRow := func(i int, d runtimePayload, exactIndex bool) (int, bool) {
		if exactIndex {
			if i < len(answers) && !used[i] {
				return i, true
			}
			return -1, false
		}
		best := -1
		for j, a := range answers {
			if used[j] {
				continue
			}
			pid := ""
			if a.ProfileID != nil {
				pid = *a.ProfileID
			}
			if a.Provider != d.Type || pid != d.ProfileID {
				continue
			}
			if best == -1 || (a.Name == d.Name && answers[best].Name != d.Name) || a.ID < answers[best].ID {
				best = j
			}
		}
		return best, best >= 0
	}
	for i, d := range declared {
		j, ok := pickRow(i, d, true)
		if !ok {
			j, ok = pickRow(i, d, false)
		}
		if !ok {
			continue
		}
		used[j] = true
		a := answers[j]
		refs = append(refs, RuntimeRef{
			ID: a.ID, Provider: d.Type, ProfileID: d.ProfileID, Name: d.Name,
			RuntimeKey: d.RuntimeKey,
		})
	}
	return refs
}

// endpointPath maps a lifecycle endpoint to its C-path.
func endpointPath(ep Endpoint, taskID string) string {
	t := url.PathEscape(taskID)
	switch ep {
	case EPStart:
		return "/api/daemon/tasks/" + t + "/start"
	case EPProgress:
		return "/api/daemon/tasks/" + t + "/progress"
	case EPMessages:
		return "/api/daemon/tasks/" + t + "/messages"
	case EPUsage:
		return "/api/daemon/tasks/" + t + "/usage"
	case EPComplete:
		return "/api/daemon/tasks/" + t + "/complete"
	case EPFail:
		return "/api/daemon/tasks/" + t + "/fail"
	case EPCancelAck:
		return "/api/daemon/tasks/" + t + "/cancel-ack"
	case EPSession:
		return "/api/daemon/tasks/" + t + "/session"
	case EPWaitLocalDir:
		return "/api/daemon/tasks/" + t + "/wait-local-directory"
	}
	return ""
}
