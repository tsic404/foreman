package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// Client is the fake client: every F→S call of §1.2 (C-1–C-21). It injects
// the daemon identity headers and Foreman's server credential, caches the
// declared real runtime set, and owns the terminal-forward retry budget.
type Client struct {
	cfg     Config
	token   string
	ctrl    *http.Client // 30s control plane (§1.5)
	lease   *http.Client // 10s prepare-lease (§1.5)
	pass    *http.Client // S29: same budget, compression negotiation off
	metrics Metrics
	log     *slog.Logger

	// startup stages the startup discovery result by provider until C1 binds
	// it; slots is the per-real-runtime catalog cache (C21).
	startup *registry.StartupCatalog
	slots   *CatalogSlots
	// taskRuntime resolves the real runtime id a task was dispatched under,
	// which the S-8 body needs (S-8).
	taskRuntime func(taskID string) string

	mu             sync.RWMutex
	disc           registry.Discovery
	refs           []RuntimeRef
	state          WorkspaceState
	workspace      string // workspace display name from C17
	source         string // declaration source: startup or discovery-probe
	profiles       profileSnapshot
	discoveryProbe DiscoveryProbeRunner

	// opMu serializes every C1 write: the startup/rebuild register and the
	// §1.6 refresh sequence (which also creates a probe Job while holding it).
	opMu sync.Mutex

	// reposMu guards reposCall, the C-22 single-flight slot.
	reposMu   sync.Mutex
	reposCall *reposRefresh

	// lost records, per provider, that a 404 "runtime not found" proved that
	// row deleted and its re-register failed (§1.5); it survives
	// so the next successful C1 still treats that runtime as rebuilt (C15 +
	// runtime-set signal) — and only that one.
	lost map[string]bool

	terminalWaits []time.Duration // §10 terminal retry budget
	relayWaits    []time.Duration // C-21 relay budget progression (tests)

	// changes is the legacy fan-in channel every notifyChange also feeds;
	// changeSubs are the per-consumer streams. The loops must each see every
	// notification, so they subscribe (Changes alone would let the two
	// consumers race for one queue item).
	changes    chan RuntimeChange
	changesMu  sync.Mutex
	changeSubs []chan RuntimeChange
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

// WithTerminalRetryWaits overrides the §10 terminal retry budget (tests).
func WithTerminalRetryWaits(waits []time.Duration) Option {
	return func(c *Client) { c.terminalWaits = waits }
}

// WithRelayBackoff overrides the C-21 relay backoff progression (tests);
// the production progression is the §1.5 1s→30s one.
func WithRelayBackoff(waits []time.Duration) Option {
	return func(c *Client) { c.relayWaits = waits }
}

// NewClient builds the fake client. token is Foreman's server credential
// (mdt_/mul_, auth.LoadServerToken); it is never forwarded to Jobs (F3).
func NewClient(cfg Config, token string, opts ...Option) *Client {
	c := &Client{
		cfg:   cfg,
		token: token,
		ctrl:  &http.Client{Timeout: controlTimeout},
		lease: &http.Client{Timeout: leaseTimeout},
		pass: &http.Client{
			Transport: passthroughTransport(),
			// No whole-request deadline: with the body streamed in both
			// directions (ADR-046) a client timeout would cut a transfer that
			// is merely slow instead of bounding a dead upstream. The bound
			// that matters — no response head within the control budget —
			// lives on the transport.
			// A 3xx is the upstream's answer to hand back verbatim: the caller
			// decides whether to follow it (§2.4: S-29 已知边界).
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		metrics: noopMetrics{},
		log:     slog.Default().With("component", "proxy-client"),
		source:  declarationSourceStartup,

		terminalWaits: terminalRetryWaits,
		changes:       make(chan RuntimeChange, 16),
	}
	if c.cfg.WorkspaceStateTimeout <= 0 {
		c.cfg.WorkspaceStateTimeout = DefaultWorkspaceStateTimeout
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.startup == nil {
		c.startup = registry.NewStartupCatalog()
	}
	if c.slots == nil {
		c.slots = NewCatalogSlots()
	}
	return c
}

// WorkspaceState returns the workspace state carrier seeded by C-1 and
// refreshed by C-22 (§1.7): repos / repos_version / settings verbatim plus the
// seeded flag that marks the pre-C1 window.
func (c *Client) WorkspaceState() WorkspaceState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.clone()
}

// WorkspaceID returns the workspace this client serves. One client serves one
// (credential, daemon identity) pair (§0).
func (c *Client) WorkspaceID() string { return c.cfg.WorkspaceID }

// DaemonID returns the daemon identity this client registers under.
func (c *Client) DaemonID() string { return c.cfg.DaemonID }

// CatalogSlots returns the per-real-runtime catalog cache this client owns.
func (c *Client) CatalogSlots() *CatalogSlots { return c.slots }

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

// WorkspaceName returns the display name cached by C17 and whether it was
// actually seeded. An unseeded name means the authority has not answered yet
// (a transient C-17 failure in the workspace-scoped form) — never a fact about
// the workspace, so callers must not treat "" as the name.
func (c *Client) WorkspaceName() (name string, seeded bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workspace, c.workspace != ""
}

// ---- C1 register (runtime.go) ----

// registerRequest is the C1 body (§1.2; field names strict, type not
// provider, legacy_daemon_ids empty). failed_profiles travels verbatim from
// the startup probe's registration body — Foreman never interprets it.
type registerRequest struct {
	WorkspaceID     string            `json:"workspace_id"`
	DaemonID        string            `json:"daemon_id"`
	LegacyDaemonIDs []string          `json:"legacy_daemon_ids"`
	DeviceName      string            `json:"device_name"`
	CLIVersion      string            `json:"cli_version"`
	LaunchedBy      string            `json:"launched_by"`
	Runtimes        []runtimePayload  `json:"runtimes"`
	FailedProfiles  []json.RawMessage `json:"failed_profiles"`
}

type runtimePayload struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	Status    string `json:"status"`
	ProfileID string `json:"profile_id"`
}

type registerResponse struct {
	Runtimes []runtimeAnswer `json:"runtimes"`
	// Repos / repos_version / settings replay verbatim into the workspace state
	// carrier (§1.7): a nil raw field means the upstream did not send the key.
	Repos        json.RawMessage `json:"repos"`
	ReposVersion json.RawMessage `json:"repos_version"`
	Settings     json.RawMessage `json:"settings"`
}

// recoverOrphans is C15, called only after a real-runtime rebuild (§1.2).
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

// Preflight runs the post-registration startup checks: C16 token renew
// (non-fatal). C1 is carried by the cached declaration unless a live runtime
// set already exists. C18 is not part of the preflight: the profile snapshot is
// taken before the startup probe (§1.6 startup order). C17 is not part of the
// preflight either: discovery/scope validation happens once at startup
// (ADR-049), and §1.5 keeps runtime credential failures inside the per-§1.4
// backoff — a runtime 401/403 is retried, never judged config (AC-86). Callers
// retry with the §1.5 backoff.
func (c *Client) Preflight(ctx context.Context) error {
	if len(c.RuntimeIDs()) == 0 {
		if err := c.Register(ctx); err != nil {
			return err
		}
	}
	if err := c.RenewToken(ctx); err != nil {
		c.log.Warn("token renew failed (non-fatal)", "err", err)
	}
	return nil
}

// ---- C5–C12 lifecycle forward ----

// endpointPath maps a lifecycle endpoint to its §1.2 path suffix.
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
// credential and returns the upstream status and body. S8 is the one
// callback whose body names a runtime — the daemon names the job runtime it
// was handed (S-6), the server knows only the real runtime id its claim
// was dispatched under — so the identity is translated exactly as S7
// translates {rid}. Terminal endpoints carry the §10 retry budget
// (4s/8s/16s/32s/64s; 5xx/408/429/network only); exhaustion returns
// ErrTerminalUndelivered for the PendingReports queue.
func (c *Client) Forward(ctx context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error) {
	return c.ForwardInWorkspace(ctx, ep, taskID, c.cfg.WorkspaceID, body)
}

// ForwardInWorkspace forwards a task-addressed callback for workspaceID. A
// Client owns exactly one (credential, daemon identity) pair (§1.1), so a
// different workspace is a routing error, never a borrowed credential.
func (c *Client) ForwardInWorkspace(ctx context.Context, ep scheduler.Endpoint, taskID, workspaceID string, body []byte) (int, []byte, error) {
	if workspaceID != "" && workspaceID != c.cfg.WorkspaceID {
		return 0, nil, fmt.Errorf("workspace %s is not served by the %s client", workspaceID, c.cfg.WorkspaceID)
	}
	path := endpointPath(ep, taskID)
	if path == "" {
		return 0, nil, fmt.Errorf("unknown endpoint %q", ep)
	}
	if ep == scheduler.EPStart {
		translated, err := c.translateStartBody(taskID, body)
		if err != nil {
			return 0, nil, err
		}
		body = translated
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

// translateStartBody rewrites the runtime identity an S8 body carries: the
// daemon names the job runtime it was handed at S6, while the server knows
// only the real runtime id its claim was dispatched under. Forwarded
// verbatim, the daemon's identity reads as a claim the server no longer
// holds, and the start is refused (409 状态不允许, S-8 +
// §2.2) — so the field is translated like S7's {rid}.
// The id comes from the task's own dispatch record; a task without one (a
// rebuilt entry) falls back to the only declared runtime, and a set that is
// not registered yet leaves the body verbatim. Every other field keeps its
// value: the re-marshal may reorder keys and HTML-escape, which §2.4 excludes
// from the fidelity face (key order has no HTTP or JSON semantics), and a body
// naming no runtime is forwarded byte-for-byte.
func (c *Client) translateStartBody(taskID string, body []byte) ([]byte, error) {
	if len(body) == 0 {
		return body, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return body, nil
	}
	if _, ok := fields["runtime_id"]; !ok {
		return body, nil
	}
	runtimeID, exact := c.runtimeForTask(taskID)
	if runtimeID == "" {
		return body, nil
	}
	if !exact {
		c.log.Warn("task dispatch record unknown; using the first declared runtime",
			"task_id", taskID, "runtime_id", runtimeID)
	}
	quoted, err := json.Marshal(runtimeID)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(fields["runtime_id"], quoted) {
		return body, nil
	}
	fields["runtime_id"] = quoted
	translated, err := json.Marshal(fields)
	if err != nil {
		return body, nil
	}
	return translated, nil
}

func (c *Client) forwardOnce(ctx context.Context, ep scheduler.Endpoint, path string, body []byte) (int, []byte, error) {
	start := time.Now()
	code, resp, err := c.doOnce(ctx, c.ctrl, http.MethodPost, path, body)
	c.metrics.Forward(string(ep), code, time.Since(start))
	return code, resp, err
}

// ---- S29 passthrough ----

// exclusionFields is the §2.4 exclusion set: the RFC 7230 §6.1 hop-by-hop
// fields, plus the two the proxy generates for its own connection and body
// (the target authority as Host, the forwarded body's real length as
// Content-Length). Every other field travels as the caller gave it.
var exclusionFields = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true, // TE (§6.1)
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Host":                true,
	"Content-Length":      true,
}

// forwardableHeader copies a header set minus the §2.4 exclusion set — the
// fields above plus whatever a Connection value names. Values keep their
// order and are never merged or dropped beyond that set; field names are
// written in canonical form (their case is not a fidelity item). This is the
// single place the exclusions apply.
func forwardableHeader(h http.Header) http.Header {
	var named map[string]bool // built only when a Connection value names fields
	for _, v := range h.Values("Connection") {
		for _, f := range strings.Split(v, ",") {
			if f = textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(f)); f != "" {
				if named == nil {
					named = map[string]bool{}
				}
				named[f] = true
			}
		}
	}
	out := make(http.Header, len(h))
	for k, vv := range h {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if exclusionFields[ck] || named[ck] {
			continue
		}
		// A set carrying the same field under two spellings keeps every
		// value, in arrival order (the spelling itself carries no meaning).
		out[ck] = append(out[ck], vv...)
	}
	return out
}

// callerVisible is the status the caller of S29 ends up seeing: upstream
// 5xx converges to 502 (§2.4), which is also the code the
// agent-api metric carries (§9.2).
func callerVisible(upstream int) int {
	if upstream >= http.StatusInternalServerError {
		return http.StatusBadGateway
	}
	return upstream
}

// passthroughTransport is the §2.4 transport for S29: compression
// negotiation is off, so no Accept-Encoding is invented for a caller that
// sent none and no upstream-encoded body is transparently decoded. A
// DefaultTransport that is not an *http.Transport falls back to the same
// connection defaults instead of panicking at startup over a mutable global.
// Only the head phase is bounded — an upstream that never answers converges to
// 502 — while the body phase has no proxy-side deadline (ADR-046).
func passthroughTransport() http.RoundTripper {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		base = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		}
	}
	t := base.Clone()
	t.DisableCompression = true
	t.ResponseHeaderTimeout = controlTimeout
	return t
}

// PassthroughResponse is one S29 upstream answer: the status line, the
// caller-visible header set (§2.4 exclusion set already applied) and the body,
// still unread. The receiver owns the body and must close it.
type PassthroughResponse struct {
	StatusCode    int
	Header        http.Header
	ContentLength int64
	Body          io.ReadCloser
}

// Passthrough forwards an agent-API call to the real server verbatim
// (§2.1 rule 2 + ADR-020 + ADR-046): method, raw path (EscapedPath, keeping
// %2F-style segments), raw query (no decode/re-encode), the caller's whole
// header set and body go out as given; the upstream status, whole header set
// and body come back as given. The exclusion set is applied here, once;
// application/json is not a fallback and Foreman's own fields never appear.
// Neither body is buffered: both stream through, so payload size bounds neither
// memory nor success, and response bytes reach the caller as the upstream
// produces them.
func (c *Client) Passthrough(ctx context.Context, method, rawPath, rawQuery string, header http.Header, body io.Reader, contentLength int64) (*PassthroughResponse, error) {
	u := c.cfg.ServerURL + rawPath
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if body != nil && contentLength >= 0 {
		// Content-Length is never forwarded as a field (§2.4 exclusion set);
		// the forwarded body's length comes from here. The caller's own
		// framing thus survives: a declared length stays declared, an
		// undeclared body stays chunked.
		req.ContentLength = contentLength
	}
	req.Header = forwardableHeader(header)
	if _, ok := req.Header["User-Agent"]; !ok {
		// Present-but-empty suppresses the client library's default value:
		// net/http only writes a User-Agent the request was given. The copy
		// above writes canonical names, so the lookup is exact.
		req.Header["User-Agent"] = nil
	}
	start := time.Now()
	resp, err := c.pass.Do(req)
	if err != nil {
		c.metrics.Forward(EndpointAgentAPI, http.StatusBadGateway, time.Since(start))
		return nil, err
	}
	c.metrics.Forward(EndpointAgentAPI, callerVisible(resp.StatusCode), time.Since(start))
	return &PassthroughResponse{
		StatusCode:    resp.StatusCode,
		Header:        forwardableHeader(resp.Header),
		ContentLength: resp.ContentLength,
		Body:          resp.Body,
	}, nil
}

// ---- C13 status / S20 gc-check ----

// TaskStatus is C13; a server-side 404 maps to scheduler.ErrTaskNotFound.
func (c *Client) TaskStatus(ctx context.Context, taskID string) (string, error) {
	code, raw, err := c.taskStatusRaw(ctx, taskID)
	if errors.Is(err, scheduler.ErrTaskNotFound) {
		return "", err
	}
	if err != nil {
		return "", err
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

// TaskStatusRaw is C13's response face for S-9: the upstream status code and
// body bytes, with only 404 mapped to the task-vanished sentinel. Everything
// else is the upstream's answer and S-9 returns it verbatim (§2.2).
func (c *Client) TaskStatusRaw(ctx context.Context, taskID string) (int, []byte, error) {
	return c.taskStatusRaw(ctx, taskID)
}

func (c *Client) taskStatusRaw(ctx context.Context, taskID string) (int, []byte, error) {
	code, raw, err := c.doOnce(ctx, c.ctrl, http.MethodGet, "/api/daemon/tasks/"+url.PathEscape(taskID)+"/status", nil)
	if err != nil {
		return 0, nil, err
	}
	if code == 404 {
		return 0, nil, scheduler.ErrTaskNotFound
	}
	return code, raw, nil
}

// GCCheck forwards the S20 gc-check query to the server verbatim (GC is
// disabled Job-side, but the endpoint stays truthful if enabled, S-20).
func (c *Client) GCCheck(ctx context.Context, taskID string) (int, []byte, error) {
	return c.doOnce(ctx, c.ctrl, http.MethodGet, "/api/daemon/tasks/"+url.PathEscape(taskID)+"/gc-check", nil)
}

// ---- C19 skill bundles ----

// ResolveSkillBundles is C19: forwards the Job daemon's resolve body with
// the {rid} rewritten to the real runtime id and Foreman's credential (S26).
func (c *Client) ResolveSkillBundles(ctx context.Context, taskID string, body []byte) (int, []byte, error) {
	rid, exact := c.runtimeForTask(taskID)
	if rid == "" {
		return 0, nil, errors.New("skill bundles: no real runtime registered")
	}
	if !exact {
		c.log.Warn("task dispatch record unknown; using the first declared runtime",
			"task_id", taskID, "runtime_id", rid)
	}
	path := fmt.Sprintf("/api/daemon/runtimes/%s/tasks/%s/skill-bundles/resolve",
		url.PathEscape(rid), url.PathEscape(taskID))
	return c.doOnce(ctx, c.ctrl, http.MethodPost, path, body)
}

// runtimeForTask resolves the real runtime id a task was dispatched under. A
// rebuilt entry carries no dispatch record (the Job is the authority store,
// ADR-002), so the first declared runtime is the best available answer; exact
// reports which of the two paths produced the id.
func (c *Client) runtimeForTask(taskID string) (runtimeID string, exact bool) {
	if c.taskRuntime != nil {
		if rid := c.taskRuntime(taskID); rid != "" {
			return rid, true
		}
	}
	ids := c.RuntimeIDs()
	if len(ids) == 0 {
		return "", false
	}
	return ids[0], false
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

// SetWorkspaceName caches the workspace display name discovered from C-17
// (ADR-049): the discovery answer names each workspace, and S-1 serves the
// name verbatim. The id itself never changes for one client. The name is never
// empty: the C-17 decode rejects an unnamed entry first, because "" would read
// back as "the authority has not answered" (WorkspaceName).
func (c *Client) SetWorkspaceName(name string) {
	c.mu.Lock()
	c.workspace = name
	c.mu.Unlock()
}

// ---- transport ----

// doOnce is the transport core: one attempt with the daemon identity
// headers; the response body is returned verbatim.
func (c *Client) doOnce(ctx context.Context, hc *http.Client, method, path string, body []byte) (int, []byte, error) {
	return c.doOnceVersion(ctx, hc, method, path, c.CLIVersion(), body)
}

// doOnceVersion is doOnce with an explicit X-Client-Version. C-1 uses it: the
// request declares a discovery whose cli_version the cache does not hold yet
// (the declaration is committed by that very answer, §1.1 客户端版本头).
func (c *Client) doOnceVersion(ctx context.Context, hc *http.Client, method, path, version string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.ServerURL+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	c.setHeaders(req, body != nil, version)
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
// verbatim and must never gain or lose an entry. version is the
// X-Client-Version value — the cached discovery's cli_version once C1 has run,
// Foreman's own version before that (C-1 的取值来源与声明域).
func (c *Client) setHeaders(req *http.Request, hasBody bool, version string) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Client-Platform", "daemon")
	req.Header.Set("X-Client-Version", version)
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
