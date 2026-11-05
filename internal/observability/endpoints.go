package observability

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/tsic404/foreman/internal/registry"
)

// Log tail bounds for /foreman/tasks/{id}/logs (§9.3).
const (
	defaultLogTail = 200
	maxLogTail     = 2000
)

// TaskIndex is the registry read view the ops endpoints need; satisfied by
// *registry.Registry.
type TaskIndex interface {
	// GetInWorkspace returns one live row; the workspace dimension is part of
	// the projection's partition (§4.1). A shared-state index reads the row
	// from the adjudication face, so every instance answers the same (§9.3
	// 多实例口径).
	GetInWorkspace(workspaceID, taskID string) (registry.TaskEntry, bool)
	// SettledInWorkspace returns the retained terminal record of an
	// already-reclaimed task (§4.3, ADR-032): the single-task endpoint answers
	// with it inside the retention window.
	SettledInWorkspace(workspaceID, taskID string) (registry.TaskEntry, bool)
	// ListInWorkspace lists the workspace's live rows ("" = every row, the
	// legacy single-workspace shape).
	ListInWorkspace(workspaceID string) []registry.TaskEntry
}

// OpsConfig carries the values the ops endpoints report. RegisterSource
// resolves the startup discovery view (S31), cropped per request to the
// caller's grant; a nil one keeps the route and answers 404. Logger defaults
// to slog.Default.
type OpsConfig struct {
	Version        string
	JobImage       string
	RegisterSource func() map[string]any
	// Credentials are the bearer credentials every ops request must present
	// (§2.2 S-25). Empty refuses every request: the surface never answers
	// without a configured credential.
	Credentials []OpsCredential
	// Health is the liveness view of the process's critical loops; nil keeps
	// the endpoint's always-ok shape (a handler mounted without loops, as in
	// the ops tests).
	Health *Health
	// WorkspaceIDs is the deployment's configured workspace set (§0,
	// ADR-047). A multi-workspace deployment must be asked with the
	// workspace dimension; a single workspace is its default. Empty keeps
	// the pre-ADR-047 single-workspace shape.
	WorkspaceIDs []string
	StartedAt    time.Time
	Logger       *slog.Logger
}

// OpsHandler is the ops surface's route table. It is the read side of the
// payload write-face census (§9.3): RegisteredPatterns reports the patterns
// recorded by register, the surface's one mount point. That every mount goes
// through register is a convention, not a language guarantee; the census
// reads this package's source and binds values: a foreign receiver, a pattern
// that does not resolve to the recorded parameter, a shadowing parameter or
// range variable, a reassigned name, an escaped mount method and a second
// route table fail it. It does not follow a pattern mutated through a pointer.
type OpsHandler struct {
	// dispatch is the mux's ServeHTTP method value. ServeHTTP admits the
	// credential before calling it, so no route is reachable without one; the
	// mux itself does not leave NewOpsHandler: no field or method hands it
	// out, and the census fails on any other mux use in this function or
	// construction in this package.
	dispatch http.HandlerFunc
	// auth is the credential face every request passes before dispatch.
	auth *opsAuth
	// workspaceIDs is the deployment's served set (§0, ADR-047); the
	// credential's grant is resolved against it.
	workspaceIDs []string
	registered   []string
}

// ServeHTTP authenticates the request and dispatches to the mounted routes.
func (h *OpsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.auth.serve(h.dispatch, w, r)
}

// RegisteredPatterns returns the patterns the surface registered, in order.
func (h *OpsHandler) RegisteredPatterns() []string {
	return append([]string(nil), h.registered...)
}

// NewOpsHandler mounts the ops surface (§2.2 S-25): /metrics plus /foreman/*,
// served on its own listener (the Job face mounts only /api). Every request
// must present one of cfg.Credentials as a bearer token; without one the
// answer is 401 and no route is reached, so a Job Token — a credential of the
// other face — never opens this surface. A nil logs reader keeps the route but
// answers 404 (optional endpoint).
func NewOpsHandler(metrics *Metrics, index TaskIndex, logs PodLogReader, cfg OpsConfig) *OpsHandler {
	if cfg.StartedAt.IsZero() {
		cfg.StartedAt = time.Now()
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "observability")
	handler := &OpsHandler{
		auth:         newOpsAuth(cfg.Credentials),
		workspaceIDs: append([]string(nil), cfg.WorkspaceIDs...),
	}
	mux := http.NewServeMux()
	// register is the single mount point: the pattern is recorded by the same
	// call that mounts it, and the census requires every mount in this closure
	// to pass that recorded binding, so the mounted set and the recorded set
	// cannot diverge. Inside the closure the mux is legal only as a mount's
	// receiver — passing it out, taking a method value or assigning it fails —
	// and the package keeps one route table: a second construction, a
	// reassigned or shadowed name, and a foreign receiver all fail.
	register := func(pattern string, h http.Handler) {
		handler.registered = append(handler.registered, pattern)
		mux.Handle(pattern, h)
	}
	registerFn := func(pattern string, h http.HandlerFunc) { register(pattern, h) }
	if metrics == nil {
		// No metrics registry was wired: the endpoint is a readiness input
		// (§9.3), so it answers "not ready" instead of dereferencing nil.
		registerFn("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusServiceUnavailable, "metrics are not wired")
		})
	} else {
		metricsHandler := metrics.Handler()
		register("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The metric set carries deployment-wide series with a
			// per-workspace label (§9.2 多 workspace 面): only the deployment
			// credential is trusted with it, never a workspace-bound one. A
			// request that reached this handler without an admitted identity
			// is no deployment credential either — fail closed like the rest
			// of the surface instead of answering every workspace.
			if identity, ok := opsIdentityFrom(r.Context()); !ok || identity.workspaces != nil {
				writeError(w, http.StatusForbidden, "deployment credential required")
				return
			}
			metricsHandler.ServeHTTP(w, r)
		}))
	}
	registerFn("GET /foreman/healthz", func(w http.ResponseWriter, _ *http.Request) {
		serveHealthz(w, cfg.Health, cfg.StartedAt)
	})
	registerFn("GET /foreman/tasks", func(w http.ResponseWriter, r *http.Request) {
		// TaskEntry.Payload is json:"-" — the list never leaks payloads (F3).
		scope := handler.scope(r)
		if scope.status != 0 {
			// §4.1: the list is partitioned by the caller's workspace grant; a
			// deployment with more than one visible workspace has no default
			// dimension, and answering every workspace would cross the
			// partition.
			writeError(w, scope.status, scope.message)
			return
		}
		entries := index.ListInWorkspace(scope.workspaceID)
		if entries == nil {
			entries = []registry.TaskEntry{}
		}
		writeJSON(w, http.StatusOK, entries)
	})
	registerFn("GET /foreman/tasks/{taskID}", func(w http.ResponseWriter, r *http.Request) {
		scope := handler.scope(r)
		if scope.status != 0 {
			writeError(w, scope.status, scope.message)
			return
		}
		workspaceID := scope.workspaceID
		taskID := r.PathValue("taskID")
		e, ok := index.GetInWorkspace(workspaceID, taskID)
		if !ok {
			// A reclaimed task keeps answering with its terminal record until
			// the retention window expires (§9.3, ADR-032):
			// the convergence instant is no longer the end of the evidence
			// window. Beyond it, a reclaimed task and one that never existed
			// are again indistinguishable.
			e, ok = index.SettledInWorkspace(workspaceID, taskID)
		}
		if !ok {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		if workspaceID != "" && e.WorkspaceID != workspaceID {
			// A cross-workspace task id is indistinguishable from one that
			// never existed (§4.1: 跨 workspace 一律 404).
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeJSON(w, http.StatusOK, e)
	})
	registerFn("GET /foreman/tasks/{taskID}/logs", func(w http.ResponseWriter, r *http.Request) {
		scope := handler.scope(r)
		if scope.status != 0 {
			writeError(w, scope.status, scope.message)
			return
		}
		if !workspaceVisible(index, r.PathValue("taskID"), scope.workspaceID) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		serveJobLogs(w, r, index, scope.workspaceID, logs, log)
	})
	registerFn("GET /foreman/tasks/{taskID}/payload-preview", func(w http.ResponseWriter, r *http.Request) {
		scope := handler.scope(r)
		if scope.status != 0 {
			writeError(w, scope.status, scope.message)
			return
		}
		workspaceID := scope.workspaceID
		e, ok := index.GetInWorkspace(workspaceID, r.PathValue("taskID"))
		if ok && !workspaceVisible(index, e.TaskID, workspaceID) {
			ok = false
		}
		if !ok {
			// A reclaimed entry's terminal record is still answerable, but the
			// payload is not part of it: the record drops the body at the
			// verdict (registry.settledRecord), and this window has always
			// been the live entry alone (§9.3).
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		// The served body is the entry's delivered payload — written by the S6
		// delivery — and the window is delivered_at until reclaim. An entry
		// that is not delivered yet holds the claim-time payload, which must
		// never be exposed as the delivery body (§9.3).
		if e.DeliveredAt.IsZero() || len(e.Payload) == 0 {
			writeError(w, http.StatusNotFound, "payload unavailable")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(e.Payload)
	})
	// S31 is read-only by design: it reports the startup discovery, the C1
	// declaration and the catalog slots, and can never trigger a discovery or
	// a refresh (ADR-025). The answer is cropped to the caller's grant: a
	// workspace the credential cannot read is absent, not rendered empty.
	registerFn("GET /foreman/register-source", func(w http.ResponseWriter, r *http.Request) {
		if cfg.RegisterSource == nil {
			writeError(w, http.StatusNotFound, "register source unavailable")
			return
		}
		writeJSON(w, http.StatusOK, handler.cropToGrant(r, cfg.RegisterSource()))
	})
	registerFn("GET /foreman/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"version":   cfg.Version,
			"job_image": cfg.JobImage,
		})
	})
	handler.dispatch = mux.ServeHTTP
	return handler
}

// serveJobLogs proxies the newest pod's log tail for the task's Job.
func serveJobLogs(w http.ResponseWriter, r *http.Request, index TaskIndex, workspaceID string, logs PodLogReader, log *slog.Logger) {
	e, ok := index.GetInWorkspace(workspaceID, r.PathValue("taskID"))
	if !ok {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	tail, err := parseTail(r.URL.Query().Get("tail"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if logs == nil {
		writeError(w, http.StatusNotFound, ErrJobLogsUnavailable.Error())
		return
	}
	data, err := logs.ReadLogs(r.Context(), e.JobNamespace, e.JobName, tail)
	switch {
	case errors.Is(err, ErrJobLogsUnavailable):
		writeError(w, http.StatusNotFound, ErrJobLogsUnavailable.Error())
		return
	case apierrors.IsForbidden(err):
		// pods/log RBAC denied is 403 + an error log; the task chain itself
		// is unaffected.
		log.ErrorContext(r.Context(), "read job logs forbidden",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		writeError(w, http.StatusForbidden, "job logs forbidden")
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "read job logs")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// parseTail applies the contract bounds: default 200, clamp to 2000.
func parseTail(raw string) (int64, error) {
	if raw == "" {
		return defaultLogTail, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 1 {
		return 0, errors.New("tail must be a positive integer")
	}
	if n > maxLogTail {
		n = maxLogTail
	}
	return n, nil
}

// opsScope is the workspace dimension an ops request resolves to: either the
// id, or the refusal the handler answers with (status 0 = resolved).
type opsScope struct {
	workspaceID string
	status      int
	message     string
}

// scope resolves the request's workspace dimension from the caller's
// credential, never from the query alone (§2.2 S-25). A request naming a
// workspace outside the grant is answered like a workspace that does not
// exist, so the query parameter cannot widen the partition.
func (h *OpsHandler) scope(r *http.Request) opsScope {
	visible := h.visibleWorkspaces(r)
	if requested := strings.TrimSpace(r.URL.Query().Get("workspace")); requested != "" {
		if visible != nil && !slices.Contains(visible, requested) {
			return opsScope{status: http.StatusNotFound, message: "workspace not found"}
		}
		return opsScope{workspaceID: requested}
	}
	if len(visible) == 1 {
		return opsScope{workspaceID: visible[0]}
	}
	if visible == nil {
		// The pre-ADR-047 single-workspace shape: the deployment has no
		// workspace dimension to choose from.
		return opsScope{}
	}
	return opsScope{status: http.StatusBadRequest, message: "workspace is required"}
}

// visibleWorkspaces is the credential's grant intersected with the deployment's
// served set. A nil result is the pre-ADR-047 shape (no configured dimension);
// an empty one grants nothing — the fail-closed answer for an identity the
// middleware could not resolve.
func (h *OpsHandler) visibleWorkspaces(r *http.Request) []string {
	identity, ok := opsIdentityFrom(r.Context())
	if !ok {
		return []string{}
	}
	if identity.workspaces == nil {
		return h.workspaceIDs
	}
	if len(h.workspaceIDs) == 0 {
		return identity.workspaces
	}
	visible := make([]string, 0, len(identity.workspaces))
	for _, workspaceID := range h.workspaceIDs {
		if slices.Contains(identity.workspaces, workspaceID) {
			visible = append(visible, workspaceID)
		}
	}
	return visible
}

// cropToGrant keeps only the workspaces the caller's credential may read. A
// deployment-wide credential keeps the view untouched.
func (h *OpsHandler) cropToGrant(r *http.Request, view map[string]any) map[string]any {
	visible := h.visibleWorkspaces(r)
	if visible == nil {
		return view
	}
	cropped := make(map[string]any, len(visible))
	for _, workspaceID := range visible {
		if entry, ok := view[workspaceID]; ok {
			cropped[workspaceID] = entry
		}
	}
	return cropped
}

// NewHealthHandler serves the Job face's liveness probe input: the liveness
// verdict alone, with no task, workspace or runtime identity field, so a Job
// pod reading it learns nothing about another workspace (§2.2 S-25). The ops
// surface answers the same shape behind its credential.
func NewHealthHandler(health *Health, startedAt time.Time) http.Handler {
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		serveHealthz(w, health, startedAt)
	})
}

// serveHealthz is the single liveness answer both faces mount. §9.3: a
// critical loop that stopped while the process kept serving is exactly the
// half-dead state the probe exists to end, so it must not answer ok (§1.5: a
// config error is logged and alarmed, not silently absorbed).
func serveHealthz(w http.ResponseWriter, health *Health, startedAt time.Time) {
	body := map[string]any{
		"status":   "ok",
		"uptime_s": int64(time.Since(startedAt).Seconds()),
	}
	status := http.StatusOK
	if stopped := health.Stopped(); len(stopped) > 0 {
		status = http.StatusServiceUnavailable
		body["status"] = "failed"
		body["stopped_components"] = stopped
	}
	writeJSON(w, status, body)
}

// workspaceVisible applies the projection's workspace partition to the
// task-addressed ops routes: an entry of another workspace is invisible (404),
// exactly like a task that never existed.
func workspaceVisible(index TaskIndex, taskID, workspaceID string) bool {
	if workspaceID == "" {
		return true
	}
	e, ok := index.GetInWorkspace(workspaceID, taskID)
	if !ok {
		e, ok = index.SettledInWorkspace(workspaceID, taskID)
	}
	return ok && e.WorkspaceID == workspaceID
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes the ops surface's error shape {"error":"<message>"}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
