package agentconfig

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// maxAdminBody bounds one configuration request body.
const maxAdminBody = 96 << 10

// AdminConfig is the configuration surface's setup.
type AdminConfig struct {
	Service   *Service
	Authorize func(*http.Request) (deploymentWide bool, ok bool)
	Actor     func(*http.Request) string
}

// AdminHandler serves the Agent configuration revisions and their projection.
type AdminHandler struct {
	cfg    AdminConfig
	routes http.Handler
}

// NewAdminHandler mounts the routes.
func NewAdminHandler(cfg AdminConfig) (*AdminHandler, error) {
	if cfg.Service == nil {
		return nil, errors.New("agentconfig: an admin surface needs the service")
	}
	if cfg.Authorize == nil || cfg.Actor == nil {
		return nil, errors.New("agentconfig: an admin surface needs an authorize and an actor resolver")
	}
	handler := &AdminHandler{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/agent-configs", handler.list)
	mux.HandleFunc("GET /admin/agent-configs/audit", handler.audit)
	mux.HandleFunc("GET /admin/agent-configs/{serverID}/{workspaceID}/{backend}", handler.get)
	mux.HandleFunc("PUT /admin/agent-configs/{serverID}/{workspaceID}/{backend}", handler.write)
	mux.HandleFunc("GET /admin/agent-configs/{serverID}/{workspaceID}/{backend}/projection", handler.projection)
	handler.routes = mux
	return handler, nil
}

// ServeHTTP answers one configuration request.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.routes.ServeHTTP(w, r)
}

func (h *AdminHandler) wide(w http.ResponseWriter, r *http.Request) bool {
	deploymentWide, ok := h.cfg.Authorize(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	if !deploymentWide {
		writeError(w, http.StatusForbidden, "deployment credential required")
		return false
	}
	return true
}

func (h *AdminHandler) list(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	views, err := h.cfg.Service.List(r.Context(),
		strings.TrimSpace(r.URL.Query().Get("server_id")),
		strings.TrimSpace(r.URL.Query().Get("workspace_id")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read Agent configurations")
		return
	}
	if views == nil {
		views = []View{}
	}
	writeJSON(w, http.StatusOK, views)
}

func (h *AdminHandler) get(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	view, ok, err := h.cfg.Service.Get(r.Context(), Scope{
		ServerID: r.PathValue("serverID"), WorkspaceID: r.PathValue("workspaceID"),
		Backend: r.PathValue("backend"), Profile: strings.TrimSpace(r.URL.Query().Get("profile")),
	})
	switch {
	case err != nil:
		writeError(w, http.StatusBadRequest, err.Error())
	case !ok:
		writeError(w, http.StatusNotFound, ErrNotFound.Error())
	default:
		writeJSON(w, http.StatusOK, view)
	}
}

func (h *AdminHandler) write(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	var body WriteRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxAdminBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "request body is not the expected JSON")
		return
	}
	view, err := h.cfg.Service.Write(r.Context(), Scope{
		ServerID: r.PathValue("serverID"), WorkspaceID: r.PathValue("workspaceID"),
		Backend: r.PathValue("backend"), Profile: body.Profile,
	}, body, h.cfg.Actor(r))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) projection(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	projection, err := h.cfg.Service.Project(r.Context(), Scope{
		ServerID: r.PathValue("serverID"), WorkspaceID: r.PathValue("workspaceID"),
		Backend: r.PathValue("backend"), Profile: strings.TrimSpace(r.URL.Query().Get("profile")),
	}, strings.TrimSpace(r.URL.Query().Get("attempt_id")))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeJSON(w, http.StatusOK, projection)
	}
}

func (h *AdminHandler) audit(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	entries, err := h.cfg.Service.Audit(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read Agent configuration audit")
		return
	}
	if entries == nil {
		entries = []AuditEntry{}
	}
	writeJSON(w, http.StatusOK, entries)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
