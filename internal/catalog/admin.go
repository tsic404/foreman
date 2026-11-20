package catalog

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// maxAdminBody bounds one administrative request body.
const maxAdminBody = 64 << 10

// AdminConfig is the management surface's configuration.
type AdminConfig struct {
	// Catalog is the service behind the routes. Required.
	Catalog *Service
	// Grant resolves the request's read scope from the caller's credential.
	// Required, and it must fail closed: ok=false means the request was not
	// admitted by the credential face, so a route never answers every
	// workspace because no grant was resolved (§8 跨域检查在服务端).
	Grant func(*http.Request) (Grant, bool)
	// Actor names the caller for the audit rows. Required.
	Actor func(*http.Request) string
}

// AdminHandler is the management surface: the directory, the binding writes,
// the overview and the audit rows the console reads (04-architecture.md §2,
// §3.1, §3.2). It is mounted on the ops listener behind the ops credential
// face, but keeps its own route table: the ops surface's route set is a
// declared, census-pinned set that this feature does not widen.
type AdminHandler struct {
	cfg     AdminConfig
	routes  http.Handler
	catalog *Service
}

// NewAdminHandler mounts the management routes. Every route requires the
// caller's grant and actor to resolve; a nil one is a construction error, not
// a route that answers everything.
func NewAdminHandler(cfg AdminConfig) (*AdminHandler, error) {
	if cfg.Catalog == nil {
		return nil, errors.New("catalog: an admin surface needs the service")
	}
	if cfg.Grant == nil || cfg.Actor == nil {
		return nil, errors.New("catalog: an admin surface needs a grant and an actor resolver")
	}
	handler := &AdminHandler{cfg: cfg, catalog: cfg.Catalog}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/servers", handler.listServers)
	mux.HandleFunc("POST /admin/servers", handler.registerServer)
	mux.HandleFunc("PATCH /admin/servers/{serverID}", handler.updateServer)
	mux.HandleFunc("POST /admin/servers/{serverID}/sync", handler.syncServer)
	mux.HandleFunc("GET /admin/servers/{serverID}/directory", handler.directory)
	mux.HandleFunc("GET /admin/agents", handler.listAgents)
	mux.HandleFunc("PUT /admin/servers/{serverID}/workspaces/{workspaceID}/agents/{agentID}/binding", handler.setBinding)
	mux.HandleFunc("GET /admin/overview", handler.overview)
	mux.HandleFunc("GET /admin/audit", handler.audit)
	handler.routes = mux
	return handler, nil
}

// ServeHTTP answers one management request.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.routes.ServeHTTP(w, r)
}

// grant resolves the caller's scope or answers 401. Every route goes through
// it, so a handler cannot be reached with an implicit deployment-wide grant.
func (h *AdminHandler) grant(w http.ResponseWriter, r *http.Request) (Grant, bool) {
	grant, ok := h.cfg.Grant(r)
	if !ok {
		writeAdminError(w, http.StatusUnauthorized, "unauthorized")
		return Grant{}, false
	}
	return grant, true
}

// listServers answers the registered sources the caller's grant reaches.
func (h *AdminHandler) listServers(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	views, err := h.catalog.ListForGrant(r.Context(), grant)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, "read sources")
		return
	}
	if views == nil {
		views = []SourceView{}
	}
	writeAdminJSON(w, http.StatusOK, views)
}

// registerServer stores a new source and probes its connection.
func (h *AdminHandler) registerServer(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !grant.IsDeploymentWide() {
		// Registering a source is a source-level act: a workspace-bound
		// credential must not create one it cannot even scope (§8).
		writeAdminError(w, http.StatusForbidden, "deployment credential required")
		return
	}
	var reg Registration
	if err := decodeAdminBody(w, r, &reg); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	view, err := h.catalog.Register(r.Context(), reg, h.cfg.Actor(r))
	switch {
	case errors.Is(err, ErrSourceExists):
		writeAdminError(w, http.StatusConflict, err.Error())
		return
	case errors.Is(err, ErrSourceURLExists):
		writeAdminError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeAdminJSON(w, http.StatusCreated, view)
}

// updateServer changes the automatic sync settings.
func (h *AdminHandler) updateServer(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !grant.IsDeploymentWide() {
		writeAdminError(w, http.StatusForbidden, "deployment credential required")
		return
	}
	var settings Settings
	if err := decodeAdminBody(w, r, &settings); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	view, err := h.catalog.UpdateSettings(r.Context(), ServerID(r.PathValue("serverID")), settings, h.cfg.Actor(r))
	switch {
	case errors.Is(err, ErrSourceNotFound):
		writeAdminError(w, http.StatusNotFound, err.Error())
		return
	case err != nil:
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

// syncServer runs one manual sync. A failure answers the report with the
// generation that stayed current, so a caller can prove nothing was lost
// (SYNC-02).
func (h *AdminHandler) syncServer(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !grant.IsDeploymentWide() {
		// A sync reads every workspace of the source, so it is never a
		// workspace-bound action.
		writeAdminError(w, http.StatusForbidden, "deployment credential required")
		return
	}
	report, err := h.catalog.Sync(r.Context(), ServerID(r.PathValue("serverID")))
	switch {
	case errors.Is(err, ErrSourceNotFound):
		writeAdminError(w, http.StatusNotFound, err.Error())
		return
	case errors.Is(err, ErrSyncInFlight):
		writeAdminError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		writeAdminJSON(w, http.StatusBadGateway, report)
		return
	}
	writeAdminJSON(w, http.StatusOK, report)
}

// directory answers one source's current generation.
func (h *AdminHandler) directory(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	workspaceID := strings.TrimSpace(r.URL.Query().Get("workspace_id"))
	serverID := ServerID(r.PathValue("serverID"))
	if workspaceID != "" && !grant.Allows(serverID, workspaceID) {
		// A workspace outside the grant is absent, not empty (§8).
		writeAdminError(w, http.StatusNotFound, "workspace not found")
		return
	}
	view, err := h.catalog.Directory(r.Context(), serverID, workspaceID, grant)
	switch {
	case errors.Is(err, ErrSourceNotFound):
		writeAdminError(w, http.StatusNotFound, err.Error())
		return
	case err != nil:
		writeAdminError(w, http.StatusInternalServerError, "read directory")
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

// listAgents answers the Agent management rows across the visible sources.
func (h *AdminHandler) listAgents(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	workspaceID := strings.TrimSpace(r.URL.Query().Get("workspace_id"))
	if workspaceID != "" && !grant.AllowsWorkspace(workspaceID) {
		writeAdminError(w, http.StatusNotFound, "workspace not found")
		return
	}
	serverID := ServerID(strings.TrimSpace(r.URL.Query().Get("server_id")))
	if serverID != "" {
		if _, visible, err := h.catalog.GetForGrant(r.Context(), serverID, grant); err != nil {
			writeAdminError(w, http.StatusInternalServerError, "read agents")
			return
		} else if !visible {
			writeAdminError(w, http.StatusNotFound, "server not found")
			return
		}
	}
	rows, err := h.catalog.Agents(r.Context(), serverID, workspaceID, grant)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, "read agents")
		return
	}
	if rows == nil {
		rows = []AgentView{}
	}
	writeAdminJSON(w, http.StatusOK, rows)
}

// setBinding writes one agent's runtime association. Only an applied result is
// 200; every other class is its own non-success status (V2-C02, BIND-03).
func (h *AdminHandler) setBinding(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	workspaceID := r.PathValue("workspaceID")
	serverID := ServerID(r.PathValue("serverID"))
	if !grant.Allows(serverID, workspaceID) {
		writeAdminError(w, http.StatusNotFound, "agent not found")
		return
	}
	var body struct {
		RuntimeID         string `json:"runtime_id"`
		ExpectedRuntimeID string `json:"expected_runtime_id"`
	}
	if err := decodeAdminBody(w, r, &body); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	outcome, err := h.catalog.SetBinding(r.Context(), BindingRequest{
		ServerID:          serverID,
		WorkspaceID:       workspaceID,
		AgentID:           r.PathValue("agentID"),
		ExpectedRuntimeID: body.ExpectedRuntimeID,
		RuntimeID:         body.RuntimeID,
	}, h.cfg.Actor(r))
	switch {
	case errors.Is(err, ErrSourceNotFound):
		writeAdminError(w, http.StatusNotFound, err.Error())
		return
	case err != nil:
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeAdminJSON(w, BindingStatusCode(outcome.Result), outcome)
}

// overview answers the home page's read model.
func (h *AdminHandler) overview(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	view, err := h.catalog.Overview(r.Context(), grant)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, "read overview")
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

// audit answers the audit rows the grant covers, newest first.
func (h *AdminHandler) audit(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	entries, err := h.catalog.Audit(r.Context(), ServerID(strings.TrimSpace(r.URL.Query().Get("server_id"))), grant)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, "read audit")
		return
	}
	if entries == nil {
		entries = []AuditEntry{}
	}
	writeAdminJSON(w, http.StatusOK, AuditView{Entries: entries})
}

// grantTouches reports whether the grant covers any (source, workspace) pair
// of the source's configured scope. A source with no configured scope (empty
// list) is reached only through its synced directory or the deployment-wide
// credential.
func grantTouches(grant Grant, src Source) bool {
	if grant.IsDeploymentWide() {
		return true
	}
	for _, workspaceID := range src.AllowedWorkspaces {
		if grant.Allows(src.ID, workspaceID) {
			return true
		}
	}
	return false
}

// decodeAdminBody reads one bounded JSON body and refuses unknown fields, so a
// misspelled key is an error rather than a silently ignored setting.
func decodeAdminBody(w http.ResponseWriter, r *http.Request, out any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxAdminBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return errors.New("request body is not the expected JSON")
	}
	return nil
}

// writeAdminJSON writes one management answer.
func writeAdminJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeAdminError writes the management surface's error shape.
func writeAdminError(w http.ResponseWriter, status int, msg string) {
	writeAdminJSON(w, status, map[string]string{"error": msg})
}
