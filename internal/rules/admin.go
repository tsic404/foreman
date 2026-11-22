package rules

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/tsic404/foreman/internal/catalog"
)

// maxAdminBody bounds one rules management request body.
const maxAdminBody = 64 << 10

// AdminConfig is the rules management surface's configuration.
type AdminConfig struct {
	// Service is the rule document and resolver behind the routes. Required.
	Service *Service
	// Grant resolves the caller's source scope from the ops credential.
	// Required, and it must fail closed.
	Grant func(*http.Request) (catalog.Grant, bool)
	// Actor names the caller for the audit rows. Required.
	Actor func(*http.Request) string
}

// AdminHandler mounts /admin/rules and its preview route.
type AdminHandler struct {
	cfg    AdminConfig
	routes http.Handler
}

// NewAdminHandler mounts the rules routes.
func NewAdminHandler(cfg AdminConfig) (*AdminHandler, error) {
	if cfg.Service == nil {
		return nil, errors.New("rules: an admin surface needs the service")
	}
	if cfg.Grant == nil || cfg.Actor == nil {
		return nil, errors.New("rules: an admin surface needs a grant and an actor resolver")
	}
	handler := &AdminHandler{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/rules", handler.state)
	mux.HandleFunc("PUT /admin/rules/draft", handler.draft)
	mux.HandleFunc("POST /admin/rules/publish", handler.publish)
	mux.HandleFunc("POST /admin/rules/rollback", handler.rollback)
	mux.HandleFunc("POST /admin/rules/preview", handler.preview)
	mux.HandleFunc("POST /admin/readiness", handler.readiness)
	handler.routes = mux
	return handler, nil
}

// ServeHTTP answers one management request.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.routes.ServeHTTP(w, r)
}

func (h *AdminHandler) grant(w http.ResponseWriter, r *http.Request) (catalog.Grant, bool) {
	grant, ok := h.cfg.Grant(r)
	if !ok {
		writeAdminError(w, http.StatusUnauthorized, "unauthorized", nil)
		return catalog.Grant{}, false
	}
	return grant, true
}

// requireDeploymentWide refuses a rule mutation a workspace-bound credential
// may not perform: a rule set spans sources and decides where Jobs run.
func requireDeploymentWide(w http.ResponseWriter, grant catalog.Grant) bool {
	if grant.IsDeploymentWide() {
		return true
	}
	writeAdminError(w, http.StatusForbidden, "deployment credential required", nil)
	return false
}

type draftRequest struct {
	BaseRevision uint64 `json:"base_revision"`
	Rules        []Rule `json:"rules"`
}

type publishRequest struct {
	BaseRevision uint64 `json:"base_revision"`
}

type rollbackRequest struct {
	Revision     uint64 `json:"revision"`
	BaseRevision uint64 `json:"base_revision"`
}

func (h *AdminHandler) state(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.grant(w, r); !ok {
		return
	}
	view, err := h.cfg.Service.State(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) draft(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	var request draftRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	view, err := h.cfg.Service.Draft(r.Context(), request.BaseRevision, request.Rules, grant, h.cfg.Actor(r))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) publish(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	var request publishRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	revision, err := h.cfg.Service.Publish(r.Context(), request.BaseRevision, grant, h.cfg.Actor(r))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revision)
}

func (h *AdminHandler) rollback(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	var request rollbackRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	revision, err := h.cfg.Service.Rollback(r.Context(), request.Revision, request.BaseRevision, grant, h.cfg.Actor(r))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revision)
}

func (h *AdminHandler) preview(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	var request Request
	if err := decodeBody(w, r, &request); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	resolution, err := h.cfg.Service.Preview(r.Context(), request, grant)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resolution)
}

func (h *AdminHandler) readiness(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	var request Request
	if err := decodeBody(w, r, &request); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	report, err := h.cfg.Service.Readiness(r.Context(), request, grant)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func decodeBody(w http.ResponseWriter, r *http.Request, out any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxAdminBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return errors.New("request body is not the expected JSON")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAdminError(w http.ResponseWriter, status int, message string, findings []Finding) {
	body := map[string]any{"error": message}
	if len(findings) > 0 {
		body["findings"] = findings
	}
	writeJSON(w, status, body)
}

// writeServiceError maps the rule document's error classes to their answers.
// A validation refusal is 409 with every finding, because the draft itself is
// what the operator has to fix.
func writeServiceError(w http.ResponseWriter, err error) {
	var validation *ValidationError
	if errors.As(err, &validation) {
		writeAdminError(w, http.StatusConflict, validation.Error(), validation.Findings)
		return
	}
	switch {
	case errors.Is(err, ErrRevisionConflict):
		writeAdminError(w, http.StatusConflict, err.Error(), nil)
	case errors.Is(err, ErrRevisionNotFound):
		writeAdminError(w, http.StatusNotFound, err.Error(), nil)
	case errors.Is(err, ErrUnauthorized):
		writeAdminError(w, http.StatusForbidden, err.Error(), nil)
	default:
		writeAdminError(w, http.StatusInternalServerError, "rule operation failed", nil)
	}
}
