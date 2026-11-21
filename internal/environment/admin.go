package environment

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// maxAdminBody bounds one environment management request body.
const maxAdminBody = 64 << 10

// AdminConfig is the environment management surface's configuration.
type AdminConfig struct {
	// Service is the registry behind the routes. Required.
	Service *Service
	// Grant resolves the caller's source scope. Required, and it must fail
	// closed: ok=false means the request was not admitted by the credential
	// face.
	Grant func(*http.Request) (Grant, bool)
	// Actor names the caller for the audit rows. Required.
	Actor func(*http.Request) string
}

// AdminHandler is the environment management surface. It is mounted on the ops
// listener behind the same credential gate as the catalog's /admin routes.
type AdminHandler struct {
	cfg    AdminConfig
	routes http.Handler
}

// NewAdminHandler mounts the environment routes.
func NewAdminHandler(cfg AdminConfig) (*AdminHandler, error) {
	if cfg.Service == nil {
		return nil, errors.New("environment: an admin surface needs the service")
	}
	if cfg.Grant == nil || cfg.Actor == nil {
		return nil, errors.New("environment: an admin surface needs a grant and an actor resolver")
	}
	handler := &AdminHandler{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/environments", handler.list)
	mux.HandleFunc("POST /admin/environments", handler.register)
	mux.HandleFunc("GET /admin/environments/retired", handler.retired)
	mux.HandleFunc("GET /admin/environments/audit", handler.audit)
	mux.HandleFunc("GET /admin/environments/{environmentID}", handler.get)
	mux.HandleFunc("PATCH /admin/environments/{environmentID}", handler.update)
	mux.HandleFunc("POST /admin/environments/{environmentID}/verify", handler.verify)
	mux.HandleFunc("POST /admin/environments/{environmentID}/drain", handler.drain)
	mux.HandleFunc("POST /admin/environments/{environmentID}/resume", handler.resume)
	mux.HandleFunc("DELETE /admin/environments/{environmentID}", handler.retire)
	mux.HandleFunc("GET /admin/environments/{environmentID}/slots", handler.slots)
	mux.HandleFunc("POST /admin/environments/{environmentID}/slots/{slotID}/confirm", handler.confirmCleanup)
	mux.HandleFunc("GET /admin/environments/{environmentID}/cleanups", handler.cleanups)
	mux.HandleFunc("POST /admin/environments/{environmentID}/cleanups/{cleanupID}/confirm", handler.confirmPendingCleanup)
	handler.routes = mux
	return handler, nil
}

// ServeHTTP answers one management request.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.routes.ServeHTTP(w, r)
}

func (h *AdminHandler) grant(w http.ResponseWriter, r *http.Request) (Grant, bool) {
	grant, ok := h.cfg.Grant(r)
	if !ok {
		writeAdminError(w, http.StatusUnauthorized, "unauthorized")
		return Grant{}, false
	}
	return grant, true
}

// requireDeploymentWide refuses a mutation a workspace-bound credential may not
// perform. Registering, relocating or retiring a remote machine is not a
// workspace-scoped act.
func requireDeploymentWide(w http.ResponseWriter, grant Grant) bool {
	if grant.DeploymentWide {
		return true
	}
	writeAdminError(w, http.StatusForbidden, "deployment credential required")
	return false
}

func (h *AdminHandler) list(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	views, err := h.cfg.Service.List(r.Context(), Filter{
		TrustDomain: strings.TrimSpace(r.URL.Query().Get("trust_domain")),
		Source:      strings.TrimSpace(r.URL.Query().Get("source_id")),
	}, grant)
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	if views == nil {
		views = []EnvironmentView{}
	}
	writeAdminJSON(w, http.StatusOK, views)
}

func (h *AdminHandler) register(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	var registration Registration
	if err := decodeAdminBody(w, r, &registration); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	view, err := h.cfg.Service.Register(r.Context(), registration, h.cfg.Actor(r))
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusCreated, view)
}

func (h *AdminHandler) get(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	view, err := h.cfg.Service.Get(r.Context(), ID(r.PathValue("environmentID")), grant)
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) update(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	var patch Patch
	if err := decodeAdminBody(w, r, &patch); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	view, err := h.cfg.Service.Update(r.Context(), ID(r.PathValue("environmentID")), patch, h.cfg.Actor(r))
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) verify(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	view, err := h.cfg.Service.Verify(r.Context(), ID(r.PathValue("environmentID")), h.cfg.Actor(r))
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) drain(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	view, err := h.cfg.Service.Drain(r.Context(), ID(r.PathValue("environmentID")), h.cfg.Actor(r))
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) resume(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	view, err := h.cfg.Service.Resume(r.Context(), ID(r.PathValue("environmentID")), h.cfg.Actor(r))
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) retire(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	retirement, err := h.cfg.Service.Retire(r.Context(), ID(r.PathValue("environmentID")), h.cfg.Actor(r))
	if err != nil {
		var blocked *BlockedError
		if errors.As(err, &blocked) {
			writeAdminJSON(w, http.StatusConflict, map[string]any{
				"error":    "retirement blocked",
				"blockers": blocked.Reasons,
			})
			return
		}
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, retirementView(retirement))
}

func (h *AdminHandler) retired(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	views, err := h.cfg.Service.Retired(r.Context(), grant)
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	if views == nil {
		views = []TombstoneView{}
	}
	writeAdminJSON(w, http.StatusOK, views)
}

func (h *AdminHandler) slots(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	views, err := h.cfg.Service.Slots(r.Context(), ID(r.PathValue("environmentID")), grant)
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	if views == nil {
		views = []SlotView{}
	}
	writeAdminJSON(w, http.StatusOK, views)
}

func (h *AdminHandler) confirmCleanup(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	view, err := h.cfg.Service.ConfirmCleanup(
		r.Context(),
		ID(r.PathValue("environmentID")),
		r.PathValue("slotID"),
		h.cfg.Actor(r),
	)
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) cleanups(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	views, err := h.cfg.Service.Cleanups(r.Context(), ID(r.PathValue("environmentID")), grant)
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	if views == nil {
		views = []CleanupView{}
	}
	writeAdminJSON(w, http.StatusOK, views)
}

func (h *AdminHandler) confirmPendingCleanup(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	id := ID(r.PathValue("environmentID"))
	if err := h.cfg.Service.ConfirmPendingCleanup(r.Context(), id, r.PathValue("cleanupID"), h.cfg.Actor(r)); err != nil {
		writeAdminServiceError(w, err)
		return
	}
	view, err := h.cfg.Service.Get(r.Context(), id, Grant{DeploymentWide: true})
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	writeAdminJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) audit(w http.ResponseWriter, r *http.Request) {
	grant, ok := h.grant(w, r)
	if !ok {
		return
	}
	if !requireDeploymentWide(w, grant) {
		return
	}
	entries, err := h.cfg.Service.Audit(r.Context())
	if err != nil {
		writeAdminServiceError(w, err)
		return
	}
	if entries == nil {
		entries = []AuditEntryView{}
	}
	writeAdminJSON(w, http.StatusOK, AuditView{Entries: entries})
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

func writeAdminJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAdminError(w http.ResponseWriter, status int, message string) {
	writeAdminJSON(w, status, map[string]string{"error": message})
}

// writeAdminServiceError maps the lifecycle's error classes to their HTTP
// answers. Validation is 400; a state that refuses the operation is 409; a
// missing environment is 404; anything else is an internal failure.
func writeAdminServiceError(w http.ResponseWriter, err error) {
	var validation *ValidationError
	if errors.As(err, &validation) {
		writeAdminError(w, http.StatusBadRequest, validation.Error())
		return
	}
	// A data-residency refusal is a policy decision the operator can fix, not
	// a controller failure. Keep the answer non-5xx and name the policy so the
	// caller knows to change the grant rather than retry the service.
	if errors.Is(err, ErrTrustDomainDenied) {
		writeAdminError(w, http.StatusConflict, "the target's trust domain is not allowed by the deployment's data-residency policy")
		return
	}
	switch {
	case errors.Is(err, ErrNotFound),
		errors.Is(err, ErrSlotNotFound),
		errors.Is(err, ErrCleanupNotFound):
		writeAdminError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrExists),
		errors.Is(err, ErrRetired),
		errors.Is(err, ErrTreeExists),
		errors.Is(err, ErrTreeChanged),
		errors.Is(err, ErrNotVerified),
		errors.Is(err, ErrDraining),
		errors.Is(err, ErrFull),
		errors.Is(err, ErrSlotExists),
		errors.Is(err, ErrSlotActive),
		errors.Is(err, ErrSlotsPresent),
		errors.Is(err, ErrCleanupExists),
		errors.Is(err, ErrConflict):
		writeAdminError(w, http.StatusConflict, err.Error())
	default:
		writeAdminError(w, http.StatusInternalServerError, "environment operation failed")
	}
}
