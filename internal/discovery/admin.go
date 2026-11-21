package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// maxAdminBody bounds one discovery request body.
const maxAdminBody = 64 << 10

// CLIResolver answers a CLI definition's adapter and current digest.
type CLIResolver interface {
	AdapterForPlatform(ctx context.Context, serverID, cliID, platform string) (adapter, digest string, ok bool)
	CurrentDigest(ctx context.Context, serverID, adapter, platform string) (digest string, ok bool)
}

// ConfigResolver answers the current Agent configuration revision.
type ConfigResolver interface {
	Revision(ctx context.Context, serverID, workspaceID, backend, profile string) (uint64, bool)
}

// Binding is the current runtime's adapter and execution platform.
type Binding struct {
	Adapter  string
	Platform string
}

// BindingResolver answers the current runtime binding for one environment.
// A failed lookup leaves the catalog generation unknown; it never widens the
// query to every adapter or platform.
type BindingResolver interface {
	RuntimeBinding(ctx context.Context, serverID, workspaceID, runtimeID string) (Binding, bool)
}

// AdminConfig is the discovery surface's setup.
type AdminConfig struct {
	Service   *Service
	CLIs      CLIResolver
	Configs   ConfigResolver
	Bindings  BindingResolver
	Authorize func(*http.Request) (deploymentWide bool, ok bool)
	Actor     func(*http.Request) string
}

// AdminHandler serves the restricted discovery face.
type AdminHandler struct {
	cfg    AdminConfig
	routes http.Handler
}

// NewAdminHandler mounts the routes.
func NewAdminHandler(cfg AdminConfig) (*AdminHandler, error) {
	if cfg.Service == nil {
		return nil, errors.New("discovery: an admin surface needs the service")
	}
	if cfg.Authorize == nil || cfg.Actor == nil {
		return nil, errors.New("discovery: an admin surface needs an authorize and an actor resolver")
	}
	handler := &AdminHandler{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/discovery/requests", handler.submit)
	mux.HandleFunc("GET /admin/discovery/requests", handler.list)
	mux.HandleFunc("GET /admin/discovery/requests/{requestID}", handler.get)
	mux.HandleFunc("GET /admin/discovery/adapters", handler.adapters)
	mux.HandleFunc("POST /admin/discovery/adapters/verify", handler.verifyAll)
	mux.HandleFunc("POST /admin/discovery/adapters/{name}/verify", handler.verify)
	mux.HandleFunc("GET /admin/discovery/models", handler.models)
	handler.routes = mux
	return handler, nil
}

// ServeHTTP answers one discovery request.
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

type submitRequest struct {
	UpstreamRequestID string  `json:"upstream_request_id"`
	ServerID          string  `json:"server_id"`
	WorkspaceID       string  `json:"workspace_id"`
	RuntimeID         string  `json:"runtime_id"`
	Profile           string  `json:"profile"`
	Kind              Kind    `json:"kind"`
	CLIID             string  `json:"cli_id"`
	Adapter           string  `json:"adapter"`
	Platform          string  `json:"platform"`
	ConfigRevision    *uint64 `json:"config_revision"`
	ConfigBackend     string  `json:"config_backend"`
	ConfigProfile     string  `json:"config_profile"`
}

func (h *AdminHandler) submit(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	var body submitRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxAdminBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "request body is not the expected JSON")
		return
	}
	adapter := strings.TrimSpace(body.Adapter)
	digest := ""
	if cliID := strings.TrimSpace(body.CLIID); cliID != "" {
		if h.cfg.CLIs == nil {
			writeError(w, http.StatusBadRequest, "no CLI registry is wired")
			return
		}
		resolved, resolvedDigest, ok := h.cfg.CLIs.AdapterForPlatform(
			r.Context(), strings.TrimSpace(body.ServerID), cliID, strings.TrimSpace(body.Platform))
		if !ok {
			writeError(w, http.StatusBadRequest, "cli_id is not registered in this source")
			return
		}
		adapter, digest = resolved, resolvedDigest
	}
	if adapter == "" {
		writeError(w, http.StatusBadRequest, "cli_id or adapter is required")
		return
	}
	var revision uint64
	if body.ConfigRevision != nil {
		revision = *body.ConfigRevision
	} else if h.cfg.Configs != nil {
		backend := strings.TrimSpace(body.ConfigBackend)
		if backend == "" {
			backend = adapter
		}
		profile := strings.TrimSpace(body.ConfigProfile)
		if profile == "" {
			profile = strings.TrimSpace(body.Profile)
		}
		if value, ok := h.cfg.Configs.Revision(r.Context(), strings.TrimSpace(body.ServerID),
			strings.TrimSpace(body.WorkspaceID), backend, profile); ok {
			revision = value
		}
	}
	request, err := h.cfg.Service.Submit(r.Context(), SubmitInput{
		UpstreamRequestID: body.UpstreamRequestID, ServerID: body.ServerID,
		WorkspaceID: body.WorkspaceID, RuntimeID: body.RuntimeID,
		Profile: body.Profile, Kind: body.Kind, Adapter: adapter,
		CLIDigest: digest, ConfigRevision: revision, Platform: body.Platform,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, request)
}

func (h *AdminHandler) list(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	requests, err := h.cfg.Service.List(r.Context(),
		strings.TrimSpace(r.URL.Query().Get("server_id")),
		strings.TrimSpace(r.URL.Query().Get("workspace_id")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read discovery requests")
		return
	}
	if requests == nil {
		requests = []Request{}
	}
	writeJSON(w, http.StatusOK, requests)
}

func (h *AdminHandler) get(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	request, err := h.cfg.Service.Get(r.Context(), r.PathValue("requestID"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "read discovery request")
	default:
		writeJSON(w, http.StatusOK, request)
	}
}

func (h *AdminHandler) adapters(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, h.cfg.Service.Adapters())
}

func (h *AdminHandler) verify(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	status, err := h.cfg.Service.VerifyAdapter(r.Context(), r.PathValue("name"))
	switch {
	case errors.Is(err, ErrAdapterNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "verify adapter")
	default:
		writeJSON(w, http.StatusOK, status)
	}
}

func (h *AdminHandler) verifyAll(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, h.cfg.Service.VerifyAll(r.Context()))
}

func (h *AdminHandler) models(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	serverID := strings.TrimSpace(r.URL.Query().Get("server_id"))
	workspaceID := strings.TrimSpace(r.URL.Query().Get("workspace_id"))
	runtimeID := strings.TrimSpace(r.URL.Query().Get("runtime_id"))
	profile := strings.TrimSpace(r.URL.Query().Get("profile"))
	adapter := strings.TrimSpace(r.URL.Query().Get("adapter"))
	platform := strings.TrimSpace(r.URL.Query().Get("platform"))
	digest := ""
	if cliID := strings.TrimSpace(r.URL.Query().Get("cli_id")); cliID != "" {
		if h.cfg.CLIs == nil {
			writeError(w, http.StatusBadRequest, "no CLI registry is wired")
			return
		}
		resolved, resolvedDigest, ok := h.cfg.CLIs.AdapterForPlatform(r.Context(), serverID, cliID, platform)
		if !ok {
			writeError(w, http.StatusBadRequest, "cli_id is not registered in this source")
			return
		}
		adapter, digest = resolved, resolvedDigest
	}
	// The console's model filter sends only server/workspace/runtime/profile.
	// Resolve the omitted generation components from the current runtime
	// binding; a failed resolution stays unknown rather than becoming a
	// wildcard.
	if adapter == "" || platform == "" {
		if h.cfg.Bindings != nil {
			if binding, ok := h.cfg.Bindings.RuntimeBinding(r.Context(), serverID, workspaceID, runtimeID); ok {
				if adapter == "" {
					adapter = binding.Adapter
				}
				if platform == "" {
					platform = binding.Platform
				}
			}
		}
	}
	if digest == "" && adapter != "" && platform != "" && h.cfg.CLIs != nil {
		if value, ok := h.cfg.CLIs.CurrentDigest(r.Context(), serverID, adapter, platform); ok {
			digest = value
		}
	}
	var revision uint64
	configKnown := false
	if h.cfg.Configs != nil && adapter != "" {
		if value, ok := h.cfg.Configs.Revision(r.Context(), serverID, workspaceID, adapter, profile); ok {
			revision, configKnown = value, true
		}
	}
	view, err := h.cfg.Service.Models(r.Context(), ModelQuery{
		ServerID: serverID, WorkspaceID: workspaceID, RuntimeID: runtimeID,
		Profile: profile, Adapter: adapter, CLIDigest: digest,
		ConfigRevision: revision, ConfigKnown: configKnown,
		Platform: strings.ToLower(platform),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read model catalog")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
