package release

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// maxAdminBody bounds one release request body.
const maxAdminBody = 64 << 10

// AdminConfig is the release management surface's configuration.
type AdminConfig struct {
	Service *Service
	// Authorize resolves the caller's credential. A workspace-bound
	// credential is admitted but not deployment-wide; release actions refuse
	// it, because a release domain spans every workspace of one source.
	Authorize func(*http.Request) (deploymentWide bool, ok bool)
	Actor     func(*http.Request) string
}

// AdminHandler is the CLI release management surface. It owns only the
// /admin/cli* routes; the catalog surface beside it keeps its own table.
type AdminHandler struct {
	cfg    AdminConfig
	routes http.Handler
}

// NewAdminHandler mounts the release routes.
func NewAdminHandler(cfg AdminConfig) (*AdminHandler, error) {
	if cfg.Service == nil {
		return nil, errors.New("release: an admin surface needs the service")
	}
	if cfg.Authorize == nil || cfg.Actor == nil {
		return nil, errors.New("release: an admin surface needs an authorize and an actor resolver")
	}
	handler := &AdminHandler{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/cli", handler.list)
	mux.HandleFunc("POST /admin/cli", handler.register)
	mux.HandleFunc("GET /admin/cli/{cliID}", handler.get)
	mux.HandleFunc("PATCH /admin/cli/{cliID}/settings", handler.settings)
	mux.HandleFunc("POST /admin/cli/{cliID}/check", handler.check)
	mux.HandleFunc("POST /admin/cli/{cliID}/versions", handler.version)
	mux.HandleFunc("POST /admin/cli/{cliID}/release", handler.publish)
	mux.HandleFunc("GET /admin/cli/{cliID}/impact", handler.impact)
	mux.HandleFunc("GET /admin/cli/{cliID}/artifacts/{digest}", handler.artifact)
	mux.HandleFunc("GET /admin/cli-audit", handler.audit)
	handler.routes = mux
	return handler, nil
}

// ServeHTTP answers one release management request.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.routes.ServeHTTP(w, r)
}

// wide resolves the caller and refuses a workspace-bound credential. Every
// release route is a source-level action.
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

type definitionRequest struct {
	CLIID              string     `json:"cli_id"`
	Kind               Kind       `json:"kind"`
	Adapter            string     `json:"adapter"`
	ServerID           string     `json:"server_id"`
	RequiredPlatforms  []Platform `json:"required_platforms"`
	MinimumVersion     string     `json:"minimum_version"`
	ManifestURL        string     `json:"manifest_url"`
	AutoCheck          *bool      `json:"auto_check"`
	CheckPeriodSeconds *int       `json:"check_period_s"`
	AutoRelease        *bool      `json:"auto_release"`
}

func (h *AdminHandler) list(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	views, err := h.cfg.Service.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read CLI definitions")
		return
	}
	if views == nil {
		views = []DefinitionView{}
	}
	writeJSON(w, http.StatusOK, views)
}

func (h *AdminHandler) register(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	var body definitionRequest
	if err := decodeBody(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	autoCheck, autoRelease := false, false
	if body.AutoCheck != nil {
		autoCheck = *body.AutoCheck
	}
	if body.AutoRelease != nil {
		autoRelease = *body.AutoRelease
	}
	definition := Definition{
		ID: body.CLIID, Kind: body.Kind, Adapter: body.Adapter,
		ServerID: body.ServerID, RequiredPlatforms: body.RequiredPlatforms,
		MinimumVersion: body.MinimumVersion, ManifestURL: body.ManifestURL,
		AutoCheck: autoCheck, AutoRelease: autoRelease,
	}
	if body.CheckPeriodSeconds != nil {
		definition.CheckPeriodSeconds = *body.CheckPeriodSeconds
	}
	view, err := h.cfg.Service.RegisterDefinition(r.Context(), definition, h.cfg.Actor(r))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

func (h *AdminHandler) get(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	view, ok, err := h.cfg.Service.Get(r.Context(), r.PathValue("cliID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read CLI definition")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, ErrNotFound.Error())
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) settings(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	var settings Settings
	if err := decodeBody(w, r, &settings); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	view, err := h.cfg.Service.UpdateSettings(r.Context(), r.PathValue("cliID"), settings, h.cfg.Actor(r))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeJSON(w, http.StatusOK, view)
	}
}

func (h *AdminHandler) check(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	report, err := h.cfg.Service.Check(r.Context(), r.PathValue("cliID"), h.cfg.Actor(r))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "check CLI versions")
	default:
		writeJSON(w, http.StatusOK, report)
	}
}

type versionRequest struct {
	Version   string     `json:"version"`
	Artifacts []Artifact `json:"artifacts"`
}

func (h *AdminHandler) version(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	var body versionRequest
	if err := decodeBody(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	view, err := h.cfg.Service.RegisterVersion(r.Context(), r.PathValue("cliID"),
		Candidate{Version: body.Version, Artifacts: body.Artifacts}, h.cfg.Actor(r))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeJSON(w, http.StatusCreated, view)
	}
}

type publishRequest struct {
	Version                   string `json:"version"`
	ExpectedGeneration        uint64 `json:"expected_generation"`
	ExpectedReleaseGeneration uint64 `json:"expected_release_generation"`
}

func (h *AdminHandler) publish(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	var body publishRequest
	if err := decodeBody(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	outcome, err := h.cfg.Service.Publish(r.Context(), r.PathValue("cliID"),
		body.Version, body.ExpectedGeneration, body.ExpectedReleaseGeneration, h.cfg.Actor(r))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrVersionNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "publish CLI version")
	case outcome.Result == PublishConflict:
		writeJSON(w, http.StatusConflict, outcome)
	case outcome.Result == PublishFailed:
		writeJSON(w, http.StatusUnprocessableEntity, outcome)
	default:
		writeJSON(w, http.StatusOK, outcome)
	}
}

func (h *AdminHandler) impact(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	generation, _ := strconv.ParseUint(strings.TrimSpace(r.URL.Query().Get("generation")), 10, 64)
	releaseGeneration, _ := strconv.ParseUint(strings.TrimSpace(r.URL.Query().Get("release_generation")), 10, 64)
	view, err := h.cfg.Service.Impact(r.Context(), ImpactRequest{
		CLIID: r.PathValue("cliID"), Version: strings.TrimSpace(r.URL.Query().Get("version")),
		ExpectedGeneration: generation, ExpectedReleaseGeneration: releaseGeneration,
	})
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrGenerationStale):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "read release impact")
	default:
		writeJSON(w, http.StatusOK, view)
	}
}

func (h *AdminHandler) artifact(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	body, err := h.cfg.Service.VerifiedArtifact(r.Context(), r.PathValue("cliID"), r.PathValue("digest"))
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrArtifactNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusServiceUnavailable, "verified artifact is unavailable")
	default:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

func (h *AdminHandler) audit(w http.ResponseWriter, r *http.Request) {
	if !h.wide(w, r) {
		return
	}
	entries, err := h.cfg.Service.Audit(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read release audit")
		return
	}
	if entries == nil {
		entries = []AuditEntry{}
	}
	writeJSON(w, http.StatusOK, entries)
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

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
