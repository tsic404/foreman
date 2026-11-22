package proxy

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/release"
)

// CLIReleaseSource is the verified release domain the download handler reads.
// The handler serves only the published version and digest the claim froze; it
// never offers "the newest" or another backend's artifact.
type CLIReleaseSource interface {
	VerifiedRelease(ctx context.Context, serverID string, kind release.Kind, adapter, version, platform string) (string, bool)
	VerifiedArtifactByDomain(ctx context.Context, serverID, digest string) ([]byte, error)
}

// TokenVerifier verifies the attempt's Job token (satisfied by *auth.Issuer).
type TokenVerifier interface {
	Verify(token string, scope auth.RequestScope) (auth.Claims, error)
}

// AttemptBindings is the authoritative view of the CLI tuples one attempt
// froze at claim time. A miss is a denial: the handler never widens a token
// to "any artifact this source published".
type AttemptBindings interface {
	FrozenAttempt(sourceID, workspaceID, taskID string) (attempt.Binding, bool)
}

// RegistryAttempts reads the frozen binding from the task registry, including
// rows restored from shared state on another instance.
type RegistryAttempts struct {
	Registry *registry.Registry
}

// FrozenAttempt answers only a binding the exact source/workspace/task row
// carries; a legacy or missing row has no CLI authorization.
func (a RegistryAttempts) FrozenAttempt(sourceID, workspaceID, taskID string) (attempt.Binding, bool) {
	if a.Registry == nil || taskID == "" {
		return attempt.Binding{}, false
	}
	entry, ok := a.Registry.TaskInScope(sourceID, workspaceID, taskID)
	if !ok || len(entry.FixedAttempt) == 0 {
		return attempt.Binding{}, false
	}
	var binding attempt.Binding
	if err := json.Unmarshal(entry.FixedAttempt, &binding); err != nil {
		return attempt.Binding{}, false
	}
	return binding, true
}

// DownloadLimiter bounds CLI downloads on their own budget. It is never
// consulted by the control-plane handlers, so a saturated download window
// cannot reject a heartbeat, lease or terminal report (V2-C28).
type DownloadLimiter struct {
	slots chan struct{}
}

// NewDownloadLimiter builds a limiter with max concurrent downloads.
func NewDownloadLimiter(max int) *DownloadLimiter {
	if max <= 0 {
		max = 4
	}
	return &DownloadLimiter{slots: make(chan struct{}, max)}
}

func (l *DownloadLimiter) acquire() bool {
	if l == nil {
		return true
	}
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *DownloadLimiter) release() {
	if l == nil {
		return
	}
	<-l.slots
}

// CLIDistribution serves one frozen CLI artifact per authorized Job. The route
// is mounted outside the daemon face (`/cli/...`) but on the same listener, so
// the Job uses the address frozen in its attempt binding (V2-C08).
type CLIDistribution struct {
	Verifier TokenVerifier
	Releases CLIReleaseSource
	// Attempts is the frozen CLI tuple this token's attempt may read.
	Attempts AttemptBindings
	Limiter  *DownloadLimiter
	Logger   *slog.Logger
}

// ServeHTTP answers GET /cli/{kind}/{version}/{os}/{arch}.
func (d *CLIDistribution) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)
	if !ok || d.Verifier == nil {
		writeError(w, http.StatusUnauthorized, "authorization required")
		return
	}
	claims, err := d.Verifier.Verify(token, auth.RequestScope{})
	if err != nil {
		writeError(w, auth.HTTPStatus(err), http.StatusText(auth.HTTPStatus(err)))
		return
	}
	if claims.TaskID == "" || claims.Role == auth.RoleCatalogProbe {
		writeError(w, http.StatusForbidden, "only a task attempt may fetch a CLI")
		return
	}
	kind, ok := cliKind(r.PathValue("kind"), r.URL.Query().Get("backend"))
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown CLI kind")
		return
	}
	backend := strings.TrimSpace(r.URL.Query().Get("backend"))
	version := strings.TrimSpace(r.PathValue("version"))
	platform := strings.ToLower(strings.TrimSpace(r.PathValue("os")) + "/" + strings.TrimSpace(r.PathValue("arch")))
	digest := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(r.URL.Query().Get("digest")), "sha256:"))
	if version == "" || digest == "" {
		writeError(w, http.StatusBadRequest, "version and digest are required")
		return
	}
	if d.Attempts == nil {
		writeError(w, http.StatusServiceUnavailable, "the attempt authorization is not wired")
		return
	}
	binding, ok := d.Attempts.FrozenAttempt(claims.SourceID, claims.WorkspaceID, claims.TaskID)
	if !ok || !frozenCLIAllowed(binding, kind, backend, version, platform, digest) {
		d.log().Warn("cli.download.refused",
			"source_id", claims.SourceID, "workspace_id", claims.WorkspaceID, "task_id", claims.TaskID,
			"kind", kind, "backend", backend, "version", version,
			"platform", platform, "digest", digest,
			"reason", "not this attempt's frozen CLI tuple")
		writeError(w, http.StatusForbidden, "not this attempt's frozen CLI tuple")
		return
	}
	if d.Releases == nil {
		writeError(w, http.StatusServiceUnavailable, "the release domain is not wired")
		return
	}
	published, found := d.Releases.VerifiedRelease(r.Context(), claims.SourceID, kind, backend, version, platform)
	if !found || published != digest {
		d.log().Warn("cli.download.refused",
			"source_id", claims.SourceID, "task_id", claims.TaskID,
			"kind", kind, "backend", backend, "version", version,
			"platform", platform, "digest", digest,
			"reason", "not this source's published frozen artifact")
		writeError(w, http.StatusForbidden, "not this source's published frozen artifact")
		return
	}
	if !d.Limiter.acquire() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "the CLI download budget is saturated")
		return
	}
	defer d.Limiter.release()
	body, err := d.Releases.VerifiedArtifactByDomain(r.Context(), claims.SourceID, digest)
	if err != nil {
		d.log().Warn("cli.download.unavailable",
			"source_id", claims.SourceID, "task_id", claims.TaskID,
			"kind", kind, "backend", backend, "version", version,
			"platform", platform, "digest", digest, "err", err.Error())
		writeError(w, http.StatusNotFound, "the frozen artifact is unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("X-CLI-Digest", "sha256:"+digest)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// frozenCLIAllowed matches the request against the attempt's two frozen CLI
// references. Kind, version, platform and digest are exact; the backend is
// compared case-insensitively like the release domain's adapter lookup.
func frozenCLIAllowed(binding attempt.Binding, kind release.Kind, backend, version, platform, digest string) bool {
	for _, cli := range binding.CLIs {
		if cli.Kind != string(kind) {
			continue
		}
		if kind == release.KindAgent && !strings.EqualFold(strings.TrimSpace(cli.Backend), strings.TrimSpace(backend)) {
			continue
		}
		if strings.TrimSpace(cli.Version) != version {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(cli.Platform), platform) {
			continue
		}
		if attempt.DigestHex(cli.Digest) != attempt.DigestHex(digest) {
			continue
		}
		return true
	}
	return false
}

func cliKind(kind, backend string) (release.Kind, bool) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "multica":
		if strings.TrimSpace(backend) != "" {
			return "", false
		}
		return release.KindMultica, true
	case "agent":
		if strings.TrimSpace(backend) == "" {
			return "", false
		}
		return release.KindAgent, true
	default:
		return "", false
	}
}

func (d *CLIDistribution) log() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}
