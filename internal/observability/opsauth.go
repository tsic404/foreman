package observability

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Environment variables of the ops surface's credential face (§2.2 S-25).
const (
	// EnvOpsToken is the deployment-wide bearer credential of the ops surface:
	// it reads every workspace the deployment serves.
	EnvOpsToken = "FOREMAN_OPS_TOKEN"
	// EnvOpsWorkspaceTokens binds additional credentials to single workspaces,
	// as comma-separated "<workspace-id>=<token>" or
	// "<server-id>/<workspace-id>=<token>" pairs. The token may itself contain
	// '=' — the pair is split at the first one. The server-qualified form is
	// what survives a second registered source holding the same workspace id;
	// the unqualified form is resolved against the source registry and refused
	// when more than one source holds the workspace.
	EnvOpsWorkspaceTokens = "FOREMAN_OPS_WORKSPACE_TOKENS"
)

// Scope is one (server, workspace) pair a credential may read. An empty
// ServerID is the workspace-only form: the management surface resolves it
// against the registered sources and refuses it when it is ambiguous.
type Scope struct {
	ServerID    string
	WorkspaceID string
}

// OpsCredential is one bearer credential of the ops surface and the scope set
// it may read. A nil Workspaces and nil Scopes grant every workspace the
// deployment serves; otherwise the credential is workspace-bound.
type OpsCredential struct {
	Token      string
	Workspaces []string
	Scopes     []Scope
}

// opsIdentity is the authenticated caller's grant. A nil scopes field grants
// every configured workspace — the deployment-wide credential.
type opsIdentity struct {
	scopes     []Scope
	workspaces []string
}

// opsIdentityKey is the request context key the credential face binds the
// authenticated identity to. A request without it resolves to an empty
// workspace grant, so a handler reached without the credential face fails
// closed instead of answering every workspace.
type opsIdentityKey struct{}

// LoadOpsCredentials parses the deployment's ops credentials. It is fail-fast:
// an empty or placeholder value, a malformed pair, or a token bound twice is a
// configuration error, never a silently dropped credential.
func LoadOpsCredentials(getenv func(string) string) ([]OpsCredential, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	var credentials []OpsCredential
	seenTokens := map[string]string{}
	seenScopes := map[string]bool{}
	if wide := strings.TrimSpace(getenv(EnvOpsToken)); wide != "" {
		if isPlaceholderCredential(wide) {
			return nil, fmt.Errorf("%s carries the shipped placeholder, not a credential", EnvOpsToken)
		}
		credentials = append(credentials, OpsCredential{Token: wide})
		seenTokens[wide] = ""
	}
	raw := strings.TrimSpace(getenv(EnvOpsWorkspaceTokens))
	if raw == "" {
		return credentials, nil
	}
	for position, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		scopeText, token, ok := strings.Cut(pair, "=")
		scopeText, token = strings.TrimSpace(scopeText), strings.TrimSpace(token)
		if !ok || scopeText == "" || token == "" {
			// The malformed entry may be the credential itself (a missing
			// separator leaves the token whole), and this error reaches the
			// container log: name the entry's position only, never its text.
			return nil, fmt.Errorf("%s entry %d is not a <[server-id/]workspace-id>=<token> pair", EnvOpsWorkspaceTokens, position+1)
		}
		if isPlaceholderCredential(token) {
			return nil, fmt.Errorf("%s carries the shipped placeholder, not a credential", EnvOpsWorkspaceTokens)
		}
		if _, duplicate := seenTokens[token]; duplicate {
			return nil, fmt.Errorf("%s repeats a token already configured", EnvOpsWorkspaceTokens)
		}
		scope, err := parseScope(scopeText)
		if err != nil {
			return nil, fmt.Errorf("%s entry %d: %w", EnvOpsWorkspaceTokens, position+1, err)
		}
		key := scope.ServerID + "\x00" + scope.WorkspaceID
		if seenScopes[key] {
			return nil, fmt.Errorf("%s binds scope %q twice", EnvOpsWorkspaceTokens, scopeText)
		}
		seenTokens[token] = scope.WorkspaceID
		seenScopes[key] = true
		// The two forms keep one representation each. An unqualified entry is
		// a local workspace grant: the ops read routes resolve it from
		// Workspaces exactly as they did before source qualification existed,
		// and admit() derives the equivalent Scope from it. A qualified entry
		// is a source-scoped grant and stays in Scopes alone, so it can never
		// be mistaken for a local workspace of the same id.
		credential := OpsCredential{Token: token}
		if scope.ServerID == "" {
			credential.Workspaces = []string{scope.WorkspaceID}
		} else {
			credential.Scopes = []Scope{scope}
		}
		credentials = append(credentials, credential)
	}
	return credentials, nil
}

// parseScope reads one "<[server-id/]workspace-id>" entry. An id is a bare
// token: a value that needed escaping would silently match nothing.
func parseScope(text string) (Scope, error) {
	serverID, workspaceID, qualified := strings.Cut(text, "/")
	serverID, workspaceID = strings.TrimSpace(serverID), strings.TrimSpace(workspaceID)
	if qualified {
		if serverID == "" || workspaceID == "" {
			return Scope{}, fmt.Errorf("a server-qualified scope needs both a server id and a workspace id")
		}
		if !bareID(serverID) {
			return Scope{}, fmt.Errorf("server id %q is not a bare id", serverID)
		}
	} else {
		workspaceID, serverID = serverID, ""
	}
	if !bareID(workspaceID) {
		return Scope{}, fmt.Errorf("workspace id %q is not a bare id", workspaceID)
	}
	return Scope{ServerID: serverID, WorkspaceID: workspaceID}, nil
}

// bareID reports whether text is one non-empty printable id token.
func bareID(text string) bool {
	if text == "" {
		return false
	}
	for _, r := range text {
		if r <= ' ' || r == 0x7f || r == '/' || r == '=' {
			return false
		}
	}
	return true
}

// ValidateOpsCredentials refuses a workspace-bound credential naming a
// workspace this deployment does not serve: the credential could never read
// anything, and keeping it would turn a deployment mistake into a silent 404.
func ValidateOpsCredentials(credentials []OpsCredential, served []string) error {
	if len(served) == 0 {
		return nil
	}
	known := make(map[string]bool, len(served))
	for _, workspaceID := range served {
		known[workspaceID] = true
	}
	for _, credential := range credentials {
		for _, workspaceID := range credential.Workspaces {
			if !known[workspaceID] {
				return fmt.Errorf("%s binds workspace %q, which this deployment does not serve",
					EnvOpsWorkspaceTokens, workspaceID)
			}
		}
		for _, scope := range credential.Scopes {
			// A server-qualified scope names a remote source, which is
			// registered after startup; only its workspace half is knowable
			// here and only when it is the unqualified form.
			if scope.ServerID == "" && !known[scope.WorkspaceID] {
				return fmt.Errorf("%s binds workspace %q, which this deployment does not serve",
					EnvOpsWorkspaceTokens, scope.WorkspaceID)
			}
		}
	}
	return nil
}

// isPlaceholderCredential reports the shipped manifest placeholder, which must
// never pass for a credential.
func isPlaceholderCredential(token string) bool {
	return token == "REPLACE_ME"
}

// opsAuth is the ops surface's authentication face: every request presents a
// bearer credential, and nothing else — a Job Token is a credential of the
// other face and never authenticates here (§2.2 S-25).
type opsAuth struct {
	credentials []OpsCredential
}

func newOpsAuth(credentials []OpsCredential) *opsAuth {
	return &opsAuth{credentials: append([]OpsCredential(nil), credentials...)}
}

// serve authenticates the request and binds its workspace grant to the request
// context. An unauthenticated request is answered 401 and never reaches a
// route, so no handler is reachable by a caller the deployment did not hand a
// credential to.
func (a *opsAuth) serve(next http.HandlerFunc, w http.ResponseWriter, r *http.Request) {
	identity, ok := a.admit(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="foreman-ops"`)
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	next(w, r.WithContext(context.WithValue(r.Context(), opsIdentityKey{}, identity)))
}

// CredentialGate admits one request against the ops credential set and binds
// the caller's grant to the request context. A sibling management surface
// reuses the deployment's one credential face through it instead of growing a
// second credential loader (§8 管理 UI 分别授权; 跨域检查在服务端).
type CredentialGate struct {
	auth *opsAuth
}

// NewCredentialGate builds a gate over credentials.
func NewCredentialGate(credentials []OpsCredential) *CredentialGate {
	return &CredentialGate{auth: newOpsAuth(credentials)}
}

// Wrap answers 401 for an unauthenticated request and passes an admitted one
// to next with its grant bound.
func (g *CredentialGate) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.auth.serve(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
		}, w, r)
	})
}

// CallerScopes returns the admitted caller's scope grant. wide is true for the
// deployment-wide credential; ok is false when the request was not admitted.
// A non-wide grant with an empty slice sees nothing.
func CallerScopes(ctx context.Context) (scopes []Scope, wide bool, ok bool) {
	identity, ok := opsIdentityFrom(ctx)
	if !ok {
		return nil, false, false
	}
	return identity.scopes, identity.scopes == nil, true
}

// admit compares the presented credential against every configured one in
// constant time and without returning early: the comparison time must not
// reveal which credential, if any, matched.
func (a *opsAuth) admit(r *http.Request) (opsIdentity, bool) {
	presented, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok || presented == "" || len(a.credentials) == 0 {
		return opsIdentity{}, false
	}
	matched := -1
	for i, credential := range a.credentials {
		if subtle.ConstantTimeCompare([]byte(presented), []byte(credential.Token)) == 1 && matched < 0 {
			matched = i
		}
	}
	if matched < 0 {
		return opsIdentity{}, false
	}
	credential := a.credentials[matched]
	if credential.Scopes == nil && credential.Workspaces == nil {
		return opsIdentity{}, true
	}
	scopes := make([]Scope, 0, len(credential.Scopes)+len(credential.Workspaces))
	scopes = append(scopes, credential.Scopes...)
	workspaces := make([]string, 0, len(credential.Workspaces))
	for _, workspaceID := range credential.Workspaces {
		scopes = append(scopes, Scope{WorkspaceID: workspaceID})
		workspaces = append(workspaces, workspaceID)
	}
	return opsIdentity{scopes: scopes, workspaces: workspaces}, true
}

// bearerToken extracts the credential of an Authorization header. Only the
// bearer scheme is accepted; its name is case-insensitive (RFC 7235).
func bearerToken(header string) (string, bool) {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(header[len(prefix):]), true
}

// opsIdentityFrom returns the caller's workspace grant and whether the request
// was admitted by the ops middleware. The grant's nil workspaces field means
// every configured workspace.
func opsIdentityFrom(ctx context.Context) (opsIdentity, bool) {
	identity, ok := ctx.Value(opsIdentityKey{}).(opsIdentity)
	return identity, ok
}
