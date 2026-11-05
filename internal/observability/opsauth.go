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
	// as comma-separated "<workspace-id>=<token>" pairs. The token may itself
	// contain '=' — the pair is split at the first one.
	EnvOpsWorkspaceTokens = "FOREMAN_OPS_WORKSPACE_TOKENS"
)

// OpsCredential is one bearer credential of the ops surface and the workspace
// set it may read. An empty Workspaces grants every workspace the deployment
// serves; a non-empty one grants exactly those (the workspace-bound form).
type OpsCredential struct {
	Token      string
	Workspaces []string
}

// opsIdentity is the authenticated caller's workspace grant. A nil workspaces
// field grants every configured workspace — the deployment-wide credential.
type opsIdentity struct {
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
	seenWorkspaces := map[string]bool{}
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
		workspaceID, token, ok := strings.Cut(pair, "=")
		workspaceID, token = strings.TrimSpace(workspaceID), strings.TrimSpace(token)
		if !ok || workspaceID == "" || token == "" {
			// The malformed entry may be the credential itself (a missing
			// separator leaves the token whole), and this error reaches the
			// container log: name the entry's position only, never its text.
			return nil, fmt.Errorf("%s entry %d is not a <workspace-id>=<token> pair", EnvOpsWorkspaceTokens, position+1)
		}
		if isPlaceholderCredential(token) {
			return nil, fmt.Errorf("%s carries the shipped placeholder, not a credential", EnvOpsWorkspaceTokens)
		}
		if _, duplicate := seenTokens[token]; duplicate {
			return nil, fmt.Errorf("%s repeats a token already configured", EnvOpsWorkspaceTokens)
		}
		if seenWorkspaces[workspaceID] {
			return nil, fmt.Errorf("%s binds workspace %q twice", EnvOpsWorkspaceTokens, workspaceID)
		}
		seenTokens[token] = workspaceID
		seenWorkspaces[workspaceID] = true
		credentials = append(credentials, OpsCredential{Token: token, Workspaces: []string{workspaceID}})
	}
	return credentials, nil
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
	return opsIdentity{workspaces: a.credentials[matched].Workspaces}, true
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
