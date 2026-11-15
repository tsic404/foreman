// Package daemonauth authenticates the long-lived multica daemons that register
// to Foreman. v1 minted one Job Token per task and verified it statelessly; v2
// has no per-task credential, so a daemon presents an operator-issued bearer
// token that names its identity and the workspace it may serve.
//
// The credential set is configuration, not state: it is loaded at startup,
// refused if malformed, and never rotated in place (a rotation restarts
// Foreman). Tokens are compared in constant time and never logged.
package daemonauth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Environment variables of the daemon credential face.
const (
	// EnvDaemonTokens is the comma-separated "<daemon-id>=<token>" pair list.
	// Each pair names the daemon identity its token authenticates and the
	// workspace it may serve is taken from EnvDaemonTokenWorkspaces.
	EnvDaemonTokens = "FOREMAN_DAEMON_TOKENS"
	// EnvDaemonTokenWorkspaces binds a daemon identity to one workspace as
	// comma-separated "<daemon-id>=<workspace-id>" pairs. A daemon without a
	// binding is refused at registration: v2 never guesses the workspace.
	EnvDaemonTokenWorkspaces = "FOREMAN_DAEMON_TOKEN_WORKSPACES"
)

// Errors of the credential face.
var (
	// ErrMissingCredential reports a request with no bearer token.
	ErrMissingCredential = errors.New("missing daemon credential")
	// ErrUnknownCredential reports a token no configured pair matches.
	ErrUnknownCredential = errors.New("unknown daemon credential")
	// ErrWorkspaceUnbound reports a daemon identity with no workspace binding.
	ErrWorkspaceUnbound = errors.New("daemon identity has no workspace binding")
)

// Credential is one configured daemon identity.
type Credential struct {
	DaemonID    string
	WorkspaceID string
	// digest is the SHA-256 of the token, the form the comparison keeps so the
	// plaintext never sits in a map key.
	digest [32]byte
}

// Store is the loaded credential set.
type Store struct {
	byDigest map[[32]byte]Credential
}

// Load reads the credential set from env.
func Load(getenv func(string) string) (*Store, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	tokens := strings.TrimSpace(getenv(EnvDaemonTokens))
	if tokens == "" {
		return nil, fmt.Errorf("%s is required: a daemon cannot register without a credential", EnvDaemonTokens)
	}
	workspaces := map[string]string{}
	for _, pair := range splitPairs(getenv(EnvDaemonTokenWorkspaces)) {
		id, ws, ok := cutPair(pair)
		if !ok {
			return nil, fmt.Errorf("%s: malformed pair %q (want <daemon-id>=<workspace-id>)", EnvDaemonTokenWorkspaces, pair)
		}
		if ws == "" {
			return nil, fmt.Errorf("%s: daemon %q has an empty workspace id", EnvDaemonTokenWorkspaces, id)
		}
		workspaces[id] = ws
	}
	store := &Store{byDigest: map[[32]byte]Credential{}}
	for _, pair := range splitPairs(tokens) {
		id, token, ok := cutPair(pair)
		if !ok || id == "" || token == "" {
			return nil, fmt.Errorf("%s: malformed pair %q (want <daemon-id>=<token>)", EnvDaemonTokens, pair)
		}
		ws, bound := workspaces[id]
		if !bound {
			return nil, fmt.Errorf("%w: %s (set %s)", ErrWorkspaceUnbound, id, EnvDaemonTokenWorkspaces)
		}
		digest := sha256.Sum256([]byte(token))
		if _, dup := store.byDigest[digest]; dup {
			return nil, fmt.Errorf("%s: two daemon identities share one token", EnvDaemonTokens)
		}
		store.byDigest[digest] = Credential{DaemonID: id, WorkspaceID: ws, digest: digest}
	}
	if len(store.byDigest) == 0 {
		return nil, fmt.Errorf("%s: no credential configured", EnvDaemonTokens)
	}
	return store, nil
}

// Verify resolves a presented token to its credential. The comparison is a
// constant-time digest match: an unknown token costs one SHA-256 and one
// compare, and no per-entry loop leaks which prefix matched.
func (s *Store) Verify(token string) (Credential, error) {
	if token == "" {
		return Credential{}, ErrMissingCredential
	}
	digest := sha256.Sum256([]byte(token))
	if cred, ok := s.byDigest[digest]; ok {
		// Defensive: the map lookup already matched the digest, but keep the
		// explicit constant-time compare so the intent is visible and a future
		// map change cannot silently become a map-key oracle.
		if subtle.ConstantTimeCompare(cred.digest[:], digest[:]) == 1 {
			return cred, nil
		}
	}
	return Credential{}, ErrUnknownCredential
}

// ID returns a short stable identifier of the credential set for the startup
// log (never the tokens themselves).
func (s *Store) ID() string {
	h := sha256.New()
	for d := range s.byDigest {
		h.Write(d[:])
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

func splitPairs(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// cutPair splits at the first '=' so a token may itself contain '='.
func cutPair(pair string) (string, string, bool) {
	left, right, found := strings.Cut(pair, "=")
	if !found {
		return "", "", false
	}
	return strings.TrimSpace(left), strings.TrimSpace(right), true
}
