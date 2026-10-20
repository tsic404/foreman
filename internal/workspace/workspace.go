// Package workspace holds the deployment's configured workspace identities.
//
// One deployment serves N workspaces (ADR-047); every control-plane action
// runs once per (credential, daemon identity) pair. This package is the single
// loader of that pair set: it never talks to the network, so the rest of the
// process only ever sees validated identities.
package workspace

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
)

// Environment variable names of the multi-workspace interface.
const (
	// EnvWorkspaces carries the identity list as inline JSON (an alternative
	// to EnvWorkspacesFile, convenient for small deployments).
	EnvWorkspaces = "FOREMAN_WORKSPACES"
	// EnvWorkspacesFile names a mounted file holding the same JSON list; the
	// Secret carrier is the expected production form.
	EnvWorkspacesFile = "FOREMAN_WORKSPACES_FILE"
	// Legacy single-workspace keys, still the legal single-workspace form.
	EnvWorkspaceID = "MULTICA_WORKSPACE_ID"
	EnvDaemonID    = "FOREMAN_DAEMON_ID"
	EnvServerToken = "MULTICA_TOKEN"

	// DefaultDaemonID is the legacy single-workspace daemon identity.
	DefaultDaemonID = "foreman"
)

// Identity is one workspace's (credential, daemon identity) pair (§0).
type Identity struct {
	WorkspaceID string `json:"workspace_id"`
	DaemonID    string `json:"daemon_id"`
	// Token is the server credential issued for this workspace + identity.
	// TokenEnv names the environment variable holding it instead, so each
	// token can live under its own Secret key.
	Token    string `json:"token,omitempty"`
	TokenEnv string `json:"token_env,omitempty"`
}

// Set is the validated deployment identity list.
type Set struct {
	identities []Identity
	byID       map[string]Identity
}

// Load reads and validates the configured workspace set. When neither
// FOREMAN_WORKSPACES nor FOREMAN_WORKSPACES_FILE is present, the legacy
// single-workspace keys are the set (a single-workspace deployment stays
// legal). Configuration errors refuse startup: a bad identity set must never
// be half-applied.
func Load(getenv func(string) string) (Set, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	raw := strings.TrimSpace(getenv(EnvWorkspaces))
	path := strings.TrimSpace(getenv(EnvWorkspacesFile))
	switch {
	case raw != "" && path != "":
		return Set{}, fmt.Errorf("%s and %s are mutually exclusive", EnvWorkspaces, EnvWorkspacesFile)
	case path != "":
		file, err := os.ReadFile(path)
		if err != nil {
			return Set{}, fmt.Errorf("%s: %w", EnvWorkspacesFile, err)
		}
		raw = string(file)
	case raw == "":
		return loadLegacy(getenv)
	}
	var identities []Identity
	if err := json.Unmarshal([]byte(raw), &identities); err != nil {
		return Set{}, fmt.Errorf("%s is not a JSON identity list: %w", EnvWorkspaces, err)
	}
	return newSet(identities, getenv)
}

// loadLegacy builds the single-workspace set from the original three keys.
func loadLegacy(getenv func(string) string) (Set, error) {
	id := strings.TrimSpace(getenv(EnvWorkspaceID))
	if id == "" {
		return Set{}, fmt.Errorf("%s is required (or configure %s)", EnvWorkspaceID, EnvWorkspaces)
	}
	daemonID := strings.TrimSpace(getenv(EnvDaemonID))
	if daemonID == "" {
		daemonID = DefaultDaemonID
	}
	token := strings.TrimSpace(getenv(EnvServerToken))
	if token == "" {
		return Set{}, fmt.Errorf("%s is required", EnvServerToken)
	}
	return newSet([]Identity{{WorkspaceID: id, DaemonID: daemonID, Token: token}}, getenv)
}

// NewSet validates identities and returns the set. The rules are the
// deployment invariants: every identity needs a workspace id, a daemon id and
// a server credential; workspace ids and daemon ids are unique (one identity
// per workspace, and no identity shared across workspaces, F2).
func NewSet(identities []Identity) (Set, error) { return newSet(identities, os.Getenv) }

func newSet(identities []Identity, getenv func(string) string) (Set, error) {
	if len(identities) == 0 {
		return Set{}, fmt.Errorf("at least one workspace identity is required")
	}
	out := Set{
		identities: make([]Identity, 0, len(identities)),
		byID:       make(map[string]Identity, len(identities)),
	}
	daemonIDs := make(map[string]string, len(identities))
	for i := range identities {
		identity := identities[i]
		identity.WorkspaceID = strings.TrimSpace(identity.WorkspaceID)
		identity.DaemonID = strings.TrimSpace(identity.DaemonID)
		token, err := resolveToken(identity, getenv)
		if err != nil {
			return Set{}, err
		}
		identity.Token, identity.TokenEnv = token, ""
		if identity.WorkspaceID == "" {
			return Set{}, fmt.Errorf("workspace identity %d has no workspace_id", i)
		}
		if identity.DaemonID == "" {
			return Set{}, fmt.Errorf("workspace %s has no daemon_id", identity.WorkspaceID)
		}
		if _, dup := out.byID[identity.WorkspaceID]; dup {
			return Set{}, fmt.Errorf("workspace %s is configured twice", identity.WorkspaceID)
		}
		if other, dup := daemonIDs[identity.DaemonID]; dup {
			return Set{}, fmt.Errorf("daemon identity %s is shared by workspaces %s and %s (F2)", identity.DaemonID, other, identity.WorkspaceID)
		}
		daemonIDs[identity.DaemonID] = identity.WorkspaceID
		out.identities = append(out.identities, identity)
		out.byID[identity.WorkspaceID] = identity
	}
	return out, nil
}

// resolveToken reads the credential from the inline field or its env key.
func resolveToken(identity Identity, getenv func(string) string) (string, error) {
	token := strings.TrimSpace(identity.Token)
	if token == "" && strings.TrimSpace(identity.TokenEnv) != "" {
		token = strings.TrimSpace(getenv(strings.TrimSpace(identity.TokenEnv)))
	}
	if token == "" {
		return "", fmt.Errorf("workspace %s has no server credential", identity.WorkspaceID)
	}
	if !strings.HasPrefix(token, "mdt_") && !strings.HasPrefix(token, "mul_") {
		return "", fmt.Errorf("workspace %s credential must be an mdt_ or mul_ token", identity.WorkspaceID)
	}
	return token, nil
}

// All returns the identities in configuration order.
func (s Set) All() []Identity {
	return slices.Clone(s.identities)
}

// Get returns the identity of workspaceID.
func (s Set) Get(workspaceID string) (Identity, bool) {
	identity, ok := s.byID[workspaceID]
	return identity, ok
}

// Has reports whether workspaceID is configured.
func (s Set) Has(workspaceID string) bool {
	_, ok := s.byID[workspaceID]
	return ok
}

// IDs returns the configured workspace ids in configuration order.
func (s Set) IDs() []string {
	ids := make([]string, 0, len(s.identities))
	for _, identity := range s.identities {
		ids = append(ids, identity.WorkspaceID)
	}
	return ids
}

// Len reports how many workspaces the deployment configures.
func (s Set) Len() int { return len(s.identities) }
