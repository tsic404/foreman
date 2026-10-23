// Package workspace holds the deployment's configured workspace identities.
//
// One deployment serves N workspaces (ADR-047); every control-plane action
// runs once per (credential, daemon identity, workspace). The credential has
// one of two scopes (ADR-049): a single user-scoped credential covering the
// user's visible workspaces, or one workspace-scoped credential per workspace.
// The two forms are distinct deployment shapes, and only the user-scoped form
// accepts a manifest that narrows the discovered set.
//
// This package is the deployment's offline face: it parses and validates the
// configured shape and never talks to the network. The discovery call that
// turns a user-scoped credential into a workspace set belongs to the caller
// (§1.1 C-17); this package turns its answer into identities.
package workspace

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Environment variable names of the multi-workspace interface.
const (
	// EnvWorkspaces carries the workspace-scoped identity list as inline JSON
	// (an alternative to EnvWorkspacesFile, convenient for small deployments).
	EnvWorkspaces = "FOREMAN_WORKSPACES"
	// EnvWorkspacesFile names a mounted file holding the same JSON list; the
	// Secret carrier is the expected production form.
	EnvWorkspacesFile = "FOREMAN_WORKSPACES_FILE"
	// EnvWorkspacesManifest is the optional narrowing list of the user-scoped
	// credential form: a JSON array of workspace ids. It only ever narrows the
	// discovered visible set (ADR-049 decision 1). Present with the
	// workspace-scoped credential form it is a configuration error.
	EnvWorkspacesManifest = "FOREMAN_WORKSPACES_MANIFEST"
	// EnvWorkspacesManifestFile names a mounted file holding the same list.
	EnvWorkspacesManifestFile = "FOREMAN_WORKSPACES_MANIFEST_FILE"
	// Legacy single-workspace keys: a legal workspace-scoped set of one.
	EnvWorkspaceID = "MULTICA_WORKSPACE_ID"
	EnvDaemonID    = "FOREMAN_DAEMON_ID"
	EnvServerToken = "MULTICA_TOKEN"

	// DefaultDaemonID is the deployment-level base identity. The user-scoped
	// discovery form derives each workspace's identity from it (ADR-049
	// decision 3); the legacy single-workspace form uses it verbatim.
	DefaultDaemonID = "foreman"
)

// CredentialScope is how the deployment's server credentials are scoped
// (ADR-049 decision 1).
type CredentialScope int

const (
	// ScopeWorkspace is the per-workspace credential form: one credential
	// bound to one workspace per configured pair. It accepts no manifest.
	ScopeWorkspace CredentialScope = iota
	// ScopeUser is the single user-scoped credential form: one credential
	// covering every workspace the user belongs to, discovered at startup,
	// optionally narrowed by a manifest.
	ScopeUser
)

// String is the event/report vocabulary of the scope (§9.1 发现来源).
func (s CredentialScope) String() string {
	if s == ScopeUser {
		return "user"
	}
	return "workspace"
}

// Identity is one workspace's (credential, daemon identity) pair (§0).
type Identity struct {
	WorkspaceID string `json:"workspace_id"`
	DaemonID    string `json:"daemon_id"`
	// Token is the server credential. TokenEnv names the environment variable
	// holding it instead, so each token can live under its own Secret key.
	Token    string `json:"token,omitempty"`
	TokenEnv string `json:"token_env,omitempty"`
}

// Ref is one discovered workspace: its platform id and display name.
type Ref struct {
	ID   string
	Name string
}

// Set is the validated workspace-scoped identity list.
type Set struct {
	identities []Identity
	byID       map[string]Identity
}

// Deployment is the parsed deployment interface (§10 多 workspace 前置).
type Deployment struct {
	Scope CredentialScope

	// ScopeWorkspace: the configured (credential, daemon identity) pairs, in
	// configuration order.
	Set Set

	// ScopeUser: the single user-scoped credential, the deployment-level base
	// identity the per-workspace identities derive from, and the optional
	// manifest (nil = none).
	UserToken    string
	BaseDaemonID string
	Manifest     []string
}

// LoadDeployment parses the deployment shape. The two forms are selected by
// the interface shape, not by a flag: a workspace-scoped identity list (or
// the legacy single-workspace keys) selects ScopeWorkspace; a server
// credential with neither selects ScopeUser, whose workspace set is
// discovered at startup. Configuration errors refuse startup: a bad shape
// must never be half-applied.
func LoadDeployment(getenv func(string) string) (Deployment, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	manifest, manifestSet, err := loadManifest(getenv)
	if err != nil {
		return Deployment{}, err
	}
	raw := strings.TrimSpace(getenv(EnvWorkspaces))
	// listVar names the key the identity list actually came from: a message
	// that always says FOREMAN_WORKSPACES sends the operator to the wrong
	// place when the list is file-borne.
	listVar := EnvWorkspaces
	path := strings.TrimSpace(getenv(EnvWorkspacesFile))
	if path != "" {
		listVar = EnvWorkspacesFile
	}
	switch {
	case raw != "" && path != "":
		return Deployment{}, fmt.Errorf("%s and %s are mutually exclusive", EnvWorkspaces, EnvWorkspacesFile)
	case manifestSet && (raw != "" || path != ""):
		// ADR-049 decision 1: the workspace-scoped credential form accepts no
		// manifest — the configured pairs are the discovery set.
		return Deployment{}, fmt.Errorf("%s is not accepted with %s: the workspace-scoped credential form derives its set from its own pairs",
			EnvWorkspacesManifest, listVar)
	case path != "":
		file, err := os.ReadFile(path)
		if err != nil {
			return Deployment{}, fmt.Errorf("%s: %w", EnvWorkspacesFile, err)
		}
		raw = string(file)
	case raw == "":
		return loadSingleOrUserScope(getenv, manifest, manifestSet)
	}
	var identities []Identity
	if err := json.Unmarshal([]byte(raw), &identities); err != nil {
		return Deployment{}, fmt.Errorf("%s is not a JSON identity list: %w", listVar, err)
	}
	set, err := newSet(identities, getenv)
	if err != nil {
		return Deployment{}, err
	}
	return Deployment{Scope: ScopeWorkspace, Set: set}, nil
}

// loadSingleOrUserScope resolves the two shapes that carry one credential: the
// legacy single-workspace pair (explicit workspace id) and the user-scoped
// discovery form (no workspace id). A manifest selects the discovery form, so
// an explicit workspace id beside a manifest is a configuration error.
func loadSingleOrUserScope(getenv func(string) string, manifest []string, manifestSet bool) (Deployment, error) {
	id := strings.TrimSpace(getenv(EnvWorkspaceID))
	token := strings.TrimSpace(getenv(EnvServerToken))
	if id != "" {
		if manifestSet {
			return Deployment{}, fmt.Errorf("%s is not accepted with %s: %s is a workspace-scoped identity",
				EnvWorkspacesManifest, EnvWorkspaceID, EnvWorkspaceID)
		}
		set, err := loadLegacy(getenv)
		if err != nil {
			return Deployment{}, err
		}
		return Deployment{Scope: ScopeWorkspace, Set: set}, nil
	}
	if token == "" {
		return Deployment{}, fmt.Errorf("%s is required (or configure %s)", EnvServerToken, EnvWorkspaces)
	}
	if !strings.HasPrefix(token, "mdt_") && !strings.HasPrefix(token, "mul_") {
		return Deployment{}, fmt.Errorf("%s must be an mdt_ or mul_ token", EnvServerToken)
	}
	base := strings.TrimSpace(getenv(EnvDaemonID))
	if base == "" {
		base = DefaultDaemonID
	}
	return Deployment{
		Scope:        ScopeUser,
		UserToken:    token,
		BaseDaemonID: base,
		Manifest:     manifest,
	}, nil
}

// loadManifest reads the optional narrowing list. A present-but-empty list is
// a configuration error (§ADR-049 decision 1), distinct from an absent one.
func loadManifest(getenv func(string) string) (manifest []string, present bool, err error) {
	raw := strings.TrimSpace(getenv(EnvWorkspacesManifest))
	path := strings.TrimSpace(getenv(EnvWorkspacesManifestFile))
	switch {
	case raw != "" && path != "":
		return nil, false, fmt.Errorf("%s and %s are mutually exclusive", EnvWorkspacesManifest, EnvWorkspacesManifestFile)
	case path != "":
		file, err := os.ReadFile(path)
		if err != nil {
			return nil, false, fmt.Errorf("%s: %w", EnvWorkspacesManifestFile, err)
		}
		raw = strings.TrimSpace(string(file))
		present = true
	case raw != "":
		present = true
	default:
		return nil, false, nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, false, fmt.Errorf("%s is not a JSON workspace-id list: %w", EnvWorkspacesManifest, err)
	}
	if len(ids) == 0 {
		return nil, false, fmt.Errorf("%s is empty: a manifest must name at least one workspace", EnvWorkspacesManifest)
	}
	out := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, false, fmt.Errorf("%s contains an empty workspace id", EnvWorkspacesManifest)
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, true, nil
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
// per workspace, and no identity shared across workspaces, F2). A workspace id
// must also be a legal Kubernetes label value: it is written into Job, pod and
// carrier labels and rendered into the workspace placeholder, so an illegal
// value would otherwise surface only as an apiserver rejection at Job creation
// (and, in a path-shaped placeholder, as an unintended node directory).
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
		if problems := validation.IsValidLabelValue(identity.WorkspaceID); len(problems) > 0 {
			return Set{}, fmt.Errorf("workspace id %q is not a legal Kubernetes label value: %s",
				identity.WorkspaceID, strings.Join(problems, "; "))
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

// DerivedDaemonID derives one workspace's deployment identity from the
// deployment-level base identity (ADR-049 decision 3): deterministic and
// stable across restarts, distinct across workspaces (F2).
func DerivedDaemonID(base, workspaceID string) string {
	return base + "-" + workspaceID
}

// Identities renders the user-scoped discovery form into per-workspace
// identities: one deployment identity per discovered workspace, all sharing
// the single user-scoped credential.
func (d Deployment) Identities(discovered []Ref) []Identity {
	out := make([]Identity, 0, len(discovered))
	for _, ws := range discovered {
		out = append(out, Identity{
			WorkspaceID: ws.ID,
			DaemonID:    DerivedDaemonID(d.BaseDaemonID, ws.ID),
			Token:       d.UserToken,
		})
	}
	return out
}

// ResolveDiscoverySet applies the manifest rule to the visible set (ADR-049
// decision 1): no manifest ⇒ the visible set is the discovery set; with a
// manifest ⇒ the discovery set is manifest ∩ visible, and a manifest entry
// outside the visible set is a configuration error naming that workspace.
// excluded carries the visible-but-not-listed workspaces, in visible order.
func ResolveDiscoverySet(visible []Ref, manifest []string) (set, excluded []Ref, err error) {
	if len(manifest) == 0 {
		return slices.Clone(visible), nil, nil
	}
	listed := make(map[string]bool, len(manifest))
	for _, id := range manifest {
		listed[id] = true
	}
	seen := make(map[string]bool, len(visible))
	set = make([]Ref, 0, len(manifest))
	for _, ws := range visible {
		seen[ws.ID] = true
		if listed[ws.ID] {
			set = append(set, ws)
		} else {
			excluded = append(excluded, ws)
		}
	}
	var missing []string
	for _, id := range manifest {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return nil, nil, fmt.Errorf("manifest workspace(s) %s are not visible to the server credential", strings.Join(missing, ", "))
	}
	if len(set) == 0 {
		return nil, nil, fmt.Errorf("the manifest selects no visible workspace")
	}
	return set, excluded, nil
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
