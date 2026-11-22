// Package rules owns environment-rule storage, draft validation, atomic
// publish and the rule → environment resolver preview and real dispatch share
// (04-architecture.md §3.4; 02-contracts.md V2-C01).
package rules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/environment"
)

const (
	// MaxRules bounds one published or drafted rule set.
	MaxRules = 256
	// MaxHistory bounds the published revisions a rollback may read.
	MaxHistory = 32
	// MaxCapabilities bounds one rule's capability lists.
	MaxCapabilities = 32
)

// Level is a rule's position in the resolution order (§3.4). A higher level
// wins; rules at one level that can both match one task must not disagree on
// where it runs.
type Level int

const (
	LevelGlobal    Level = 0 // no server, no workspace
	LevelServer    Level = 1 // server default
	LevelWorkspace Level = 2 // workspace default
	LevelSingleDim Level = 3 // workspace + agent, or workspace + project
	LevelCombined  Level = 4 // workspace + agent + project
)

// String names the level as the console shows it.
func (l Level) String() string {
	switch l {
	case LevelGlobal:
		return "global"
	case LevelServer:
		return "server"
	case LevelWorkspace:
		return "workspace"
	case LevelSingleDim:
		return "single-dimension"
	case LevelCombined:
		return "combined"
	default:
		return "unknown"
	}
}

// Rule is one environment rule. A rule never changes the runtime or the Job
// template; it only votes on the environment (§3.4).
type Rule struct {
	ID                   string   `json:"id"`
	ServerID             string   `json:"server_id,omitempty"`
	WorkspaceID          string   `json:"workspace_id,omitempty"`
	AgentID              string   `json:"agent_id,omitempty"`
	ProjectID            string   `json:"project_id,omitempty"`
	EnvironmentID        string   `json:"environment_id,omitempty"`
	TrustDomain          string   `json:"trust_domain,omitempty"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	PreferCapabilities   []string `json:"prefer_capabilities,omitempty"`
	Enabled              bool     `json:"enabled"`
}

// Level classifies the rule's scope shape.
func (r Rule) Level() Level {
	switch {
	case r.ServerID == "" && r.WorkspaceID == "":
		return LevelGlobal
	case r.WorkspaceID == "":
		return LevelServer
	case r.AgentID != "" && r.ProjectID != "":
		return LevelCombined
	case r.AgentID != "" || r.ProjectID != "":
		return LevelSingleDim
	default:
		return LevelWorkspace
	}
}

// Matches reports whether every condition the rule names equals the request's
// value. An empty condition matches every value, which is what makes a default
// rule a fallback rather than a wildcard override.
func (r Rule) Matches(serverID, workspaceID, agentID, projectID string) bool {
	if r.ServerID != "" && r.ServerID != serverID {
		return false
	}
	if r.WorkspaceID != "" && r.WorkspaceID != workspaceID {
		return false
	}
	if r.AgentID != "" && r.AgentID != agentID {
		return false
	}
	if r.ProjectID != "" && r.ProjectID != projectID {
		return false
	}
	return true
}

// Effect is the part of a rule that decides where a task runs. Two rules are
// interchangeable only when their effects are equal; otherwise a same-level
// pair that can both match one task is a conflict.
type Effect struct {
	EnvironmentID string
	TrustDomain   string
	Required      []string
	Prefer        []string
}

func (r Rule) effect() Effect {
	return Effect{
		EnvironmentID: r.EnvironmentID,
		TrustDomain:   r.TrustDomain,
		Required:      append([]string{}, r.RequiredCapabilities...),
		Prefer:        append([]string{}, r.PreferCapabilities...),
	}
}

func (e Effect) equal(other Effect) bool {
	return e.EnvironmentID == other.EnvironmentID &&
		e.TrustDomain == other.TrustDomain &&
		slices.Equal(e.Required, other.Required) &&
		slices.Equal(e.Prefer, other.Prefer)
}

// Revision is one published rule set. History keeps the content so a rollback
// re-validates the old rules instead of trusting them.
type Revision struct {
	Revision       uint64    `json:"revision"`
	Rules          []Rule    `json:"rules"`
	PublishedAt    time.Time `json:"published_at"`
	Actor          string    `json:"actor,omitempty"`
	RolledBackFrom uint64    `json:"rolled_back_from,omitempty"`
}

func (r Revision) clone() Revision {
	out := r
	out.Rules = cloneRules(r.Rules)
	return out
}

// Finding is one publish or preview finding. Every finding names the item and
// the console page that repairs it.
type Finding struct {
	Code          string      `json:"code"`
	Message       string      `json:"message"`
	RuleID        string      `json:"rule_id,omitempty"`
	Field         string      `json:"field,omitempty"`
	EnvironmentID string      `json:"environment_id,omitempty"`
	Remediation   Remediation `json:"remediation"`
}

// Remediation is the console entry a finding points at.
type Remediation struct {
	Label string `json:"label"`
	Path  string `json:"path"`
}

var (
	remediationAgents       = Remediation{Label: "Agent 管理", Path: "/admin/agents"}
	remediationServers      = Remediation{Label: "Multica 管理", Path: "/admin/servers"}
	remediationEnvironments = Remediation{Label: "运行环境", Path: "/admin/environments"}
	remediationRules        = Remediation{Label: "规则管理", Path: "/admin/rules"}
	remediationCLIs         = Remediation{Label: "CLI 发布", Path: "/admin/cli"}
	remediationConfigs      = Remediation{Label: "Agent 配置", Path: "/admin/agent-configs"}
)

// Snapshot is the stored rule document: the published revision, the draft an
// editor is working on and the published history a rollback reads.
type Snapshot struct {
	Revision          uint64     `json:"revision"`
	Published         []Rule     `json:"published"`
	Draft             []Rule     `json:"draft"`
	DraftBaseRevision uint64     `json:"draft_base_revision"`
	History           []Revision `json:"history"`
}

func (s Snapshot) clone() Snapshot {
	out := s
	out.Published = cloneRules(s.Published)
	out.Draft = cloneRules(s.Draft)
	out.History = make([]Revision, len(s.History))
	for i := range s.History {
		out.History[i] = s.History[i].clone()
	}
	return out
}

func cloneRules(rules []Rule) []Rule {
	if rules == nil {
		return nil
	}
	out := make([]Rule, len(rules))
	for i, rule := range rules {
		rule.RequiredCapabilities = append([]string{}, rule.RequiredCapabilities...)
		rule.PreferCapabilities = append([]string{}, rule.PreferCapabilities...)
		out[i] = rule
	}
	return out
}

// Store persists the rule document. The service serializes read-modify-write
// sequences, so a store never merges concurrent writers.
type Store interface {
	Load(ctx context.Context) (Snapshot, error)
	Save(ctx context.Context, snapshot Snapshot) error
}

// MemoryStore is the in-process store.
type MemoryStore struct {
	mu   sync.Mutex
	snap Snapshot
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Load answers the current document.
func (m *MemoryStore) Load(context.Context) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap.clone(), nil
}

// Save replaces the document.
func (m *MemoryStore) Save(_ context.Context, snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = snap.clone()
	return nil
}

// FileStore keeps the document in one JSON file and replaces it atomically.
type FileStore struct {
	mu   sync.Mutex
	path string
	snap Snapshot
}

// NewFileStore loads the document at path, or starts empty when the file does
// not exist. A malformed document is an error: starting empty would discard
// rules the operator published.
func NewFileStore(path string) (*FileStore, error) {
	store := &FileStore{path: path}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the rule document: %w", err)
	}
	if err := json.Unmarshal(raw, &store.snap); err != nil {
		return nil, fmt.Errorf("decode the rule document %s: %w", path, err)
	}
	return store, nil
}

// Load answers the current document.
func (f *FileStore) Load(context.Context) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap.clone(), nil
}

// Save writes the document to a temporary file and renames it into place.
func (f *FileStore) Save(_ context.Context, snap Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode the rule document: %w", err)
	}
	dir := filepath.Dir(f.path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create the rule state directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".rules-*.json")
	if err != nil {
		return fmt.Errorf("create the rule state file: %w", err)
	}
	name := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return fmt.Errorf("write the rule state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("close the rule state file: %w", err)
	}
	if err := os.Rename(name, f.path); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("replace the rule state file: %w", err)
	}
	f.snap = snap.clone()
	return nil
}

// Config is the service's configuration.
type Config struct {
	// Store persists the rule document. Defaults to an empty MemoryStore.
	Store Store
	// Environments answers the registered-environment facts validation and
	// the resolver read. Required.
	Environments EnvironmentSource
	// Catalog answers the directory facts validation and the resolver read.
	// Required.
	Catalog CatalogSource
	// Issues resolves an Issue's project identity. Nil leaves the identity
	// unknown, which the resolver reports rather than guesses.
	Issues IssueProjects
	// Releases answers the two published CLI lines. Nil leaves both unknown.
	Releases ReleaseSource
	// Configs answers the selected Agent configuration revision. Nil leaves
	// the configuration facts unknown.
	Configs ConfigSource
	// SourceID resolves the source a workspace belongs to. The claim path
	// names only its workspace; preview always names the server explicitly.
	SourceID func(ctx context.Context, workspaceID string) (string, bool)
	// Sessions maps a prior session to the environment that owns it. Nil
	// means a continuation has no affinity evidence.
	Sessions SessionIndex
	// References is the environment registry's rule-reference index. Drafted
	// and published rules both block retirement, so both are indexed.
	References *environment.MemoryRuleReferences
	// Platform is the target OS/architecture the two CLIs must publish for.
	Platform string
	// JobNamespace is the fixed template's namespace, projected by preview.
	JobNamespace string
	// Clock is the service's single clock seam.
	Clock func() time.Time
	// Logger defaults to slog.Default.
	Logger *slog.Logger
}

// Service owns the rule document and the shared resolver.
type Service struct {
	cfg        Config
	mu         sync.Mutex
	now        func() time.Time
	log        *slog.Logger
	references map[string]map[string]bool
}

// New builds the service.
func New(cfg Config) (*Service, error) {
	if cfg.Environments == nil {
		return nil, errors.New("rules: the environment registry is required")
	}
	if cfg.Catalog == nil {
		return nil, errors.New("rules: the catalog directory is required")
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Platform == "" {
		cfg.Platform = "linux/amd64"
	}
	service := &Service{
		cfg:        cfg,
		now:        cfg.Clock,
		log:        cfg.Logger.With("component", "rules"),
		references: map[string]map[string]bool{},
	}
	snap, err := cfg.Store.Load(context.Background())
	if err != nil {
		return nil, err
	}
	service.syncReferences(snap)
	return service, nil
}

// Errors the management face answers with.
var (
	// ErrRevisionConflict means the published revision moved since the caller
	// read it; the caller must reread before editing or publishing.
	ErrRevisionConflict = errors.New("the rules changed since this base revision")
	// ErrRevisionNotFound means a rollback named a revision history no longer
	// retains.
	ErrRevisionNotFound = errors.New("no such published rule revision")
	// ErrUnauthorized means the caller's grant does not cover the rule scope.
	ErrUnauthorized = errors.New("the credential does not cover this rule scope")
)

// ValidationError carries the findings that refuse a publish.
type ValidationError struct {
	Findings []Finding
}

func (e *ValidationError) Error() string {
	if len(e.Findings) == 0 {
		return "the rule set is invalid"
	}
	return fmt.Sprintf("the rule set is invalid: %s", e.Findings[0].Message)
}

// StateView is the management face: published rules, the draft, the history
// and the findings the current draft produces.
type StateView struct {
	Revision          uint64     `json:"revision"`
	Published         []Rule     `json:"published"`
	Draft             []Rule     `json:"draft"`
	DraftBaseRevision uint64     `json:"draft_base_revision"`
	History           []Revision `json:"history"`
	Findings          []Finding  `json:"findings"`
}

// State answers the current document and the draft's findings.
func (s *Service) State(ctx context.Context) (StateView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.cfg.Store.Load(ctx)
	if err != nil {
		return StateView{}, err
	}
	return stateView(snap, s.validate(ctx, snap.Draft)), nil
}

// Draft stores the draft rules. A stale base revision is refused so an editor
// cannot overwrite a publish it never saw; the draft itself may still hold
// conflicts, which the returned findings explain.
func (s *Service) Draft(ctx context.Context, baseRevision uint64, draft []Rule, grant catalog.Grant, actor string) (StateView, error) {
	if len(draft) > MaxRules {
		return StateView{}, &ValidationError{Findings: []Finding{{
			Code: "too_many_rules", Field: "rules",
			Message:     fmt.Sprintf("at most %d rules fit one publication", MaxRules),
			Remediation: remediationRules,
		}}}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.cfg.Store.Load(ctx)
	if err != nil {
		return StateView{}, err
	}
	if baseRevision != snap.Revision {
		return StateView{}, ErrRevisionConflict
	}
	snap.Draft = normalizeRules(draft)
	snap.DraftBaseRevision = snap.Revision
	if err := s.cfg.Store.Save(ctx, snap); err != nil {
		return StateView{}, err
	}
	s.syncReferences(snap)
	return stateView(snap, s.validate(ctx, snap.Draft)), nil
}

// Publish replaces the published rules with the draft in one step. Validation
// runs first, so a refused publish leaves the published revision untouched.
func (s *Service) Publish(ctx context.Context, baseRevision uint64, grant catalog.Grant, actor string) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.cfg.Store.Load(ctx)
	if err != nil {
		return Revision{}, err
	}
	if baseRevision != snap.Revision {
		return Revision{}, ErrRevisionConflict
	}
	findings := s.validate(ctx, snap.Draft)
	if len(findings) > 0 {
		return Revision{}, &ValidationError{Findings: findings}
	}
	return s.publishLocked(ctx, &snap, snap.Draft, actor, 0)
}

// Rollback publishes one historical rule set as a new revision. The old
// content is validated again, so a retired environment is refused rather than
// restored.
func (s *Service) Rollback(ctx context.Context, revision, baseRevision uint64, grant catalog.Grant, actor string) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.cfg.Store.Load(ctx)
	if err != nil {
		return Revision{}, err
	}
	if baseRevision != snap.Revision {
		return Revision{}, ErrRevisionConflict
	}
	var source *Revision
	for i := range snap.History {
		if snap.History[i].Revision == revision {
			source = &snap.History[i]
			break
		}
	}
	if source == nil {
		return Revision{}, ErrRevisionNotFound
	}
	findings := s.validate(ctx, source.Rules)
	if len(findings) > 0 {
		return Revision{}, &ValidationError{Findings: findings}
	}
	return s.publishLocked(ctx, &snap, source.Rules, actor, revision)
}

func (s *Service) publishLocked(ctx context.Context, snap *Snapshot, rules []Rule, actor string, rolledBackFrom uint64) (Revision, error) {
	normalized := normalizeRules(rules)
	now := s.now()
	revision := Revision{
		Revision:       snap.Revision + 1,
		Rules:          cloneRules(normalized),
		PublishedAt:    now,
		Actor:          actor,
		RolledBackFrom: rolledBackFrom,
	}
	snap.Revision = revision.Revision
	snap.Published = cloneRules(normalized)
	snap.Draft = cloneRules(normalized)
	snap.DraftBaseRevision = revision.Revision
	snap.History = append([]Revision{revision.clone()}, snap.History...)
	if len(snap.History) > MaxHistory {
		snap.History = snap.History[:MaxHistory]
	}
	if err := s.cfg.Store.Save(ctx, *snap); err != nil {
		return Revision{}, err
	}
	s.syncReferences(*snap)
	s.log.Info("rules.published", "revision", revision.Revision, "rules", len(normalized),
		"actor", actor, "rolled_back_from", rolledBackFrom)
	return revision, nil
}

// syncReferences rebuilds the environment registry's rule index from the
// published and drafted sets. A draft still names its environments, so a
// retired environment cannot be deleted out from under an editor.
func (s *Service) syncReferences(snap Snapshot) {
	if s.cfg.References == nil {
		return
	}
	next := map[string]map[string]bool{}
	add := func(rules []Rule) {
		for _, rule := range rules {
			if rule.EnvironmentID == "" {
				continue
			}
			if next[rule.EnvironmentID] == nil {
				next[rule.EnvironmentID] = map[string]bool{}
			}
			next[rule.EnvironmentID][rule.ID] = true
		}
	}
	add(snap.Published)
	add(snap.Draft)
	for envID, ids := range s.references {
		for ruleID := range ids {
			if !next[envID][ruleID] {
				s.cfg.References.Remove(environment.ID(envID), ruleID)
			}
		}
	}
	for envID, ids := range next {
		for ruleID := range ids {
			if !s.references[envID][ruleID] {
				s.cfg.References.Add(environment.ID(envID), ruleID)
			}
		}
	}
	s.references = next
}

func stateView(snap Snapshot, findings []Finding) StateView {
	view := StateView{
		Revision:          snap.Revision,
		Published:         cloneRules(snap.Published),
		Draft:             cloneRules(snap.Draft),
		DraftBaseRevision: snap.DraftBaseRevision,
		History:           append([]Revision{}, snap.History...),
		Findings:          findings,
	}
	if view.Published == nil {
		view.Published = []Rule{}
	}
	if view.Draft == nil {
		view.Draft = []Rule{}
	}
	if view.History == nil {
		view.History = []Revision{}
	}
	if view.Findings == nil {
		view.Findings = []Finding{}
	}
	return view
}

var ruleIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// validate runs every check a publish must pass. Authorization is included:
// the grant must cover the (server, workspace) pair a rule names, and a global
// rule needs the deployment-wide grant.
func (s *Service) validate(ctx context.Context, rules []Rule) []Finding {
	findings := make([]Finding, 0)
	normalized := normalizeRules(rules)
	seen := map[string]bool{}
	for _, rule := range normalized {
		findings = append(findings, shapeFindings(rule)...)
		if rule.ID == "" {
			continue
		}
		if seen[rule.ID] {
			findings = append(findings, Finding{
				Code: "duplicate_rule_id", RuleID: rule.ID, Field: "id",
				Message:     fmt.Sprintf("rule id %q appears more than once", rule.ID),
				Remediation: remediationRules,
			})
		}
		seen[rule.ID] = true
	}
	findings = append(findings, s.referenceFindings(ctx, normalized)...)
	findings = append(findings, conflictFindings(normalized)...)
	return findings
}

func shapeFindings(rule Rule) []Finding {
	findings := make([]Finding, 0)
	add := func(code, field, message string) {
		findings = append(findings, Finding{
			Code: code, RuleID: rule.ID, Field: field, Message: message,
			Remediation: remediationRules,
		})
	}
	if !ruleIDPattern.MatchString(rule.ID) {
		add("invalid_rule_id", "id", "a rule id is 1-64 letters, digits, dots, dashes or underscores")
	}
	if rule.WorkspaceID != "" && rule.ServerID == "" {
		add("workspace_without_server", "server_id", "a workspace is a sub-domain of one server")
	}
	if (rule.AgentID != "" || rule.ProjectID != "") && rule.WorkspaceID == "" {
		add("condition_without_workspace", "workspace_id", "Agent and Project are cross-dimensions inside one workspace")
	}
	if len(rule.RequiredCapabilities) > MaxCapabilities || len(rule.PreferCapabilities) > MaxCapabilities {
		add("too_many_capabilities", "capabilities", "a rule carries at most 32 capabilities per list")
	}
	return findings
}

// referenceFindings checks the authorization and the dangling references a
// publish must refuse: an environment that is not registered (a retired one is
// absent, not softened) and an Agent or Project the current directory
// generation does not contain.
func (s *Service) referenceFindings(ctx context.Context, rules []Rule) []Finding {
	findings := make([]Finding, 0)
	environments, err := s.cfg.Environments.List(ctx, environment.Filter{}, environment.Grant{DeploymentWide: true})
	if err != nil {
		return append(findings, Finding{
			Code: "environment_registry_unreadable", Field: "environment_id",
			Message:     "the environment registry could not be read",
			Remediation: remediationEnvironments,
		})
	}
	byID := make(map[string]environment.EnvironmentView, len(environments))
	for _, view := range environments {
		byID[view.ID] = view
	}
	type workspaceKey struct{ server, workspace string }
	directories := map[workspaceKey]catalog.DirectoryView{}
	for _, rule := range rules {
		if rule.EnvironmentID != "" {
			view, ok := byID[rule.EnvironmentID]
			if !ok {
				findings = append(findings, Finding{
					Code: "dangling_environment", RuleID: rule.ID, Field: "environment_id",
					EnvironmentID: rule.EnvironmentID,
					Message:       fmt.Sprintf("environment %q is not registered or has been retired", rule.EnvironmentID),
					Remediation:   remediationEnvironments,
				})
			} else {
				if rule.TrustDomain != "" && view.TrustDomain != rule.TrustDomain {
					findings = append(findings, Finding{
						Code: "environment_domain_mismatch", RuleID: rule.ID, Field: "trust_domain",
						EnvironmentID: rule.EnvironmentID,
						Message:       fmt.Sprintf("environment %q lives in trust domain %q, not %q", rule.EnvironmentID, view.TrustDomain, rule.TrustDomain),
						Remediation:   remediationEnvironments,
					})
				}
				if missing := missingCapabilities(view.Capabilities, rule.RequiredCapabilities); len(missing) > 0 {
					findings = append(findings, Finding{
						Code: "environment_capability_gap", RuleID: rule.ID, Field: "required_capabilities",
						EnvironmentID: rule.EnvironmentID,
						Message:       fmt.Sprintf("environment %q does not declare %s", rule.EnvironmentID, strings.Join(missing, ", ")),
						Remediation:   remediationEnvironments,
					})
				}
			}
		}
		if rule.AgentID == "" && rule.ProjectID == "" {
			continue
		}
		key := workspaceKey{server: rule.ServerID, workspace: rule.WorkspaceID}
		directory, loaded := directories[key]
		if !loaded {
			view, err := s.cfg.Catalog.Directory(ctx, catalog.ServerID(rule.ServerID), rule.WorkspaceID, catalog.Grant{})
			if err == nil {
				directory = view
			}
			directories[key] = directory
		}
		if directory.SyncedAt == "" {
			findings = append(findings, Finding{
				Code: "reference_unknown", RuleID: rule.ID,
				Message:     fmt.Sprintf("workspace %q has no synced directory generation; Agent and Project references cannot be verified", rule.WorkspaceID),
				Remediation: remediationServers,
			})
			continue
		}
		if rule.AgentID != "" {
			if _, ok := findAgent(directory, rule.AgentID); !ok {
				findings = append(findings, Finding{
					Code: "dangling_agent", RuleID: rule.ID, Field: "agent_id",
					Message:     fmt.Sprintf("agent %q is not in the workspace directory", rule.AgentID),
					Remediation: remediationAgents,
				})
			}
		}
		if rule.ProjectID != "" {
			if !hasProject(directory, rule.ProjectID) {
				findings = append(findings, Finding{
					Code: "dangling_project", RuleID: rule.ID, Field: "project_id",
					Message:     fmt.Sprintf("project %q is not in the workspace directory", rule.ProjectID),
					Remediation: remediationRules,
				})
			}
		}
	}
	return findings
}

// conflictFindings refuses same-level rules that can both match one task but
// disagree on where it runs. A published combined rule masks a single-dimension
// pair at the same scope: that pair is only ambiguous when the combined rule is
// disabled (§3.4).
func conflictFindings(rules []Rule) []Finding {
	findings := make([]Finding, 0)
	enabled := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		if rule.Enabled {
			enabled = append(enabled, rule)
		}
	}
	combined := map[string]bool{}
	for _, rule := range enabled {
		if rule.Level() == LevelCombined {
			combined[rule.ServerID+"\x00"+rule.WorkspaceID+"\x00"+rule.AgentID+"\x00"+rule.ProjectID] = true
		}
	}
	for i := 0; i < len(enabled); i++ {
		for j := i + 1; j < len(enabled); j++ {
			a, b := enabled[i], enabled[j]
			if a.Level() != b.Level() {
				continue
			}
			if a.Level() == LevelSingleDim && maskedByCombined(combined, a, b) {
				continue
			}
			if !compatible(a, b) || a.effect().equal(b.effect()) {
				continue
			}
			findings = append(findings, Finding{
				Code:   "rule_conflict",
				RuleID: a.ID,
				Message: fmt.Sprintf("rules %q and %q can both match one task at the %s level but select different environments",
					a.ID, b.ID, a.Level()),
				Remediation: remediationRules,
			})
		}
	}
	return findings
}

// maskedByCombined reports whether one combined rule covers the exact
// agent-and-project pair the two single-dimension rules disagree about. A
// combined rule at the same workspace for a different pair leaves this pair
// ambiguous when that combined rule is disabled, so it must not hide the
// conflict.
func maskedByCombined(combined map[string]bool, a, b Rule) bool {
	agentID, projectID := a.AgentID, a.ProjectID
	if agentID == "" {
		agentID = b.AgentID
	}
	if projectID == "" {
		projectID = b.ProjectID
	}
	if agentID == "" || projectID == "" {
		return false
	}
	return combined[a.ServerID+"\x00"+a.WorkspaceID+"\x00"+agentID+"\x00"+projectID]
}

func compatible(a, b Rule) bool {
	if a.ServerID != "" && b.ServerID != "" && a.ServerID != b.ServerID {
		return false
	}
	if a.WorkspaceID != "" && b.WorkspaceID != "" && a.WorkspaceID != b.WorkspaceID {
		return false
	}
	if a.AgentID != "" && b.AgentID != "" && a.AgentID != b.AgentID {
		return false
	}
	if a.ProjectID != "" && b.ProjectID != "" && a.ProjectID != b.ProjectID {
		return false
	}
	return true
}

func normalizeRules(rules []Rule) []Rule {
	out := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		rule.ServerID = strings.TrimSpace(rule.ServerID)
		rule.WorkspaceID = strings.TrimSpace(rule.WorkspaceID)
		rule.AgentID = strings.TrimSpace(rule.AgentID)
		rule.ProjectID = strings.TrimSpace(rule.ProjectID)
		rule.EnvironmentID = strings.TrimSpace(rule.EnvironmentID)
		rule.TrustDomain = strings.TrimSpace(rule.TrustDomain)
		rule.RequiredCapabilities = normalizeCapabilities(rule.RequiredCapabilities)
		rule.PreferCapabilities = normalizeCapabilities(rule.PreferCapabilities)
		out = append(out, rule)
	}
	return out
}

func normalizeCapabilities(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}

func missingCapabilities(have, required []string) []string {
	missing := make([]string, 0)
	for _, want := range required {
		if !slices.Contains(have, want) {
			missing = append(missing, want)
		}
	}
	return missing
}

func findAgent(directory catalog.DirectoryView, agentID string) (catalog.AgentView, bool) {
	for _, agent := range directory.Agents {
		if agent.AgentID == agentID && !agent.Tombstoned {
			return agent, true
		}
	}
	return catalog.AgentView{}, false
}

func hasProject(directory catalog.DirectoryView, projectID string) bool {
	for _, project := range directory.Projects {
		if project.ProjectID == projectID {
			return true
		}
	}
	return false
}
