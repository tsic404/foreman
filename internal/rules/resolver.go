package rules

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/catalog"
	"github.com/tsic404/foreman/internal/environment"
	"github.com/tsic404/foreman/internal/release"
)

// EnvironmentSource is the registered-environment facts the resolver reads and
// the slot ledger a real dispatch reserves in. *environment.Service satisfies
// it.
type EnvironmentSource interface {
	List(ctx context.Context, filter environment.Filter, grant environment.Grant) ([]environment.EnvironmentView, error)
	AcquireSlot(ctx context.Context, id environment.ID, slotID string) (environment.Slot, error)
	ReleaseSlot(ctx context.Context, id environment.ID, slotID string, cleanupConfirmed bool) (environment.Slot, error)
}

// CatalogSource is the directory read. *catalog.Service satisfies it.
type CatalogSource interface {
	Directory(ctx context.Context, id catalog.ServerID, workspaceID string, grant catalog.Grant) (catalog.DirectoryView, error)
}

// IssueProjects resolves one Issue's project identity.
// *platform.ProjectQueries satisfies it.
type IssueProjects interface {
	ByIssue(ctx context.Context, workspaceID, issueID string) (string, error)
}

// ReleaseSource answers the two published CLI lines. *release.Service
// satisfies it.
type ReleaseSource interface {
	CurrentRelease(ctx context.Context, serverID string, kind release.Kind, adapter string) (string, map[string]string, bool)
}

// ConfigSource answers the selected Agent configuration revision.
// *agentconfig.Service satisfies it.
type ConfigSource interface {
	Resolve(ctx context.Context, scope agentconfig.Scope) (agentconfig.Revision, bool, error)
}

// SessionIndex maps a prior session to the environment that owns it. A
// continuation may only use that environment (§3.4).
type SessionIndex interface {
	EnvironmentForSession(ctx context.Context, workspaceID, sessionID string) (string, bool)
	RecordSession(ctx context.Context, workspaceID, sessionID, environmentID string)
}

// MemorySessionIndex is the bounded in-process affinity index. It is learned
// on a successful reservation; a deployment that needs the mapping across a
// restart replaces it with a durable implementation.
type MemorySessionIndex struct {
	mu      sync.Mutex
	order   []string
	entries map[string]string
	limit   int
}

// NewMemorySessionIndex returns an index bounded to limit entries (1024 when
// limit is not positive).
func NewMemorySessionIndex(limit int) *MemorySessionIndex {
	if limit <= 0 {
		limit = 1024
	}
	return &MemorySessionIndex{entries: map[string]string{}, limit: limit}
}

func sessionKey(workspaceID, sessionID string) string {
	return workspaceID + "\x00" + sessionID
}

// EnvironmentForSession answers the environment that owns one session.
func (m *MemorySessionIndex) EnvironmentForSession(_ context.Context, workspaceID, sessionID string) (string, bool) {
	if m == nil || sessionID == "" {
		return "", false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	environmentID, ok := m.entries[sessionKey(workspaceID, sessionID)]
	return environmentID, ok
}

// RecordSession remembers where a session ran.
func (m *MemorySessionIndex) RecordSession(_ context.Context, workspaceID, sessionID, environmentID string) {
	if m == nil || sessionID == "" || environmentID == "" {
		return
	}
	key := sessionKey(workspaceID, sessionID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.entries[key]; !exists {
		if len(m.order) >= m.limit {
			delete(m.entries, m.order[0])
			m.order = m.order[1:]
		}
		m.order = append(m.order, key)
	}
	m.entries[key] = environmentID
}

// Request is one resolution input. Preview and real dispatch pass the same
// value; Reserve is the only difference between them.
type Request struct {
	ServerID       string `json:"server_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	IssueID        string `json:"issue_id,omitempty"`
	ProjectID      string `json:"project_id,omitempty"`
	PriorSessionID string `json:"prior_session_id,omitempty"`
	AttemptID      string `json:"attempt_id,omitempty"`
	Reserve        bool   `json:"-"`
}

// Outcome is the resolver's verdict. Preview reports the same five classes the
// readiness check will, limited to the facts preview reads.
type Outcome string

const (
	OutcomeReady                Outcome = "ready"
	OutcomeConfigBlocked        Outcome = "config_blocked"
	OutcomeTemporarilySaturated Outcome = "temporarily_saturated"
	OutcomeUnknown              Outcome = "unknown"
	OutcomeUnmanaged            Outcome = "unmanaged"
)

// RuleDecision is one published rule's fate in this resolution.
type RuleDecision struct {
	RuleID   string `json:"rule_id"`
	Level    string `json:"level"`
	Matched  bool   `json:"matched"`
	Selected bool   `json:"selected"`
	Reason   string `json:"reason,omitempty"`
}

// EnvironmentDecision is one environment's fate in this resolution.
type EnvironmentDecision struct {
	ID        string `json:"id"`
	Selected  bool   `json:"selected"`
	FreeSlots int    `json:"free_slots"`
	Reason    string `json:"reason,omitempty"`
}

// EnvironmentChoice is the selected environment's management face.
type EnvironmentChoice struct {
	ID           string   `json:"id"`
	Endpoint     string   `json:"endpoint,omitempty"`
	TLSRef       string   `json:"tls_ref,omitempty"`
	StateRoot    string   `json:"state_root,omitempty"`
	Generation   string   `json:"generation,omitempty"`
	TrustDomain  string   `json:"trust_domain"`
	Capabilities []string `json:"capabilities"`
	Concurrency  int      `json:"concurrency"`
	FreeSlots    int      `json:"free_slots"`
}

// CLIFacts are the two published CLI lines' versions and digests for the
// target platform.
type CLIFacts struct {
	Platform       string `json:"platform"`
	MulticaVersion string `json:"multica_version,omitempty"`
	MulticaDigest  string `json:"multica_sha256,omitempty"`
	AgentVersion   string `json:"agent_version,omitempty"`
	AgentDigest    string `json:"agent_sha256,omitempty"`
}

// ConfigFacts are the selected Agent configuration reference. Readability is
// evidence, never an assumption: without a projection-identity verification
// fact it stays unverified (02-contracts V2-C01).
type ConfigFacts struct {
	Known       bool   `json:"known"`
	Revision    uint64 `json:"revision"`
	SecretRef   string `json:"secret_ref,omitempty"`
	FileName    string `json:"file_name,omitempty"`
	Digest      string `json:"sha256,omitempty"`
	Readability string `json:"readability"`
}

// TemplateView is the fixed Job template's sanitized face. A rule never
// changes it; preview states that rather than inventing a spec.
type TemplateView struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Note      string `json:"note"`
}

// Resolution is the answer preview and dispatch share.
type Resolution struct {
	ServerID             string                `json:"server_id"`
	WorkspaceID          string                `json:"workspace_id"`
	AgentID              string                `json:"agent_id"`
	IssueID              string                `json:"issue_id,omitempty"`
	ProjectID            string                `json:"project_id,omitempty"`
	ProjectKnown         bool                  `json:"project_known"`
	RuntimeID            string                `json:"runtime_id,omitempty"`
	Provider             string                `json:"provider,omitempty"`
	Management           string                `json:"management,omitempty"`
	SourceGeneration     uint64                `json:"source_generation,omitempty"`
	SourceSyncedAt       string                `json:"source_synced_at,omitempty"`
	RulesRevision        uint64                `json:"rules_revision"`
	Outcome              Outcome               `json:"outcome"`
	RuleID               string                `json:"rule_id,omitempty"`
	RuleLevel            string                `json:"rule_level,omitempty"`
	RuleDecisions        []RuleDecision        `json:"rule_decisions"`
	EnvironmentDecisions []EnvironmentDecision `json:"environment_decisions"`
	Environment          *EnvironmentChoice    `json:"environment,omitempty"`
	SlotID               string                `json:"slot_id,omitempty"`
	CLIs                 CLIFacts              `json:"clis"`
	Config               ConfigFacts           `json:"config"`
	Template             TemplateView          `json:"template"`
	Findings             []Finding             `json:"findings"`
	CheckedAt            time.Time             `json:"checked_at"`

	sourceKnown      bool
	agentKnown       bool
	bindingKnown     bool
	rulesKnown       bool
	environmentKnown bool
	cliKnown         bool
}

// Binding is the reservation a real dispatch freezes.
type Binding struct {
	EnvironmentID string
	SlotID        string
	RuleID        string
	Resolution    Resolution
}

// Errors the dispatch seam maps to its own behaviour.
var (
	// ErrEnvironmentSaturated means the task is eligible but no slot is free;
	// a continuation waits for its own endpoint instead of switching.
	ErrEnvironmentSaturated = errors.New("no environment slot is available")
	// ErrEnvironmentBlocked means no configuration change-free path exists.
	ErrEnvironmentBlocked = errors.New("the task is not eligible for any registered environment")
	// ErrEnvironmentUnknown means the facts are not trustworthy enough to
	// place the task.
	ErrEnvironmentUnknown = errors.New("the environment facts are not trustworthy")
)

// Preview answers the management preview. It never reserves a slot, never
// records a session and never writes the environment registry.
func (s *Service) Preview(ctx context.Context, req Request, grant catalog.Grant) (Resolution, error) {
	req.Reserve = false
	return s.resolve(ctx, req, grant)
}

// Bind is the real dispatch entry point: the same resolver with Reserve=true.
func (s *Service) Bind(ctx context.Context, req Request) (Binding, error) {
	req.Reserve = true
	res, err := s.resolve(ctx, req, catalog.Grant{})
	if err != nil {
		return Binding{}, err
	}
	binding := Binding{
		EnvironmentID: environmentID(res),
		SlotID:        res.SlotID,
		RuleID:        res.RuleID,
		Resolution:    res,
	}
	switch res.Outcome {
	case OutcomeReady:
		return binding, nil
	case OutcomeTemporarilySaturated:
		return binding, fmt.Errorf("%w: %s", ErrEnvironmentSaturated, primaryMessage(res.Findings))
	case OutcomeUnknown:
		return binding, fmt.Errorf("%w: %s", ErrEnvironmentUnknown, primaryMessage(res.Findings))
	default:
		return binding, fmt.Errorf("%w: %s", ErrEnvironmentBlocked, primaryMessage(res.Findings))
	}
}

// Release returns one reservation. An unconfirmed remote close leaves the slot
// isolated for the operator, which is the honest state (§3.3).
func (s *Service) Release(ctx context.Context, binding Binding) error {
	if binding.EnvironmentID == "" || binding.SlotID == "" {
		return nil
	}
	_, err := s.cfg.Environments.ReleaseSlot(ctx, environment.ID(binding.EnvironmentID), binding.SlotID, false)
	if errors.Is(err, environment.ErrNotFound) || errors.Is(err, environment.ErrSlotNotFound) {
		return nil
	}
	return err
}

func environmentID(res Resolution) string {
	if res.Environment == nil {
		return ""
	}
	return res.Environment.ID
}

func primaryMessage(findings []Finding) string {
	if len(findings) == 0 {
		return "no finding was recorded"
	}
	return findings[0].Message
}

func (s *Service) resolve(ctx context.Context, req Request, grant catalog.Grant) (Resolution, error) {
	if req.ServerID == "" && req.WorkspaceID != "" && s.cfg.SourceID != nil {
		if serverID, ok := s.cfg.SourceID(ctx, req.WorkspaceID); ok {
			req.ServerID = serverID
		}
	}
	res := Resolution{
		ServerID: req.ServerID, WorkspaceID: req.WorkspaceID, AgentID: req.AgentID,
		IssueID: req.IssueID, ProjectID: req.ProjectID,
		RuleDecisions:        []RuleDecision{},
		EnvironmentDecisions: []EnvironmentDecision{},
		Findings:             []Finding{},
		CheckedAt:            s.now(),
		Template: TemplateView{
			Name:      "foreman-job",
			Namespace: s.cfg.JobNamespace,
			Note:      "the fixed template is rendered by the Job builder; a rule never changes it",
		},
	}
	if req.ServerID == "" || req.WorkspaceID == "" || req.AgentID == "" {
		res.Outcome = OutcomeConfigBlocked
		res.Findings = append(res.Findings, Finding{
			Code: "request_incomplete", Message: "server, workspace and agent are required",
			Remediation: remediationAgents,
		})
		return res, nil
	}
	if !grant.Allows(catalog.ServerID(req.ServerID), req.WorkspaceID) {
		return Resolution{}, ErrUnauthorized
	}
	directory, err := s.cfg.Catalog.Directory(ctx, catalog.ServerID(req.ServerID), req.WorkspaceID, grant)
	if err != nil {
		res.Outcome = OutcomeUnknown
		res.Findings = append(res.Findings, Finding{
			Code: "source_unknown", Message: "the source directory could not be read",
			Remediation: remediationServers,
		})
		return res, nil
	}
	res.SourceGeneration = directory.Generation
	res.SourceSyncedAt = directory.SyncedAt
	if directory.SyncedAt == "" {
		res.Outcome = OutcomeUnknown
		res.Findings = append(res.Findings, Finding{
			Code: "source_unsynced", Message: "the source has no synced directory generation",
			Remediation: remediationServers,
		})
		return res, nil
	}
	if directory.Stale || directory.Sync != string(catalog.SyncOK) || directory.Connection != string(catalog.ConnectionOK) {
		res.Outcome = OutcomeUnknown
		res.Findings = append(res.Findings, Finding{
			Code: "source_stale", Message: "the source directory is stale or its last connection failed",
			Remediation: remediationServers,
		})
		return res, nil
	}
	res.sourceKnown = true
	agent, ok := findAgent(directory, req.AgentID)
	if !ok {
		res.Outcome = OutcomeUnknown
		res.Findings = append(res.Findings, Finding{
			Code: "agent_unknown", Message: fmt.Sprintf("agent %q is not in the current directory generation", req.AgentID),
			Remediation: remediationAgents,
		})
		return res, nil
	}
	res.agentKnown = true
	res.Management = string(agent.Management)
	res.RuntimeID = agent.RuntimeID
	if agent.Management == string(catalog.ManagedUnmanaged) {
		res.Outcome = OutcomeUnmanaged
		res.Findings = append(res.Findings, Finding{
			Code: "unmanaged_runtime", Message: "the agent's actual runtime is not operated by Foreman",
			Remediation: remediationAgents,
		})
		return res, nil
	}
	if agent.Management != string(catalog.ManagedForeman) || !agent.RuntimeBound || agent.RuntimeID == "" {
		res.Outcome = OutcomeConfigBlocked
		res.Findings = append(res.Findings, Finding{
			Code: "runtime_unbound", Message: "the agent is not bound to a Foreman runtime",
			Remediation: remediationAgents,
		})
		return res, nil
	}
	res.bindingKnown = true
	provider := runtimeProvider(directory, agent.RuntimeID)
	res.Provider = provider

	// The project identity is a fact about this Issue. An unknown identity is
	// not "no project": a rule that names a project is eliminated rather than
	// guessed, and the caller sees why.
	res.ProjectKnown = true
	if req.ProjectID == "" && req.IssueID != "" {
		if s.cfg.Issues == nil {
			res.ProjectKnown = false
			res.Findings = append(res.Findings, Finding{
				Code: "project_unknown", Message: "the issue project lookup is not wired",
				Remediation: remediationServers,
			})
		} else if projectID, err := s.cfg.Issues.ByIssue(ctx, req.WorkspaceID, req.IssueID); err != nil {
			res.ProjectKnown = false
			res.Findings = append(res.Findings, Finding{
				Code: "project_unknown", Message: "the issue's project identity could not be read",
				Remediation: remediationServers,
			})
		} else {
			res.ProjectID = projectID
		}
	}
	if res.ProjectID != "" && !hasProject(directory, res.ProjectID) {
		res.ProjectKnown = false
		res.Findings = append(res.Findings, Finding{
			Code: "project_unknown", Message: fmt.Sprintf("project %q is not in the current directory generation", res.ProjectID),
			Remediation: remediationRules,
		})
	}

	snap, err := s.cfg.Store.Load(ctx)
	if err != nil {
		return Resolution{}, err
	}
	res.RulesRevision = snap.Revision
	res.rulesKnown = true
	type match struct {
		rule   Rule
		reason string
	}
	matched := make([]Rule, 0, len(snap.Published))
	reasons := make(map[string]string, len(snap.Published))
	for _, rule := range snap.Published {
		switch {
		case !rule.Enabled:
			reasons[rule.ID] = "disabled"
		case rule.ProjectID != "" && !res.ProjectKnown:
			reasons[rule.ID] = "the project identity is unknown"
		case !rule.Matches(req.ServerID, req.WorkspaceID, req.AgentID, res.ProjectID):
			reasons[rule.ID] = "the rule's conditions do not match this issue"
		default:
			matched = append(matched, rule)
		}
	}
	if len(matched) == 0 {
		for _, rule := range snap.Published {
			res.RuleDecisions = append(res.RuleDecisions, RuleDecision{
				RuleID: rule.ID, Level: rule.Level().String(), Reason: reasons[rule.ID],
			})
		}
		res.Outcome = OutcomeConfigBlocked
		res.Findings = append(res.Findings, Finding{
			Code: "no_matching_rule", Message: "no published rule matches this server, workspace, agent and project",
			Remediation: remediationRules,
		})
		return res, nil
	}
	top := Level(-1)
	for _, rule := range matched {
		if rule.Level() > top {
			top = rule.Level()
		}
	}
	winners := make([]Rule, 0, len(matched))
	for _, rule := range matched {
		if rule.Level() == top {
			winners = append(winners, rule)
		}
	}
	winner := winners[0]
	conflict := false
	for _, rule := range winners[1:] {
		if !rule.effect().equal(winner.effect()) {
			conflict = true
		}
		if rule.ID < winner.ID {
			winner = rule
		}
	}
	if conflict {
		for _, rule := range snap.Published {
			decision := RuleDecision{RuleID: rule.ID, Level: rule.Level().String(), Reason: reasons[rule.ID]}
			if rule.Level() == top && reasons[rule.ID] == "" {
				decision.Matched = true
				decision.Reason = "same-level rule with a different effect"
			}
			res.RuleDecisions = append(res.RuleDecisions, decision)
		}
		res.Outcome = OutcomeConfigBlocked
		res.Findings = append(res.Findings, Finding{
			Code: "rule_conflict", Message: fmt.Sprintf("%d rules at the %s level select different environments", len(winners), top),
			Remediation: remediationRules,
		})
		return res, nil
	}
	for _, rule := range snap.Published {
		decision := RuleDecision{RuleID: rule.ID, Level: rule.Level().String(), Reason: reasons[rule.ID]}
		switch {
		case reasons[rule.ID] != "":
		case rule.ID == winner.ID:
			decision.Matched = true
			decision.Selected = true
			decision.Reason = "highest-priority match"
		case rule.Level() == top:
			decision.Matched = true
			decision.Reason = "same-level rule with an identical effect"
		default:
			decision.Matched = true
			decision.Reason = "a higher-priority rule wins"
		}
		res.RuleDecisions = append(res.RuleDecisions, decision)
	}
	res.RuleID = winner.ID
	res.RuleLevel = top.String()

	// An unknown project identity is not "no project": a project rule that
	// would have won cannot be ruled out, so neither preview nor dispatch may
	// present a placement as decided (04-architecture §3④).
	if !res.ProjectKnown {
		res.Outcome = OutcomeUnknown
		return res, nil
	}

	environments, err := s.cfg.Environments.List(ctx, environment.Filter{}, environment.Grant{Sources: []string{req.ServerID}})
	if err != nil {
		res.Outcome = OutcomeUnknown
		res.Findings = append(res.Findings, Finding{
			Code: "environment_registry_unreadable", Message: "the environment registry could not be read",
			Remediation: remediationEnvironments,
		})
		return res, nil
	}
	res.environmentKnown = true
	byID := make(map[string]environment.EnvironmentView, len(environments))
	for _, view := range environments {
		byID[view.ID] = view
	}
	pool := environments
	if winner.EnvironmentID != "" {
		view, ok := byID[winner.EnvironmentID]
		if !ok {
			res.Outcome = OutcomeConfigBlocked
			res.Findings = append(res.Findings, Finding{
				Code: "environment_missing", EnvironmentID: winner.EnvironmentID,
				Message:     fmt.Sprintf("the rule's environment %q is not registered or has been retired", winner.EnvironmentID),
				Remediation: remediationEnvironments,
			})
			return res, nil
		}
		pool = []environment.EnvironmentView{view}
	}
	continuation := ""
	if req.PriorSessionID != "" && s.cfg.Sessions != nil {
		if environmentID, ok := s.cfg.Sessions.EnvironmentForSession(ctx, req.WorkspaceID, req.PriorSessionID); ok {
			continuation = environmentID
		}
	}
	if continuation != "" {
		filtered := make([]environment.EnvironmentView, 0, 1)
		for _, view := range pool {
			if view.ID == continuation {
				filtered = append(filtered, view)
			}
		}
		if len(filtered) == 0 {
			res.Outcome = OutcomeConfigBlocked
			res.Findings = append(res.Findings, Finding{
				Code: "continuation_environment_mismatch", EnvironmentID: continuation,
				Message:     "the resuming session's environment is no longer eligible under the published rule",
				Remediation: remediationRules,
			})
			return res, nil
		}
		pool = filtered
	}
	type candidate struct {
		view environment.EnvironmentView
		hits int
	}
	best := make([]candidate, 0, len(pool))
	saturated := false
	eligible := func(view environment.EnvironmentView) (int, string, bool) {
		if !view.Schedulable {
			return 0, fmt.Sprintf("not schedulable (phase %s, verification %s)", view.Phase, view.Verification.State), false
		}
		if winner.TrustDomain != "" && view.TrustDomain != winner.TrustDomain {
			return 0, fmt.Sprintf("trust domain %q does not match the rule's %q", view.TrustDomain, winner.TrustDomain), false
		}
		if missing := missingCapabilities(view.Capabilities, winner.RequiredCapabilities); len(missing) > 0 {
			return 0, "missing capabilities " + strings.Join(missing, ", "), false
		}
		if view.FreeSlots <= 0 {
			return 0, "no free slot", true
		}
		hits := 0
		for _, capability := range winner.PreferCapabilities {
			if slices.Contains(view.Capabilities, capability) {
				hits++
			}
		}
		return hits, "", false
	}
	for _, view := range pool {
		hits, reason, full := eligible(view)
		decision := EnvironmentDecision{ID: view.ID, FreeSlots: view.FreeSlots}
		if reason != "" {
			decision.Reason = reason
			res.EnvironmentDecisions = append(res.EnvironmentDecisions, decision)
			if full {
				saturated = true
			}
			continue
		}
		best = append(best, candidate{view: view, hits: hits})
		res.EnvironmentDecisions = append(res.EnvironmentDecisions, decision)
	}
	if len(best) == 0 {
		if saturated {
			res.Outcome = OutcomeTemporarilySaturated
			res.Findings = append(res.Findings, Finding{
				Code: "environment_saturated", Message: "every eligible environment is at its concurrency limit",
				Remediation: remediationEnvironments,
			})
		} else {
			res.Outcome = OutcomeConfigBlocked
			res.Findings = append(res.Findings, Finding{
				Code: "no_environment", Message: "no registered environment matches the rule's trust domain and capabilities",
				Remediation: remediationEnvironments,
			})
		}
		return res, nil
	}
	sort.SliceStable(best, func(i, j int) bool {
		if best[i].hits != best[j].hits {
			return best[i].hits > best[j].hits
		}
		if best[i].view.FreeSlots != best[j].view.FreeSlots {
			return best[i].view.FreeSlots > best[j].view.FreeSlots
		}
		return best[i].view.ID < best[j].view.ID
	})
	chosen := best[0].view
	for i := range res.EnvironmentDecisions {
		if res.EnvironmentDecisions[i].ID == chosen.ID {
			res.EnvironmentDecisions[i].Selected = true
			res.EnvironmentDecisions[i].Reason = "selected"
		}
	}
	res.Environment = &EnvironmentChoice{
		ID: chosen.ID, Endpoint: chosen.Endpoint, TLSRef: chosen.TLSRef,
		StateRoot: chosen.StateRoot, Generation: chosen.Verification.TreeID,
		TrustDomain:  chosen.TrustDomain,
		Capabilities: append([]string{}, chosen.Capabilities...),
		Concurrency:  chosen.Concurrency, FreeSlots: chosen.FreeSlots,
	}

	blocked := s.resolveCLIAndConfig(ctx, &res, req, provider)

	if blocked {
		res.Outcome = OutcomeConfigBlocked
		return res, nil
	}
	res.Outcome = OutcomeReady
	if !req.Reserve {
		return res, nil
	}
	if req.AttemptID == "" {
		return Resolution{}, errors.New("rules: a reservation needs an attempt id")
	}
	// A Multica resume path is an affinity key, not an rsbx session. The
	// ledger starts idle; the Job binds the remote session it actually opened.
	slot, err := s.cfg.Environments.AcquireSlot(ctx, environment.ID(chosen.ID), req.AttemptID)
	if err != nil {
		switch {
		case errors.Is(err, environment.ErrFull):
			res.Outcome = OutcomeTemporarilySaturated
			res.Findings = append(res.Findings, Finding{
				Code: "environment_saturated", EnvironmentID: chosen.ID,
				Message:     "the selected environment filled between the read and the reservation",
				Remediation: remediationEnvironments,
			})
		case errors.Is(err, environment.ErrNotFound),
			errors.Is(err, environment.ErrNotVerified),
			errors.Is(err, environment.ErrDraining),
			errors.Is(err, environment.ErrTrustDomainDenied):
			res.Outcome = OutcomeConfigBlocked
			res.Findings = append(res.Findings, Finding{
				Code: "environment_unavailable", EnvironmentID: chosen.ID,
				Message:     "the selected environment is no longer available",
				Remediation: remediationEnvironments,
			})
		default:
			return Resolution{}, err
		}
		return res, nil
	}
	res.SlotID = slot.ID
	if s.cfg.Sessions != nil && req.PriorSessionID != "" {
		s.cfg.Sessions.RecordSession(ctx, req.WorkspaceID, req.PriorSessionID, chosen.ID)
	}
	return res, nil
}

// resolveCLIAndConfig reads the two published CLI lines and the selected Agent
// configuration. It is shared by preview, dispatch and the readiness check so
// all three answer from the same facts; the readiness wrapper decides whether
// an unverified Secret is a block or an unknown.
func (s *Service) resolveCLIAndConfig(ctx context.Context, res *Resolution, req Request, provider string) bool {
	blocked := false
	res.CLIs.Platform = s.cfg.Platform
	res.cliKnown = true
	if s.cfg.Releases == nil {
		res.Findings = append(res.Findings, Finding{
			Code: "cli_unknown", Message: "the CLI release registry is not wired",
			Remediation: remediationCLIs,
		})
	} else {
		version, digests, ok := s.cfg.Releases.CurrentRelease(ctx, req.ServerID, release.KindMultica, "")
		if !ok || digests[s.cfg.Platform] == "" {
			blocked = true
			res.Findings = append(res.Findings, Finding{
				Code: "multica_cli_missing", Field: "platform",
				Message:     fmt.Sprintf("the published Multica CLI has no artifact for %s", s.cfg.Platform),
				Remediation: remediationCLIs,
			})
		} else {
			res.CLIs.MulticaVersion = version
			res.CLIs.MulticaDigest = digests[s.cfg.Platform]
		}
		if provider == "" {
			blocked = true
			res.Findings = append(res.Findings, Finding{
				Code: "provider_unknown", Message: "the agent's runtime has no provider in the current directory",
				Remediation: remediationAgents,
			})
		} else {
			agentVersion, agentDigests, agentOK := s.cfg.Releases.CurrentRelease(ctx, req.ServerID, release.KindAgent, provider)
			if !agentOK || agentDigests[s.cfg.Platform] == "" {
				blocked = true
				res.Findings = append(res.Findings, Finding{
					Code: "agent_cli_missing", Field: "platform",
					Message:     fmt.Sprintf("the published Agent CLI for %q has no artifact for %s", provider, s.cfg.Platform),
					Remediation: remediationCLIs,
				})
			} else {
				res.CLIs.AgentVersion = agentVersion
				res.CLIs.AgentDigest = agentDigests[s.cfg.Platform]
			}
		}
	}

	if s.cfg.Configs != nil && provider != "" {
		scope := agentconfig.Scope{ServerID: req.ServerID, WorkspaceID: req.WorkspaceID, Backend: provider}
		revision, found, err := s.cfg.Configs.Resolve(ctx, scope)
		switch {
		case err != nil:
			res.Config.Readability = "unknown"
			res.Findings = append(res.Findings, Finding{
				Code: "config_unknown", Message: "the selected Agent configuration could not be read",
				Remediation: remediationConfigs,
			})
		case !found:
			res.Config.Known = true
			res.Config.Readability = "not_required"
		default:
			res.Config = ConfigFacts{
				Known: true, Revision: revision.Revision, SecretRef: revision.SecretRef,
				FileName: revision.FileName, Digest: revision.Digest, Readability: "unverified",
			}
			if revision.SecretRef == "" {
				blocked = true
				res.Findings = append(res.Findings, Finding{
					Code: "config_secret_missing", Message: "the selected configuration revision names no Secret",
					Remediation: remediationConfigs,
				})
			} else {
				res.Findings = append(res.Findings, Finding{
					Code:        "config_unverified",
					Message:     "the selected configuration Secret has no verified projection-identity readability fact",
					Remediation: remediationConfigs,
				})
			}
		}
	}
	return blocked
}

func runtimeProvider(directory catalog.DirectoryView, runtimeID string) string {
	for _, runtime := range directory.Runtimes {
		if runtime.RuntimeID == runtimeID {
			return runtime.Provider
		}
	}
	return ""
}
