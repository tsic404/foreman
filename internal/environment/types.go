package environment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ID is the operator-supplied stable environment identifier. It is never
// derived from an endpoint, a state root or a display name: a replacement
// physical tree at the same address gets a new ID (04-architecture.md §3.3).
type ID string

const (
	// DefaultProbeTimeout bounds one verification attempt.
	DefaultProbeTimeout = 30 * time.Second
	// MaxConcurrency bounds the configured slot limit.
	MaxConcurrency = 1024
	// AuditRetention bounds the in-memory audit ring.
	AuditRetention = 1024
)

// Phase is the environment's lifecycle face. A pending target has never
// verified; only an active, verified environment is a scheduling candidate.
type Phase string

const (
	PhasePending  Phase = "pending"
	PhaseActive   Phase = "active"
	PhaseDraining Phase = "draining"
)

// VerificationState is the last verification attempt's outcome. A stale
// environment was edited and must be verified again before it can take slots.
type VerificationState string

const (
	VerificationNever  VerificationState = "never"
	VerificationOK     VerificationState = "ok"
	VerificationFailed VerificationState = "failed"
	VerificationStale  VerificationState = "stale"
)

// Target is the operator's registration input for one remote rsbxd machine.
type Target struct {
	Endpoint       string
	TLSRef         string
	StateRoot      string
	TrustDomain    string
	AllowedSources []string
	Capabilities   []string
	Concurrency    int
}

// Verification is the facts one verification attempt observed. TreeID is the
// ownership fact and is frozen only after the first successful verification;
// ObservedTreeID carries what a failed attempt saw without claiming ownership.
type Verification struct {
	State          VerificationState
	TreeID         string
	ObservedTreeID string
	Identity       string
	Capabilities   []string
	Capacity       int
	Version        string
	VerifiedAt     time.Time
	LastAttemptAt  time.Time
	Error          string
}

// SlotState is one slot's ledger state. An isolated slot is a remote session
// whose close was not confirmed; it stays out of the free capacity and blocks
// retirement until an operator confirms the cleanup.
type SlotState string

const (
	SlotActive   SlotState = "active"
	SlotIsolated SlotState = "isolated"
)

// SlotPhase records how far the Job-side attempt got with a reservation. It
// lives in the shared ledger, so a rebuilt Pod reads Started/opening from it
// instead of relying on a container-local file.
type SlotPhase string

const (
	SlotPhaseIdle      SlotPhase = ""
	SlotPhaseOpening   SlotPhase = "opening"
	SlotPhaseConnected SlotPhase = "connected"
	SlotPhaseStarted   SlotPhase = "started"
)

// Slot is one capacity reservation inside an environment.
type Slot struct {
	ID      string
	State   SlotState
	Phase   SlotPhase
	Started bool
	// ClaimID is the execution lease that owns the current phase transition.
	// Begin mints it, recovery takes it over atomically, and only the holder
	// may bind a remote session, commit dispatch or release this reservation.
	ClaimID    string
	SessionID  string
	AcquiredAt time.Time
	ReleasedAt time.Time
	LastError  string
}

// Cleanup is one non-slot cleanup the maintenance path has not confirmed.
// Isolated slots cover session closes; this record covers other resources such
// as a worktree or a Nix root.
type Cleanup struct {
	ID        string
	Ref       string
	CreatedAt time.Time
}

// Environment is one registered target and its mutable lifecycle state.
type Environment struct {
	ID               ID
	Target           Target
	ExpectedIdentity string
	Phase            Phase
	// Registered records that this target once completed a trusted
	// verification. It survives later failed attempts and a drain; a target
	// that never verified stays false even after a lifecycle transition.
	Registered   bool
	Verification Verification
	Slots        map[string]Slot
	Cleanups     map[string]Cleanup
	Revision     uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// clone returns a deep copy, so a store reader cannot mutate live state.
func (e Environment) clone() Environment {
	out := e
	out.Target.AllowedSources = append([]string(nil), e.Target.AllowedSources...)
	out.Target.Capabilities = append([]string(nil), e.Target.Capabilities...)
	out.Verification.Capabilities = append([]string(nil), e.Verification.Capabilities...)
	out.Slots = make(map[string]Slot, len(e.Slots))
	for id, slot := range e.Slots {
		out.Slots[id] = slot
	}
	out.Cleanups = make(map[string]Cleanup, len(e.Cleanups))
	for id, cleanup := range e.Cleanups {
		out.Cleanups[id] = cleanup
	}
	return out
}

func (e Environment) activeSlots() int {
	count := 0
	for _, slot := range e.Slots {
		if slot.State == SlotActive {
			count++
		}
	}
	return count
}

func (e Environment) isolatedSlots() int {
	count := 0
	for _, slot := range e.Slots {
		if slot.State == SlotIsolated {
			count++
		}
	}
	return count
}

func (e Environment) usedSlots() int {
	return len(e.Slots)
}

func (e Environment) isSchedulable() bool {
	return e.Phase == PhaseActive && e.Verification.State == VerificationOK
}

func (e Environment) allowsSource(source string) bool {
	if len(e.Target.AllowedSources) == 0 {
		return true
	}
	for _, allowed := range e.Target.AllowedSources {
		if allowed == source {
			return true
		}
	}
	return false
}

// Tombstone is the durable record a retirement leaves behind. It keeps the
// stable ID unusable, but it is not a remote deletion.
type Tombstone struct {
	ID             ID
	Endpoint       string
	StateRoot      string
	TreeID         string
	TrustDomain    string
	AllowedSources []string
	RetiredAt      time.Time
	Actor          string
}

func (t Tombstone) clone() Tombstone {
	t.AllowedSources = append([]string(nil), t.AllowedSources...)
	return t
}

// AuditResult is one audit row's outcome.
type AuditResult string

const (
	AuditOK     AuditResult = "ok"
	AuditFailed AuditResult = "failed"
)

// Audit actions. They are stable event names, not user-facing prose.
const (
	AuditRegister        = "environment.register"
	AuditUpdate          = "environment.update"
	AuditVerify          = "environment.verify"
	AuditDrain           = "environment.drain"
	AuditResume          = "environment.resume"
	AuditRetire          = "environment.retire"
	AuditSupersede       = "environment.supersede"
	AuditSlotAcquire     = "environment.slot.acquire"
	AuditSlotAdvance     = "environment.slot.advance"
	AuditSlotRelease     = "environment.slot.release"
	AuditSlotIsolate     = "environment.slot.isolate"
	AuditCleanupConfirm  = "environment.cleanup.confirm"
	AuditCleanupPending  = "environment.cleanup.pending"
	AuditCleanupResolved = "environment.cleanup.resolved"
)

// AuditEntry is one environment operation. Detail never carries a credential
// or a secret value.
type AuditEntry struct {
	At            time.Time
	Actor         string
	Action        string
	EnvironmentID string
	Result        AuditResult
	Detail        string
}

// Registration is the create input.
type Registration struct {
	ID               string   `json:"id"`
	Endpoint         string   `json:"endpoint"`
	TLSRef           string   `json:"tls_ref"`
	StateRoot        string   `json:"state_root"`
	TrustDomain      string   `json:"trust_domain"`
	AllowedSources   []string `json:"allowed_sources"`
	Capabilities     []string `json:"capabilities"`
	Concurrency      int      `json:"concurrency"`
	ExpectedIdentity string   `json:"expected_identity"`
}

// Patch is the edit input. Nil fields are omitted; an empty list is a
// deliberate clear.
type Patch struct {
	Endpoint         *string   `json:"endpoint"`
	TLSRef           *string   `json:"tls_ref"`
	StateRoot        *string   `json:"state_root"`
	TrustDomain      *string   `json:"trust_domain"`
	AllowedSources   *[]string `json:"allowed_sources"`
	Capabilities     *[]string `json:"capabilities"`
	Concurrency      *int      `json:"concurrency"`
	ExpectedIdentity *string   `json:"expected_identity"`
}

// Filter narrows the management list.
type Filter struct {
	TrustDomain string
	Source      string
}

// CandidateFilter narrows the scheduling pool. It contains environment facts
// only: no node selector, node label or Pod affinity belongs here.
type CandidateFilter struct {
	TrustDomain  string
	Source       string
	Capabilities []string
}

// Grant is the caller's source scope, resolved server-side from the ops
// credential. DeploymentWide covers every environment.
type Grant struct {
	DeploymentWide bool
	Sources        []string
}

// AllowsSource reports whether the grant reaches one source id.
func (g Grant) AllowsSource(source string) bool {
	if g.DeploymentWide {
		return true
	}
	for _, allowed := range g.Sources {
		if allowed == source {
			return true
		}
	}
	return false
}

// AllowsEnvironment reports whether the grant reaches one environment. A
// source-unbound environment is visible only to a deployment-wide grant.
func (g Grant) AllowsEnvironment(env Environment) bool {
	if g.DeploymentWide {
		return true
	}
	if len(env.Target.AllowedSources) == 0 {
		// An unrestricted environment is open to a resolved source grant, but
		// a credential that resolved to no source at all still sees nothing.
		return len(g.Sources) > 0
	}
	for _, source := range env.Target.AllowedSources {
		if g.AllowsSource(source) {
			return true
		}
	}
	return false
}

// AllowsTombstone reports whether the grant reaches one retirement record.
func (g Grant) AllowsTombstone(tomb Tombstone) bool {
	if g.DeploymentWide {
		return true
	}
	if len(tomb.AllowedSources) == 0 {
		return len(g.Sources) > 0
	}
	for _, source := range tomb.AllowedSources {
		if g.AllowsSource(source) {
			return true
		}
	}
	return false
}

// Candidate is one environment the scheduler may select. FreeSlots already
// accounts for active and isolated slots.
type Candidate struct {
	ID           ID
	TrustDomain  string
	Capabilities []string
	Concurrency  int
	FreeSlots    int
}

// Retirement is the answer to a successful retirement. The tombstone is the
// durable registration record; no remote data is touched.
type Retirement struct {
	ID        ID
	RetiredAt time.Time
	Tombstone Tombstone
}

// RuleReferenceSource answers which published or drafted rules still name one
// environment. The rule manager owns the data; this package only asks before
// deleting a registration.
type RuleReferenceSource interface {
	ReferencingRules(ctx context.Context, environmentID ID) ([]string, error)
}

// MemoryRuleReferences is the in-process reference index the rule manager can
// fill. It is safe for concurrent use.
type MemoryRuleReferences struct {
	mu   sync.RWMutex
	refs map[ID]map[string]bool
}

// NewMemoryRuleReferences returns an empty index.
func NewMemoryRuleReferences() *MemoryRuleReferences {
	return &MemoryRuleReferences{refs: map[ID]map[string]bool{}}
}

// Add records that ruleID references environmentID.
func (m *MemoryRuleReferences) Add(environmentID ID, ruleID string) {
	if m == nil || ruleID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refs == nil {
		m.refs = map[ID]map[string]bool{}
	}
	if m.refs[environmentID] == nil {
		m.refs[environmentID] = map[string]bool{}
	}
	m.refs[environmentID][ruleID] = true
}

// Remove drops one reference.
func (m *MemoryRuleReferences) Remove(environmentID ID, ruleID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if rules := m.refs[environmentID]; rules != nil {
		delete(rules, ruleID)
		if len(rules) == 0 {
			delete(m.refs, environmentID)
		}
	}
}

// ReferencingRules answers the sorted rule ids.
func (m *MemoryRuleReferences) ReferencingRules(_ context.Context, environmentID ID) ([]string, error) {
	if m == nil {
		return nil, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	rules := make([]string, 0, len(m.refs[environmentID]))
	for ruleID := range m.refs[environmentID] {
		rules = append(rules, ruleID)
	}
	sortStrings(rules)
	return rules, nil
}

// Errors the lifecycle answers with. They are sentinels so the HTTP layer can
// map each class to its own status.
var (
	ErrNotFound          = errors.New("environment is not registered")
	ErrExists            = errors.New("environment id is already registered")
	ErrRetired           = errors.New("environment id is retired")
	ErrTreeExists        = errors.New("the target's persistent tree is already registered")
	ErrTreeChanged       = errors.New("the target reports a different persistent tree; register a new environment id")
	ErrNotVerified       = errors.New("environment is not verified")
	ErrTrustDomainDenied = errors.New("the target's trust domain is not allowed by this deployment")
	ErrDraining          = errors.New("environment is draining")
	ErrFull              = errors.New("environment has no free slot")
	ErrSlotExists        = errors.New("slot is already recorded")
	ErrSlotNotFound      = errors.New("slot is not recorded")
	ErrSlotActive        = errors.New("slot is still active")
	ErrSlotIsolated      = errors.New("slot is isolated")
	ErrSlotPhase         = errors.New("slot phase transition is not allowed")
	ErrSlotClaimed       = errors.New("slot is owned by another attempt execution")
	ErrSlotSessionBound  = errors.New("slot is already bound to another remote session")
	ErrSlotsPresent      = errors.New("environment has live or isolated slots")
	ErrCleanupExists     = errors.New("cleanup is already recorded")
	ErrCleanupNotFound   = errors.New("cleanup is not recorded")
	ErrConflict          = errors.New("environment changed during the operation; retry")
	ErrRetireBlocked     = errors.New("environment retirement is blocked")
)

// BlockedError carries every retirement blocker, not just the first one, so an
// operator sees the whole gate in one answer.
type BlockedError struct {
	Reasons []string
	Err     error
}

// Error renders the blockers.
func (e *BlockedError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Err, strings.Join(e.Reasons, "; "))
}

// Unwrap exposes the sentinel.
func (e *BlockedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ValidationError is one operator-input refusal. The HTTP layer answers it
// with 400 instead of the lifecycle's conflict class.
type ValidationError struct {
	Message string
}

// Error renders the refusal.
func (e *ValidationError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func invalid(format string, args ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, args...)}
}
