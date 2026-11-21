package environment

import "time"

// VerificationView is the verification face of one environment.
type VerificationView struct {
	State          string   `json:"state"`
	TreeID         string   `json:"tree_id,omitempty"`
	ObservedTreeID string   `json:"observed_tree_id,omitempty"`
	Identity       string   `json:"identity,omitempty"`
	Capabilities   []string `json:"capabilities"`
	Capacity       int      `json:"capacity,omitempty"`
	Version        string   `json:"version,omitempty"`
	VerifiedAt     string   `json:"verified_at,omitempty"`
	LastAttemptAt  string   `json:"last_attempt_at,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// EnvironmentView is one registration as the management surface answers it.
type EnvironmentView struct {
	ID                 string           `json:"id"`
	Endpoint           string           `json:"endpoint"`
	TLSRef             string           `json:"tls_ref"`
	StateRoot          string           `json:"state_root"`
	TrustDomain        string           `json:"trust_domain"`
	AllowedSources     []string         `json:"allowed_sources"`
	Capabilities       []string         `json:"capabilities"`
	Concurrency        int              `json:"concurrency"`
	ExpectedIdentity   string           `json:"expected_identity,omitempty"`
	Phase              string           `json:"phase"`
	Registered         bool             `json:"registered"`
	Verified           bool             `json:"verified"`
	PolicyAllowed      bool             `json:"policy_allowed"`
	Schedulable        bool             `json:"schedulable"`
	Verification       VerificationView `json:"verification"`
	ActiveSlots        int              `json:"active_slots"`
	IsolatedSlots      int              `json:"isolated_slots"`
	UsedSlots          int              `json:"used_slots"`
	FreeSlots          int              `json:"free_slots"`
	PendingCleanups    int              `json:"pending_cleanups"`
	RetirementBlockers []string         `json:"retirement_blockers"`
	CreatedAt          string           `json:"created_at,omitempty"`
	UpdatedAt          string           `json:"updated_at,omitempty"`
	Revision           uint64           `json:"revision"`
}

// SlotView is one slot ledger row.
type SlotView struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	SessionID  string `json:"session_id,omitempty"`
	AcquiredAt string `json:"acquired_at,omitempty"`
	ReleasedAt string `json:"released_at,omitempty"`
	LastError  string `json:"last_error,omitempty"`
}

// CleanupView is one pending non-slot cleanup.
type CleanupView struct {
	ID        string `json:"id"`
	Ref       string `json:"ref,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

// TombstoneView is one retirement record.
type TombstoneView struct {
	ID             string   `json:"id"`
	Endpoint       string   `json:"endpoint"`
	StateRoot      string   `json:"state_root"`
	TreeID         string   `json:"tree_id,omitempty"`
	TrustDomain    string   `json:"trust_domain"`
	AllowedSources []string `json:"allowed_sources"`
	RetiredAt      string   `json:"retired_at,omitempty"`
	Actor          string   `json:"actor,omitempty"`
}

// RetirementView is the answer to a successful retirement.
type RetirementView struct {
	ID        string        `json:"id"`
	RetiredAt string        `json:"retired_at"`
	Tombstone TombstoneView `json:"tombstone"`
}

// AuditEntryView is one audit row projection.
type AuditEntryView struct {
	At            string `json:"at"`
	Actor         string `json:"actor"`
	Action        string `json:"action"`
	EnvironmentID string `json:"environment_id,omitempty"`
	Result        string `json:"result"`
	Detail        string `json:"detail,omitempty"`
}

// AuditView is the audit list answer.
type AuditView struct {
	Entries []AuditEntryView `json:"entries"`
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func verificationView(verification Verification) VerificationView {
	return VerificationView{
		State:          string(verification.State),
		TreeID:         verification.TreeID,
		ObservedTreeID: verification.ObservedTreeID,
		Identity:       verification.Identity,
		Capabilities:   append([]string{}, verification.Capabilities...),
		Capacity:       verification.Capacity,
		Version:        verification.Version,
		VerifiedAt:     stamp(verification.VerifiedAt),
		LastAttemptAt:  stamp(verification.LastAttemptAt),
		Error:          verification.Error,
	}
}

func environmentView(env Environment, blockers []string, policyAllowed bool) EnvironmentView {
	active := env.activeSlots()
	isolated := env.isolatedSlots()
	free := env.Target.Concurrency - env.usedSlots()
	if free < 0 {
		free = 0
	}
	schedulable := env.isSchedulable() && policyAllowed
	return EnvironmentView{
		ID:                 string(env.ID),
		Endpoint:           env.Target.Endpoint,
		TLSRef:             env.Target.TLSRef,
		StateRoot:          env.Target.StateRoot,
		TrustDomain:        env.Target.TrustDomain,
		AllowedSources:     append([]string{}, env.Target.AllowedSources...),
		Capabilities:       append([]string{}, env.Target.Capabilities...),
		Concurrency:        env.Target.Concurrency,
		ExpectedIdentity:   env.ExpectedIdentity,
		Phase:              string(env.Phase),
		Registered:         env.Registered,
		Verified:           env.Verification.State == VerificationOK,
		PolicyAllowed:      policyAllowed,
		Schedulable:        schedulable,
		Verification:       verificationView(env.Verification),
		ActiveSlots:        active,
		IsolatedSlots:      isolated,
		UsedSlots:          env.usedSlots(),
		FreeSlots:          free,
		PendingCleanups:    len(env.Cleanups),
		RetirementBlockers: append([]string{}, blockers...),
		CreatedAt:          stamp(env.CreatedAt),
		UpdatedAt:          stamp(env.UpdatedAt),
		Revision:           env.Revision,
	}
}

func slotView(slot Slot) SlotView {
	return SlotView{
		ID:         slot.ID,
		State:      string(slot.State),
		SessionID:  slot.SessionID,
		AcquiredAt: stamp(slot.AcquiredAt),
		ReleasedAt: stamp(slot.ReleasedAt),
		LastError:  slot.LastError,
	}
}

func cleanupView(cleanup Cleanup) CleanupView {
	return CleanupView{
		ID:        cleanup.ID,
		Ref:       cleanup.Ref,
		CreatedAt: stamp(cleanup.CreatedAt),
	}
}

func tombstoneView(tomb Tombstone) TombstoneView {
	return TombstoneView{
		ID:             string(tomb.ID),
		Endpoint:       tomb.Endpoint,
		StateRoot:      tomb.StateRoot,
		TreeID:         tomb.TreeID,
		TrustDomain:    tomb.TrustDomain,
		AllowedSources: append([]string{}, tomb.AllowedSources...),
		RetiredAt:      stamp(tomb.RetiredAt),
		Actor:          tomb.Actor,
	}
}

func retirementView(retirement Retirement) RetirementView {
	return RetirementView{
		ID:        string(retirement.ID),
		RetiredAt: stamp(retirement.RetiredAt),
		Tombstone: tombstoneView(retirement.Tombstone),
	}
}

func auditEntryView(entry AuditEntry) AuditEntryView {
	return AuditEntryView{
		At:            stamp(entry.At),
		Actor:         entry.Actor,
		Action:        entry.Action,
		EnvironmentID: entry.EnvironmentID,
		Result:        string(entry.Result),
		Detail:        entry.Detail,
	}
}
