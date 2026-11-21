package catalog

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// ServerID is a source's stable identifier (04-architecture.md §3.1: 登记
// serverId). It is the only association key a source has: display names,
// workspace names and runtime names are never identity (§3.1 显示名不作关联键).
type ServerID string

// Bounds of the registration face. The values are configuration validation
// only; they do not describe an upstream capability.
const (
	// DefaultSyncPeriod is the automatic sync cadence a registration gets when
	// it does not name one.
	DefaultSyncPeriod = 5 * time.Minute
	// MinSyncPeriod is the shortest automatic cadence: a shorter one would
	// spend the source's traffic budget on directory reads.
	MinSyncPeriod = 10 * time.Second
	// MaxSyncPeriod is the longest automatic cadence.
	MaxSyncPeriod = 24 * time.Hour
	// DefaultRetryBudget is how many consecutive automatic syncs keep their
	// normal cadence before the source backs off to one attempt per period.
	DefaultRetryBudget = 3
	// MaxRetryBudget bounds the configured retry budget.
	MaxRetryBudget = 10
	// AuditRetention bounds the in-memory audit ring.
	AuditRetention = 1024
)

var (
	serverIDPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	credentialRefPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

// Connection is the source's connection face: whether the credential was
// verified against the upstream and stayed valid (04-architecture.md §2: 完成
// 握手且连接状态有效). "unverified" is a registered source that has not been
// proven yet; it is never counted as connected.
type Connection string

const (
	ConnectionUnverified   Connection = "unverified"
	ConnectionOK           Connection = "ok"
	ConnectionFailed       Connection = "failed"
	ConnectionUnauthorized Connection = "unauthorized"
)

// SyncPhase is the source's directory sync face.
type SyncPhase string

const (
	SyncNever  SyncPhase = "never"
	SyncOK     SyncPhase = "ok"
	SyncFailed SyncPhase = "failed"
)

// Source is one registered Multica server and its sync state. The credential
// is carried as a reference: the value never enters the catalog, a response or
// an audit row (§8: Secret 及上游宽权凭据不出现在日志/预览).
type Source struct {
	ID            ServerID
	URL           string
	CredentialRef string
	// AllowedWorkspaces narrows the source's access scope. Empty means every
	// workspace the credential can act for.
	AllowedWorkspaces []string
	AutoSync          bool
	SyncPeriod        time.Duration
	RetryBudget       int
	CreatedAt         time.Time
	UpdatedAt         time.Time

	// Connection and Sync are the faces the overview and the management page
	// read. They are projections of the last attempt, never intentions.
	Connection    Connection
	Sync          SyncPhase
	Generation    uint64
	LastAttemptAt time.Time
	LastSuccessAt time.Time
	NextSyncAt    time.Time
	// LastError is the sanitized failure of the last attempt; it never carries
	// a credential or a URL with userinfo.
	LastError    string
	FailureCount int
}

// Management classifies an agent by its actual upstream runtime binding. It is
// derived from the synced binding, never from an option list or a local
// intention (04-architecture.md §3.2).
type Management string

const (
	// ManagedForeman means the agent's actual runtime is one Foreman registered
	// in this source and workspace. Only this class counts as managed.
	ManagedForeman Management = "foreman"
	// ManagedUnmanaged means the agent is bound to a runtime Foreman does not
	// operate; no new Foreman dispatch may follow from it.
	ManagedUnmanaged Management = "unmanaged"
	// ManagedUnbound means the agent has no runtime at all.
	ManagedUnbound Management = "unbound"
)

// Workspace is one directory row. WorkspaceID is the upstream identifier; the
// pair (source, WorkspaceID) is the partition (§3.1 按 (serverId,
// workspaceId) 去重).
type Workspace struct {
	ID   string
	Name string
}

// Project is one workspace project. Title is the human-readable name and ID the
// opaque identity; the catalog never resolves one from the other.
type Project struct {
	WorkspaceID string
	ID          string
	Title       string
}

// Runtime is one workspace runtime row as the upstream answered it.
type Runtime struct {
	WorkspaceID string
	ID          string
	Name        string
	Provider    string
	Status      string
}

// Agent is one workspace agent row and its actual runtime binding.
type Agent struct {
	WorkspaceID  string
	ID           string
	Name         string
	RuntimeID    string
	RuntimeBound bool
	Management   Management
}

// TombstoneKind names the directory row a tombstone removed.
type TombstoneKind string

const (
	TombstoneWorkspace TombstoneKind = "workspace"
	TombstoneProject   TombstoneKind = "project"
	TombstoneAgent     TombstoneKind = "agent"
	TombstoneRuntime   TombstoneKind = "runtime"
)

// Tombstone records an object a complete sync confirmed deleted. It stops new
// bindings to the object but keeps the run audit (§3.1 完整同步确认删除的对象作
// 墓碑，停止新绑定但保留运行审计).
type Tombstone struct {
	Kind        TombstoneKind
	WorkspaceID string
	ID          string
	RemovedAt   time.Time
	Generation  uint64
}

// Snapshot is one complete generation of a source's directory. It is replaced
// atomically: readers see the previous generation until the new one is whole.
type Snapshot struct {
	ServerID   ServerID
	Generation uint64
	SyncedAt   time.Time
	Workspaces []Workspace
	Projects   []Project
	Agents     []Agent
	Runtimes   []Runtime
	Tombstones []Tombstone
}

// clone returns a deep copy, so a reader can never mutate stored state.
func (s Snapshot) clone() Snapshot {
	out := s
	out.Workspaces = append([]Workspace(nil), s.Workspaces...)
	out.Projects = append([]Project(nil), s.Projects...)
	out.Agents = append([]Agent(nil), s.Agents...)
	out.Runtimes = append([]Runtime(nil), s.Runtimes...)
	out.Tombstones = append([]Tombstone(nil), s.Tombstones...)
	return out
}

// WorkspaceCatalog is one workspace's directory answer from the upstream.
type WorkspaceCatalog struct {
	Projects []Project
	Agents   []Agent
	Runtimes []Runtime
}

// BindingResult is the only vocabulary a binding write may answer with. Only
// applied is success (02-contracts.md V2-C02, 04-architecture.md §3.2).
type BindingResult string

const (
	// BindingApplied means the upstream read-back showed the requested runtime.
	BindingApplied BindingResult = "applied"
	// BindingRejected means the upstream refused the write; nothing changed.
	BindingRejected BindingResult = "rejected"
	// BindingConflict means the upstream binding was not the expected old value
	// (a competing edit); the write was not sent.
	BindingConflict BindingResult = "conflict"
	// BindingNotApplied means the write returned but the read-back still shows
	// the previous runtime.
	BindingNotApplied BindingResult = "not_applied"
	// BindingUnknown means the result could not be established, not even by
	// read-back. It is never reported as success.
	BindingUnknown BindingResult = "unknown"
	// BindingUnsupported means this source cannot write bindings (revoked
	// credential or an unreadable directory); editing is disabled.
	BindingUnsupported BindingResult = "unsupported"
)

// BindingRequest is one binding write. ExpectedRuntimeID is the value the
// caller saw; a differing upstream value is a conflict and the write is not
// sent (04-architecture.md §3.2 按旧值/上游版本做比较).
type BindingRequest struct {
	ServerID          ServerID
	WorkspaceID       string
	AgentID           string
	ExpectedRuntimeID string
	RuntimeID         string
}

// BindingOutcome is the observable result of one binding write.
type BindingOutcome struct {
	Result             BindingResult
	ServerID           string
	WorkspaceID        string
	AgentID            string
	RequestedRuntimeID string
	// PreviousRuntimeID is the binding read before the write. It is the old
	// value the audit row names, distinct from ObservedRuntimeID when the
	// read-back returned a value nobody asked for.
	PreviousRuntimeID string
	ObservedRuntimeID string
	ObservedBound     bool
	Management        Management
	Detail            string
	UpstreamStatus    int
	ObservedAt        time.Time
}

// SyncReport is one sync attempt's observable result. A failed attempt names
// the previous generation it kept, so the caller can prove nothing was lost.
type SyncReport struct {
	ServerID       string    `json:"server_id"`
	Result         string    `json:"result"`
	Generation     uint64    `json:"generation"`
	KeptGeneration uint64    `json:"kept_generation,omitempty"`
	SyncedAt       time.Time `json:"synced_at,omitzero"`
	Workspaces     int       `json:"workspaces"`
	Projects       int       `json:"projects"`
	Agents         int       `json:"agents"`
	Runtimes       int       `json:"runtimes"`
	Tombstones     int       `json:"tombstones"`
	Detail         string    `json:"detail,omitempty"`
}

// AuditEntry records one administrative action. It carries identities and
// runtime ids only: never a credential, an auth header or a credential-bearing
// URL (04-architecture.md §3.6, §8).
type AuditEntry struct {
	At           time.Time `json:"at"`
	Actor        string    `json:"actor"`
	Action       string    `json:"action"`
	ServerID     string    `json:"server_id,omitempty"`
	WorkspaceID  string    `json:"workspace_id,omitempty"`
	AgentID      string    `json:"agent_id,omitempty"`
	OldRuntimeID string    `json:"old_runtime_id,omitempty"`
	NewRuntimeID string    `json:"new_runtime_id,omitempty"`
	Result       string    `json:"result"`
	Detail       string    `json:"detail,omitempty"`
}

// audit actions and results.
const (
	AuditRegisterServer = "register_server"
	AuditUpdateServer   = "update_server"
	AuditSyncSource     = "sync_source"
	AuditBindingWrite   = "binding_write"

	AuditOK         = "ok"
	AuditFailed     = "failed"
	AuditRejected   = "rejected"
	AuditConflict   = "conflict"
	AuditUnknown    = "unknown"
	AuditNotApplied = "not_applied"
)

// Registration is the POST /admin/servers body. Pointer fields distinguish
// "omitted" from an explicit value.
type Registration struct {
	ServerID          string   `json:"server_id"`
	URL               string   `json:"url"`
	CredentialRef     string   `json:"credential_ref"`
	AllowedWorkspaces []string `json:"allowed_workspaces"`
	AutoSync          *bool    `json:"auto_sync"`
	SyncPeriodSeconds *int     `json:"sync_period_s"`
	RetryBudget       *int     `json:"retry_budget"`
}

// Settings is the PATCH /admin/servers/{id} body.
type Settings struct {
	AutoSync          *bool `json:"auto_sync"`
	SyncPeriodSeconds *int  `json:"sync_period_s"`
	RetryBudget       *int  `json:"retry_budget"`
}

// RunAttempt is one in-flight attempt the overview renders. It comes from the
// coordinator's run facts, never from the directory: a later rename, runtime
// switch or drain must not rewrite the row (04-architecture.md §2 运行绑定使用
// 当时快照).
type RunAttempt struct {
	ServerID        string
	WorkspaceID     string
	AgentID         string
	AgentName       string
	IssueID         string
	IssueIdentifier string
	Environment     string
	Status          string
	StartedAt       time.Time
}

// RunIndex is the coordinator seam the overview reads attempts from. A nil one
// reports the run facts as unavailable instead of an empty, healthy-looking
// list.
type RunIndex interface {
	RunningAttempts() []RunAttempt
}

// WriteBack is the Agent management page's edit capability. Editing is disabled
// with a reason rather than faked when the source cannot be written
// (04-architecture.md §3.2 缺相应接口/写权时禁用编辑并显示原因).
type WriteBack struct {
	Enabled bool   `json:"enabled"`
	Reason  string `json:"reason,omitempty"`
}

// Validation helpers.

// ValidateRegistration checks the registration face. It refuses a URL carrying
// userinfo: a credential embedded there would reach logs and the catalog.
func ValidateRegistration(reg Registration) (Source, error) {
	id := ServerID(strings.TrimSpace(reg.ServerID))
	if !serverIDPattern.MatchString(string(id)) {
		return Source{}, errors.New("server_id must be 1-64 characters of letters, digits, '.', '_' or '-'")
	}
	parsed, err := url.Parse(strings.TrimSpace(reg.URL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return Source{}, errors.New("url must be an absolute http(s) URL")
	}
	if parsed.User != nil {
		return Source{}, errors.New("url must not carry credentials")
	}
	ref := strings.TrimSpace(reg.CredentialRef)
	if !credentialRefPattern.MatchString(ref) {
		return Source{}, errors.New("credential_ref must be a bare credential reference, not a secret")
	}
	autoSync := true
	if reg.AutoSync != nil {
		autoSync = *reg.AutoSync
	}
	period := DefaultSyncPeriod
	if reg.SyncPeriodSeconds != nil {
		period = time.Duration(*reg.SyncPeriodSeconds) * time.Second
	}
	if period < MinSyncPeriod || period > MaxSyncPeriod {
		return Source{}, fmt.Errorf("sync_period_s must be between %d and %d", int(MinSyncPeriod.Seconds()), int(MaxSyncPeriod.Seconds()))
	}
	retry := DefaultRetryBudget
	if reg.RetryBudget != nil {
		retry = *reg.RetryBudget
	}
	if retry < 1 || retry > MaxRetryBudget {
		return Source{}, fmt.Errorf("retry_budget must be between 1 and %d", MaxRetryBudget)
	}
	allowed := make([]string, 0, len(reg.AllowedWorkspaces))
	seen := map[string]bool{}
	for _, workspaceID := range reg.AllowedWorkspaces {
		workspaceID = strings.TrimSpace(workspaceID)
		if workspaceID == "" {
			return Source{}, errors.New("allowed_workspaces must not contain an empty id")
		}
		if seen[workspaceID] {
			continue
		}
		seen[workspaceID] = true
		allowed = append(allowed, workspaceID)
	}
	return Source{
		ID:                id,
		URL:               strings.TrimRight(parsed.String(), "/"),
		CredentialRef:     ref,
		AllowedWorkspaces: allowed,
		AutoSync:          autoSync,
		SyncPeriod:        period,
		RetryBudget:       retry,
	}, nil
}

// managementFor classifies one agent's binding against the Foreman runtime set
// of its workspace.
func managementFor(agent Agent, managed map[string]bool) Management {
	if agent.RuntimeID == "" || !agent.RuntimeBound {
		return ManagedUnbound
	}
	if managed[agent.RuntimeID] {
		return ManagedForeman
	}
	return ManagedUnmanaged
}
