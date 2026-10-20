package registry

import (
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"
)

// ProbeMode is the probe session's command face: whether the heartbeat ack may
// hand the probe daemon a model-catalog request at all (§1.3 ack, §1.6).
type ProbeMode int

const (
	// ProbeStartupDiscovery mints the catalog command id on Foreman's side and
	// absorbs its result into the startup slot store (ADR-025).
	ProbeStartupDiscovery ProbeMode = iota
	// ProbeCommandDriven carries a C2 ack's pending_model_list.id verbatim to
	// the probe Job's daemon (§1.3 待办只有一处消费面).
	ProbeCommandDriven
	// ProbeRegisterOnly is the §1.6 registration-discovery probe: registration
	// is the whole completion signal, so the session owns no command and its
	// heartbeat ack never carries pending_model_list.
	ProbeRegisterOnly
)

// ProbeSession is the authoritative in-memory state of one catalog-probe Job
// (ADR-019). The probe never enters the task mapping table, so this row is the
// only place its identity, command ownership and scope checks come from; the
// table itself is never rebuilt from K8s (the token hash cannot be recovered
// from the Job). Its fields are written by three producers — Start, the fake
// server's S27 path and the scheduler's pod observation — and each of them
// only touches its own face.
type ProbeSession struct {
	JobName       string
	Mode          ProbeMode
	RequestID     string // C2 ack pending_model_list.id verbatim; ProbeCommandDriven only
	SelfMintedID  string // startup-discovery command id; ProbeStartupDiscovery only
	TokenHash     string // sha256 hex of the probe Job Token
	JobRuntime    string // uuid5(NS_JOB_RUNTIME, JobName)
	RealRuntimeID string // real runtime id results are relayed to; ProbeCommandDriven only
	// WorkspaceID pins the probe to one workspace (catalog probes). Empty on
	// startup / registration-discovery probes: their registration scope spans
	// every configured workspace and their S-2 / S-3 answers serve any of them
	// (§1.2, §6.3).
	WorkspaceID string
	Deadline    time.Time

	// Result face (written by the fake server's S27 path / scheduler).
	RelayedAt   time.Time
	RelayedCode int
	NodeName    string

	// Registration face: written by the fake server's S4 probe branch for the
	// modes whose registration body is a declaration (CapturesRegistration).
	// Registrations is the per-workspace truth; the four fields below mirror
	// the first registration for the single-workspace readers.
	RegisteredAt   time.Time
	CLIVersion     string
	Runtimes       []DiscoveredRuntime
	FailedProfiles []json.RawMessage
	Registrations  map[string]ProbeRegistration
}

// ProbeRegistration is one workspace's S4 declaration captured by a
// registration probe. The daemon config decides which workspaces a probe
// registers to, so one probe may carry several of these (§1.2).
type ProbeRegistration struct {
	RegisteredAt   time.Time
	CLIVersion     string
	Runtimes       []DiscoveredRuntime
	FailedProfiles []json.RawMessage
}

// Registration returns one workspace's captured declaration, or false when
// the probe never registered to that workspace (the configured-but-unserved
// case of AC-87).
func (s ProbeSession) Registration(workspaceID string) (ProbeRegistration, bool) {
	registration, ok := s.Registrations[workspaceID]
	if !ok {
		return ProbeRegistration{}, false
	}
	registration.Runtimes = slices.Clone(registration.Runtimes)
	registration.FailedProfiles = cloneRawMessages(registration.FailedProfiles)
	return registration, true
}

// Discovery renders one workspace's captured registration as C1's input.
func (r ProbeRegistration) Discovery() Discovery {
	return Discovery{
		CLIVersion:     r.CLIVersion,
		Runtimes:       slices.Clone(r.Runtimes),
		FailedProfiles: cloneRawMessages(r.FailedProfiles),
	}
}

// CapturesRegistration reports whether the session's S4 body is the C1 truth
// source: the startup and registration-discovery probes declare the runtime
// set, while a command-driven probe's registration is not a declaration
// (§1.2 C-1 的取值来源与声明域).
func (s ProbeSession) CapturesRegistration() bool {
	return s.Mode != ProbeCommandDriven
}

// DiscoveredRuntime is one entry of the probe's S4 registration body `runtimes[]`.
// ProfileID is the custom runtime profile's identifier; the builtin entry
// carries the empty string (ADR-043 identity `(type, profile_id)`).
type DiscoveredRuntime struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	ProfileID string `json:"profile_id"`
}

// Discovery is the S4 registration body of the startup probe = the truth
// source of C-1 (§1.2「注册体的取值来源与声明域」).
type Discovery struct {
	CLIVersion     string
	Runtimes       []DiscoveredRuntime
	FailedProfiles []json.RawMessage
}

// clone returns a session sharing no backing array with s. Callers (auth's
// Verify, the proxy, the scheduler) hold a session across network waits, so a
// lookup hands out its own slices and the table can never be written through
// a caller's append or element assignment.
func (s ProbeSession) clone() ProbeSession {
	out := s
	out.Runtimes = slices.Clone(s.Runtimes)
	out.FailedProfiles = cloneRawMessages(s.FailedProfiles)
	if s.Registrations != nil {
		out.Registrations = make(map[string]ProbeRegistration, len(s.Registrations))
		for workspaceID, registration := range s.Registrations {
			registration.Runtimes = slices.Clone(registration.Runtimes)
			registration.FailedProfiles = cloneRawMessages(registration.FailedProfiles)
			out.Registrations[workspaceID] = registration
		}
	}
	return out
}

// cloneRawMessages deep-copies a raw-body list; each element aliases its own
// bytes because the bodies are kept verbatim for the C1 replay.
func cloneRawMessages(in []json.RawMessage) []json.RawMessage {
	if in == nil {
		return nil
	}
	out := make([]json.RawMessage, len(in))
	for i, m := range in {
		out[i] = slices.Clone(m)
	}
	return out
}

// ProbeRegistry is the probe session table, a sibling of Registry that neither
// reads nor writes it (ADR-019). One mutex guards the table and its runtime
// reverse index together, so a session's JobRuntime and its index row never
// disagree.
type ProbeRegistry struct {
	mu        sync.RWMutex
	probes    map[string]ProbeSession // job name → session
	byRuntime map[string]string       // job runtime id → job name
}

// NewProbeRegistry returns an empty probe session table.
func NewProbeRegistry() *ProbeRegistry {
	return &ProbeRegistry{
		probes:    make(map[string]ProbeSession),
		byRuntime: make(map[string]string),
	}
}

// PutProbe inserts or replaces the session keyed by JobName and maintains the
// runtime reverse index. A session without a job name is rejected: the name is
// the credential's scope key, so an unnamed row could never be matched by
// Verify. An empty JobRuntime is allowed (the session is simply not reachable
// by runtime) and replaces any index row the previous session held.
func (r *ProbeRegistry) PutProbe(s ProbeSession) error {
	if s.JobName == "" {
		return errors.New("job_name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.probes[s.JobName]; ok {
		r.dropRuntimeIndexLocked(old.JobRuntime, s.JobName)
	}
	e := s.clone()
	r.probes[e.JobName] = e
	if e.JobRuntime != "" {
		r.byRuntime[e.JobRuntime] = e.JobName
	}
	return nil
}

// ProbeByJob returns a copy of the session of jobName; it backs the probe
// branch of auth.Verify (the entry reads token hash, request ids and runtime).
func (r *ProbeRegistry) ProbeByJob(jobName string) (ProbeSession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.probes[jobName]
	if !ok {
		return ProbeSession{}, false
	}
	return s.clone(), true
}

// ProbeByRuntime returns a copy of the session owning jobRuntimeID; it backs
// the ownership check of S5 and S27. An unknown or empty runtime id misses.
func (r *ProbeRegistry) ProbeByRuntime(jobRuntimeID string) (ProbeSession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	jobName, ok := r.byRuntime[jobRuntimeID]
	if !ok {
		return ProbeSession{}, false
	}
	s, ok := r.probes[jobName]
	if !ok {
		return ProbeSession{}, false
	}
	return s.clone(), true
}

// DeleteProbe removes the session of jobName together with its reverse index
// row. It is the credential's invalidation face and therefore idempotent:
// deleting an absent session reports no error, because the caller's intent
// (the credential must be dead) already holds.
func (r *ProbeRegistry) DeleteProbe(jobName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.probes[jobName]; ok {
		r.dropRuntimeIndexLocked(s.JobRuntime, jobName)
		delete(r.probes, jobName)
	}
	return nil
}

// ListProbes returns a copy of every session, for the ops view and for tests.
func (r *ProbeRegistry) ListProbes() []ProbeSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ProbeSession, 0, len(r.probes))
	for _, s := range r.probes {
		out = append(out, s.clone())
	}
	return out
}

// UpdateProbe applies mutate to the session of jobName under the lock and
// writes it back, so a single-field write (RelayedAt from the fake server's
// S27 path, NodeName from the scheduler) can never clobber a concurrent one
// the way a whole-session PutProbe of a stale snapshot would. ok=false when
// the session is gone: the caller lost the race with DeleteProbe and must not
// resurrect it. JobName stays pinned to the lookup key (a rename is not an
// update), and the reverse index is repaired when the mutation moved
// JobRuntime.
func (r *ProbeRegistry) UpdateProbe(jobName string, mutate func(*ProbeSession)) (ProbeSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.probes[jobName]
	if !ok {
		return ProbeSession{}, false
	}
	oldRuntime := s.JobRuntime
	s = s.clone()
	mutate(&s)
	s.JobName = jobName
	if s.JobRuntime != oldRuntime {
		r.dropRuntimeIndexLocked(oldRuntime, jobName)
	}
	r.probes[jobName] = s
	if s.JobRuntime != "" {
		r.byRuntime[s.JobRuntime] = jobName
	}
	return s.clone(), true
}

// dropRuntimeIndexLocked removes the runtime index row of jobName; the lookup
// by value keeps a stale drop from evicting another session that has since
// claimed the same runtime id. Callers hold the lock.
func (r *ProbeRegistry) dropRuntimeIndexLocked(jobRuntimeID, jobName string) {
	if jobRuntimeID == "" {
		return
	}
	if r.byRuntime[jobRuntimeID] == jobName {
		delete(r.byRuntime, jobRuntimeID)
	}
}

// RuntimeIdentityKey is the identity key of a real runtime: provider plus
// runtime profile id, with the empty profile id naming the builtin entry
// (ADR-043; §0 real runtime).
func RuntimeIdentityKey(provider, profileID string) string {
	return provider + "\x00" + profileID
}

// StartupCatalog stages the startup discovery result by real runtime identity
// until the C1 response binds identities to real runtime ids (§1.2 C-21).
// The startup probe registers before its runtime exists — C1 creates it — so
// the body is parked under `(provider, profile_id)` and taken once the real
// runtime id is known; taking removes the entry, which is where 「已落槽」 is
// recorded.
type StartupCatalog struct {
	mu         sync.RWMutex
	byIdentity map[string][]byte
}

// NewStartupCatalog returns an empty catalog.
func NewStartupCatalog() *StartupCatalog {
	return &StartupCatalog{byIdentity: make(map[string][]byte)}
}

// Put stores body as the staged catalog of the identity, replacing an earlier
// body: a repeated registration describes the same identity now, and the
// replay must be verbatim. The bytes are copied, so the caller's buffer is
// never aliased into the catalog.
func (c *StartupCatalog) Put(provider, profileID string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byIdentity[RuntimeIdentityKey(provider, profileID)] = slices.Clone(body)
}

// Take removes and returns the staged body of the identity; ok=false when the
// identity has no body (the normal initial state, answered by starting a fresh
// probe).
func (c *StartupCatalog) Take(provider, profileID string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := RuntimeIdentityKey(provider, profileID)
	body, ok := c.byIdentity[key]
	if !ok {
		return nil, false
	}
	delete(c.byIdentity, key)
	return body, true
}

// Snapshot returns a copy of every staged body, keyed by RuntimeIdentityKey,
// for observability and tests; it leaves the catalog unchanged.
func (c *StartupCatalog) Snapshot() map[string][]byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string][]byte, len(c.byIdentity))
	for key, body := range c.byIdentity {
		out[key] = slices.Clone(body)
	}
	return out
}
