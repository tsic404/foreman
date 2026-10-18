package registry

import (
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"
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
	RequestID     string // C2 ack pending_model_list.id verbatim; "" in startup-discovery mode
	SelfMintedID  string // startup-discovery command id; "" in command-driven mode
	TokenHash     string // sha256 hex of the probe Job Token
	JobRuntime    string // uuid5(NS_JOB_RUNTIME, JobName)
	RealRuntimeID string // real runtime id results are relayed to; "" in startup-discovery mode
	Deadline      time.Time

	// Result face (written by the fake server's S27 path / scheduler).
	RelayedAt   time.Time
	RelayedCode int
	NodeName    string

	// Startup-discovery face: written by the fake server's S4 probe branch
	// only in startup-discovery mode (RealRuntimeID == "").
	RegisteredAt   time.Time
	CLIVersion     string
	Runtimes       []DiscoveredRuntime
	FailedProfiles []json.RawMessage
}

// DiscoveredRuntime is one entry of the probe's S4 registration body `runtimes[]`.
type DiscoveredRuntime struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Version string `json:"version"`
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

// StartupCatalog stages the startup discovery result by provider until the C1
// response binds providers to real runtime ids (§1.2 C-21).
// The startup probe registers before its runtime exists — C1 creates it — so
// the body is parked under the provider type and taken once the real runtime id
// is known; taking removes the entry, which is where 「已落槽」 is recorded.
type StartupCatalog struct {
	mu         sync.RWMutex
	byProvider map[string][]byte
}

// NewStartupCatalog returns an empty catalog.
func NewStartupCatalog() *StartupCatalog {
	return &StartupCatalog{byProvider: make(map[string][]byte)}
}

// Put stores body as the staged catalog of provider, replacing an earlier
// body: a repeated registration describes the same provider now, and the
// replay must be verbatim. The bytes are copied, so the caller's buffer is
// never aliased into the catalog.
func (c *StartupCatalog) Put(provider string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byProvider[provider] = slices.Clone(body)
}

// Take removes and returns the staged body of provider; ok=false when the
// provider has no body (its runtime's normal initial state, answered by
// starting a fresh probe).
func (c *StartupCatalog) Take(provider string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	body, ok := c.byProvider[provider]
	if !ok {
		return nil, false
	}
	delete(c.byProvider, provider)
	return body, true
}

// Snapshot returns a copy of every staged body, for observability and tests;
// it leaves the catalog unchanged.
func (c *StartupCatalog) Snapshot() map[string][]byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string][]byte, len(c.byProvider))
	for provider, body := range c.byProvider {
		out[provider] = slices.Clone(body)
	}
	return out
}
