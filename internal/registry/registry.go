package registry

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// DoneIndexTTL is the retention of terminal task IDs (contract §3.1); within
// the window the index deduplicates late reports and repeated claims.
const DoneIndexTTL = 24 * time.Hour

// ErrTerminalImmutable rejects a write that would move a terminal entry back
// to a non-terminal state (task-mapping invariant 4: terminal is irreversible).
var ErrTerminalImmutable = errors.New("terminal entry cannot become non-terminal")

// Registry is the in-memory mapping index. The K8s Job objects are the
// durable truth (ADR-002); nothing here is persisted. Entries stay in the
// live index until Delete — including terminal entries whose Job/Secret
// cleanup is still pending — so Inflight counts only non-terminal rows.
type Registry struct {
	mu   sync.RWMutex
	now  func() time.Time
	live map[string]TaskEntry // task_id → entry (until Delete)

	byJob    map[string]string // job_name → task_id
	byDaemon map[string]string // daemon_id → task_id

	done *ttlMap // terminal task_id → completion time
	// issue_id → node_name history feeds jobbuilder's soft node-reuse
	// affinity; retention is by design (task-mapping §内部结构: the index
	// covers in-flight AND historical mappings). One string per issue, so
	// growth tracks distinct issues, not tasks.
	lastNode map[string]string

	runtimes        map[string]JobRuntime // job_runtime_id → runtime
	runtimeByDaemon map[string]string     // daemon_id → job_runtime_id
}

// New returns an empty Registry. now defaults to time.Now (tests inject a
// fixed clock).
func New(now func() time.Time) *Registry {
	if now == nil {
		now = time.Now
	}
	return &Registry{
		now:             now,
		live:            make(map[string]TaskEntry),
		byJob:           make(map[string]string),
		byDaemon:        make(map[string]string),
		done:            newTTLMap(DoneIndexTTL, now),
		lastNode:        make(map[string]string),
		runtimes:        make(map[string]JobRuntime),
		runtimeByDaemon: make(map[string]string),
	}
}

// Put inserts or replaces e, keyed by TaskID, and maintains the secondary
// indexes. A terminal entry can only be overwritten by a terminal one.
func (r *Registry) Put(e TaskEntry) error {
	if e.TaskID == "" {
		return errors.New("task_id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.live[e.TaskID]; ok {
		if old.State == StateTerminal && e.State != StateTerminal {
			return fmt.Errorf("%w: task %s", ErrTerminalImmutable, e.TaskID)
		}
		r.dropIndexesLocked(old)
	}
	r.live[e.TaskID] = e
	if e.JobName != "" {
		r.byJob[e.JobName] = e.TaskID
	}
	if e.DaemonID != "" {
		r.byDaemon[e.DaemonID] = e.TaskID
	}
	return nil
}

// Get returns the entry for taskID.
func (r *Registry) Get(taskID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.live[taskID]
	return e, ok
}

// ByJob returns the entry whose Job is jobName.
func (r *Registry) ByJob(jobName string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.get(r.byJob[jobName])
}

// ByDaemon returns the entry served by daemonID.
func (r *Registry) ByDaemon(daemonID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.get(r.byDaemon[daemonID])
}

func (r *Registry) get(taskID string) (TaskEntry, bool) {
	e, ok := r.live[taskID]
	return e, ok
}

// Delete removes taskID from the live index together with its job runtime
// rows (DeleteRuntime), so no index grows with process lifetime. The
// terminal record stays in the done index until its TTL expires.
func (r *Registry) Delete(taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.live[taskID]; ok {
		r.dropIndexesLocked(e)
		delete(r.live, taskID)
		r.dropRuntimeLocked(e.JobRuntimeID, e.DaemonID)
	}
	return nil
}

// SetLastStatusSeen records the latest C13 answer for a task in place,
// without touching any other field: callers holding a pre-network snapshot
// must never Put it back wholesale (that would clobber concurrent
// transitions such as MarkDelivering/MarkStarted).
func (r *Registry) SetLastStatusSeen(taskID, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.live[taskID]; ok {
		e.LastStatusSeen = status
		r.live[taskID] = e
	}
}

// List returns a snapshot of all live entries, including terminal ones
// whose K8s cleanup has not finished yet.
func (r *Registry) List() []TaskEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskEntry, 0, len(r.live))
	for _, e := range r.live {
		out = append(out, e)
	}
	return out
}

// Inflight counts non-terminal entries (contract §3.1).
func (r *Registry) Inflight() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, e := range r.live {
		if e.State != StateTerminal {
			n++
		}
	}
	return n
}

// IsDone reports whether taskID reached terminal state within the retention
// window; it backs the late-replay idempotent path and claim dedup.
func (r *Registry) IsDone(taskID string) bool {
	return r.done.has(taskID)
}

// MarkTerminal is the single write path into the terminal state
// (task-mapping pseudocode markTerminal): it sets state+result atomically
// with the done-index record. completedAt defaults to now.
func (r *Registry) MarkTerminal(taskID string, result Result, completedAt time.Time) (TaskEntry, error) {
	if completedAt.IsZero() {
		completedAt = r.now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok {
		return TaskEntry{}, fmt.Errorf("unknown task %s", taskID)
	}
	e.State = StateTerminal
	e.Result = result
	r.live[taskID] = e
	r.done.put(taskID, completedAt)
	return e, nil
}

// MarkDaemonRegistered advances job_created → daemon_registered on S4
// (idempotent: re-registration refreshes the timestamp without regressing
// delivering/running entries).
func (r *Registry) MarkDaemonRegistered(daemonID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.get(r.byDaemon[daemonID])
	if !ok || e.State == StateTerminal || e.State == StatePending {
		// pending has no Job yet, so no daemon can exist for it.
		return TaskEntry{}, false
	}
	if e.State == StateJobCreated || e.State == StateDaemonRegistered {
		e.State = StateDaemonRegistered
	}
	e.DaemonRegisteredAt = at
	r.live[e.TaskID] = e
	return e, true
}

// MarkDelivering atomically performs the S6 at-most-once delivery check:
// only a daemon_registered entry not yet delivered transitions. Without the
// single-lock check, two concurrent claims could both pass a delivered_at
// test before either writes.
func (r *Registry) MarkDelivering(daemonID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.get(r.byDaemon[daemonID])
	if !ok || e.State != StateDaemonRegistered || !e.DeliveredAt.IsZero() {
		return TaskEntry{}, false
	}
	e.State = StateDelivering
	e.DeliveredAt = at
	r.live[e.TaskID] = e
	return e, true
}

// MarkStarted records S8 under the registry lock so a concurrent terminal
// write always wins over a late start. started_at is set once; the state
// advances to running from any non-terminal state (a rebuilt job_created
// entry must accept its running daemon's reports).
func (r *Registry) MarkStarted(taskID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok || e.State == StateTerminal {
		return TaskEntry{}, false
	}
	if e.StartedAt.IsZero() {
		e.StartedAt = at
	}
	e.State = StateRunning
	r.live[taskID] = e
	return e, true
}

// SetNode records the node a task's pod landed on and feeds the soft
// node-reuse affinity (NodeIndex) for follow-up tasks of the same issue.
func (r *Registry) SetNode(taskID, nodeName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.live[taskID]
	if !ok {
		return
	}
	e.NodeName = nodeName
	r.live[taskID] = e
	if e.IssueID != "" {
		r.lastNode[e.IssueID] = nodeName
	}
}

// LastNodeForIssue implements jobbuilder.NodeIndex.
func (r *Registry) LastNodeForIssue(issueID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastNode[issueID]
}

func (r *Registry) dropIndexesLocked(e TaskEntry) {
	delete(r.byJob, e.JobName)
	delete(r.byDaemon, e.DaemonID)
}

// PutRuntime upserts the fake runtime of a Job's daemon (S4 registration,
// contract §3.3).
func (r *Registry) PutRuntime(rt JobRuntime) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runtimes[rt.JobRuntimeID] = rt
	if rt.DaemonID != "" {
		r.runtimeByDaemon[rt.DaemonID] = rt.JobRuntimeID
	}
}

// RuntimeByID returns the job runtime registered under jobRuntimeID.
func (r *Registry) RuntimeByID(jobRuntimeID string) (JobRuntime, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rt, ok := r.runtimes[jobRuntimeID]
	return rt, ok
}

// RuntimeByDaemon returns the job runtime of daemonID.
func (r *Registry) RuntimeByDaemon(daemonID string) (JobRuntime, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rt, ok := r.runtimes[r.runtimeByDaemon[daemonID]]
	return rt, ok
}

// TouchHeartbeat refreshes the liveness timestamp of a job runtime; S5
// heartbeats and WS heartbeat acks share this single clock (scenario #6).
func (r *Registry) TouchHeartbeat(jobRuntimeID string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[jobRuntimeID]
	if !ok {
		return
	}
	rt.LastHeartbeatAt = at
	rt.Online = true
	r.runtimes[jobRuntimeID] = rt
}

// DeleteRuntime removes a job runtime and its daemon reverse index.
func (r *Registry) DeleteRuntime(jobRuntimeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropRuntimeLocked(jobRuntimeID, "")
}

// dropRuntimeLocked removes the runtime row; daemonID may be empty (looked
// up from the row). Callers hold the lock.
func (r *Registry) dropRuntimeLocked(jobRuntimeID, daemonID string) {
	if jobRuntimeID == "" {
		return
	}
	rt, ok := r.runtimes[jobRuntimeID]
	if !ok {
		return
	}
	if daemonID == "" {
		daemonID = rt.DaemonID
	}
	delete(r.runtimes, jobRuntimeID)
	delete(r.runtimeByDaemon, daemonID)
}
