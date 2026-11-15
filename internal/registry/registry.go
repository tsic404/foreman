package registry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Errors the state machine returns.
var (
	// ErrTerminalImmutable rejects any write that would move a terminal entry
	// back to a live state (§4.2: a verdict is final).
	ErrTerminalImmutable = errors.New("terminal entry is immutable")
	// ErrDuplicateTask rejects a Put of a task id already present.
	ErrDuplicateTask = errors.New("task already registered")
	// ErrUnknownTask reports a lookup or transition on a task Foreman does not
	// hold.
	ErrUnknownTask = errors.New("unknown task")
)

// doneRetention bounds the settled-task index: a task that reached terminal
// state is remembered this long so a server re-dispatch is recognised as a
// duplicate instead of running twice (§4.1, invariant 4).
const doneRetention = 24 * time.Hour

// Registry is the process-local task index plus the daemon table.
type Registry struct {
	mu              sync.RWMutex
	tasks           map[string]TaskEntry
	byDaemon        map[string]string    // daemonID → taskID (one task per daemon at a time)
	done            map[string]time.Time // taskID → verdict instant
	runtimes        map[string]Runtime
	byRuntimeDaemon map[string]string // runtimeID → daemonID

	now func() time.Time
}

// Option customizes a Registry.
type Option func(*Registry)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(r *Registry) { r.now = now }
}

// New returns an empty registry.
func New(opts ...Option) *Registry {
	r := &Registry{
		tasks:           map[string]TaskEntry{},
		byDaemon:        map[string]string{},
		done:            map[string]time.Time{},
		runtimes:        map[string]Runtime{},
		byRuntimeDaemon: map[string]string{},
		now:             time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Now returns the registry's clock. Every lifecycle stamp reads it, so the
// proxy, the dispatcher and the ops projection share one timeline.
func (r *Registry) Now() time.Time { return r.now() }

// ---- tasks ----

// Put inserts a new entry. A task id already present is ErrDuplicateTask; a
// terminal entry may never be re-inserted as live (ErrTerminalImmutable).
func (r *Registry) Put(e TaskEntry) error {
	if e.TaskID == "" {
		return errors.New("task id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.tasks[e.TaskID]; ok {
		if old.IsTerminal() && !e.IsTerminal() {
			return ErrTerminalImmutable
		}
		return ErrDuplicateTask
	}
	if e.State == StateTerminal {
		return fmt.Errorf("%w: cannot insert a terminal entry", ErrTerminalImmutable)
	}
	r.tasks[e.TaskID] = e
	r.indexLocked(e)
	return nil
}

// Update applies fn to the entry and stores the result. A terminal entry is
// never rewritten into a live state.
func (r *Registry) Update(taskID string, fn func(*TaskEntry)) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tasks[taskID]
	if !ok {
		return TaskEntry{}, false
	}
	prevState := e.State
	r.dropIndexLocked(e)
	fn(&e)
	if prevState == StateTerminal && e.State != StateTerminal {
		e.State = prevState
	}
	r.tasks[taskID] = e
	r.indexLocked(e)
	return e, true
}

// Get returns one entry.
func (r *Registry) Get(taskID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.tasks[taskID]
	return e, ok
}

// GetInWorkspace returns a live row, applying the workspace partition.
func (r *Registry) GetInWorkspace(workspaceID, taskID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.tasks[taskID]
	if !ok {
		return TaskEntry{}, false
	}
	if workspaceID != "" && e.WorkspaceID != workspaceID {
		return TaskEntry{}, false
	}
	return e, true
}

// SettledInWorkspace returns the retained terminal record of an
// already-reclaimed task. v2 keeps the verdict instant in the settled index;
// the full record is gone with the live row.
func (r *Registry) SettledInWorkspace(workspaceID, taskID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.tasks[taskID]; ok {
		if workspaceID != "" && e.WorkspaceID != workspaceID {
			return TaskEntry{}, false
		}
		return e, true
	}
	if at, done := r.done[taskID]; done {
		return TaskEntry{TaskID: taskID, WorkspaceID: workspaceID, State: StateTerminal, CompletedAt: at}, true
	}
	return TaskEntry{}, false
}

// ListInWorkspace lists a workspace's live rows ("" = every row).
func (r *Registry) ListInWorkspace(workspaceID string) []TaskEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskEntry, 0, len(r.tasks))
	for _, e := range r.tasks {
		if workspaceID != "" && e.WorkspaceID != workspaceID {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out
}

// ByDaemonInWorkspace returns the entry handed to daemonID in workspaceID.
func (r *Registry) ByDaemonInWorkspace(workspaceID, daemonID string) (TaskEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	taskID, ok := r.byDaemon[daemonID]
	if !ok {
		return TaskEntry{}, false
	}
	e, ok := r.tasks[taskID]
	if !ok || e.WorkspaceID != workspaceID {
		return TaskEntry{}, false
	}
	return e, true
}

// Delete drops a live entry (the terminal cleanup path); a terminal entry's
// id stays in the settled index so a re-dispatch is still caught.
func (r *Registry) Delete(taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tasks[taskID]
	if !ok {
		return ErrUnknownTask
	}
	r.dropIndexLocked(e)
	delete(r.tasks, taskID)
	if e.IsTerminal() {
		r.noteDoneLocked(e)
	}
	return nil
}

// List returns every live entry, ordered by claim instant.
func (r *Registry) List() []TaskEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskEntry, 0, len(r.tasks))
	for _, e := range r.tasks {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ClaimedAt.Equal(out[j].ClaimedAt) {
			return out[i].TaskID < out[j].TaskID
		}
		return out[i].ClaimedAt.Before(out[j].ClaimedAt)
	})
	return out
}

// Inflight counts live (non-terminal) entries.
func (r *Registry) Inflight() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, e := range r.tasks {
		if !e.IsTerminal() {
			n++
		}
	}
	return n
}

// InflightFor counts one workspace's live entries.
func (r *Registry) InflightFor(workspaceID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, e := range r.tasks {
		if !e.IsTerminal() && e.WorkspaceID == workspaceID {
			n++
		}
	}
	return n
}

// IsDone reports whether workspaceID's task reached terminal state inside the
// retention window.
func (r *Registry) IsDone(workspaceID, taskID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	at, ok := r.done[taskID]
	if !ok {
		return false
	}
	return r.now().Sub(at) < doneRetention
}

// DoneAt returns the verdict instant of a settled task.
func (r *Registry) DoneAt(taskID string) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	at, ok := r.done[taskID]
	return at, ok
}

// MarkDaemonRegistered records the S4 registration instant on the task the
// daemon is handling.
func (r *Registry) MarkDaemonRegistered(daemonID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	taskID, ok := r.byDaemon[daemonID]
	if !ok {
		return TaskEntry{}, false
	}
	e, ok := r.tasks[taskID]
	if !ok || e.IsTerminal() {
		return TaskEntry{}, false
	}
	e.State = StateDaemonRegistered
	r.tasks[taskID] = e
	return e, true
}

// MarkDelivering commits the delivery: the payload is handed to the daemon.
func (r *Registry) MarkDelivering(daemonID string, at time.Time, payload []byte) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	taskID, ok := r.byDaemon[daemonID]
	if !ok {
		return TaskEntry{}, false
	}
	e, ok := r.tasks[taskID]
	if !ok || e.IsTerminal() {
		return TaskEntry{}, false
	}
	if e.State == StatePending || e.State == StateDaemonRegistered {
		e.State = StateDelivering
	}
	e.AssignedAt = at
	e.Payload = append([]byte(nil), payload...)
	r.tasks[taskID] = e
	return e, true
}

// MarkStarted records the S8 start fact: the daemon accepted the task.
func (r *Registry) MarkStarted(taskID string, at time.Time) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tasks[taskID]
	if !ok || e.IsTerminal() {
		return TaskEntry{}, false
	}
	e.State = StateRunning
	if e.StartedAt.IsZero() {
		e.StartedAt = at
	}
	r.tasks[taskID] = e
	return e, true
}

// MarkTerminal writes the verdict. It is the single terminal write path: the
// entry becomes terminal, the settled index learns the instant, and the daemon
// slot is released. first reports whether this call settled it.
func (r *Registry) MarkTerminal(taskID string, result Result, reason string, at time.Time) (TaskEntry, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tasks[taskID]
	if !ok {
		return TaskEntry{}, false, ErrUnknownTask
	}
	if e.IsTerminal() {
		return e, false, nil
	}
	e.State = StateTerminal
	e.Result = result
	e.FailureReason = reason
	e.CompletedAt = at
	r.dropIndexLocked(e)
	r.tasks[taskID] = e
	r.noteDoneLocked(e)
	r.releaseRuntimeLocked(e)
	return e, true, nil
}

// SetLeaseKeepUntil projects the server's prepare-lease deadline (§10).
func (r *Registry) SetLeaseKeepUntil(taskID string, until time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tasks[taskID]
	if !ok || e.IsTerminal() {
		return
	}
	e.LeaseKeepUntil = until
	r.tasks[taskID] = e
}

// SetTokenHash records the delivered credential's digest (ADR-017).
func (r *Registry) SetTokenHash(taskID, hash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tasks[taskID]
	if !ok {
		return ErrUnknownTask
	}
	e.TokenHash = hash
	r.tasks[taskID] = e
	return nil
}

// ---- daemons ----

// PutRuntime inserts or replaces one daemon's row, keeping its live counters.
func (r *Registry) PutRuntime(rt Runtime) {
	if rt.RuntimeID == "" || rt.DaemonID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.runtimes[rt.RuntimeID]; ok {
		if rt.Busy == 0 {
			rt.Busy = prev.Busy
		}
		if rt.Sessions == nil {
			rt.Sessions = prev.Sessions
		}
	}
	if rt.Sessions == nil {
		rt.Sessions = map[string]bool{}
	}
	if rt.RegisteredAt.IsZero() {
		rt.RegisteredAt = r.now()
	}
	r.runtimes[rt.RuntimeID] = rt
	r.byRuntimeDaemon[rt.RuntimeID] = rt.DaemonID
}

// RuntimeByID returns a daemon row.
func (r *Registry) RuntimeByID(runtimeID string) (Runtime, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rt, ok := r.runtimes[runtimeID]
	return rt, ok
}

// RuntimeByDaemon returns the daemon row registered under daemonID.
func (r *Registry) RuntimeByDaemon(daemonID string) (Runtime, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, rt := range r.runtimes {
		if rt.DaemonID == daemonID {
			return rt, true
		}
	}
	return Runtime{}, false
}

// Runtimes returns every daemon row in registration order.
func (r *Registry) Runtimes() []Runtime {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Runtime, 0, len(r.runtimes))
	for _, rt := range r.runtimes {
		out = append(out, rt)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RegisteredAt.Equal(out[j].RegisteredAt) {
			return out[i].RuntimeID < out[j].RuntimeID
		}
		return out[i].RegisteredAt.Before(out[j].RegisteredAt)
	})
	return out
}

// RuntimesForWorkspace returns the daemon rows of one workspace, online first,
// in registration order. The C1 declaration is derived from this set.
func (r *Registry) RuntimesForWorkspace(workspaceID string) []Runtime {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Runtime, 0, len(r.runtimes))
	for _, rt := range r.runtimes {
		if rt.WorkspaceID != workspaceID {
			continue
		}
		out = append(out, rt)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Online != out[j].Online {
			return out[i].Online
		}
		if out[i].RegisteredAt.Equal(out[j].RegisteredAt) {
			return out[i].RuntimeID < out[j].RuntimeID
		}
		return out[i].RegisteredAt.Before(out[j].RegisteredAt)
	})
	return out
}

// TouchHeartbeat refreshes one daemon's liveness clock and marks it online.
func (r *Registry) TouchHeartbeat(runtimeID string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[runtimeID]
	if !ok {
		return
	}
	rt.LastHeartbeatAt = at
	rt.Online = true
	r.runtimes[runtimeID] = rt
}

// SetRuntimeOffline marks a daemon offline (S19 or a lapsed heartbeat).
func (r *Registry) SetRuntimeOffline(runtimeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[runtimeID]
	if !ok {
		return
	}
	rt.Online = false
	r.runtimes[runtimeID] = rt
}

// DeleteRuntime drops a daemon row entirely (deregistration).
func (r *Registry) DeleteRuntime(runtimeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.runtimes, runtimeID)
	delete(r.byRuntimeDaemon, runtimeID)
}

// LearnSession records that runtimeID now holds sessionID's files.
func (r *Registry) LearnSession(workspaceID, sessionID, runtimeID string) {
	if sessionID == "" || runtimeID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.runtimes[runtimeID]
	if !ok || rt.WorkspaceID != workspaceID {
		return
	}
	if rt.Sessions == nil {
		rt.Sessions = map[string]bool{}
	}
	rt.Sessions[sessionID] = true
	r.runtimes[runtimeID] = rt
}

// ---- internal ----

func (r *Registry) indexLocked(e TaskEntry) {
	if e.DaemonID != "" && !e.IsTerminal() {
		r.byDaemon[e.DaemonID] = e.TaskID
	}
}

func (r *Registry) dropIndexLocked(e TaskEntry) {
	if e.DaemonID != "" {
		if cur, ok := r.byDaemon[e.DaemonID]; ok && cur == e.TaskID {
			delete(r.byDaemon, e.DaemonID)
		}
	}
}

func (r *Registry) noteDoneLocked(e TaskEntry) {
	at := e.CompletedAt
	if at.IsZero() {
		at = r.now()
	}
	r.done[e.TaskID] = at
	r.pruneDoneLocked()
}

func (r *Registry) pruneDoneLocked() {
	now := r.now()
	for id, at := range r.done {
		if now.Sub(at) >= doneRetention {
			delete(r.done, id)
		}
	}
}

// releaseRuntimeLocked decrements the daemon's busy count when its task
// settles.
func (r *Registry) releaseRuntimeLocked(e TaskEntry) {
	// The daemon table is keyed by the daemon's own runtime id
	// (DaemonRuntimeID); RuntimeID is the server-assigned id and is not a key.
	key := e.DaemonRuntimeID
	if key == "" {
		key = e.RuntimeID
	}
	if key == "" {
		return
	}
	rt, ok := r.runtimes[key]
	if !ok {
		return
	}
	if rt.Busy > 0 {
		rt.Busy--
	}
	r.runtimes[key] = rt
}
