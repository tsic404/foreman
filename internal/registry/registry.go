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
	mu       sync.RWMutex
	tasks    map[string]TaskEntry
	byDaemon map[string][]string  // daemonID → taskIDs, oldest first (a daemon may hold several)
	done     map[string]time.Time // taskID → verdict instant
	// doneOwner remembers which daemon settled each retained task id, so a
	// report arriving after the live row is gone can still be checked against
	// the daemon that owned it. Pruned on the same clock as done.
	doneOwner map[string]string
	// doneWorkspace records which workspace settled each retained task id, so
	// IsDone can answer for the workspace it was asked about.
	doneWorkspace   map[string]string
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
		byDaemon:        map[string][]string{},
		done:            map[string]time.Time{},
		doneOwner:       map[string]string{},
		doneWorkspace:   map[string]string{},
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
	// A fresh live generation must not inherit the previous one's verdict: the
	// retained index answers OwnerOf/DoneAt for re-dispatched ids otherwise.
	r.forgetVerdictLocked(e.TaskID)
	r.tasks[e.TaskID] = e
	r.indexLocked(e)
	return nil
}

// forgetVerdictLocked drops any retained verdict for a task id that is being
// admitted as a new live generation.
func (r *Registry) forgetVerdictLocked(taskID string) {
	delete(r.done, taskID)
	delete(r.doneOwner, taskID)
	delete(r.doneWorkspace, taskID)
}

// BindOrInsert stores e, inserting it when absent and applying the daemon
// binding when a live entry already exists from an earlier round that found no
// idle daemon. It reports whether the entry was inserted.
func (r *Registry) BindOrInsert(e TaskEntry) (TaskEntry, bool, error) {
	if e.TaskID == "" {
		return TaskEntry{}, false, errors.New("task id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old, ok := r.tasks[e.TaskID]
	if !ok {
		if e.State == StateTerminal {
			return TaskEntry{}, false, fmt.Errorf("%w: cannot insert a terminal entry", ErrTerminalImmutable)
		}
		r.forgetVerdictLocked(e.TaskID)
		r.tasks[e.TaskID] = e
		r.indexLocked(e)
		return e, true, nil
	}
	if old.IsTerminal() {
		return old, false, ErrTerminalImmutable
	}
	// Re-bind: the daemon choice and its runtime id are what the S-6 claim
	// addresses; every other field stays as the entry already had it.
	r.dropIndexLocked(old)
	old.DaemonID = e.DaemonID
	old.DaemonRuntimeID = e.DaemonRuntimeID
	old.RuntimeID = e.RuntimeID
	old.State = e.State
	r.tasks[e.TaskID] = old
	r.indexLocked(old)
	return old, false, nil
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

// OwnerOf reports which daemon held a task, live or settled. It answers "" for
// a task Foreman has never seen, or one already pruned from the settled index.
// Row expiry is the prune's job: it drops the owner and workspace rows together
// with the timestamp, so this lookup cannot disagree with IsDone.
func (r *Registry) OwnerOf(taskID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.tasks[taskID]; ok {
		return e.DaemonID
	}
	return r.doneOwner[taskID]
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
	e, ok := r.daemonTaskLocked(daemonID)
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

// ForgetVanished drops an entry whose task the server deleted (§4.6). The id
// still lands in the settled index, without a verdict: the deletion is
// remembered (a redelivery must not re-run the task), but no result is
// invented for a run nobody observed. Without the index entry, a server
// re-dispatch of the deleted id would pass IsDone and execute again.
func (r *Registry) ForgetVanished(taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tasks[taskID]
	if !ok {
		return ErrUnknownTask
	}
	r.dropIndexLocked(e)
	delete(r.tasks, taskID)
	r.noteDoneLocked(e)
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
	// The index is keyed by task id alone, so the workspace the caller asks
	// about has to be checked here: task ids are server-assigned UUIDs and
	// collision is not a real risk, but a caller passing a workspace must not
	// be told "done" for another workspace's settle.
	if ws, ok := r.doneWorkspace[taskID]; ok && workspaceID != "" && ws != "" && ws != workspaceID {
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
	e, ok := r.daemonTaskLocked(daemonID)
	if !ok {
		return TaskEntry{}, false
	}
	e.State = StateDaemonRegistered
	r.tasks[e.TaskID] = e
	return e, true
}

// MarkDelivering commits the delivery: the payload is handed to the daemon.
func (r *Registry) MarkDelivering(daemonID string, at time.Time, payload []byte) (TaskEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.daemonTaskLocked(daemonID)
	if !ok {
		return TaskEntry{}, false
	}
	if e.State == StatePending || e.State == StateDaemonRegistered {
		e.State = StateDelivering
	}
	e.AssignedAt = at
	e.Payload = append([]byte(nil), payload...)
	r.tasks[e.TaskID] = e
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

// PutRuntime inserts or replaces one daemon's row, preserving the learned
// session set across re-registrations (a daemon that reconnects still holds
// the session files it holds on disk).
func (r *Registry) PutRuntime(rt Runtime) {
	if rt.RuntimeID == "" || rt.DaemonID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.runtimes[rt.RuntimeID]; ok && rt.Sessions == nil {
		rt.Sessions = prev.Sessions
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

// RuntimesByDaemon returns every runtime row one daemon registered. A daemon
// declares its whole runtime set in one C-1, so teardown must reach all of
// them — answering one arbitrary row (map order) would leave the rest
// dispatchable until the heartbeat sweep caught up.
func (r *Registry) RuntimesByDaemon(daemonID string) []Runtime {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Runtime, 0, 4)
	for _, rt := range r.runtimes {
		if rt.DaemonID == daemonID {
			out = append(out, rt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuntimeID < out[j].RuntimeID })
	return out
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

// BusyFor counts the non-terminal tasks currently bound to the daemon with
// the given runtime id. The count is derived from the task rows on every
// call — never accumulated — so a daemon that reconnects mid-task, a lost
// release, or a restart cannot leak a stale count (the counter has no
// state to leak).
func (r *Registry) BusyFor(runtimeID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, e := range r.tasks {
		if e.IsTerminal() || e.DaemonRuntimeID != runtimeID {
			continue
		}
		n++
	}
	return n
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

// SetDaemonOffline marks every runtime one daemon registered offline, returning
// the ids it flipped. Deregistration (C-14) carries a daemon identity, not a
// runtime id: a daemon that declared several runtimes is gone as a whole, so
// taking only one offline would leave the others dispatchable until the
// heartbeat sweep caught up.
func (r *Registry) SetDaemonOffline(daemonID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var flipped []string
	for id, rt := range r.runtimes {
		if rt.DaemonID != daemonID || !rt.Online {
			continue
		}
		rt.Online = false
		r.runtimes[id] = rt
		flipped = append(flipped, id)
	}
	sort.Strings(flipped)
	return flipped
}

// ExpireStale marks offline every daemon whose last heartbeat is older than
// staleAfter, returning the ids it flipped. A resident daemon that stops
// heartbeating (crashed, partitioned, killed) never deregisters, so without
// this sweep its row stays "online" forever and the dispatcher keeps handing
// it tasks it can never claim. The heartbeat interval is 15s upstream, so a
// sweep window of a few intervals tells a slow tick from a dead process.
func (r *Registry) ExpireStale(now time.Time, staleAfter time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var expired []string
	for id, rt := range r.runtimes {
		if !rt.Online || rt.LastHeartbeatAt.IsZero() {
			continue
		}
		if now.Sub(rt.LastHeartbeatAt) <= staleAfter {
			continue
		}
		rt.Online = false
		r.runtimes[id] = rt
		expired = append(expired, id)
	}
	return expired
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

// daemonTaskLocked picks the task a daemon should be handed next: the oldest
// non-terminal one bound to it. A daemon runs several tasks concurrently
// (--max-concurrent-tasks), so the binding is a queue, not a single slot.
func (r *Registry) daemonTaskLocked(daemonID string) (TaskEntry, bool) {
	for _, id := range r.byDaemon[daemonID] {
		e, ok := r.tasks[id]
		if !ok || e.IsTerminal() || e.State == StateDelivering || e.State == StateRunning {
			continue
		}
		return e, true
	}
	return TaskEntry{}, false
}

func (r *Registry) indexLocked(e TaskEntry) {
	if e.DaemonID == "" || e.IsTerminal() {
		return
	}
	for _, id := range r.byDaemon[e.DaemonID] {
		if id == e.TaskID {
			return
		}
	}
	r.byDaemon[e.DaemonID] = append(r.byDaemon[e.DaemonID], e.TaskID)
}

func (r *Registry) dropIndexLocked(e TaskEntry) {
	if e.DaemonID != "" {
		if cur, ok := r.byDaemon[e.DaemonID]; ok {
			out := cur[:0]
			for _, id := range cur {
				if id != e.TaskID {
					out = append(out, id)
				}
			}
			if len(out) == 0 {
				delete(r.byDaemon, e.DaemonID)
			} else {
				r.byDaemon[e.DaemonID] = out
			}
		}
	}
}

func (r *Registry) noteDoneLocked(e TaskEntry) {
	at := e.CompletedAt
	if at.IsZero() {
		at = r.now()
	}
	r.done[e.TaskID] = at
	// Note the owner unconditionally: a "" owner means "settled with no daemon
	// binding", which is still an answer OwnerOf must not invent around.
	r.doneOwner[e.TaskID] = e.DaemonID
	if e.WorkspaceID != "" {
		r.doneWorkspace[e.TaskID] = e.WorkspaceID
	}
	r.pruneDoneLocked()
}

func (r *Registry) pruneDoneLocked() {
	now := r.now()
	for id, at := range r.done {
		if now.Sub(at) >= doneRetention {
			delete(r.done, id)
			delete(r.doneOwner, id)
			delete(r.doneWorkspace, id)
		}
	}
}
