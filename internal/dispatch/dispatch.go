// Package dispatch is v2's scheduling core. It is what replaced v1's
// scheduler: instead of turning a claimed task into a Kubernetes Job, it picks
// one already-registered long-lived daemon and hands the task to it.
//
// The flow is: claim a task from the multica server (proxy's C-3), choose the
// daemon that should run it (session affinity first, then load, then
// registration order), record the mapping, and answer the daemon's S-6 claim
// with the payload. Terminal reports come back through the same daemon and are
// forwarded to the server unchanged.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/registry"
)

// Errors surfaced to the fake server's claim handler, which maps them to HTTP
// statuses (the daemon face's §2.1 rules).
var (
	// ErrUnknownDaemon rejects a claim from a daemon Foreman has no row for.
	ErrUnknownDaemon = errors.New("unknown daemon")
	// ErrRuntimeMismatch rejects a claim whose runtime id is not the daemon's.
	ErrRuntimeMismatch = errors.New("runtime does not belong to this daemon")
	// ErrNoDaemon reports a claim round with no eligible daemon: the task
	// stays undispatched and the server's lease expiry converges it.
	ErrNoDaemon = errors.New("no daemon available for this task")
	// ErrUnnameableClaim reports a claim payload with no readable id.
	ErrUnnameableClaim = errors.New("claim payload has no task id")
	// ErrTaskNotFound reports the server no longer knows the task (C13, §4.6).
	ErrTaskNotFound = errors.New("task not found on server")
)

// PrepareLeaseTTL is the server-side prepare lease (C-3): a claim stamps it and
// every C-4 renewal restamps it. An entry's LeaseKeepUntil is its last
// successful renewal plus this TTL (§10).
const PrepareLeaseTTL = 45 * time.Second

// Server is the dispatcher's view of the fake client (the proxy's F→S face).
type Server interface {
	// TaskStatus is C13: the server-side status of a task, or
	// ErrTaskNotFound.
	TaskStatus(ctx context.Context, taskID string) (string, error)
	// ForwardInWorkspace posts one lifecycle callback (C5–C12) and returns its
	// status and body.
	ForwardInWorkspace(ctx context.Context, ep Endpoint, taskID, workspaceID string, body []byte) (int, []byte, error)
	// PrepareLease is C4, renewing a claimed-but-unstarted task's lease on the
	// runtime it was dispatched under.
	PrepareLease(ctx context.Context, runtimeID, taskID string) (ready bool, err error)
}

// Endpoint names a lifecycle callback (§2.2). It lives here (not in the proxy)
// so the dispatcher and the fake server share one vocabulary without the
// dispatcher importing protocol details.
type Endpoint string

// Task lifecycle endpoints (§2.2, exhaustive).
const (
	EPStart        Endpoint = "start"
	EPProgress     Endpoint = "progress"
	EPMessages     Endpoint = "messages"
	EPUsage        Endpoint = "usage"
	EPComplete     Endpoint = "complete"
	EPFail         Endpoint = "fail"
	EPCancelAck    Endpoint = "cancel-ack"
	EPSession      Endpoint = "session"
	EPWaitLocalDir Endpoint = "wait-local-directory"
)

// IsTerminal reports whether ep settles the task on the server.
func (ep Endpoint) IsTerminal() bool {
	return ep == EPComplete || ep == EPFail || ep == EPCancelAck
}

// TerminalResult maps a terminal endpoint to its result classification.
func (ep Endpoint) TerminalResult() registry.Result {
	switch ep {
	case EPComplete:
		return registry.ResultCompleted
	case EPFail:
		return registry.ResultFailed
	case EPCancelAck:
		return registry.ResultCancelled
	}
	return ""
}

// Revoker feeds a revoked credential digest to the token authority (satisfied
// by *auth.Issuer). The reclaim path calls it before the entry goes, so a
// settled task's credential cannot be replayed.
type Revoker interface {
	Revoke(tokenHash, workspaceID string)
}

// Metrics is the dispatcher's observability seam.
type Metrics interface {
	TaskClaimed()
	TaskTerminal(result string)
	InflightJobs(n int)
	DuplicateDispatch()
	CredReclaimed(reason string, drainSeconds float64)
	ClaimError(reason string)
	TokenUsage(provider, model string, input, output, cacheRead, cacheWrite int64)
}

// Config is the dispatcher's runtime configuration.
type Config struct {
	// BootTimeout bounds claim → daemon start. Zero means no bound.
	BootTimeout time.Duration
	// TaskMaxDuration bounds one task once started (the token lifetime must
	// cover it, checked at startup). Zero means no bound.
	TaskMaxDuration time.Duration
	// ReconcileInterval is the convergence round's cadence.
	ReconcileInterval time.Duration
}

// Delivery is the outcome of one S-6 delivery attempt (§5).
type Delivery struct {
	Tasks     []json.RawMessage
	Delivered bool
	// Latency is the claim → dispatch-commit span, measured on the registry's
	// clock so both ends share one timeline.
	Latency time.Duration
}

// Dispatcher owns the task → daemon mapping.
type Dispatcher struct {
	cfg     Config
	reg     *registry.Registry
	server  Server
	revoker Revoker
	metrics Metrics
	log     *slog.Logger
	now     func() time.Time

	// taskMu serializes the per-task transitions (a claim against a
	// concurrent settlement), keyed per task id.
	taskMu *keyedMutex

	// mu guards the last-claim bookkeeping that is not per-task.
	mu sync.Mutex
}

// Option customizes a Dispatcher.
type Option func(*Dispatcher)

// WithLogger overrides the logger.
func WithLogger(l *slog.Logger) Option {
	return func(d *Dispatcher) { d.log = l.With("component", "dispatcher") }
}

// WithRevoker wires the credential revocation face.
func WithRevoker(r Revoker) Option {
	return func(d *Dispatcher) { d.revoker = r }
}

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(d *Dispatcher) { d.now = now }
}

// New builds a dispatcher.
func New(cfg Config, reg *registry.Registry, server Server, metrics Metrics, opts ...Option) *Dispatcher {
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 30 * time.Second
	}
	d := &Dispatcher{
		cfg:     cfg,
		reg:     reg,
		server:  server,
		metrics: orNoopMetrics(metrics),
		log:     slog.Default().With("component", "dispatcher"),
		now:     reg.Now,
		taskMu:  newKeyedMutex(),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// claimHead is the tolerant read of a claim payload: only the identity and the
// affinity/scheduling inputs.
type claimHead struct {
	ID             string `json:"id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	IssueID        string `json:"issue_id"`
	IssueIdentifier string `json:"issue_identifier"`
	AutopilotRunID  string `json:"autopilot_run_id"`
	ChatSessionID   string `json:"chat_session_id"`
	RuntimeID       string `json:"runtime_id"`
	PriorSessionID  string `json:"prior_session_id"`
	Agent           struct {
		Name string `json:"name"`
	} `json:"agent"`
}

// OnClaim maps one claimed task onto a daemon. The task is indexed in pending
// first, then the best online daemon is chosen and recorded; the daemon's own
// S-6 claim picks the payload up. A round with no eligible daemon leaves the
// entry in pending and returns ErrNoDaemon: the server re-dispatches after the
// prepare lease lapses.
func (d *Dispatcher) OnClaim(ctx context.Context, workspaceID string, task json.RawMessage) error {
	var head claimHead
	if err := json.Unmarshal(task, &head); err != nil || head.ID == "" {
		id := claimID(task)
		if id == "" {
			d.metrics.ClaimError("unnameable")
			return fmt.Errorf("%w: %v", ErrUnnameableClaim, err)
		}
		head.ID = id
	}
	if workspaceID != "" {
		if head.WorkspaceID != "" && head.WorkspaceID != workspaceID {
			d.metrics.ClaimError("workspace_mismatch")
			return fmt.Errorf("claim workspace %q does not match payload %q", workspaceID, head.WorkspaceID)
		}
		head.WorkspaceID = workspaceID
	}

	unlock := d.taskMu.Lock(head.ID)
	defer unlock()

	if _, ok := d.reg.Get(head.ID); ok {
		d.metrics.DuplicateDispatch()
		d.log.Warn("duplicate_claim", "task_id", head.ID, "action", "keep_existing")
		return nil
	}
	if d.reg.IsDone(head.WorkspaceID, head.ID) {
		d.metrics.DuplicateDispatch()
		d.log.Warn("duplicate_dispatch", "task_id", head.ID, "action", "drop_terminal_done")
		return nil
	}

	now := d.now()
	e := registry.TaskEntry{
		TaskID:          head.ID,
		AgentID:         head.AgentID,
		IssueID:         head.IssueID,
		IssueIdentifier: head.IssueIdentifier,
		AutopilotRunID:  head.AutopilotRunID,
		ChatSessionID:   head.ChatSessionID,
		WorkspaceID:     head.WorkspaceID,
		State:           registry.StatePending,
		Attempt:         1,
		ClaimedAt:       now,
		LeaseKeepUntil:  now.Add(PrepareLeaseTTL),
		Payload:         append(json.RawMessage(nil), task...),
		PriorSessionID:  head.PriorSessionID,
	}
	// Bind the task to a daemon before it is indexed: the S-6 claim addresses
	// the entry by (workspace, daemon id), so the mapping must be part of the
	// first write.
	rt, ok := d.pickDaemon(head)
	if !ok {
		if err := d.reg.Put(e); err != nil && !errors.Is(err, registry.ErrDuplicateTask) {
			return err
		}
		d.metrics.ClaimError("no_daemon")
		return ErrNoDaemon
	}
	e.DaemonID = rt.DaemonID
	e.RuntimeID = rt.RuntimeID
	e.State = registry.StateDaemonRegistered
	if err := d.reg.Put(e); err != nil {
		return fmt.Errorf("register task %s: %w", head.ID, err)
	}
	d.reg.PutRuntime(bumpBusy(rt))
	d.log.Info("task.claimed",
		"task_id", e.TaskID, "workspace_id", e.WorkspaceID,
		"daemon_id", e.DaemonID, "runtime_id", e.RuntimeID,
		"issue_identifier", e.IssueIdentifier, "agent_id", e.AgentID)
	return nil
}

// pickDaemon chooses the daemon for a claim: session affinity (the daemon that
// already holds prior_session_id) first, then the least busy online daemon of
// the workspace, ties broken by registration order.
func (d *Dispatcher) pickDaemon(head claimHead) (registry.Runtime, bool) {
	candidates := make([]registry.Runtime, 0, 8)
	for _, rt := range d.reg.Runtimes() {
		if !rt.Online {
			continue
		}
		if head.WorkspaceID != "" && rt.WorkspaceID != head.WorkspaceID {
			continue
		}
		candidates = append(candidates, rt)
	}
	if len(candidates) == 0 {
		return registry.Runtime{}, false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if ah, bh := a.HoldsSession(head.PriorSessionID), b.HoldsSession(head.PriorSessionID); ah != bh {
			return ah
		}
		if a.Busy != b.Busy {
			return a.Busy < b.Busy
		}
		return a.RegisteredAt.Before(b.RegisteredAt)
	})
	return candidates[0], true
}

// IdleDaemons counts the online, not-busy daemons of this dispatcher's
// workspace: the number of tasks it can dispatch right now. The claim loop's
// capacity reads it, so Foreman never claims more tasks than it can hand to a
// daemon.
func (d *Dispatcher) IdleDaemons() int {
	n := 0
	for _, rt := range d.reg.Runtimes() {
		if rt.Online && rt.Busy == 0 {
			n++
		}
	}
	return n
}

// Deliver answers a daemon's S-6 claim with its pending task (§5). The daemon
// is identified by its token's daemon id; the runtime id it names must be its
// own.
func (d *Dispatcher) Deliver(ctx context.Context, workspaceID, daemonID, runtimeID string, maxTasks int) (Delivery, error) {
	if maxTasks < 1 {
		return Delivery{}, nil
	}
	e, ok := d.reg.ByDaemonInWorkspace(workspaceID, daemonID)
	if !ok {
		return Delivery{}, fmt.Errorf("%w: %s", ErrUnknownDaemon, daemonID)
	}
	if runtimeID != "" && e.RuntimeID != "" && runtimeID != e.RuntimeID {
		return Delivery{}, fmt.Errorf("%w: %s", ErrRuntimeMismatch, runtimeID)
	}
	unlock := d.taskMu.Lock(e.TaskID)
	defer unlock()

	e, ok = d.reg.Get(e.TaskID)
	if !ok || e.IsTerminal() || e.State == registry.StateRunning {
		return Delivery{}, nil
	}
	if len(e.Payload) == 0 {
		d.log.Warn("task.payload_missing", "task_id", e.TaskID, "daemon_id", e.DaemonID)
		return Delivery{}, nil
	}
	out, err := rewriteRuntime(e.Payload, e.RuntimeID)
	if err != nil {
		return Delivery{}, fmt.Errorf("rewrite runtime for task %s: %w", e.TaskID, err)
	}
	now := d.now()
	marked, ok := d.reg.MarkDelivering(daemonID, now, out)
	if !ok {
		return Delivery{}, nil
	}
	d.log.Info("task.delivered",
		"task_id", marked.TaskID, "daemon_id", marked.DaemonID, "runtime_id", marked.RuntimeID,
		"deliver_latency_ms", now.Sub(marked.ClaimedAt).Milliseconds())
	return Delivery{
		Tasks:     []json.RawMessage{out},
		Delivered: true,
		Latency:   now.Sub(marked.ClaimedAt),
	}, nil
}

// OnReport forwards one lifecycle callback (S8, S10–S17) to the server and
// advances the local entry. The returned code/body are the server's answer,
// handed back to the daemon unchanged.
func (d *Dispatcher) OnReport(ctx context.Context, ep Endpoint, e registry.TaskEntry, body []byte, sessionID string) (int, []byte, error) {
	unlock := d.taskMu.Lock(e.TaskID)
	defer unlock()

	// The S-8 body names the runtime the daemon was handed; the server knows
	// the runtime its claim was dispatched under, so it is translated back.
	out := body
	if ep == EPStart && e.RuntimeID != "" {
		if translated, err := rewriteRuntime(body, e.RuntimeID); err == nil {
			out = translated
		}
	}
	code, resp, err := d.server.ForwardInWorkspace(ctx, ep, e.TaskID, e.WorkspaceID, out)
	if err != nil {
		return 0, nil, err
	}
	now := d.now()
	switch {
	case ep == EPStart && code >= 200 && code < 300:
		d.reg.MarkStarted(e.TaskID, now)
	case ep.IsTerminal() && code >= 200 && code < 300:
		result := ep.TerminalResult()
		updated, first, terr := d.reg.MarkTerminal(e.TaskID, result, "", now)
		if terr != nil {
			return code, resp, nil
		}
		if first {
			d.metrics.TaskTerminal(string(result))
			d.metrics.InflightJobs(d.reg.Inflight())
			if sessionID != "" && updated.RuntimeID != "" {
				d.reg.LearnSession(updated.WorkspaceID, sessionID, updated.RuntimeID)
			}
			d.reclaim(updated)
		}
	}
	return code, resp, nil
}

// reclaim revokes the settled task's credential and drops the entry, releasing
// the daemon slot. The settled index keeps the id, so a re-dispatch is still
// caught as a duplicate.
func (d *Dispatcher) reclaim(e registry.TaskEntry) {
	if e.TokenHash != "" && d.revoker != nil {
		d.revoker.Revoke(e.TokenHash, e.WorkspaceID)
	}
	drain := d.now().Sub(e.CompletedAt)
	d.metrics.CredReclaimed("task_terminated", drain.Seconds())
	if err := d.reg.Delete(e.TaskID); err != nil {
		d.log.Warn("task.reclaim_failed", "task_id", e.TaskID, "err", err)
	}
}

// OnTaskVanished drops an entry whose task the server deleted (§4.6).
func (d *Dispatcher) OnTaskVanished(ctx context.Context, taskID string) error {
	unlock := d.taskMu.Lock(taskID)
	defer unlock()
	e, ok := d.reg.Get(taskID)
	if !ok {
		return nil
	}
	d.reclaim(e)
	return nil
}

// OnBootTimeout fails a claimed task that never started before its boot bound.
func (d *Dispatcher) OnBootTimeout(ctx context.Context, taskID string) error {
	return d.failLocally(ctx, taskID, "job_boot_timeout")
}

// failLocally writes a synthetic failed verdict for a task Foreman itself
// judged (a boot bound or a dead daemon), then forwards the failure to the
// server so the two sides agree.
func (d *Dispatcher) failLocally(ctx context.Context, taskID, reason string) error {
	unlock := d.taskMu.Lock(taskID)
	defer unlock()
	e, ok := d.reg.Get(taskID)
	if !ok || e.IsTerminal() {
		return nil
	}
	if d.cfg.BootTimeout > 0 && !e.StartedAt.IsZero() {
		return nil
	}
	body, _ := json.Marshal(map[string]any{"error": reason})
	if _, _, err := d.server.ForwardInWorkspace(ctx, EPFail, taskID, e.WorkspaceID, body); err != nil {
		d.log.Warn("synthetic_fail_forward_failed", "task_id", taskID, "err", err)
	}
	updated, first, err := d.reg.MarkTerminal(taskID, registry.ResultFailed, reason, d.now())
	if err != nil {
		return err
	}
	if first {
		d.metrics.TaskTerminal(string(registry.ResultFailed))
		d.metrics.InflightJobs(d.reg.Inflight())
		d.reclaim(updated)
	}
	return nil
}

// Reconcile converges in-flight tasks whose daemon or lease went away. It is
// the v2 successor of v1's Job reconciler: there are no K8s objects to inspect,
// so the round reads the local entries, renews leases, and settles the tasks
// whose bound lapsed.
func (d *Dispatcher) Reconcile(ctx context.Context) error {
	for _, e := range d.reg.List() {
		if e.IsTerminal() {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		d.convergeOne(ctx, e)
	}
	return nil
}

func (d *Dispatcher) convergeOne(ctx context.Context, e registry.TaskEntry) {
	now := d.now()
	// A task whose daemon went away can never start: fail it locally rather
	// than wait out the server's lease.
	if rt, ok := d.reg.RuntimeByID(e.RuntimeID); ok && !rt.Online && e.StartedAt.IsZero() {
		d.failLocally(ctx, e.TaskID, "daemon_offline")
		return
	}
	// Lease renewal for a claimed-but-unstarted task (§10: renew every ≤20s).
	if e.StartedAt.IsZero() && e.State == registry.StateDaemonRegistered && e.RuntimeID != "" {
		ready, err := d.server.PrepareLease(ctx, e.RuntimeID, e.TaskID)
		switch {
		case err == nil && ready:
			d.reg.SetLeaseKeepUntil(e.TaskID, now.Add(PrepareLeaseTTL))
		case err == nil && !ready:
			// The task left the renewable pre-start state: converge via C13.
			d.convergeStatus(ctx, e)
		case errors.Is(err, ErrTaskNotFound):
			_ = d.OnTaskVanished(ctx, e.TaskID)
		}
		return
	}
	// Boot bound: a task that never started past its bound is failed.
	if d.cfg.BootTimeout > 0 && e.StartedAt.IsZero() && now.Sub(e.ClaimedAt) > d.cfg.BootTimeout {
		d.failLocally(ctx, e.TaskID, "job_boot_timeout")
		return
	}
	// A running task past its max duration is settled by the server's own
	// status; Foreman only reads it.
	if d.cfg.TaskMaxDuration > 0 && !e.StartedAt.IsZero() && now.Sub(e.StartedAt) > d.cfg.TaskMaxDuration {
		d.convergeStatus(ctx, e)
	}
}

// convergeStatus reads C13 and aligns the local entry with the server.
func (d *Dispatcher) convergeStatus(ctx context.Context, e registry.TaskEntry) {
	status, err := d.server.TaskStatus(ctx, e.TaskID)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			_ = d.OnTaskVanished(ctx, e.TaskID)
		}
		return
	}
	switch {
	case registry.IsTerminalResult(status):
		unlock := d.taskMu.Lock(e.TaskID)
		defer unlock()
		updated, first, terr := d.reg.MarkTerminal(e.TaskID, registry.Result(status), "", d.now())
		if terr == nil && first {
			d.metrics.TaskTerminal(status)
			d.metrics.InflightJobs(d.reg.Inflight())
			d.reclaim(updated)
		}
	case status == "running":
		d.reg.MarkStarted(e.TaskID, d.now())
	}
}

// claimID tolerantly reads the id of a claim payload whose full parse failed.
func claimID(task json.RawMessage) string {
	var probe struct {
		ID        string `json:"id"`
		TaskID    string `json:"task_id"`
	}
	if err := json.Unmarshal(task, &probe); err != nil {
		return ""
	}
	if probe.ID != "" {
		return probe.ID
	}
	return probe.TaskID
}

// rewriteRuntime sets payload's runtime_id to runtimeID, keeping every other
// field (§2.5: S-6 may change only the runtime id).
func rewriteRuntime(payload json.RawMessage, runtimeID string) (json.RawMessage, error) {
	if runtimeID == "" {
		return payload, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(runtimeID)
	if err != nil {
		return nil, err
	}
	fields["runtime_id"] = raw
	return json.Marshal(fields)
}

// bumpBusy returns rt with its busy count incremented.
func bumpBusy(rt registry.Runtime) registry.Runtime {
	rt.Busy++
	return rt
}

// ---- keyed mutex ----

// keyedMutex serializes work per key without an unbounded map of channels: the
// entry is dropped when the last holder leaves.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedEntry
}

type keyedEntry struct {
	mu   sync.Mutex
	refs int
}

func newKeyedMutex() *keyedMutex {
	return &keyedMutex{locks: map[string]*keyedEntry{}}
}

// Lock acquires the key's lock and returns its release.
func (k *keyedMutex) Lock(key string) func() {
	k.mu.Lock()
	e, ok := k.locks[key]
	if !ok {
		e = &keyedEntry{}
		k.locks[key] = e
	}
	e.refs++
	k.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		k.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}

// ---- metrics no-op ----

type noopMetrics struct{}

func (noopMetrics) TaskClaimed()          {}
func (noopMetrics) TaskTerminal(string)   {}
func (noopMetrics) InflightJobs(int)      {}
func (noopMetrics) DuplicateDispatch()    {}
func (noopMetrics) CredReclaimed(string, float64) {}
func (noopMetrics) ClaimError(string)     {}
func (noopMetrics) TokenUsage(string, string, int64, int64, int64, int64) {}

func orNoopMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}
