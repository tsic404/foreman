// Package scheduler orchestrates the task→Job mapping: it decides how many
// tasks may run concurrently, creates and deletes Jobs/Secrets, advances the
// state machine, settles terminal tasks, and rebuilds the mapping from K8s
// after a restart. Design: docs/05-modules/task-mapping.md, contracts §3.4.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

// jobGoneGracePeriod tolerates the first-visibility delay of a freshly
// created Job before the missing-Job compensation engages
// (failure-handling reconcileOne step 1).
const jobGoneGracePeriod = 60 * time.Second

// jobNamePollInterval is the cadence at which a claim retries a Job create
// whose name is still held by a foreground-terminating predecessor.
const jobNamePollInterval = 500 * time.Millisecond

// jobNameWait bounds that retry. The Job controller deletes the pods first
// and the Job object follows once they are gone, normally within the 30s
// terminationGracePeriodSeconds of the template (05-modules/job-template.md).
const jobNameWait = 45 * time.Second

// errJobNameHeld reports that a Job name was still held by a terminating
// predecessor after jobNameWait: the claim is deferred to the server's
// re-dispatch, never reported as this task's failure.
var errJobNameHeld = errors.New("job name still held by a terminating job")

// Failure reasons carried in the synthesized fail report (C10 body
// failure_reason) for every compensation path (failure-handling 场景矩阵).
const (
	FailureReasonJobCreateFailed = "job_create_failed"
	FailureReasonJobMissing      = "job_missing"
	FailureReasonJobBootTimeout  = "job_boot_timeout"
	FailureReasonJobFailed       = "job_failed"
	FailureReasonJobDeadline     = "job_deadline_exceeded"
	FailureReasonJobEvicted      = "job_evicted"
	// FailureReasonInvalidJobTemplate is the build-time invariant fallback
	// (failure-handling scenario #0): the overlay passed startup validation,
	// so a hit here is a configuration/implementation defect, never retried.
	FailureReasonInvalidJobTemplate = "invalid_job_template"
	// FailureReasonInvalidTaskID is scenario #0's other half: the task ID
	// cannot become a legal Job name (05-modules/job-template.md §错误处理).
	FailureReasonInvalidTaskID = "invalid_task_id"
)

// claimHead is the subset of the claim payload the scheduler reads to index
// the task (proxy.md 数据结构). Everything else passes through opaque.
type claimHead struct {
	ID              string `json:"id"`
	AgentID         string `json:"agent_id"`
	IssueID         string `json:"issue_id"`
	IssueIdentifier string `json:"issue_identifier"`
	WorkspaceID     string `json:"workspace_id"`
}

// keyedMutex serializes OnClaim per task: two concurrent claims of the same
// task must not interleave their DeleteSecret→CreateSecret→CreateJob
// sequence (the loser's AlreadyExists would otherwise fail a healthy task).
type keyedMutex struct {
	mu sync.Mutex
	m  map[string]*refMutex
}

type refMutex struct {
	mu   sync.Mutex
	refs int
}

func newKeyedMutex() *keyedMutex { return &keyedMutex{m: make(map[string]*refMutex)} }

// Lock locks key and returns the unlock function. Entries are refcounted and
// dropped once unused, so the map does not grow with task count.
func (k *keyedMutex) Lock(key string) func() {
	k.mu.Lock()
	rm, ok := k.m[key]
	if !ok {
		rm = &refMutex{}
		k.m[key] = rm
	}
	rm.refs++
	k.mu.Unlock()
	rm.mu.Lock()
	return func() {
		rm.mu.Unlock()
		k.mu.Lock()
		rm.refs--
		if rm.refs == 0 {
			delete(k.m, key)
		}
		k.mu.Unlock()
	}
}

// failReport is a synthetic fail report (C10) whose forward has not landed
// yet; contract §4 forbids dropping a terminal callback, so the entry is
// retained until a reconcile round delivers it.
type failReport struct {
	reason  string
	message string
}

// Option customizes a Scheduler.
type Option func(*Scheduler)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(s *Scheduler) { s.now = now }
}

// WithReconciler wires the recovery module's convergence driver; Reconcile
// then delegates to it instead of the built-in startup convergence.
func WithReconciler(r Reconciler) Option {
	return func(s *Scheduler) { s.reconciler = r }
}

// WithPendingReports wires the recovery module's durable terminal-report
// queue; terminal forwards that exhaust the proxy's retry budget are
// enqueued instead of dropped (contract §4).
func WithPendingReports(p PendingReports) Option {
	return func(s *Scheduler) { s.pendingReports = p }
}

// WithLogger overrides the logger (tests).
func WithLogger(l *slog.Logger) Option {
	return func(s *Scheduler) { s.log = l.With("component", "scheduler") }
}

// Scheduler owns the task mapping state machine and the Job/Secret
// lifecycle. The claim loop lives in proxy; the scheduling actions are here.
type Scheduler struct {
	cfg     Config
	reg     *registry.Registry
	jobs    JobClient
	builder JobBuilder
	server  ServerClient
	metrics Metrics
	log     *slog.Logger

	reconciler     Reconciler
	pendingReports PendingReports
	now            func() time.Time

	claimMu     *keyedMutex
	pendingFail sync.Map // task_id → failReport: terminal reports not yet delivered

	// nodeMu serializes the per-node soft cap refresh: concurrent claims
	// would otherwise interleave the marker replacement and log the same
	// transition twice.
	nodeMu sync.Mutex
}

// New builds a Scheduler. reg, jobs, builder and server are required.
func New(cfg Config, reg *registry.Registry, jobs JobClient, builder JobBuilder, server ServerClient, metrics Metrics, opts ...Option) (*Scheduler, error) {
	if reg == nil || jobs == nil || builder == nil || server == nil {
		return nil, errors.New("registry, job client, job builder and server client are required")
	}
	s := &Scheduler{
		cfg:     cfg,
		reg:     reg,
		jobs:    jobs,
		builder: builder,
		server:  server,
		metrics: orNoopMetrics(metrics),
		log:     slog.Default().With("component", "scheduler"),
		now:     time.Now,

		claimMu: newKeyedMutex(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Run is the orchestration main loop: rebuild the index from K8s (ADR-002),
// converge once against the server, then serve until ctx is done. The claim
// ticker lives in proxy; the periodic convergence in recovery.
func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.Rebuild(ctx); err != nil {
		return fmt.Errorf("rebuild registry from k8s jobs: %w", err)
	}
	if err := s.Reconcile(ctx); err != nil {
		s.log.WarnContext(ctx, "initial reconcile failed", "err", err)
	}
	<-ctx.Done()
	return nil
}

// ClaimBudget is how many tasks the proxy may claim right now:
// min(max-inflight − inflight, claim-batch-max). Queueing stays on the
// server; Foreman never pre-fetches (task-mapping §并发与排队策略).
func (s *Scheduler) ClaimBudget() int {
	free := s.cfg.MaxInflightJobs - s.reg.Inflight()
	if free < 0 {
		free = 0
	}
	return min(free, s.cfg.ClaimBatchMax)
}

// RefreshNodeSaturation recomputes the per-node soft cap
// (FOREMAN_MAX_JOBS_PER_NODE, ADR-006 §决策结果 3) from the live index and
// republishes the saturation markers: every node holding at least
// MaxJobsPerNode non-terminal Jobs is marked, so the next claim drops its
// reuse affinity (jobbuilder) and adds no further load to it. Jobs are never
// deleted and nothing is queued or refused — the cap is a placement
// preference, not admission control (task-mapping §同节点并发).
//
// Node names appear one reconcile round after placement (the round reads
// spec.nodeName), so a node's load is the Jobs it held as of that round.
// It returns the observed per-node load.
func (s *Scheduler) RefreshNodeSaturation(ctx context.Context) map[string]int {
	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()

	load := s.reg.ActiveJobsByNode()
	saturated := make([]string, 0, len(load))
	for node, n := range load {
		if n >= s.cfg.MaxJobsPerNode {
			saturated = append(saturated, node)
		}
	}
	sort.Strings(saturated)
	previous := s.reg.SaturatedNodes()
	s.reg.SetSaturatedNodes(saturated)

	// Log only the transitions: the load of every node would flood the log
	// once per round, while a transition carries the count and the cap the
	// operator needs (observability.md §结构化日志).
	was := make(map[string]bool, len(previous))
	for _, node := range previous {
		was[node] = true
	}
	now := make(map[string]bool, len(saturated))
	for _, node := range saturated {
		now[node] = true
		if !was[node] {
			s.log.InfoContext(ctx, "node.saturation",
				"node", node, "active_jobs", load[node],
				"max_jobs_per_node", s.cfg.MaxJobsPerNode, "saturated", true)
		}
	}
	for _, node := range previous {
		if !now[node] {
			s.log.InfoContext(ctx, "node.saturation",
				"node", node, "active_jobs", load[node],
				"max_jobs_per_node", s.cfg.MaxJobsPerNode, "saturated", false)
		}
	}
	return load
}

// OnClaim registers a freshly claimed task and creates its Secret + Job
// (pending → job_created). A task the registry already knows goes through
// the duplicate-claim rule; no second Job is ever created concurrently.
func (s *Scheduler) OnClaim(ctx context.Context, task json.RawMessage) error {
	var head claimHead
	if err := json.Unmarshal(task, &head); err != nil {
		return fmt.Errorf("parse claim payload head: %w", err)
	}
	if head.ID == "" {
		return errors.New("claim payload has no id")
	}
	unlock := s.claimMu.Lock(head.ID)
	defer unlock()

	attempt := 1
	if existing, ok := s.reg.Get(head.ID); ok {
		proceed, priorAttempt, err := s.resolveDuplicateClaim(ctx, existing)
		if err != nil || !proceed {
			return err
		}
		attempt = priorAttempt + 1
	} else if s.reg.IsDone(head.ID) {
		// The task settled within the done window yet the server re-dispatched
		// it: re-executing would double-run it (invariant 4). The terminal
		// report retry belongs to the pending-report queue.
		s.metrics.DuplicateDispatch()
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", head.ID, "old_job_name", "", "action", "drop_terminal_done")
		return nil
	}

	now := s.now()
	jobName := "fm-" + head.ID
	e := registry.TaskEntry{
		TaskID:          head.ID,
		AgentID:         head.AgentID,
		IssueID:         head.IssueID,
		IssueIdentifier: head.IssueIdentifier,
		WorkspaceID:     head.WorkspaceID,
		JobName:         jobName,
		JobNamespace:    s.cfg.JobNamespace,
		DaemonID:        jobName, // per-Job daemon identity (ADR-003)
		JobRuntimeID:    jobRuntimeID(jobName),
		State:           registry.StatePending,
		Attempt:         attempt,
		ClaimedAt:       now,
		BootDeadline:    now.Add(s.cfg.JobBootTimeout),
		Payload:         append(json.RawMessage(nil), task...),
		PayloadVersion:  1,
	}
	if err := s.reg.Put(e); err != nil {
		return fmt.Errorf("register task %s: %w", e.TaskID, err)
	}
	s.log.InfoContext(ctx, "task.claimed",
		"task_id", e.TaskID, "job_name", e.JobName,
		"issue_identifier", e.IssueIdentifier, "agent_id", e.AgentID)

	// The per-node soft cap is applied right before the Job is rendered
	// ("下一次 claim 前把该节点从候选里排除", task-mapping §同节点并发): the
	// refresh marks the nodes at their cap so the affinity below skips them.
	s.RefreshNodeSaturation(ctx)

	job, secret, err := s.builder.Build(jobbuilderEntry(e))
	if err != nil {
		s.failClaim(ctx, e, err)
		return err
	}
	// An idempotent retry may have left the Secret behind: delete-then-create
	// (NotFound counts as success inside the JobClient).
	if err := s.jobs.DeleteSecret(ctx, credName(jobName)); err != nil {
		s.failClaim(ctx, e, fmt.Errorf("clear stale credential secret: %w", err))
		return err
	}
	if err := s.jobs.CreateSecret(ctx, secret); err != nil {
		s.failClaim(ctx, e, fmt.Errorf("create credential secret: %w", err))
		return err
	}
	if err := s.createJob(ctx, e, job); err != nil {
		// Nothing of this claim is left in the cluster: drop the credential
		// Secret again. A name still held by a terminating predecessor is
		// not this task's failure — the entry goes and the server
		// re-dispatches the task once the lease lapses, the same recovery
		// as an undeliverable claim (failure-handling 场景 #1/#2).
		_ = s.jobs.DeleteSecret(ctx, credName(jobName))
		if errors.Is(err, errJobNameHeld) {
			_ = s.reg.Delete(e.TaskID)
			s.metrics.InflightJobs(s.reg.Inflight())
			s.log.WarnContext(ctx, "task.claim_deferred",
				"task_id", e.TaskID, "job_name", e.JobName, "err", err.Error())
			return err
		}
		s.failClaim(ctx, e, fmt.Errorf("create job: %w", err))
		return err
	}

	e.State = registry.StateJobCreated
	e.JobCreatedAt = s.now()
	if err := s.reg.Put(e); err != nil {
		return err
	}
	s.metrics.TaskClaimed()
	s.metrics.JobCreateSeconds(e.JobCreatedAt.Sub(e.ClaimedAt).Seconds())
	s.metrics.InflightJobs(s.reg.Inflight())
	s.log.InfoContext(ctx, "job.created",
		"task_id", e.TaskID, "job_name", e.JobName,
		"node_name", e.NodeName, "image", jobImage(job))
	return nil
}

// createJob places the entry's Job, resolving the two meanings of
// AlreadyExists. A Job that is there and not terminating belongs to this
// attempt (restart replay, or a retry of the same claim) and is kept. A Job
// that is terminating — or one whose state cannot be read — holds the name
// (job_name = fm-<task_id>, task-mapping invariant 2) until its pods are
// gone; swallowing that would bind the entry to a disappearing Job
// (job_missing). Wait (bounded) and retry; a name that never frees returns
// errJobNameHeld, a deferred claim rather than this task's failure.
func (s *Scheduler) createJob(ctx context.Context, e registry.TaskEntry, job *batchv1.Job) error {
	deadline := s.now().Add(jobNameWait)
	// maxPolls backs the clock bound so the loop cannot spin on a clock the
	// caller froze (tests) or on a view that keeps reporting the name as
	// free while Create disagrees.
	maxPolls := int(jobNameWait / jobNamePollInterval)
	for poll := 0; ; poll++ {
		err := s.jobs.CreateJob(ctx, job)
		if err == nil {
			return nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing, probeErr := s.jobs.GetJob(ctx, e.JobName)
		if probeErr == nil && existing != nil && existing.DeletionTimestamp == nil {
			return nil
		}
		if probeErr != nil {
			// Truth unknown: the name may still be held, so keep polling.
			// Transient read failures stay in Debug (cf. prepare-lease);
			// task.claim_deferred at the bound is the alert, and the outcome
			// here is a deferred claim, never a task failure (AC-09).
			s.log.DebugContext(ctx, "job.probe_failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", probeErr)
		}
		if poll >= maxPolls || s.now().After(deadline) {
			return fmt.Errorf("%w: %s", errJobNameHeld, e.JobName)
		}
		if probeErr == nil && existing == nil {
			// The name just freed (GetJob's nil means the object is gone):
			// retry the create without waiting out a poll interval.
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jobNamePollInterval):
		}
	}
}

// resolveDuplicateClaim applies the duplicate-claim rule (task-mapping
// §重复 claim): C13 decides the truth. It reports whether the claim proceeds
// as a replacement (old pre-start entry discarded) and the prior attempt.
func (s *Scheduler) resolveDuplicateClaim(ctx context.Context, existing registry.TaskEntry) (bool, int, error) {
	s.metrics.DuplicateDispatch()
	if existing.IsTerminal() {
		// Terminal is irreversible (invariant 4): even if the server
		// re-dispatched because our terminal report has not landed yet, the
		// task already ran — never build a second Job for it.
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "drop_terminal_entry")
		return false, 0, nil
	}
	st, err := s.server.TaskStatus(ctx, existing.TaskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "drop_task_gone")
		// The claim proceeds as a replacement: leftover objects are handled
		// idempotently by the new claim (delete-then-create Secret, tolerated
		// AlreadyExists Job), so a failed delete needs no retry handle.
		_ = s.deleteObjects(ctx, existing)
		_ = s.reg.Delete(existing.TaskID)
		return false, 0, nil
	case err != nil:
		// Truth unknown: keep the existing entry, drop the new claim; the
		// reconcile loop converges later.
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "keep_old_status_unknown", "err", err)
		return false, 0, nil
	}
	// In-place field update only: the pre-C13 snapshot must not be Put back
	// wholesale — daemon callbacks may have advanced the entry meanwhile.
	s.reg.SetLastStatusSeen(existing.TaskID, st)

	switch {
	case st == "running":
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "keep_running")
		return false, 0, nil
	case registry.IsTerminalResult(st):
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "drop_terminal")
		return false, 0, nil
	case st == "queued" || st == "dispatched":
		// The old entry cannot have started: discard it (no duplicate
		// execution) and let the new claim build a fresh Job.
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "replace_pre_start")
		// The claim proceeds as a replacement: leftover objects are handled
		// idempotently by the new claim (delete-then-create Secret, tolerated
		// AlreadyExists Job), so a failed delete needs no retry handle.
		_ = s.deleteObjects(ctx, existing)
		_ = s.reg.Delete(existing.TaskID)
		return true, existing.Attempt, nil
	default:
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "keep_old_status_"+st)
		return false, 0, nil
	}
}

// Deliver hands the Job's task to its daemon (S6): at most once per task,
// and only the runtime_id field of the original claim payload is rewritten.
func (s *Scheduler) Deliver(daemonID string, runtimeID string, maxTasks int) ([]json.RawMessage, error) {
	if maxTasks < 1 {
		return nil, nil
	}
	e, ok := s.reg.ByDaemon(daemonID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownDaemon, daemonID)
	}
	if runtimeID != e.JobRuntimeID {
		return nil, fmt.Errorf("%w: %s", ErrRuntimeMismatch, runtimeID)
	}
	if len(e.Payload) == 0 {
		// The full payload never leaves process memory (F3); a rebuilt entry
		// has nothing to deliver and is converged away instead.
		s.log.Warn("task.payload_missing",
			"task_id", e.TaskID, "job_name", e.JobName)
		return nil, nil
	}

	// The only rewritten field (contract §1.2 S6); every other field keeps
	// its value (the map round-trip may reorder keys). Rewritten before the
	// state transition so a marshal failure does not burn the single delivery.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.Payload, &fields); err != nil {
		return nil, fmt.Errorf("parse stored payload for task %s: %w", e.TaskID, err)
	}
	runtimeRaw, err := json.Marshal(e.JobRuntimeID)
	if err != nil {
		return nil, err
	}
	fields["runtime_id"] = runtimeRaw
	out, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}

	now := s.now()
	e, ok = s.reg.MarkDelivering(daemonID, now)
	if !ok {
		return nil, nil
	}
	if rt, ok := s.reg.RuntimeByID(e.JobRuntimeID); ok {
		rt.DeliveredTaskID = e.TaskID
		s.reg.PutRuntime(rt)
	}
	s.log.Info("task.delivered",
		"task_id", e.TaskID, "job_name", e.JobName,
		"deliver_latency_ms", now.Sub(e.ClaimedAt).Milliseconds())
	return []json.RawMessage{out}, nil
}

// OnReport applies the state transition for a forwarded lifecycle callback
// (S8–S15) and returns the upstream status and body for the daemon. The
// Job/Secret are deleted only after the terminal report succeeded — never
// the other way around (task-mapping invariant 3).
func (s *Scheduler) OnReport(ctx context.Context, ep Endpoint, e registry.TaskEntry, body []byte) (int, []byte, error) {
	now := s.now()
	switch {
	case ep == EPStart:
		started, ok := s.reg.MarkStarted(e.TaskID, now)
		if ok {
			s.metrics.JobBootSeconds(now.Sub(started.ClaimedAt).Seconds())
			s.log.InfoContext(ctx, "task.started",
				"task_id", e.TaskID, "job_name", e.JobName,
				"boot_seconds", now.Sub(started.ClaimedAt).Seconds())
		}
		// A late start on a terminal entry is still forwarded: the server's
		// 409 tells the daemon the task is settled.
	case ep.IsTerminal():
		if !e.IsTerminal() {
			result := registry.Result(ep.TerminalResult())
			if _, err := s.reg.MarkTerminal(e.TaskID, result, now); err != nil {
				return 0, nil, err
			}
			e.State = registry.StateTerminal
			e.Result = result
			s.metrics.TaskTerminal(string(result))
			s.log.InfoContext(ctx, "task.terminal",
				"task_id", e.TaskID, "job_name", e.JobName,
				"result", string(result),
				"duration_seconds", now.Sub(e.ClaimedAt).Seconds())
		}
	}

	code, resp, err := s.server.Forward(ctx, ep, e.TaskID, body)
	if err != nil {
		if ep.IsTerminal() {
			// The proxy's retry budget is exhausted (or the transport is
			// down): contract §4 forbids dropping the report — queue it and
			// tell the daemon the upstream is unavailable (proxy.md 转发).
			s.enqueuePending(ctx, e, ep, body)
			return http.StatusBadGateway, upstreamUnavailableBody, nil
		}
		return 0, nil, fmt.Errorf("forward %s for task %s: %w", ep, e.TaskID, err)
	}
	if ep.IsTerminal() {
		defer s.metrics.InflightJobs(s.reg.Inflight())
		switch {
		case code >= 200 && code < 300:
			s.cleanup(ctx, e)
		case code == 404:
			// The server already forgot the task: the report is moot and the
			// cleanup idempotent (proxy.md 转发 switch).
			s.cleanup(ctx, e)
			return 200, resp, nil
		case code == 400 || code == 403 || code == 409:
			// Permanent failure (§1.1 failure table): no retry, no queue;
			// the entry stays terminal and the objects stay for inspection.
			s.log.ErrorContext(ctx, "task.forward_failed",
				"task_id", e.TaskID, "endpoint", string(ep), "status", code)
		default:
			// Transient failure with the retry budget exhausted: queue the
			// report; recovery drains it until the server accepts (§4).
			s.log.ErrorContext(ctx, "task.forward_failed",
				"task_id", e.TaskID, "endpoint", string(ep), "status", code)
			s.enqueuePending(ctx, e, ep, body)
			return http.StatusBadGateway, upstreamUnavailableBody, nil
		}
		return code, resp, nil
	}
	if code == 404 {
		// A non-terminal forward answered 404: the server deleted the task
		// (proxy.md 转发 switch → failure-handling scenario #9).
		if err := s.OnTaskVanished(ctx, e.TaskID); err != nil {
			s.log.ErrorContext(ctx, "task vanish cleanup failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		}
		return 404, resp, nil
	}
	if code >= 500 || code == http.StatusRequestTimeout || code == http.StatusTooManyRequests {
		// Transient upstream failure: the §1.2/proxy.md 转发 switch maps it
		// to 502 for the daemon (the daemon owns the retry).
		return http.StatusBadGateway, upstreamUnavailableBody, nil
	}
	return code, resp, nil
}

// upstreamUnavailableBody is the §1.2 error body for a transiently
// undeliverable forward (proxy.md 转发 switch).
var upstreamUnavailableBody = []byte(`{"error":"upstream unavailable"}`)

// enqueuePending lands a terminal report in the recovery module's durable
// queue (contract §4: terminal callbacks are never dropped). Without a
// wired queue the entry stays terminal and reconcile keeps the objects.
func (s *Scheduler) enqueuePending(ctx context.Context, e registry.TaskEntry, ep Endpoint, body []byte) {
	if s.pendingReports == nil {
		s.log.ErrorContext(ctx, "task.forward_undelivered",
			"task_id", e.TaskID, "endpoint", string(ep),
			"reason", "no pending-report queue wired")
		return
	}
	if err := s.pendingReports.Enqueue(e.TaskID, ep, body); err != nil {
		s.log.ErrorContext(ctx, "task.forward_undelivered",
			"task_id", e.TaskID, "endpoint", string(ep),
			"reason", "pending-report enqueue failed", "err", err)
	}
}

// OnTaskVanished handles failure-handling scenario #9: the server deleted
// the task (C4/C13 404). The Job/Secret are removed and the entry dropped;
// no terminal report is sent for a task the server no longer knows.
func (s *Scheduler) OnTaskVanished(ctx context.Context, taskID string) error {
	if e, ok := s.reg.Get(taskID); ok {
		s.log.InfoContext(ctx, "task.vanished",
			"task_id", e.TaskID, "job_name", e.JobName)
	}
	return s.ReleaseTask(ctx, taskID)
}

// OnBootTimeout handles failure-handling scenario #4: the Job's daemon never
// started within the boot deadline. The task is failed on the daemon's
// behalf (C10, failure_reason=job_boot_timeout) and the objects removed.
func (s *Scheduler) OnBootTimeout(ctx context.Context, taskID string) error {
	e, ok := s.reg.Get(taskID)
	if !ok || e.IsTerminal() || !e.StartedAt.IsZero() {
		return nil
	}
	if _, err := s.reg.MarkTerminal(taskID, registry.ResultFailed, s.now()); err != nil {
		return err
	}
	s.metrics.TaskTerminal(string(registry.ResultFailed))
	s.log.ErrorContext(ctx, "task.failed_compensated",
		"task_id", e.TaskID, "job_name", e.JobName,
		"failure_reason", FailureReasonJobBootTimeout)
	report := failReport{reason: FailureReasonJobBootTimeout, message: "job daemon did not start before the boot deadline"}
	if err := s.reportFail(ctx, taskID, report); err != nil {
		// Keep the terminal entry: a reconcile round retries the report
		// before the objects may be deleted (contract §4).
		s.pendingFail.Store(taskID, report)
		s.metrics.InflightJobs(s.reg.Inflight())
		return nil
	}
	s.metrics.InflightJobs(s.reg.Inflight())
	return s.cleanup(ctx, e)
}

// ConvergeLeaseRefused settles an entry whose prepare-lease was refused
// (C4 400: the task left the renewable pre-start state). C13 decides:
// running marks the entry started (leasing stops), terminal settles and
// cleans up, a deleted task vanishes; anything else waits for the next
// lease round (proxy.md §prepare-lease 保活).
func (s *Scheduler) ConvergeLeaseRefused(ctx context.Context, taskID string) error {
	e, ok := s.reg.Get(taskID)
	if !ok || e.IsTerminal() {
		return nil
	}
	st, err := s.server.TaskStatus(ctx, taskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		return s.OnTaskVanished(ctx, taskID)
	case err != nil:
		return err
	}
	s.reg.SetLastStatusSeen(taskID, st)
	switch {
	case st == "running":
		// The daemon started without Foreman seeing S8 (e.g. a restart):
		// stop leasing, keep the Job, wait for the daemon's reports.
		s.reg.MarkStarted(taskID, s.now())
		return nil
	case registry.IsTerminalResult(st):
		if _, err := s.reg.MarkTerminal(taskID, registry.Result(st), s.now()); err != nil {
			return err
		}
		return s.cleanup(ctx, e)
	default:
		// queued/dispatched/waiting_*: no decisive fact — retry next round.
		return nil
	}
}

// OnJobGone is the compensation entry for a Job object that disappeared
// without a daemon report (failure-handling 场景 #1 step 1 / #2 / #9 / #11).
// The grace period tolerates the first-visibility delay of a freshly created
// Job; C13 then decides what to settle.
func (s *Scheduler) OnJobGone(ctx context.Context, jobName string) error {
	e, ok := s.reg.ByJob(jobName)
	if !ok {
		return nil
	}
	if e.IsTerminal() {
		return s.cleanup(ctx, e)
	}
	if s.now().Sub(e.ClaimedAt) < jobGoneGracePeriod {
		// Freshly created: tolerate the first-visibility delay.
		return nil
	}
	return s.FailJob(ctx, jobName, FailureReasonJobMissing)
}

// FailJob compensates a Job whose container ended without the daemon
// reporting a terminal state (failure-handling 场景 #3/#5/#11, and #1's
// missing-Job path via OnJobGone). reason is the failure_reason reported to
// the server; C13 decides first what to report (判据模型): a terminal server
// status is settled as-is, a task the server deleted is released without a
// report, and only a still-active task gets the synthesized fail.
// Idempotent: an already-terminal entry is only cleaned up (顺序规则 2).
func (s *Scheduler) FailJob(ctx context.Context, jobName, reason string) error {
	e, ok := s.reg.ByJob(jobName)
	if !ok {
		return nil
	}
	if e.IsTerminal() {
		return s.cleanup(ctx, e)
	}
	st, err := s.server.TaskStatus(ctx, e.TaskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		// Task deleted server-side (scenario #9): clean up, report nothing.
		return s.ReleaseTask(ctx, e.TaskID)
	case err != nil:
		return fmt.Errorf("check status of task %s: %w", e.TaskID, err)
	}
	s.reg.SetLastStatusSeen(e.TaskID, st)
	if registry.IsTerminalResult(st) {
		return s.SettleTerminal(ctx, e.TaskID, st)
	}
	s.log.ErrorContext(ctx, "task.failed_compensated",
		"task_id", e.TaskID, "job_name", e.JobName, "failure_reason", reason)
	if _, err := s.reg.MarkTerminal(e.TaskID, registry.ResultFailed, s.now()); err != nil {
		return err
	}
	s.metrics.TaskTerminal(string(registry.ResultFailed))
	// Contract §4: a terminal callback is never dropped. If the report does
	// not land, keep the terminal entry — a later round retries it before
	// the objects may be deleted.
	report := failReport{reason: reason, message: "container ended without a terminal report"}
	if err := s.reportFail(ctx, e.TaskID, report); err != nil {
		s.pendingFail.Store(e.TaskID, report)
		return nil
	}
	e, ok = s.reg.Get(e.TaskID)
	if !ok {
		return nil
	}
	return s.cleanup(ctx, e)
}

// SettleTerminal converges an entry the server already holds as terminal
// (C13 completed/failed/cancelled, or a rebuilt entry whose cleanup is
// still pending): a synthetic fail report still queued in memory lands
// first (contract §4), then the entry is marked terminal and the Job/Secret
// go (顺序规则 1). A report that fails keeps the entry for the next round.
func (s *Scheduler) SettleTerminal(ctx context.Context, taskID, result string) error {
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	if report, ok := s.pendingFail.Load(taskID); ok {
		if err := s.reportFail(ctx, taskID, report.(failReport)); err != nil {
			return nil // keep the entry; next round retries
		}
		s.pendingFail.Delete(taskID)
	}
	if !e.IsTerminal() {
		if _, err := s.reg.MarkTerminal(taskID, registry.Result(result), s.now()); err != nil {
			return err
		}
		s.metrics.TaskTerminal(result)
		s.log.InfoContext(ctx, "task.terminal",
			"task_id", e.TaskID, "job_name", e.JobName, "result", result)
	}
	return s.cleanup(ctx, e)
}

// AdoptRunning records that the server considers the task running (C13),
// e.g. after a restart rebuilt the entry: the Job is kept, lease renewals
// stop (started_at is set) and the daemon's reports are accepted
// (failure-handling 场景 #1).
func (s *Scheduler) AdoptRunning(ctx context.Context, taskID string) error {
	e, ok := s.reg.MarkStarted(taskID, s.now())
	if !ok {
		return nil
	}
	s.log.InfoContext(ctx, "task.adopted_running",
		"task_id", e.TaskID, "job_name", e.JobName)
	return nil
}

// ReleaseTask removes the Job/Secret and drops the mapping without any
// terminal report: the server already holds the truth (failure-handling
// 场景 #2 redispatch, #8 cancel timeout, #9 deleted task). A failed deletion
// keeps the entry: it is the only retry handle, and dropping it would leave
// a zombie Job or leak the credential Secret (顺序规则 2).
func (s *Scheduler) ReleaseTask(ctx context.Context, taskID string) error {
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	// A queued synthetic report is moot once the task is released or gone.
	s.pendingFail.Delete(taskID)
	if err := s.deleteObjects(ctx, e); err != nil {
		return err
	}
	if err := s.reg.Delete(taskID); err != nil {
		return err
	}
	s.metrics.InflightJobs(s.reg.Inflight())
	return nil
}

// Reconcile converges the live index with the server. With a recovery
// reconciler wired it delegates; otherwise it runs the built-in startup
// convergence (task-mapping §持久化与重建 step 3) over every live entry.
// The per-node soft cap is refreshed first: its markers are derived state of
// the live index and must not outlive the load they describe.
func (s *Scheduler) Reconcile(ctx context.Context) error {
	s.RefreshNodeSaturation(ctx)
	if s.reconciler != nil {
		return s.reconciler.Reconcile(ctx)
	}
	var firstErr error
	for _, e := range s.reg.List() {
		if err := s.convergeEntry(ctx, e); err != nil {
			s.log.WarnContext(ctx, "reconcile entry failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Rebuild reconstructs the in-memory index from the foreman Jobs still
// present in K8s (ADR-002): labels/annotations restore the identity fields;
// the state starts at job_created and the following Reconcile advances it.
func (s *Scheduler) Rebuild(ctx context.Context) error {
	jobs, err := s.jobs.ListJobs(ctx)
	if err != nil {
		return err
	}
	rebuilt := 0
	for _, job := range jobs {
		e, err := registry.TaskEntryFromJob(job)
		if err != nil {
			s.log.ErrorContext(ctx, "skip malformed job", "job_name", job.GetName(), "err", err)
			continue
		}
		if err := s.reg.Put(e); err != nil {
			s.log.ErrorContext(ctx, "restore entry failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", err)
			continue
		}
		s.reg.PutRuntime(registry.JobRuntime{
			JobRuntimeID: e.JobRuntimeID,
			DaemonID:     e.DaemonID,
			TaskID:       e.TaskID,
			Provider:     "omp",
			Name:         "foreman-job",
			Online:       true,
			// The daemon was alive when its Job was built; seeding the shared
			// heartbeat clock at rebuild time keeps the suspect check
			// (scenario #6) from judging it stale before its first S5.
			LastHeartbeatAt: s.now(),
		})
		rebuilt++
	}
	// Payloads never leave process memory (F3), so every rebuilt entry lacks
	// one; the C13 convergence releases pre-start entries for redispatch.
	s.log.InfoContext(ctx, "registry.rebuilt", "jobs", rebuilt)
	return nil
}

// convergeEntry settles one rebuilt or lingering entry against the server's
// status (C13): running keeps the Job, queued/dispatched releases it for
// server redispatch, terminal cleans up, 404 cleans up without a report.
// Every registry write is an in-lock transition — a snapshot Put after the
// network round-trip could clobber a concurrent daemon callback.
func (s *Scheduler) convergeEntry(ctx context.Context, e registry.TaskEntry) error {
	if e.IsTerminal() {
		return s.SettleTerminal(ctx, e.TaskID, string(e.Result))
	}
	st, err := s.server.TaskStatus(ctx, e.TaskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		return s.ReleaseTask(ctx, e.TaskID)
	case err != nil:
		return err
	}
	s.reg.SetLastStatusSeen(e.TaskID, st)
	switch {
	case st == "running":
		// The task is executing: keep the Job and accept the daemon's
		// reports. AdoptRunning sets started_at once; it only gates the boot
		// timeout, so the rebuild-time approximation is safe. A miss means
		// the entry turned terminal or was deleted mid-round — benign.
		return s.AdoptRunning(ctx, e.TaskID)
	case st == "queued" || st == "dispatched":
		// Not started and the payload is gone with the restart: release the
		// Job so the server re-dispatches after its reclaim window.
		return s.ReleaseTask(ctx, e.TaskID)
	case registry.IsTerminalResult(st):
		return s.SettleTerminal(ctx, e.TaskID, st)
	default:
		// e.g. waiting_local_directory: no decisive fact yet — keep waiting
		// (failure-handling 判据模型).
		return nil
	}
}

// failClaim settles a task whose Job could not be created
// (pending → terminal, result=failed) and tells the server. A report that
// does not land is kept in pendingFail; the entry stays until a reconcile
// round delivers it (contract §4).
func (s *Scheduler) failClaim(ctx context.Context, e registry.TaskEntry, cause error) {
	if _, err := s.reg.MarkTerminal(e.TaskID, registry.ResultFailed, time.Time{}); err != nil {
		s.log.ErrorContext(ctx, "mark terminal failed", "task_id", e.TaskID, "err", err)
	}
	s.metrics.TaskTerminal(string(registry.ResultFailed))
	// A rejected template is not a create failure: the Job must not be
	// retried, the operator has to fix the overlay (failure-handling #0).
	reason := FailureReasonJobCreateFailed
	var invalid *jobbuilder.ValidationError
	switch {
	case errors.As(cause, &invalid):
		reason = FailureReasonInvalidJobTemplate
	case errors.Is(cause, jobbuilder.ErrInvalidTaskID):
		reason = FailureReasonInvalidTaskID
	}
	s.log.ErrorContext(ctx, "task.failed_compensated",
		"task_id", e.TaskID, "job_name", e.JobName, "failure_reason", reason, "err", cause.Error())
	report := failReport{reason: reason, message: cause.Error()}
	if err := s.reportFail(ctx, e.TaskID, report); err != nil {
		s.pendingFail.Store(e.TaskID, report)
	}
	s.metrics.InflightJobs(s.reg.Inflight())
}

// reportFail forwards a synthetic fail report (C10) for a task the daemon
// could not settle itself. A 404/409 means the server already settled it, so
// it counts as delivered; anything else is an error worth retrying.
func (s *Scheduler) reportFail(ctx context.Context, taskID string, report failReport) error {
	body, err := json.Marshal(map[string]string{"error": report.message, "failure_reason": report.reason})
	if err != nil {
		return fmt.Errorf("encode fail report: %w", err)
	}
	code, _, err := s.server.Forward(ctx, EPFail, taskID, body)
	if err == nil && ((code >= 200 && code < 300) || code == 404 || code == 409) {
		return nil
	}
	if err == nil {
		err = fmt.Errorf("server returned %d", code)
	}
	s.log.ErrorContext(ctx, "task.forward_failed",
		"task_id", taskID, "endpoint", string(EPFail), "status", code, "err", err)
	return err
}

// cleanup deletes the Job and its credential Secret, then drops the entry.
// The entry is kept when either deletion fails so the next reconcile round
// retries it (顺序规则 2); both objects are attempted in either case so one
// failure cannot leak the other.
func (s *Scheduler) cleanup(ctx context.Context, e registry.TaskEntry) error {
	if err := s.deleteObjects(ctx, e); err != nil {
		return err
	}
	return s.reg.Delete(e.TaskID)
}

// deleteObjects removes the K8s objects without touching the entry; used on
// paths that discard the mapping separately. The joined error is the caller's
// retry handle: the entry stays live until every delete succeeded.
func (s *Scheduler) deleteObjects(ctx context.Context, e registry.TaskEntry) error {
	var errs []error
	if err := s.jobs.DeleteJob(ctx, e.JobName); err != nil {
		s.log.ErrorContext(ctx, "job.delete_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		errs = append(errs, fmt.Errorf("delete job %s: %w", e.JobName, err))
	}
	if err := s.jobs.DeleteSecret(ctx, credName(e.JobName)); err != nil {
		s.log.ErrorContext(ctx, "job.delete_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		errs = append(errs, fmt.Errorf("delete secret %s: %w", credName(e.JobName), err))
	}
	return errors.Join(errs...)
}

// jobbuilderEntry maps the registry entry to the builder's input view.
func jobbuilderEntry(e registry.TaskEntry) jobbuilder.TaskEntry {
	return jobbuilder.TaskEntry{
		TaskID:          e.TaskID,
		AgentID:         e.AgentID,
		IssueID:         e.IssueID,
		IssueIdentifier: e.IssueIdentifier,
		WorkspaceID:     e.WorkspaceID,
		JobRuntimeID:    e.JobRuntimeID,
		ClaimedAt:       e.ClaimedAt,
		Attempt:         e.Attempt,
	}
}

// credName is the credential Secret of a Job (contract §1.3: fm-<taskid>-cred).
func credName(jobName string) string {
	return jobName + "-cred"
}

// jobImage extracts the agent container's image reference for the job.created
// log event (AC-10: the FOREMAN_JOB_IMAGE value verbatim).
func jobImage(job *batchv1.Job) string {
	if job == nil {
		return ""
	}
	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name == "agent" {
			return c.Image
		}
	}
	return ""
}
