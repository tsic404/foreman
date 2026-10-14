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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"

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

// PrepareLeaseTTL is the server-side prepare lease (§1.1 C3): the claim
// stamps a 45s lease and every C4 renewal restamps it. An entry's
// lease_keep_until (§3.1) is its last successful renewal plus this TTL — a
// local upper bound of the server's own prepare_lease_expires_at, since the
// renewal response carries no expiry and Foreman projects the deadline with
// its own clock after the response returns.
const PrepareLeaseTTL = 45 * time.Second

// Failure reasons carried in the synthesized fail report (C10 body
// failure_reason) for every compensation path (failure-handling 场景矩阵).
const (
	FailureReasonJobCreateFailed = "job_create_failed"
	FailureReasonJobMissing      = "job_missing"
	FailureReasonJobBootTimeout  = "job_boot_timeout"
	FailureReasonJobFailed       = "job_failed"
	FailureReasonJobDeadline     = "job_deadline_exceeded"
	FailureReasonJobEvicted      = "job_evicted"
	// FailureReasonInvalidTaskID is failure-handling scenario #0: the task ID
	// cannot become a legal Job name (05-modules/job-template.md §错误处理).
	FailureReasonInvalidTaskID = "invalid_task_id"
	// FailureReasonStaleClaim is the S8 rejection: the server answered the
	// forwarded start with 409 状态不允许 — the claim is stale or the task
	// is no longer startable (03-contracts.md §1.2 S8, 05-modules/proxy.md
	// §转发). Permanent by that switch, so the task cannot run under this
	// claim and is settled failed instead of being re-dispatched.
	FailureReasonStaleClaim = "stale_claim"
)

// Credential-reclaim reasons (ADR-017): the drain window closed because the
// Job's pods all terminated, or because FOREMAN_JOB_DRAIN_TIMEOUT expired
// first.
const (
	JobTerminationReasonJobTerminated = "job_terminated"
	JobTerminationReasonDrainTimeout  = "drain_timeout"
)

// Pod termination clock domains (contract §7): k8s = the kubelet's
// state.terminated.finishedAt (node clock), observed = the pod objects are
// gone and the fact degrades to Foreman's observation moment (Foreman clock).
// The reclaim log carries the domain so the "reclaim is never earlier than
// pod termination" criterion can be read without a cross-clock comparison.
const (
	PodTerminatedAtSourceK8s      = "k8s"
	PodTerminatedAtSourceObserved = "observed"
)

// podJobNameLabel is the Job controller's back-reference carried by every
// Job pod; it is the ownership key of the termination evidence.
const podJobNameLabel = "job-name"

// jobNameSelector selects the pods of exactly one Job instance's owner name.
func jobNameSelector(jobName string) string {
	return labels.Set{podJobNameLabel: jobName}.String()
}

// claimHead is the subset of the claim payload the scheduler reads to index
// the task (proxy.md 数据结构). Everything else passes through opaque. The
// trigger-context fields are optional: a missing or empty value means that
// context does not exist and never rejects the claim (ADR-014).
type claimHead struct {
	ID              string `json:"id"`
	AgentID         string `json:"agent_id"`
	IssueID         string `json:"issue_id"`
	IssueIdentifier string `json:"issue_identifier"`
	AutopilotRunID  string `json:"autopilot_run_id"`
	ChatSessionID   string `json:"chat_session_id"`
	WorkspaceID     string `json:"workspace_id"`
	// PriorSessionID locates the session this task resumes; it is the soft
	// affinity key source and belongs to no trigger context (ADR-015).
	PriorSessionID string `json:"prior_session_id"`
}

// keyedMutex serializes the per-task state transitions: two concurrent
// claims of the same task must not interleave their
// DeleteSecret→CreateSecret→CreateJob sequence (the loser's AlreadyExists
// would otherwise fail a healthy task), and no settlement path may settle an
// entry while the S8 round that decides its `running` write is still in
// flight (failure-handling §判据模型: one decisive fact per transition).
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

// failReport is a synthetic fail report (C10) for a task the daemon could
// not settle itself. A forward that does not land hands the very same bytes
// to the durable pending-report queue (contract §4), so the report is
// delivered even if this process dies right after.
type failReport struct {
	reason  string
	message string
}

// encode renders the C10 request body.
func (r failReport) encode() []byte {
	// A map[string]string cannot fail to marshal.
	body, _ := json.Marshal(map[string]string{"error": r.message, "failure_reason": r.reason})
	return body
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
// queue; a terminal report whose forward failed outright or exhausted the
// proxy's retry budget is enqueued instead of dropped (contract §4).
func WithPendingReports(p PendingReports) Option {
	return func(s *Scheduler) { s.pendingReports = p }
}

// WithLogger overrides the logger (tests).
func WithLogger(l *slog.Logger) Option {
	return func(s *Scheduler) { s.log = l.With("component", "scheduler") }
}

// WithRevoker wires the Job Token revocation authority the credential
// reclaim feeds (ADR-017). Required: without it the reclaim could delete the
// credential objects but leave the digest valid, which is exactly the
// same-name-reuse face the revocation set exists for.
func WithRevoker(r Revoker) Option {
	return func(s *Scheduler) { s.revoker = r }
}

// Scheduler owns the task mapping state machine and the Job/Secret
// lifecycle. The claim loop lives in proxy; the scheduling actions are here.
type Scheduler struct {
	cfg     Config
	reg     *registry.Registry
	jobs    JobClient
	builder JobBuilder
	server  ServerClient
	revoker Revoker
	metrics Metrics
	log     *slog.Logger

	reconciler     Reconciler
	pendingReports PendingReports
	now            func() time.Time

	// taskMu serializes the per-task transitions (OnClaim and every
	// settlement path): a settlement that ran concurrently with an in-flight
	// S8 round would decide on a registry snapshot the start is about to
	// change (failure-handling §判据模型).
	taskMu *keyedMutex

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

		taskMu: newKeyedMutex(),
	}
	for _, opt := range opts {
		opt(s)
	}
	// Contract §4: a terminal report the server did not take must have a
	// durable home. Without the queue the scheduler could only log it and
	// drop it, so a missing queue is a wiring error, not a mode.
	if s.pendingReports == nil {
		return nil, errors.New("pending-report queue is required")
	}
	// ADR-017: the credential reclaim must be able to revoke the digest it
	// recorded; a no-op revoker would silently reopen the same-name reuse
	// face.
	if s.revoker == nil {
		return nil, errors.New("job token revoker is required")
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

// SyncInflight re-derives foreman_inflight_jobs from the live index. The
// gauge is defined as Registry.Inflight() (observability.md §指标), so it is
// a derived value, never an independently updated one: every settlement path
// re-derives it on exit, and recovery's ticker repeats it every interval.
// That bound is the ticker's, not the round's — a round can sit in its own
// I/O (terminal-report retry budgets, object deletes) for minutes.
func (s *Scheduler) SyncInflight() { s.metrics.InflightJobs(s.reg.Inflight()) }

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
	defer s.SyncInflight()
	var head claimHead
	if err := json.Unmarshal(task, &head); err != nil {
		return fmt.Errorf("parse claim payload head: %w", err)
	}
	if head.ID == "" {
		return errors.New("claim payload has no id")
	}
	unlock := s.taskMu.Lock(head.ID)
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
	jobName := jobNameFor(head.ID)
	e := registry.TaskEntry{
		TaskID:          head.ID,
		AgentID:         head.AgentID,
		IssueID:         head.IssueID,
		IssueIdentifier: head.IssueIdentifier,
		AutopilotRunID:  head.AutopilotRunID,
		ChatSessionID:   head.ChatSessionID,
		WorkspaceID:     head.WorkspaceID,
		JobName:         jobName,
		JobNamespace:    s.cfg.JobNamespace,
		DaemonID:        jobName, // per-Job daemon identity (ADR-003)
		JobRuntimeID:    jobRuntimeID(jobName),
		State:           registry.StatePending,
		Attempt:         attempt,
		ClaimedAt:       now,
		BootDeadline:    now.Add(s.cfg.JobBootTimeout),
		// The claim just stamped the server's 45s prepare lease (§1.1 C3),
		// so the entry starts out with a live deadline and never projects
		// the zero value (contract §3.1).
		LeaseKeepUntil: now.Add(PrepareLeaseTTL),
		Payload:        append(json.RawMessage(nil), task...),
		PayloadVersion: 1,
		// Memory-only: the session key of the Built Job's affinity item
		// (ADR-015) — never annotated, never projected.
		PriorSessionID: head.PriorSessionID,
	}
	if err := s.reg.Put(e); err != nil {
		return fmt.Errorf("register task %s: %w", e.TaskID, err)
	}
	s.log.InfoContext(ctx, "task.claimed",
		"task_id", e.TaskID, "job_name", e.JobName,
		"issue_identifier", e.IssueIdentifier,
		"autopilot_run_id", e.AutopilotRunID, "chat_session_id", e.ChatSessionID,
		"agent_id", e.AgentID)

	// The per-node soft cap is applied right before the Job is rendered
	// ("下一次 claim 前把该节点从候选里排除", task-mapping §同节点并发): the
	// refresh marks the nodes at their cap so the affinity below skips them.
	s.RefreshNodeSaturation(ctx)

	job, secret, tokenHash, err := s.builder.Build(jobbuilderEntry(e))
	if err != nil {
		s.failClaim(ctx, e, err)
		return err
	}
	// token_hash 的唯一写入点 (ADR-017): the digest is recorded on the live
	// entry while the token is still in this process's hands, so the reclaim
	// path can revoke it — including on the failure paths below, which settle
	// the entry before the final Put. The local copy carries it too, because
	// that Put writes the whole entry back. It never reaches an annotation: a
	// rebuilt entry cannot restore it and degrades visibly instead
	// (cred.revoke_unavailable).
	s.reg.SetTokenHash(e.TaskID, tokenHash)
	e.TokenHash = tokenHash
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
	created, err := s.createJob(ctx, e, job)
	if err != nil {
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

	placedAt := s.now()
	// job_created_at is the apiserver's creationTimestamp, never a local
	// clock reading: it is the instance-filter basis that separates this
	// Job's pods from a previous instance's when the name is reused
	// (contract §3.1). Recorded in place: this claim's snapshot predates the
	// Job create, and writing it back wholesale would revert a renewal the
	// LeaseKeeper landed while the name was held.
	marked, ok := s.reg.MarkJobCreated(e.TaskID, created.CreationTimestamp.Time)
	if !ok {
		// The entry left the registry inside the create window — settled
		// (e.g. cancelled while the Job name was held) or reclaimed. The
		// Secret and Job built above then have no owner left, and nothing
		// else reclaims an entry-less Job: failure-handling #16 skips a Job
		// that still exists and ListJobs is read only by the startup rebuild
		// (TaskEntryFromJob), so the objects are rolled back here. The
		// snapshot still carries the identity those rollbacks need.
		return errors.Join(
			fmt.Errorf("task %s settled or vanished before job_created was recorded", e.TaskID),
			s.deleteObjects(ctx, e))
	}
	e = marked
	s.metrics.TaskClaimed()
	// The create latency stays on one clock: claimed_at and the observation
	// are Foreman's, while job_created_at is the apiserver's.
	s.metrics.JobCreateSeconds(placedAt.Sub(e.ClaimedAt).Seconds())
	s.log.InfoContext(ctx, "job.created",
		"task_id", e.TaskID, "job_name", e.JobName,
		"node_name", e.NodeName, "image", jobImage(job))
	return nil
}

// createJob places the entry's Job and returns the object the apiserver
// holds, resolving the two meanings of AlreadyExists. A Job that is there and
// not terminating belongs to this attempt (restart replay, or a retry of the
// same claim) and is kept — its creationTimestamp is the instance basis, so
// the existing object is returned rather than the rendered one. A Job that is
// terminating — or one whose state cannot be read — holds the name
// (job_name = fm-<task_id>, task-mapping invariant 2) until its pods are
// gone; swallowing that would bind the entry to a disappearing Job
// (job_missing). Wait (bounded) and retry; a name that never frees returns
// errJobNameHeld, a deferred claim rather than this task's failure.
func (s *Scheduler) createJob(ctx context.Context, e registry.TaskEntry, job *batchv1.Job) (*batchv1.Job, error) {
	deadline := s.now().Add(jobNameWait)
	// maxPolls backs the clock bound so the loop cannot spin on a clock the
	// caller froze (tests) or on a view that keeps reporting the name as
	// free while Create disagrees.
	maxPolls := int(jobNameWait / jobNamePollInterval)
	for poll := 0; ; poll++ {
		created, err := s.jobs.CreateJob(ctx, job)
		if err == nil {
			return created, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		existing, probeErr := s.jobs.GetJob(ctx, e.JobName)
		if probeErr == nil && existing != nil && existing.DeletionTimestamp == nil {
			return existing, nil
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
			return nil, fmt.Errorf("%w: %s", errJobNameHeld, e.JobName)
		}
		if probeErr == nil && existing == nil {
			// The name just freed (GetJob's nil means the object is gone):
			// retry the create without waiting out a poll interval.
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(jobNamePollInterval):
		}
	}
}

// resolveDuplicateClaim applies the duplicate-claim rule (task-mapping
// §重复 claim): C13 decides the truth. It reports whether the claim proceeds
// as a replacement (old pre-start entry discarded) and the prior attempt.
func (s *Scheduler) resolveDuplicateClaim(ctx context.Context, existing registry.TaskEntry) (bool, int, error) {
	defer s.SyncInflight()
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
		// AlreadyExists Job), so a failed discard needs no retry handle — but
		// the old credential is revoked first, because the replacement Job
		// reuses its name (ADR-017).
		s.discardQuietly(ctx, existing)
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
		// AlreadyExists Job), so a failed discard needs no retry handle — but
		// the old credential is revoked first, and that revoke is the point
		// of this branch: the replacement Job reuses the name, so the old
		// pod must never be able to claim the re-run task (ADR-017).
		s.discardQuietly(ctx, existing)
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

	// The delivered body is handed to the registry so the entry keeps the
	// delivered form, not the claim-time one (registry.MarkDelivering).
	now := s.now()
	e, ok = s.reg.MarkDelivering(daemonID, now, out)
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
// (S8–S15) and returns the upstream status and body for the daemon. The Job
// is deleted only after the terminal report succeeded — never the other way
// around (task-mapping invariant 3) — and the credential then waits for the
// Job's own termination (ADR-017). S8 is the one endpoint
// whose transition follows the answer: 2xx marks the entry running, 409
// settles it through OnStartRejected — and its whole round (forward, answer,
// transition) runs under the task's lock, so no settlement path can settle
// the entry while that answer is still outstanding.
func (s *Scheduler) OnReport(ctx context.Context, ep Endpoint, e registry.TaskEntry, body []byte) (int, []byte, error) {
	unlock := func() {}
	if ep == EPStart {
		unlock = s.taskMu.Lock(e.TaskID)
	}
	defer unlock()
	now := s.now()
	if ep.IsTerminal() {
		// The gauge follows the index on every exit of a terminal report —
		// including the transport-error exit below, which only queues the
		// report: the terminal write above already left the index, so a
		// refresh registered after the forward would leave the pre-terminal
		// value standing (observability.md §指标, AC-10).
		defer s.SyncInflight()
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
		switch {
		case code >= 200 && code < 300:
			s.deleteJob(ctx, e)
		case code == 404:
			// The server already forgot the task: the report is moot and the
			// cleanup idempotent (proxy.md 转发 switch).
			s.deleteJob(ctx, e)
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
	if ep == EPStart {
		// The running write waits for the server's answer: marking the entry
		// running before the forward would leave it running for a task the
		// server never started (409 状态不允许, §1.2 S8 + proxy.md §转发), and
		// the re-dispatched task would be re-claimed into another Job that is
		// refused the same way — an unbounded dispatch loop. A late start on
		// an already settled entry is still forwarded: the 409 tells the
		// daemon the task is settled (the settlement below is a no-op then).
		switch {
		case code >= 200 && code < 300:
			if started, ok := s.reg.MarkStarted(e.TaskID, now); ok {
				s.metrics.JobBootSeconds(now.Sub(started.ClaimedAt).Seconds())
				s.log.InfoContext(ctx, "task.started",
					"task_id", e.TaskID, "job_name", e.JobName,
					"boot_seconds", now.Sub(started.ClaimedAt).Seconds())
			}
			return code, resp, nil
		case code == http.StatusConflict:
			if err := s.OnStartRejected(ctx, e.TaskID); err != nil {
				s.log.ErrorContext(ctx, "task.settle_failed",
					"task_id", e.TaskID, "job_name", e.JobName,
					"endpoint", string(ep), "err", err)
			}
			return code, resp, nil
		}
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
// queue (contract §4: terminal callbacks are never dropped). A write that
// fails is the queue's own error row: it keeps the report staged and retries
// it every drain (错误处理表「退避后重试」), and Queued reports it as
// undelivered so the entry's objects stay put. The error is logged, never
// acted on — dropping the report here is exactly what §4 forbids.
func (s *Scheduler) enqueuePending(ctx context.Context, e registry.TaskEntry, ep Endpoint, body []byte) {
	if err := s.pendingReports.Enqueue(e.TaskID, ep, body); err != nil {
		s.log.ErrorContext(ctx, "task.forward_undelivered",
			"task_id", e.TaskID, "endpoint", string(ep),
			"reason", "pending-report enqueue failed", "err", err)
	}
}

// OnStartRejected settles an entry whose S8 start the server refused with
// 409 (03-contracts.md §1.2 S8 + 05-modules/proxy.md §转发: 状态不允许).
// 顺序规则 3 puts C13 before the compensation: a 409 says "not startable",
// not "not running", so running adopts, terminal settles, 404 releases, and
// only queued/dispatched proves the claim itself stale — that one is settled
// failed (stale_claim) and its Job/Secret go, ending the re-dispatch loop.
//
// The caller holds the task's lock (OnReport's S8 round), so the C13 answer
// and the transition cannot be overtaken by another settlement path.
func (s *Scheduler) OnStartRejected(ctx context.Context, taskID string) error {
	defer s.SyncInflight()
	e, ok := s.reg.Get(taskID)
	if !ok || e.IsTerminal() || !e.StartedAt.IsZero() {
		return nil
	}
	st, err := s.server.TaskStatus(ctx, taskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		return s.OnTaskVanished(ctx, taskID)
	case err != nil:
		return fmt.Errorf("check status of task %s: %w", taskID, err)
	}
	s.reg.SetLastStatusSeen(taskID, st)
	switch {
	case st == "running":
		// The start landed after all: keep the Job, wait for the daemon's
		// reports (failure-handling 场景 #1).
		return s.AdoptRunning(ctx, taskID)
	case registry.IsTerminalResult(st):
		return s.SettleTerminal(ctx, taskID, st)
	case st == "queued" || st == "dispatched":
		return s.failEntry(ctx, e, failReport{
			reason:  FailureReasonStaleClaim,
			message: "server rejected the task start: task claim is stale or task is no longer startable",
		})
	default:
		// waiting_*: no decisive fact (proxy.md §prepare-lease 保活) — the
		// daemon's own reports converge this entry.
		return nil
	}
}

// OnBootTimeout handles failure-handling scenario #4: the Job's daemon never
// started within the boot deadline. The task is failed on the daemon's
// behalf (C10, failure_reason=job_boot_timeout) and the objects removed.
// The task's lock keeps it from settling an entry whose S8 answer is still
// outstanding: that answer either lands first (started_at set, this is a
// no-op) or the start really never happened.
func (s *Scheduler) OnBootTimeout(ctx context.Context, taskID string) error {
	defer s.SyncInflight()
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
	e, ok := s.reg.Get(taskID)
	if !ok || e.IsTerminal() || !e.StartedAt.IsZero() {
		return nil
	}
	return s.failEntry(ctx, e, failReport{
		reason:  FailureReasonJobBootTimeout,
		message: "job daemon did not start before the boot deadline",
	})
}

// ConvergeLeaseRefused settles an entry whose prepare-lease was refused
// (C4 400: the task left the renewable pre-start state). C13 decides:
// running marks the entry started (leasing stops), terminal settles and
// cleans up, a deleted task vanishes; anything else waits for the next
// lease round (proxy.md §prepare-lease 保活).
func (s *Scheduler) ConvergeLeaseRefused(ctx context.Context, taskID string) error {
	defer s.SyncInflight()
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
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
		return s.deleteJob(ctx, e)
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
	defer s.SyncInflight()
	e, ok := s.reg.ByJob(jobName)
	if !ok {
		return nil
	}
	if e.IsTerminal() {
		return s.deleteJob(ctx, e)
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
// status is settled as-is, a task the server deleted settles terminal with an
// empty result (the drain starts either way), and only a still-active task
// gets the synthesized fail.
// Idempotent: an already-terminal entry only retries its Job deletion
// (顺序规则 2).
func (s *Scheduler) FailJob(ctx context.Context, jobName, reason string) error {
	defer s.SyncInflight()
	e, ok := s.reg.ByJob(jobName)
	if !ok {
		return nil
	}
	// The key is the task id, so it is resolved before the lock and the
	// entry re-read under it: a concurrent path must not be settled on a
	// snapshot it already replaced.
	unlock := s.taskMu.Lock(e.TaskID)
	defer unlock()
	e, ok = s.reg.ByJob(jobName)
	if !ok {
		return nil
	}
	if e.IsTerminal() {
		return s.deleteJob(ctx, e)
	}
	st, err := s.server.TaskStatus(ctx, e.TaskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		// Task deleted server-side (scenario #9): settle it without a
		// report and start the drain.
		return s.OnTaskVanished(ctx, e.TaskID)
	case err != nil:
		return fmt.Errorf("check status of task %s: %w", e.TaskID, err)
	}
	s.reg.SetLastStatusSeen(e.TaskID, st)
	if registry.IsTerminalResult(st) {
		return s.SettleTerminal(ctx, e.TaskID, st)
	}
	return s.failEntry(ctx, e, failReport{
		reason:  reason,
		message: "container ended without a terminal report",
	})
}

// failEntry settles a task as failed on the daemon's behalf: the terminal
// write first (markTerminal is the single write path, 顺序规则 1), then the
// C10 report, then the Job deletion. The verdict also starts the drain, so
// the credential and the entry stay until the Job terminates or the upper
// bound expires (ADR-017). A report that does not land is in the durable
// queue now, so the Job stays until a drain lands it (contract §4) —
// deleting it would let a Job deletion stand in for a delivered report.
func (s *Scheduler) failEntry(ctx context.Context, e registry.TaskEntry, report failReport) error {
	s.log.ErrorContext(ctx, "task.failed_compensated",
		"task_id", e.TaskID, "job_name", e.JobName, "failure_reason", report.reason)
	if _, err := s.reg.MarkTerminal(e.TaskID, registry.ResultFailed, s.now()); err != nil {
		return err
	}
	s.metrics.TaskTerminal(string(registry.ResultFailed))
	if err := s.reportFail(ctx, e, report); err != nil {
		return nil
	}
	e, ok := s.reg.Get(e.TaskID)
	if !ok {
		return nil
	}
	return s.deleteJob(ctx, e)
}

// SettleTerminal converges an entry the server already holds as terminal
// (C13 completed/failed/cancelled, or a rebuilt entry whose deletion is
// still pending): the entry is marked terminal and the Job is deleted
// (顺序规则 1) — the credential and the entry then wait for the Job's pods
// to terminate or for the drain upper bound (ADR-017). A terminal report
// still waiting in the durable queue holds the Job back until it lands
// (contract §4): the recovery loop drains the queue every round and the
// round that saw it delivered deletes the Job.
func (s *Scheduler) SettleTerminal(ctx context.Context, taskID, result string) error {
	defer s.SyncInflight()
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	if s.pendingReports.Queued(taskID) {
		// 场景 #7/#10: the objects wait for the report — a deleted Job must
		// never stand in for a delivered one (contract §4).
		return nil
	}
	if !e.IsTerminal() {
		if _, err := s.reg.MarkTerminal(taskID, registry.Result(result), s.now()); err != nil {
			return err
		}
		s.metrics.TaskTerminal(result)
		s.log.InfoContext(ctx, "task.terminal",
			"task_id", e.TaskID, "job_name", e.JobName, "result", result)
	}
	return s.deleteJob(ctx, e)
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

// DiscardTask removes the objects and the mapping of a task that will be
// re-run, without any terminal report and without entering drain
// (failure-handling 场景 #2 redispatch, and the pre-start half of the
// duplicate-claim rule). The successor Job shares the name fm-<task_id>, so
// the predecessor's credential is revoked first: its pod must never be able
// to claim the re-run task (ADR-017 — "宁可噪音、不可重跑"). A rebuilt entry
// has no digest to revoke; that degradation is logged as
// cred.revoke_unavailable. A failed deletion keeps the entry: it is the only
// retry handle, and dropping it would leave a zombie Job or leak the
// credential Secret (顺序规则 2).
func (s *Scheduler) DiscardTask(ctx context.Context, taskID string) error {
	defer s.SyncInflight()
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	s.revokeCredential(ctx, e)
	if err := s.deleteObjects(ctx, e); err != nil {
		return err
	}
	return s.reg.Delete(taskID)
}

// OnTaskVanished settles a task the server deleted (failure-handling 场景
// #9, C13/C4 404). No terminal report is sent for a task the server no
// longer knows, but the verdict still starts the drain: the entry is marked
// terminal with an empty result — the only source of result="" — and the Job
// goes, while the credential and the entry stay until the Job terminates or
// the drain upper bound expires (ADR-017).
func (s *Scheduler) OnTaskVanished(ctx context.Context, taskID string) error {
	defer s.SyncInflight()
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	if !e.IsTerminal() {
		s.log.InfoContext(ctx, "task.vanished",
			"task_id", e.TaskID, "job_name", e.JobName)
		if _, err := s.reg.MarkTerminal(taskID, "", s.now()); err != nil {
			return err
		}
	}
	return s.deleteJob(ctx, e)
}

// revokeCredential feeds the entry's token digest to the revocation set. It
// is the in-process half of the reclaim and must not depend on the K8s API;
// an entry without a digest (rebuilt from annotations) degrades visibly
// (ADR-017 已知边界).
func (s *Scheduler) revokeCredential(ctx context.Context, e registry.TaskEntry) {
	if e.TokenHash == "" {
		s.log.WarnContext(ctx, "cred.revoke_unavailable",
			"task_id", e.TaskID, "job_name", e.JobName)
		return
	}
	s.revoker.Revoke(e.TokenHash)
}

// discardQuietly runs the non-drain discard of a duplicate-claim replacement
// and only logs a failure: the replacement handles leftover objects
// idempotently (delete-then-create Secret, tolerated AlreadyExists Job), and
// the revocation it performed first is what the branch depends on.
func (s *Scheduler) discardQuietly(ctx context.Context, e registry.TaskEntry) {
	if err := s.DiscardTask(ctx, e.TaskID); err != nil {
		s.log.WarnContext(ctx, "task.discard_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
	}
}

// OnJobTerminated is the credential-reclaim entry point — the only one
// (ADR-017). It runs the teardown in its fixed order: ① revoke the digest
// (in-process, no K8s dependency) → ② delete the Secret → ③ drop the entry.
// A Secret deletion that fails keeps the entry in drain for the next round:
// the entry is the only retry handle, and dropping it first would leave an
// orphan Secret with no sweep face. Idempotent; a no-op for an entry that is
// not draining. The verdict-time Job deletion is caught up outside that
// sequence, under the contract §4 report gate — see the body.
//
// podTerminatedAt is the termination fact the caller measured (zero when the
// evidence carries none), orphanPods the pods still alive at reclaim time
// (instance-filtered); both are evidence fields of the cred.reclaimed event
// (observability.md §结构化日志). The clock domain the value belongs to is
// derived here and declared alongside it: the drain upper bound closes the
// window without any termination fact (empty value, observed domain), a
// kubelet-reported finish is the node clock (k8s domain), and a fact that
// exists only as the observation moment is Foreman's clock (observed domain).
func (s *Scheduler) OnJobTerminated(ctx context.Context, jobName, reason string, podTerminatedAt time.Time, orphanPods int) error {
	defer s.SyncInflight()
	e, ok := s.reg.ByJob(jobName)
	if !ok || !e.IsDraining() {
		return nil
	}
	source := PodTerminatedAtSourceK8s
	switch {
	case reason == JobTerminationReasonDrainTimeout:
		// The bound closed the window, not the Job: no termination fact is
		// claimed at all (ADR-017 §决策结果「兜底上界」).
		podTerminatedAt = time.Time{}
		source = PodTerminatedAtSourceObserved
	case podTerminatedAt.IsZero():
		// The pod objects are gone, so the fact degrades to the observation
		// moment — declared as Foreman's clock, never as a kubelet fact.
		podTerminatedAt = s.now()
		source = PodTerminatedAtSourceObserved
	}
	// ① Revoke first: it is in-process and must not depend on the K8s API,
	// so a failing teardown can never leave the credential usable.
	s.revokeCredential(ctx, e)
	// ② The Secret is the credential's carrier; a failure here keeps the
	// entry as the retry handle (顺序与幂等规则 7).
	if err := s.jobs.DeleteSecret(ctx, credName(jobName)); err != nil {
		s.log.ErrorContext(ctx, "cred.reclaim_failed",
			"task_id", e.TaskID, "job_name", jobName, "err", err)
		return err
	}
	// ③ The entry goes last.
	if err := s.reg.Delete(e.TaskID); err != nil {
		return err
	}
	drainSeconds := s.drainSeconds(e.TaskID)
	s.metrics.CredReclaimed(reason, drainSeconds)
	podTerminatedAtField := ""
	if !podTerminatedAt.IsZero() {
		podTerminatedAtField = podTerminatedAt.UTC().Format(time.RFC3339)
	}
	s.log.InfoContext(ctx, "cred.reclaimed",
		"task_id", e.TaskID, "job_name", jobName, "reason", reason,
		"drain_seconds", drainSeconds,
		"pod_terminated_at", podTerminatedAtField,
		"pod_terminated_at_source", source,
		"orphan_pods", orphanPods)

	// The verdict-time Job deletion may still be outstanding: it is held back
	// while a terminal report is queued (contract §4 — a deleted Job must
	// never stand in for a delivered report), so a drain that closed inside
	// that window would otherwise leave the Job with no deleter at all. The
	// catch-up runs after the reclaim sequence, under the same gate, and is
	// deliberately not part of it: a failure neither stalls the credential's
	// bound nor keeps the entry — 顺序与幂等规则 2 owns the leftover Job via
	// its ttlSecondsAfterFinished.
	if !s.pendingReports.Queued(e.TaskID) {
		_ = s.deleteJob(ctx, e)
	}
	return nil
}

// drainSeconds is the time between the task's verdict and its credential
// reclaim — the drain duration the histogram reports. The done index holds
// the verdict moment; it is written once, at the alive → draining
// transition, so repeated terminal reports cannot restart the measurement.
func (s *Scheduler) drainSeconds(taskID string) float64 {
	at, ok := s.reg.DoneAt(taskID)
	if !ok {
		return 0
	}
	return s.now().Sub(at).Seconds()
}

// jobPods is the single ownership + instance filter behind every termination
// fact (task-mapping §内部结构): the pods the Job controller labelled with
// this Job's name, restricted to the current instance — a pod created before
// this Job instance belongs to a reused name's predecessor, whose disposal is
// the revoke-and-discard path, never this Job's credential verdict. since is
// the Job's apiserver creationTimestamp, so both sides of the comparison are
// apiserver timestamps (contract §3.1).
func (s *Scheduler) jobPods(ctx context.Context, jobName string, since time.Time) ([]corev1.Pod, error) {
	pods, err := s.jobs.ListPods(ctx, jobNameSelector(jobName))
	if err != nil {
		return nil, err
	}
	current := make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		if pods[i].CreationTimestamp.Time.Before(since) {
			continue
		}
		current = append(current, pods[i])
	}
	return current, nil
}

// JobTerminated reports whether every pod of the Job has terminated: the pod
// objects are gone, or their phase is Succeeded/Failed. An empty set counts
// as terminated (the objects are gone); an API error is not a fact and is
// returned as such, so a read failure can never be mistaken for a verdict.
func (s *Scheduler) JobTerminated(ctx context.Context, jobName string, since time.Time) (bool, error) {
	pods, err := s.jobPods(ctx, jobName, since)
	if err != nil {
		return false, fmt.Errorf("list pods of job %s: %w", jobName, err)
	}
	for i := range pods {
		if !podTerminated(&pods[i]) {
			return false, nil
		}
	}
	return true, nil
}

// LastPodTerminatedAt is the termination fact of the Job's last pod, taken
// from the kubelet-reported state.terminated.finishedAt (node clock). Zero
// when no pod carries such a fact — the pod objects are gone, or none ever
// reported a finish — which the reclaim reads as "no k8s-domain fact" and
// records as the observation moment in the observed domain (contract §7: the
// clock domain must be declared with the value, because only the observed
// domain compares directly against Foreman's clock).
func (s *Scheduler) LastPodTerminatedAt(ctx context.Context, jobName string, since time.Time) (time.Time, error) {
	pods, err := s.jobPods(ctx, jobName, since)
	if err != nil {
		return time.Time{}, fmt.Errorf("list pods of job %s: %w", jobName, err)
	}
	var last time.Time
	for i := range pods {
		for _, cs := range pods[i].Status.ContainerStatuses {
			terminated := cs.State.Terminated
			if terminated == nil || terminated.FinishedAt.IsZero() {
				continue
			}
			if finished := terminated.FinishedAt.Time; finished.After(last) {
				last = finished
			}
		}
	}
	return last, nil
}

// AlivePods counts the pods of the Job that are still alive at reclaim time
// (orphan_pods): non-zero on the drain_timeout path means the credential was
// reclaimed while a pod was still running — the bound closed the window, not
// the Job.
func (s *Scheduler) AlivePods(ctx context.Context, jobName string, since time.Time) (int, error) {
	pods, err := s.jobPods(ctx, jobName, since)
	if err != nil {
		return 0, fmt.Errorf("list pods of job %s: %w", jobName, err)
	}
	alive := 0
	for i := range pods {
		if !podTerminated(&pods[i]) {
			alive++
		}
	}
	return alive, nil
}

// podTerminated is the pod half of the termination fact: the phase is
// Succeeded or Failed. A terminating pod (deletionTimestamp set, still
// Running) is alive — that is exactly the drain window.
func podTerminated(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// SweepOrphanSecrets deletes credential Secrets left behind by a reclaim a
// restart interrupted (failure-handling 场景 #16). The criterion is
// two-sided: the registry holds no entry for the task-id (any state) AND
// GetJob explicitly answers NotFound. Either half alone would delete a
// Secret that is still needed — the OnClaim CreateSecret → CreateJob window
// has the entry but no Job yet, and a Job without its Secret boots into a
// silent job_auth_failed.
func (s *Scheduler) SweepOrphanSecrets(ctx context.Context) error {
	secrets, err := s.jobs.ListSecrets(ctx)
	if err != nil {
		return fmt.Errorf("list credential secrets: %w", err)
	}
	var firstErr error
	for _, secret := range secrets {
		taskID := secret.Labels[registry.LabelTaskID]
		if taskID == "" {
			continue
		}
		if _, ok := s.reg.Get(taskID); ok {
			continue
		}
		jobName := jobNameFor(taskID)
		job, err := s.jobs.GetJob(ctx, jobName)
		if err != nil {
			// An API error is not a NotFound: the criterion demands an
			// explicit one, so the Secret stays this round.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if job != nil {
			continue
		}
		if err := s.jobs.DeleteSecret(ctx, secret.Name); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		s.log.InfoContext(ctx, "cred.orphan_secret_removed",
			"task_id", taskID, "job_name", jobName, "secret", secret.Name)
	}
	return firstErr
}

// Reconcile converges the live index with the server. With a recovery
// reconciler wired it delegates; otherwise it runs the built-in startup
// convergence (task-mapping §持久化与重建 step 3) over every live entry.
// The per-node soft cap and the inflight gauge are refreshed first: both are
// derived state of the live index and must not outlive the load they describe.
func (s *Scheduler) Reconcile(ctx context.Context) error {
	s.RefreshNodeSaturation(ctx)
	s.SyncInflight()
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
		return s.OnTaskVanished(ctx, e.TaskID)
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
		// Not started and the payload is gone with the restart: discard the
		// objects so the server re-dispatches after its reclaim window. This
		// path deliberately does not enter drain — the task will be re-run
		// (ADR-017).
		return s.DiscardTask(ctx, e.TaskID)
	case registry.IsTerminalResult(st):
		return s.SettleTerminal(ctx, e.TaskID, st)
	default:
		// e.g. waiting_local_directory: no decisive fact yet — keep waiting
		// (failure-handling 判据模型).
		return nil
	}
}

// failClaim settles a task whose Job could not be created
// (pending → terminal, result=failed) and tells the server. A report whose
// forward does not land goes to the durable queue, and the terminal entry
// stays until a round drains it (contract §4).
func (s *Scheduler) failClaim(ctx context.Context, e registry.TaskEntry, cause error) {
	defer s.SyncInflight()
	if _, err := s.reg.MarkTerminal(e.TaskID, registry.ResultFailed, time.Time{}); err != nil {
		s.log.ErrorContext(ctx, "mark terminal failed", "task_id", e.TaskID, "err", err)
	}
	s.metrics.TaskTerminal(string(registry.ResultFailed))
	// An invalid task ID is not a create failure: the Job must not be
	// retried, the operator has to fix the task (failure-handling #0).
	reason := FailureReasonJobCreateFailed
	if errors.Is(cause, jobbuilder.ErrInvalidTaskID) {
		reason = FailureReasonInvalidTaskID
	}
	s.log.ErrorContext(ctx, "task.failed_compensated",
		"task_id", e.TaskID, "job_name", e.JobName, "failure_reason", reason, "err", cause.Error())
	report := failReport{reason: reason, message: cause.Error()}
	// A report whose forward does not land goes to the durable queue
	// (场景 #10); the entry stays terminal until a round drains it.
	_ = s.reportFail(ctx, e, report)
}

// reportFail forwards a synthetic fail report (C10) for a task the daemon
// could not settle itself. A 404/409 means the server already settled it, so
// it counts as delivered. A forward that does not land is handed to the
// durable queue (场景 #10), so the report survives this process.
func (s *Scheduler) reportFail(ctx context.Context, e registry.TaskEntry, report failReport) error {
	body := report.encode()
	code, _, err := s.server.Forward(ctx, EPFail, e.TaskID, body)
	if err == nil && ((code >= 200 && code < 300) || code == 404 || code == 409) {
		return nil
	}
	if err == nil {
		err = fmt.Errorf("server returned %d", code)
	}
	s.log.ErrorContext(ctx, "task.forward_failed",
		"task_id", e.TaskID, "endpoint", string(EPFail), "status", code, "err", err)
	s.enqueuePending(ctx, e, EPFail, body)
	return err
}

// deleteJob removes the Job object of a settled task — the "撤下调度与资源"
// half of the verdict (顺序规则 1: the state is written first). The credential
// and the entry stay: they are reclaimed when the Job terminates or the drain
// upper bound expires (ADR-017). The entry is kept when the deletion fails so
// the next round retries it (顺序规则 2).
func (s *Scheduler) deleteJob(ctx context.Context, e registry.TaskEntry) error {
	if err := s.jobs.DeleteJob(ctx, e.JobName); err != nil {
		s.log.ErrorContext(ctx, "job.delete_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		return fmt.Errorf("delete job %s: %w", e.JobName, err)
	}
	return nil
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
		AutopilotRunID:  e.AutopilotRunID,
		ChatSessionID:   e.ChatSessionID,
		WorkspaceID:     e.WorkspaceID,
		JobRuntimeID:    e.JobRuntimeID,
		ClaimedAt:       e.ClaimedAt,
		Attempt:         e.Attempt,
		PriorSessionID:  e.PriorSessionID,
	}
}

// jobNameFor is the single source of the Job name shape (task-mapping
// invariant 2: job_name = daemon_id = fm-<task_id>).
func jobNameFor(taskID string) string {
	return "fm-" + taskID
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
