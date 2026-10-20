// Package scheduler orchestrates the task→Job mapping: it decides how many
// tasks may run concurrently, creates and deletes Jobs/Secrets, advances the
// state machine, settles terminal tasks, and rebuilds the mapping from K8s
// after a restart. Design: 02-contracts.md §4 (mapping table, state machine).
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
	"unicode/utf8"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

// jobGoneGracePeriod tolerates the first-visibility delay of a freshly
// created Job before the missing-Job compensation engages (§4.6).
const jobGoneGracePeriod = 60 * time.Second

// malformedClaimLogInterval bounds the log noise of a server that keeps
// re-dispatching a claim Foreman cannot name: the loop is unbounded, the log
// is not. The metric counts every occurrence.
const malformedClaimLogInterval = time.Minute

// jobNamePollInterval is the cadence at which a claim retries a Job create
// whose name is still held by a foreground-terminating predecessor.
const jobNamePollInterval = 500 * time.Millisecond

// jobNameWait bounds that retry. The Job controller deletes the pods first
// and the Job object follows once they are gone, normally within the 30s
// terminationGracePeriodSeconds of the template (§8.7).
const jobNameWait = 45 * time.Second

// errJobNameHeld reports that a Job name was still held after jobNameWait (by
// a terminating predecessor or an object this task does not own): the claim is
// deferred to the server's re-dispatch, never reported as this task's failure.
var errJobNameHeld = errors.New("job name still held")

// PrepareLeaseTTL is the server-side prepare lease (C-3): the claim
// stamps a 45s lease and every C-4 renewal restamps it. An entry's
// lease_keep_until (§10) is its last successful renewal plus this TTL — a
// local upper bound of the server's own prepare_lease_expires_at, since the
// renewal response carries no expiry and Foreman projects the deadline with
// its own clock after the response returns.
const PrepareLeaseTTL = 45 * time.Second

// Failure reasons carried in the synthesized fail report (C10 body
// failure_reason) for every compensation path (§4.4).
const (
	FailureReasonJobCreateFailed = "job_create_failed"
	FailureReasonJobMissing      = "job_missing"
	FailureReasonJobBootTimeout  = "job_boot_timeout"
	FailureReasonJobFailed       = "job_failed"
	FailureReasonJobDeadline     = "job_deadline_exceeded"
	FailureReasonJobEvicted      = "job_evicted"
	// FailureReasonInvalidTaskID is the invalid-name form: the task ID
	// cannot become a legal Job name (§8.1, §4.4).
	FailureReasonInvalidTaskID = "invalid_task_id"
	// FailureReasonStaleClaim is the S-8 rejection: the server answered the
	// forwarded start with 409 状态不允许 — the claim is stale or the task
	// is no longer startable (S-8, §1.5). Permanent by that switch, so the
	// task cannot run under this claim and is settled failed instead of
	// being re-dispatched.
	FailureReasonStaleClaim = "stale_claim"
	// FailureReasonDaemonNotRegistered is the S-4 refusal: the Job's daemon
	// declared no runtime for the provider its claim was dispatched under, so
	// the task is settled instead of waiting out the boot deadline (S-4,
	// §2.3).
	FailureReasonDaemonNotRegistered = "daemon_not_registered"
	// FailureReasonDeliveryUnconfirmed is the S-6 lost-response form: the
	// delivery was committed but no start fact arrived within the delivery
	// confirmation window, so the payload never reached the daemon
	// (S-6, §5, ADR-032). It is a transport verdict, not a Job
	// one — the Job was created, its pod ran and its daemon registered.
	FailureReasonDeliveryUnconfirmed = "delivery_unconfirmed"
)

// job.create_step step values: the claim chain's five steps, in order
// (§9.1). A step's trace line is emitted once the step finished, a failed
// step included.
const (
	createStepBuild            = "build"
	createStepClearStaleSecret = "clear_stale_secret"
	createStepCreateSecret     = "create_secret"
	createStepCreateJob        = "create_job"
	createStepJobCreated       = "job_created"
)

// job.create_failed phase values: the step whose failure settled the claim —
// the credential Secret was never created, or the Job object was not placed.
const (
	createPhaseSecret = "secret"
	createPhaseJob    = "job"
)

// Credential-reclaim reasons (ADR-017): the drain window closed because the
// Job's pods all terminated, or because FOREMAN_JOB_DRAIN_TIMEOUT expired
// first.
const (
	JobTerminationReasonJobTerminated = "job_terminated"
	JobTerminationReasonDrainTimeout  = "drain_timeout"
	// JobTerminationReasonNoObjects is the reclaim reason of an entry whose
	// instance never produced a Job or Secret (an invalid task id, or a claim
	// payload that could not be mapped): there is no termination fact to wait
	// for and no object to inspect.
	JobTerminationReasonNoObjects = "no_objects"
)

// Pod termination clock domains (§10): k8s = the kubelet's
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
// the task. Everything else passes through opaque. The
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
	// Agent is the payload's agent object: its name is the agent axis's
	// keyspace and the <agent-segment> / <agent-name> source (ADR-035 keyspace
	// = names). Absent or nameless means that axis has no identity.
	Agent struct {
		Name string `json:"name"`
	} `json:"agent"`
	// PriorSessionID locates the session this task resumes; it is the soft
	// affinity key source and belongs to no trigger context (ADR-015).
	PriorSessionID string `json:"prior_session_id"`
	// RuntimeID is the real runtime the server dispatched the claim under:
	// it fixes the Job's provider and addresses C4/C19 (ADR-025).
	RuntimeID string `json:"runtime_id"`
}

// keyedMutex serializes the per-task state transitions: two concurrent
// claims of the same task must not interleave their
// DeleteSecret→CreateSecret→CreateJob sequence (the loser's AlreadyExists
// would otherwise reclaim the winner's live Job), and no settlement path may settle an
// entry while the S-8 round that decides its `running` write is still in
// flight (§4.2: one decisive fact per transition).
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
// to the durable pending-report queue (§10), so the report is
// delivered even if this process dies right after.
//
// sessionID is the raw session value the learning slot already read from the
// S13/S14 body (ADR-031): a suppression of the daemon's self-report would
// otherwise lose the server's session record and the resuming task's
// prior_session_id affinity. The convergence path has no session value to
// carry — the daemon never reported — so it stays empty there.
type failReport struct {
	reason    string
	message   string
	sessionID string
}

// encode renders the C10 request body. session_id appears only when a raw
// value exists; the no-session form keeps the existing two-key shape.
func (r failReport) encode() []byte {
	fields := map[string]string{"error": r.message, "failure_reason": r.reason}
	if r.sessionID != "" {
		fields["session_id"] = r.sessionID
	}
	// A map[string]string cannot fail to marshal.
	body, _ := json.Marshal(fields)
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

// WithPendingReports wires the durable terminal-report queue; a terminal
// report whose forward failed outright or exhausted the proxy's retry budget
// is enqueued instead of dropped (§10).
func WithPendingReports(p PendingReportQueue) Option {
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

// RuntimeIdentityResolver translates a claim's real runtime id into the
// declared identity it was dispatched under (registry.RuntimeIdentityKey).
// The proxy owns the declaration and supplies it; a nil resolver — or one that
// cannot resolve the id — leaves the entry without an identity, which S-4
// refuses rather than guesses (§2.3 零命中即拒绝注册).
type RuntimeIdentityResolver func(runtimeID string) (identity string, ok bool)

// WithRuntimeIdentity wires the claim-time identity resolution: the entry
// carries the identity for its whole life, so a later rebuild of the runtime
// set (ids move, identities do not) cannot blind S-4's selection.
func WithRuntimeIdentity(r RuntimeIdentityResolver) Option {
	return func(s *Scheduler) { s.runtimeIdentity = r }
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

	probes         *registry.ProbeRegistry
	reconciler     Reconciler
	pendingReports PendingReportQueue
	now            func() time.Time
	// runtimeIdentity resolves a claim's real runtime id into its declared
	// identity; nil leaves entries without one (S-4 then refuses).
	runtimeIdentity RuntimeIdentityResolver

	// attributionWait is the attribution gate's wait upper bound (ADR-031):
	// the implementation constant defaultAttributionWait. It is kept as a
	// field only so package tests can shrink it — it is not a configuration
	// item and no env carries it.
	attributionWait time.Duration

	// beforeJobLock is a test seam in OnJobTerminated between the Job-name
	// resolution and the task lock: it lets a test land a successor entry in
	// that window. Nil in production; the nil check is its only cost.
	beforeJobLock func()

	// taskMu serializes the per-task transitions (OnClaim and every
	// settlement path): a settlement that ran concurrently with an in-flight
	// S-8 round would decide on a registry snapshot the start is about to
	// change (§4.2).
	taskMu *keyedMutex

	// nodeMu serializes the per-node soft cap refresh: concurrent claims
	// would otherwise interleave the marker replacement and log the same
	// transition twice.
	nodeMu sync.Mutex

	// lifecycle is the process context Start was called with; detached probe
	// trackers derive from it so shutdown stops them. Nil before Start (tests
	// calling StartProbe directly).
	lifecycleMu sync.Mutex
	lifecycle   context.Context

	// malformedClaimLoggedAt rate-limits the unnameable-claim log.
	malformedClaimMu       sync.Mutex
	malformedClaimLoggedAt time.Time

	// project memoizes the read-only project reverse lookup per issue id, so a
	// burst of tasks from one issue does not query the platform once per task
	// (ADR-048 decision 4 — the cache is not a contract face).
	project projectCache

	// usageMu guards usagePairs, the token metric's admitted (provider, model)
	// label space (§9.2). Both values come from a task-controlled S-12 body,
	// so the space is bounded rather than trusted.
	usageMu    sync.Mutex
	usagePairs map[usageLabelPair]struct{}
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

		attributionWait: defaultAttributionWait,
		usagePairs:      make(map[usageLabelPair]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	// Contract §10: a terminal report the server did not take must have a
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
	// The catalog probe's identity, command delivery and result routing all
	// live in the probe session table; without it a probe could never register.
	if s.probes == nil {
		return nil, errors.New("probe session table is required")
	}
	return s, nil
}

// Start installs the process lifecycle, rebuilds the index from K8s
// (ADR-002) and converges once against the server. It returns only after the
// rebuild is complete, and callers must not start a claim or recovery loop
// before that: a Job created by a claim can be listed by the rebuild, and the
// payload-less projection would overwrite the live entry (§4.3).
func (s *Scheduler) Start(ctx context.Context) error {
	s.lifecycleMu.Lock()
	s.lifecycle = ctx
	s.lifecycleMu.Unlock()
	if err := s.Rebuild(ctx); err != nil {
		return fmt.Errorf("rebuild registry from k8s jobs: %w", err)
	}
	if err := s.Reconcile(ctx); err != nil {
		s.log.WarnContext(ctx, "initial reconcile failed", "err", err)
	}
	return nil
}

// ClaimBudget is how many tasks the proxy may claim right now:
// min(max-inflight − inflight, claim-batch-max). Queueing stays on the
// server; Foreman never pre-fetches (AC-02).
func (s *Scheduler) ClaimBudget() int {
	free := s.cfg.MaxInflightJobs - s.reg.Inflight()
	if free < 0 {
		free = 0
	}
	return min(free, s.cfg.ClaimBatchMax)
}

// SyncInflight re-derives foreman_inflight_jobs from the live index. The
// gauge is defined as Registry.Inflight() (§9.2), so it is
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
// preference, not admission control (§8.6, AC-02).
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
	// operator needs (§9.1).
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

// createSteps traces the claim chain's steps (job.create_step, debug, at
// FOREMAN_LOG_LEVEL=debug): each call closes the step it names and emits one
// line carrying that step's own duration. A failed step is traced the same
// way — job.create_failed is the failure's event, so the step line never
// repeats its error field.
func (s *Scheduler) createSteps(ctx context.Context, e registry.TaskEntry) func(step string) {
	last := s.now()
	return func(step string) {
		at := s.now()
		s.log.DebugContext(ctx, "job.create_step",
			"task_id", e.TaskID, "job_name", e.JobName,
			"step", step, "duration_ms", at.Sub(last).Milliseconds())
		last = at
	}
}

// logJobCreateFailed records the claim chain's failure (job.create_failed,
// error). phase names the step that failed; err carries the summary only —
// never a payload or a credential.
func (s *Scheduler) logJobCreateFailed(ctx context.Context, e registry.TaskEntry, phase string, cause error) {
	s.log.ErrorContext(ctx, "job.create_failed",
		"task_id", e.TaskID, "job_name", e.JobName, "phase", phase, "err", cause.Error())
}

// OnClaim registers a freshly claimed task and creates its Secret + Job
// (pending → job_created). A task the registry already knows goes through
// the duplicate-claim rule; no second Job is ever created concurrently.
func (s *Scheduler) OnClaim(ctx context.Context, task json.RawMessage) error {
	defer s.SyncInflight()
	var head claimHead
	if err := json.Unmarshal(task, &head); err != nil {
		// The full head parse failed. A tolerant id read is the difference
		// between a dead letter (the task can be settled on the server) and an
		// unnameable claim (only the lease/expiry converges).
		if id := claimID(task); id != "" {
			return s.settleMalformedClaim(ctx, id, task, err)
		}
		s.noteUnnameableClaim(ctx, err, len(task))
		return fmt.Errorf("%w: %v", ErrUnnameableClaim, err)
	}
	if head.ID == "" {
		err := errors.New("claim payload has no id")
		s.noteUnnameableClaim(ctx, err, len(task))
		return fmt.Errorf("%w: %v", ErrUnnameableClaim, err)
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
	// Identity is derived here and never follows the display name: daemon_id =
	// fm-<task_id> is the daemon-id label, the credential Secret's base, the
	// Job Token's j claim and the uuid5 seed of job_runtime_id (ADR-034
	// decision 8). The display name is the template's business, so it is
	// resolved through the builder — the same function Build runs.
	daemonID := jobbuilder.DaemonIDFor(head.ID)
	e := registry.TaskEntry{
		TaskID:          head.ID,
		AgentID:         head.AgentID,
		IssueID:         head.IssueID,
		IssueIdentifier: head.IssueIdentifier,
		AutopilotRunID:  head.AutopilotRunID,
		ChatSessionID:   head.ChatSessionID,
		WorkspaceID:     head.WorkspaceID,
		JobNamespace:    s.cfg.JobNamespace,
		DaemonID:        daemonID, // per-Job daemon identity (ADR-003)
		JobRuntimeID:    jobRuntimeID(daemonID),
		State:           registry.StatePending,
		Attempt:         attempt,
		ClaimedAt:       now,
		// The claim just stamped the server's 45s prepare lease (C-3),
		// so the entry starts out with a live deadline and never projects
		// the zero value (§10).
		LeaseKeepUntil: now.Add(PrepareLeaseTTL),
		Payload:        append(json.RawMessage(nil), task...),
		// Memory-only: the session key of the Built Job's affinity item
		// (ADR-015) — never annotated, never projected.
		PriorSessionID: head.PriorSessionID,
		// Memory-only: the two identity inputs of the three-axis policy
		// (ADR-048) — the agent name from the payload and the platform project
		// identifier from the payload's top-level key or the read-only lookup,
		// both filled in below once the policy table has had its say.
		AgentName: head.Agent.Name,
		// Memory-only: the runtime the server dispatched this claim under,
		// which addresses C4/C19 and the S8 body translation (ADR-025).
		RealRuntimeID: head.RuntimeID,
		// Memory-only: the same dispatch record as an identity, so S-4 can
		// match the Job's reported runtimes long after a rebuild moved the id
		// (§2.3, ADR-043).
		RuntimeIdentity: s.resolveRuntimeIdentity(head.RuntimeID),
	}
	// A switched-off bound (0 = no upper bound) writes no deadline at
	// all: the zero value is "no bound", never an expired one, and must not
	// degrade to claimed_at (§4.2, ADR-026).
	if s.cfg.JobBootTimeout > 0 {
		e.BootDeadline = now.Add(s.cfg.JobBootTimeout)
	}
	// The name format was validated at startup, so this is the render-time
	// final check; a failure refuses the claim before any object exists.
	// The project identity is resolved only when a declared tier reads it
	// (ADR-048 decision 4), and always before the name, which the policy
	// cannot influence.
	if s.builder.RequiresProjectIdentity(jobbuilderEntry(e)) {
		e.ProjectID, e.ProjectSource = s.projectIdentity(ctx, task, head)
	}
	name, nameErr := s.builder.ResolveName(jobbuilderEntry(e))
	if nameErr != nil {
		// No object will exist under a name that could not be composed, so the
		// entry carries the identity name for attribution only. An empty
		// JobName would leave the registry's byJob index without a key, and a
		// terminal + draining row is only ever reclaimed through a Job-name
		// lookup (OnJobTerminated) — it would leak forever.
		name = daemonID
	}
	e.JobName = name
	if err := s.reg.Put(e); err != nil {
		return fmt.Errorf("register task %s: %w", e.TaskID, err)
	}
	s.log.InfoContext(ctx, "task.claimed",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"issue_identifier", e.IssueIdentifier,
		"autopilot_run_id", e.AutopilotRunID, "chat_session_id", e.ChatSessionID,
		"agent_id", e.AgentID)
	if nameErr != nil {
		s.failClaim(ctx, e, nameErr)
		return nameErr
	}

	// The per-node soft cap is applied right before the Job is rendered
	// ("下一次 claim 前把该节点从候选里排除", §8.6, AC-02): the
	// refresh marks the nodes at their cap so the affinity below skips them.
	s.RefreshNodeSaturation(ctx)

	trace := s.createSteps(ctx, e)
	job, secret, tokenHash, err := s.builder.Build(jobbuilderEntry(e))
	trace(createStepBuild)
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
	// (NotFound counts as success inside the JobClient). The clear is
	// best-effort and does not settle the claim — the CreateSecret below
	// surfaces residue or a denied permission as a phase=secret failure.
	_ = s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(daemonID))
	trace(createStepClearStaleSecret)
	err = s.jobs.CreateSecret(ctx, secret)
	trace(createStepCreateSecret)
	if err != nil {
		cause := fmt.Errorf("create credential secret: %w", err)
		s.logJobCreateFailed(ctx, e, createPhaseSecret, cause)
		s.failClaim(ctx, e, cause)
		return err
	}
	created, err := s.createJob(ctx, e, job)
	trace(createStepCreateJob)
	if err != nil {
		// Nothing of this claim is left in the cluster: drop the credential
		// Secret again. A name still held by a terminating predecessor is
		// not this task's failure — the entry goes and the server
		// re-dispatches the task once the lease lapses, the same recovery
		// as an undeliverable claim (§4.3).
		_ = s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(daemonID))
		if errors.Is(err, errJobNameHeld) {
			_ = s.reg.Delete(e.TaskID)
			s.metrics.InflightJobs(s.reg.Inflight())
			s.log.WarnContext(ctx, "task.claim_deferred",
				"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
				"err", err.Error())
			return err
		}
		cause := fmt.Errorf("create job: %w", err)
		s.logJobCreateFailed(ctx, e, createPhaseJob, cause)
		s.failClaim(ctx, e, cause)
		return err
	}

	placedAt := s.now()
	// job_created_at is the apiserver's creationTimestamp, never a local
	// clock reading: it is the instance-filter basis that separates this
	// Job's pods from a previous instance's when the name is reused
	// (§4.1). Recorded in place: this claim's snapshot predates the
	// Job create, and writing it back wholesale would revert a renewal the
	// LeaseKeeper landed while the name was held.
	marked, ok := s.reg.MarkJobCreated(e.TaskID, created.CreationTimestamp.Time)
	trace(createStepJobCreated)
	if !ok {
		// The entry left the registry inside the create window — settled
		// (e.g. cancelled while the Job name was held) or reclaimed. The
		// Secret and Job built above then have no owner left, and nothing
		// else reclaims an entry-less Job: §6.5 skips a Job
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
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"node_name", e.NodeName, "image", jobImage(job))
	return nil
}

// createJob places the entry's Job and returns the object the apiserver
// holds. AlreadyExists is never an adoption: adoption is the rebuild row of
// the disposal table and needs the payload (§4.3), while this claim carries no
// facts of any existing Job — the object is a predecessor corpse. It is
// deleted and the create retried within jobNameWait; adopting it would burn
// the whole boot bound and mix the pod instance domains (AC-08). A terminating
// object holds its name until its pods are gone (bounded wait, then
// errJobNameHeld — a deferred claim, never this task's failure).
func (s *Scheduler) createJob(ctx context.Context, e registry.TaskEntry, job *batchv1.Job) (*batchv1.Job, error) {
	deadline := s.now().Add(jobNameWait)
	// maxPolls backs the clock bound so the loop cannot spin on a clock the
	// caller froze (tests) or on a view that keeps reporting the name as
	// free while Create disagrees.
	maxPolls := int(jobNameWait / jobNamePollInterval)
	// staleDeleteErr keeps the last failed attempt to free the name, so a
	// deferred claim carries its cause.
	var staleDeleteErr error
	for poll := 0; ; poll++ {
		created, err := s.jobs.CreateJob(ctx, job)
		if err == nil {
			return created, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		existing, probeErr := s.jobs.GetJob(ctx, e.JobName)
		if probeErr != nil {
			// Truth unknown: the name may still be held, so keep polling.
			// Transient read failures stay in Debug (cf. prepare-lease);
			// task.claim_deferred at the bound is the alert, and the outcome
			// here is a deferred claim, never a task failure (AC-09).
			s.log.DebugContext(ctx, "job.probe_failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", probeErr)
		} else if existing != nil && existing.DeletionTimestamp == nil {
			if !jobOwnedBy(existing, e) {
				// The name is not a locating face for deletion (§3): an
				// object this task does not own is never removed. Defer the
				// claim instead; the occupied name stays visible through
				// task.claim_deferred.
				return nil, fmt.Errorf("%w: %s (held by an object this task does not own)", errJobNameHeld, e.JobName)
			}
			if derr := s.jobs.DeleteJob(ctx, e.JobName); derr != nil {
				staleDeleteErr = derr
				s.log.DebugContext(ctx, "job.stale_delete_failed",
					"task_id", e.TaskID, "job_name", e.JobName, "err", derr)
			} else {
				staleDeleteErr = nil
				s.log.InfoContext(ctx, "job.stale_deleted",
					"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID)
			}
		}
		if poll >= maxPolls || s.now().After(deadline) {
			if staleDeleteErr != nil {
				return nil, fmt.Errorf("%w: %s (delete stale job: %v)", errJobNameHeld, e.JobName, staleDeleteErr)
			}
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

// jobOwnedBy reports whether the existing object is a Foreman task Job of this
// entry's task. Ownership is read from labels, never from the name (§3): the
// name only says which object the create collided with.
func jobOwnedBy(existing *batchv1.Job, e registry.TaskEntry) bool {
	labels := existing.GetLabels()
	return labels[registry.LabelManagedBy] == registry.ManagedByForeman &&
		labels[registry.LabelTaskID] == e.TaskID
}

// resolveDuplicateClaim applies the duplicate-claim rule (§10): C13 decides
// the truth. It reports whether the claim proceeds as a replacement (old
// pre-start entry discarded) and the prior attempt.
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
		// The task no longer exists server-side: this is the vanished face of
		// §4.6 (the status query reported it absent), not the pre-start
		// replacement — the task will not be re-run, so its entry is settled
		// terminal with an empty result and enters drain; the Job goes and
		// the credential is reclaimed when it terminates (ADR-017). The new
		// claim is dropped: nothing is built for a deleted task.
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "drop_task_gone")
		return false, 0, s.onTaskVanishedLocked(ctx, existing.TaskID)
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
		// C13 says pre-start, but the round snapshot is not the decision:
		// the start fact is re-read from the live entry under the task lock
		// (ADR-026 决策 5). A start that landed while C13 was in flight
		// keeps the old attempt — a replacement Job would double-run it.
		if !s.discardQuietly(ctx, existing) {
			s.log.WarnContext(ctx, "duplicate_dispatch",
				"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "keep_started")
			return false, 0, nil
		}
		// The claim proceeds as a replacement: leftover objects are handled
		// idempotently by the new claim (delete-then-create Secret, tolerated
		// AlreadyExists Job), so a failed discard needs no retry handle — but
		// the old credential is revoked first, and that revoke is the point
		// of this branch: the replacement Job reuses the name, so the old
		// pod must never be able to claim the re-run task (ADR-017).
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", existing.TaskID, "old_job_name", existing.JobName, "action", "replace_pre_start")
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
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID)
		return nil, nil
	}

	// The only rewritten field (S-6); every other field keeps
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
	// The entry's pre-commit face is the one that says whether this claim is
	// the first hand-over or a re-delivery the expired window released.
	prevDelivered := e.DeliveredAt
	prevClaims := e.ClaimsAfterDelivery
	now := s.now()
	e, ok = s.reg.MarkDelivering(daemonID, now, out)
	if !ok {
		return nil, nil
	}
	s.reg.SetRuntimeDeliveredTask(e.JobRuntimeID, e.TaskID)
	s.log.Info("task.delivered",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"deliver_latency_ms", now.Sub(e.ClaimedAt).Milliseconds())
	if e.Deliveries > 1 {
		// The window's exit (ADR-032): the delivery committed at
		// delivered_at was never confirmed, and this claim released it. The
		// claim itself is the evidence that the daemon is not holding that
		// copy — a Job daemon claims only with a free task slot, and it holds
		// one for the whole task.
		s.log.Warn("task.delivery_rearmed",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"delivered_at", registry.ProjectedTime(prevDelivered),
			"window_seconds", e.DeliveryDeadline.Sub(e.DeliveredAt).Seconds(),
			"claims_after_delivery", prevClaims,
			"deliveries", e.Deliveries)
	}
	return []json.RawMessage{out}, nil
}

// usageReport is the token-usage shape the upstream daemon sends on S-12.
// Only the metric's label values and the four token counters are read; the
// optional cost figure stays with the upstream.
type usageReport struct {
	Usage []struct {
		Provider         string `json:"provider"`
		Model            string `json:"model"`
		InputTokens      int64  `json:"input_tokens"`
		OutputTokens     int64  `json:"output_tokens"`
		CacheReadTokens  int64  `json:"cache_read_tokens"`
		CacheWriteTokens int64  `json:"cache_write_tokens"`
	} `json:"usage"`
}

// The S-12 body is the Job daemon's own report, so its provider and model
// strings are task-controlled. Both are cut to usageLabelMaxBytes and the
// admitted label pairs are capped at usageSeriesLimit: unbounded, one task
// could pin arbitrary label values and series in the process's memory. An
// entry past the bound still counts under usageOverflowLabel, so the tokens
// are never lost — only their label granularity.
const (
	usageLabelMaxBytes = 64
	usageSeriesLimit   = 1024
	usageOverflowLabel = "other"
)

// usageLabelPair is the token metric's label key (§9.2). It is a struct, not a
// separator-joined string: both values come from a task-controlled body, so a
// pair like ("a\x00b", "c") and ("a", "b\x00c") would share one joined key and
// one budget slot.
type usageLabelPair struct {
	provider string
	model    string
}

// recordTokenUsage accumulates one S-12 report's entries into the token
// metric (§9.2 / ADR-009 decision 4). A body that does not decode is
// forwarded verbatim and contributes nothing: the upstream owns its
// validation, and a metric must never change the forward's outcome.
func (s *Scheduler) recordTokenUsage(body []byte) {
	var report usageReport
	if err := json.Unmarshal(body, &report); err != nil {
		return
	}
	for _, u := range report.Usage {
		provider, model := s.usageLabels(u.Provider, u.Model)
		s.metrics.TokenUsage(provider, model,
			u.InputTokens, u.OutputTokens, u.CacheReadTokens, u.CacheWriteTokens)
	}
}

// usageLabels admits one report entry's provider and model into the token
// metric's label space, returning the values the metric records.
func (s *Scheduler) usageLabels(provider, model string) (string, string) {
	provider, model = boundUsageLabel(provider), boundUsageLabel(model)
	key := usageLabelPair{provider: provider, model: model}
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	if _, ok := s.usagePairs[key]; ok {
		return provider, model
	}
	if len(s.usagePairs) >= usageSeriesLimit {
		return usageOverflowLabel, usageOverflowLabel
	}
	s.usagePairs[key] = struct{}{}
	return provider, model
}

// boundUsageLabel cuts one label value to usageLabelMaxBytes at a rune
// boundary: the body is decoded JSON, so its values are valid UTF-8, and a cut
// inside a sequence would be the one place this process emits an invalid one.
func boundUsageLabel(v string) string {
	if len(v) <= usageLabelMaxBytes {
		return v
	}
	cut := usageLabelMaxBytes
	for cut > 0 && !utf8.RuneStart(v[cut]) {
		cut--
	}
	return v[:cut]
}

// OnReport applies the state transition for a forwarded lifecycle callback
// (S8–S15) and returns the upstream status and body for the daemon. The Job
// is deleted only after the terminal report succeeded — never the other way
// around (§10) — and the credential then waits for the Job's own
// termination (ADR-017). S8 is the one endpoint
// whose transition follows the answer: 2xx marks the entry running, 409
// settles it through OnStartRejected — and its whole round (forward, answer,
// transition) runs under the task's lock, so no settlement path can settle
// the entry while that answer is still outstanding.
//
// sessionID is the raw session value the learning slot read from an S13/S14
// body: an S14 the attribution gate suppresses reports it back in the
// compensation body (ADR-031).
func (s *Scheduler) OnReport(ctx context.Context, ep Endpoint, e registry.TaskEntry, body []byte, sessionID string) (int, []byte, error) {
	locked := ep == EPStart
	unlock := func() {}
	if locked {
		unlock = s.taskMu.Lock(e.TaskID)
	}
	defer unlock()
	if ep == EPUsage {
		// §9.2 / ADR-009 decision 4: the usage face is accumulated while the
		// report is forwarded. The body stays opaque everywhere else; only
		// the metric's two label values and the four token counters are read,
		// and a malformed report is still forwarded verbatim.
		s.recordTokenUsage(body)
	}
	now := s.now()
	// undecided is the instance's first terminal decision inside this report:
	// it separates a fresh verdict from an already-decided replay, which
	// §4.4's 404 branch must not rewrite.
	undecided := false
	if ep.IsTerminal() {
		// The gauge follows the index on every exit of a terminal report —
		// including the transport-error exit below, which leaves the
		// write-ahead queue entry in place: the terminal write above already
		// left the index, so a refresh registered after the forward would
		// leave the pre-terminal value standing (§9.2, AC-10).
		defer s.SyncInflight()
		verdict := gateUndecided
		if !e.IsTerminal() && len(e.DecidedReport) == 0 && ep == EPFail {
			// §4.4/ADR-031: the S14 branch's first step is the attribution
			// gate. It faces only this shape — an undecided inbound daemon
			// failure self-report; a decided instance and the forwards
			// produced by a verdict (compensation, queue replay) never enter.
			e, body, verdict = s.attributionGate(ctx, e, body, sessionID)
			now = s.now()
		}
		// The terminal write and the decided-body registration run under the
		// task's lock, on a fresh read: the gate's wait opens a window in
		// which another path (the convergence compensation, a late report)
		// can settle this instance, and §4.4 forbids rewriting a decided one.
		// The gate's verdict distinguishes its own settlement from such a
		// decision; both skip this report's write.
		unlockTerminal := s.taskMu.Lock(e.TaskID)
		if fresh, ok := s.reg.Get(e.TaskID); ok {
			e = fresh
		}
		undecided = verdict == gateUndecided && !e.IsTerminal() && len(e.DecidedReport) == 0
		if undecided {
			// §10 write-ahead: the report is durable before the verdict
			// exists only in memory. Marking first and enqueueing on failure
			// left a window where a crash lost both the verdict and the
			// report; the rebuilt Job then read a completed task as failed.
			s.enqueuePending(ctx, e, ep, "", body)
			result := registry.Result(ep.TerminalResult())
			_, first, err := s.reg.MarkTerminal(e.TaskID, result, "", now)
			if err != nil {
				unlockTerminal()
				return 0, nil, err
			}
			if first {
				s.metrics.TaskTerminal(string(result))
				s.log.InfoContext(ctx, "task.terminal",
					"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
					"result", string(result),
					"duration_seconds", now.Sub(e.ClaimedAt).Seconds())
				// The instance's first terminal forward decides its body:
				// every later terminal forward takes this one (§4.4).
				if registered, ok := s.reg.RegisterDecidedReport(e.TaskID, body); ok {
					e = registered
				}
			}
		}
		unlockTerminal()
		if decided, ok := s.reg.DecidedReport(e.TaskID); ok {
			body = decided
		}
		if !undecided && !s.pendingReports.HasQueuedEntry(e.TaskID) {
			// Every terminal forward has a queue landing before it (§10
			// 补偿与重放同源). A verdict decided on another path already
			// wrote one; a duplicate late report has no such decision to
			// rely on. The known-membership face is deliberate: an
			// uncompleted carrier list must not suppress this write, and it
			// is the write itself that is being decided, not a settlement.
			s.enqueuePending(ctx, e, ep, "", body)
		}
	}

	code, resp, err := s.server.Forward(ctx, ep, e.TaskID, body)
	if err != nil {
		if ep.IsTerminal() {
			// The proxy's retry budget is exhausted (or the transport is
			// down): the report stays in the write-ahead queue for the
			// recovery drain (§10), and the daemon is told the upstream is
			// unavailable.
			return http.StatusBadGateway, upstreamUnavailableBody, nil
		}
		return 0, nil, fmt.Errorf("forward %s for task %s: %w", ep, e.TaskID, err)
	}
	if ep.IsTerminal() {
		switch {
		case terminalReportDelivered(code):
			// 2xx = written; 404 = the server no longer holds the task;
			// 409 = it already holds a verdict. All three leave nothing to
			// retry: the write-ahead entry goes, and the Job goes now (the
			// convergence round would delete it anyway).
			s.pendingReports.Drop(ctx, e.TaskID)
			if code == 404 && undecided {
				// §4.4's in-place decision: the server no longer holds the
				// task, so nothing is writable. A report that was still
				// undecided settles with no result; an already-decided replay
				// keeps the verdict it was decided with. The compare-and-set
				// guard keeps any other path's verdict (§4.2).
				s.reg.SettleNoResult(e.TaskID, registry.Result(ep.TerminalResult()))
			}
			s.deleteJob(ctx, e)
			if code == 404 || code == 409 {
				return 200, resp, nil
			}
		case permanentTerminalRejection(code):
			// Permanent failure (§1.5: 4xx is a permanent failure): no retry,
			// no queue — the write-ahead entry goes too. The verdict is
			// written, so the Job is torn down like every other terminal
			// path; only the log records the loss.
			s.pendingReports.Drop(ctx, e.TaskID)
			s.log.ErrorContext(ctx, "task.forward_failed",
				"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
				"endpoint", string(ep), "status", code)
			s.deleteJob(ctx, e)
		default:
			// Transient failure with the retry budget exhausted: the
			// write-ahead entry stays queued; recovery drains it until the
			// server accepts (§10).
			s.log.ErrorContext(ctx, "task.forward_failed",
				"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
				"endpoint", string(ep), "status", code)
			return http.StatusBadGateway, upstreamUnavailableBody, nil
		}
		return code, resp, nil
	}
	if ep == EPStart {
		// The running write waits for the server's answer: marking the entry
		// running before the forward would leave it running for a task the
		// server never started (409 状态不允许, S-8 + §1.5), and
		// the re-dispatched task would be re-claimed into another Job that is
		// refused the same way — an unbounded dispatch loop. A late start on
		// an already settled entry is still forwarded: the 409 tells the
		// daemon the task is settled (the settlement below is a no-op then).
		switch {
		case code >= 200 && code < 300:
			if started, ok := s.reg.MarkStarted(e.TaskID, now); ok {
				s.metrics.JobBootSeconds(now.Sub(started.ClaimedAt).Seconds())
				s.log.InfoContext(ctx, "task.started",
					"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", started.DaemonID,
					"boot_seconds", now.Sub(started.ClaimedAt).Seconds())
			}
			return code, resp, nil
		case code == http.StatusConflict:
			if err := s.OnStartRejected(ctx, e.TaskID); err != nil {
				s.log.ErrorContext(ctx, "task.settle_failed",
					"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
					"endpoint", string(ep), "err", err)
			}
			return code, resp, nil
		}
	}
	if code == 404 {
		// A non-terminal forward answered 404: the server deleted the task
		// (§4.6). The S-8 round already holds the task lock; every other
		// endpoint takes it inside OnTaskVanished.
		vanish := s.OnTaskVanished
		if locked {
			vanish = s.onTaskVanishedLocked
		}
		if err := vanish(ctx, e.TaskID); err != nil {
			s.log.ErrorContext(ctx, "task vanish cleanup failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		}
		return 404, resp, nil
	}
	if code >= 500 || code == http.StatusRequestTimeout || code == http.StatusTooManyRequests {
		// Transient upstream failure: the forward switch maps it
		// to 502 for the daemon (the daemon owns the retry).
		return http.StatusBadGateway, upstreamUnavailableBody, nil
	}
	return code, resp, nil
}

// upstreamUnavailableBody is the fixed error body for a transiently
// undeliverable forward.
var upstreamUnavailableBody = []byte(`{"error":"upstream unavailable"}`)

// enqueuePending lands a terminal report in the scheduler's durable queue —
// the write-ahead entry every terminal forward is preceded by (§10). The
// body is the one actually forwarded, so a replay sends the instance's
// decided body. A write that fails is the queue's own error row: it keeps the
// report staged and retries it every drain (§1.5 退避后重试), and Queued
// reports it as undelivered so the entry's objects stay put. The error is
// logged, never acted on. The one report the queue itself discards is the one
// its retention bound refuses (§4.6): the queue emitted that drop's error row
// already, so it is not repeated here.
func (s *Scheduler) enqueuePending(ctx context.Context, e registry.TaskEntry, ep Endpoint, failureReason string, body []byte) {
	if err := s.pendingReports.Enqueue(ctx, e.TaskID, e.JobName, e.DaemonID, ep, body, failureReason); err != nil {
		if errors.Is(err, ErrPendingQueueFull) {
			return
		}
		s.log.ErrorContext(ctx, "task.forward_undelivered",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"endpoint", string(ep),
			"reason", "pending-report enqueue failed", "err", err)
	}
}

// OnStartRejected settles an entry whose S-8 start the server refused with
// 409 (S-8 + §1.5: 状态不允许).
// The ordering puts C13 before the compensation: a 409 says "not startable",
// not "not running", so a post-delivery status adopts, terminal settles, 404
// releases, and only queued/dispatched proves the claim itself stale — that
// one is settled failed (stale_claim) and its Job/Secret go, ending the
// re-dispatch loop.
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
		return s.onTaskVanishedLocked(ctx, taskID)
	case err != nil:
		return fmt.Errorf("check status of task %s: %w", taskID, err)
	}
	s.reg.SetLastStatusSeen(taskID, st)
	switch {
	case st == "running" || st == "waiting_local_directory":
		// Post-delivery (ADR-026 决策结果 1 证据面②): the daemon holds the
		// task, so record the start fact, stop renewals and keep the Job.
		return s.adoptRunningLocked(ctx, taskID, st)
	case registry.IsTerminalResult(st):
		return s.settleTerminalLocked(ctx, taskID, st)
	case st == "queued" || st == "dispatched":
		return s.failEntry(ctx, e, failReport{
			reason:  FailureReasonStaleClaim,
			message: "server rejected the task start: task claim is stale or task is no longer startable",
		})
	default:
		s.logUnclassifiedStatus(ctx, e, st)
		return nil
	}
}

// OnBootTimeout handles the boot-deadline form (§4.2): the Job's daemon never
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

// OnDeliveryUnconfirmed handles the S-6 lost-response form (§5, ADR-032) at
// its terminal bound: the entry reached the boot deadline on a committed
// delivery with no start fact, so the payload never took — every attempt the
// confirmation window released went unconfirmed too.
// The task is failed on the daemon's behalf (C10,
// failure_reason=delivery_unconfirmed) and the objects removed, with the
// attribution the form actually deserves rather than the boot face's. The
// task's lock keeps it from settling an entry whose S8 answer is still
// outstanding.
//
// The window itself never fails an entry: it releases the delivery for
// another attempt on the daemon's next claim. The boot bound is what ends the
// form, and it is claim-relative, so the number of attempts is bounded by it.
func (s *Scheduler) OnDeliveryUnconfirmed(ctx context.Context, taskID string) error {
	defer s.SyncInflight()
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
	e, ok := s.reg.Get(taskID)
	if !ok || e.IsTerminal() || !e.StartedAt.IsZero() || e.DeliveredAt.IsZero() {
		return nil
	}
	s.metrics.DeliveryUnconfirmed()
	s.log.WarnContext(ctx, "task.delivery_unconfirmed",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"delivered_at", registry.ProjectedTime(e.DeliveredAt),
		"window_seconds", e.DeliveryDeadline.Sub(e.DeliveredAt).Seconds(),
		"claims_after_delivery", e.ClaimsAfterDelivery,
		"deliveries", e.Deliveries)
	return s.failEntry(ctx, e, failReport{
		reason:  FailureReasonDeliveryUnconfirmed,
		message: "task delivery was never confirmed by the job daemon",
	})
}

// ConvergeLeaseRefused settles an entry whose prepare-lease was refused
// (C4 400: the task left the renewable pre-start state). C13 decides:
// a post-delivery status records the start fact (leasing stops, state
// untouched), terminal settles and cleans up, a deleted task vanishes;
// pre-start statuses retry next round, any value outside the domain is
// reported and never guessed (S-7 + §10: 租约不超时, ADR-026 决策结果 1/3).
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
		return s.onTaskVanishedLocked(ctx, taskID)
	case err != nil:
		return err
	}
	s.reg.SetLastStatusSeen(taskID, st)
	switch {
	case st == "running" || st == "waiting_local_directory":
		// The daemon holds the task without Foreman seeing S8 (e.g. a
		// restart): record the start fact, stop leasing, keep the Job and
		// wait for the daemon's reports. Only a report advances the state.
		s.reg.MarkStartFact(taskID, s.now())
		return nil
	case registry.IsTerminalResult(st):
		if _, _, err := s.reg.MarkTerminal(taskID, registry.Result(st), "", s.now()); err != nil {
			return err
		}
		return s.deleteJob(ctx, e)
	case st == "queued" || st == "dispatched":
		// Pre-start: no decisive fact — retry next round.
		return nil
	default:
		s.logUnclassifiedStatus(ctx, e, st)
		return nil
	}
}

// logUnclassifiedStatus reports a C13 value outside the enumerated domain
// (§4.2: queued, dispatched, running, waiting_local_directory and the
// terminal trio). The conservative branch keeps the entry, so this report is
// its only visible signal; widening the domain is a design change, not a
// guess (ADR-026 决策结果 3).
func (s *Scheduler) logUnclassifiedStatus(ctx context.Context, e registry.TaskEntry, status string) {
	s.log.ErrorContext(ctx, "task.status_unclassified",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID, "status", status)
}

// OnJobGone is the compensation entry for a Job object that disappeared
// without a daemon report (§4.6: Job gone, task deleted server-side).
// The grace period tolerates the first-visibility delay of a freshly created
// Job; C13 then decides what to settle.
func (s *Scheduler) OnJobGone(ctx context.Context, jobName string) error {
	defer s.SyncInflight()
	e, ok := s.reg.ByJob(jobName)
	if !ok {
		return nil
	}
	if e.IsTerminal() {
		if s.pendingReports.Queued(e.TaskID) {
			// A queued report owns the terminal: the Job goes only once the
			// replay landed (§10).
			return nil
		}
		return s.deleteJob(ctx, e)
	}
	if s.now().Sub(e.ClaimedAt) < jobGoneGracePeriod {
		// Freshly created: tolerate the first-visibility delay.
		return nil
	}
	return s.FailJob(ctx, jobName, FailureReasonJobMissing)
}

// FailJob compensates a Job whose container ended without the daemon
// reporting a terminal state (§4.6 Job failure/eviction, §4.4 platform
// termination, and the missing-Job path via OnJobGone). reason is the
// failure_reason reported to the server; C13 decides first what to report
// (§4.2): a terminal server status is settled as-is, a task the server
// deleted settles terminal with an empty result (the drain starts either
// way), and only a still-active task gets the synthesized fail.
// Idempotent: an already-terminal entry only retries its Job deletion
// (§4.2).
func (s *Scheduler) FailJob(ctx context.Context, jobName, reason string) error {
	defer s.SyncInflight()
	// §4.3: the carrier list is the premise of "this task has no pending
	// report". Until it completes, a rebuilt entry may hold a queued terminal
	// report the process has not seen — settling it here would synthesize a
	// terminal that rewrites the report's verdict. This entry point is
	// reachable outside the recovery round (the proxy's refused-registration
	// path), so the gate lives here, not only in the round.
	if !s.pendingReports.Rebuilt() {
		return nil
	}
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
		if s.pendingReports.Queued(e.TaskID) {
			return nil
		}
		return s.deleteJob(ctx, e)
	}
	if s.pendingReports.Queued(e.TaskID) {
		// §4.3: a task with a pending entry converges only through the
		// entry's replay and the two settlement classes — never through a
		// synthesized terminal. This covers the rebuilt entry whose Job
		// ended after the crash that queued its report.
		return nil
	}
	st, err := s.server.TaskStatus(ctx, e.TaskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		// Task deleted server-side (scenario #9): settle it without a
		// report and start the drain.
		return s.onTaskVanishedLocked(ctx, e.TaskID)
	case err != nil:
		return fmt.Errorf("check status of task %s: %w", e.TaskID, err)
	}
	s.reg.SetLastStatusSeen(e.TaskID, st)
	if registry.IsTerminalResult(st) {
		return s.settleTerminalLocked(ctx, e.TaskID, st)
	}
	return s.failEntry(ctx, e, failReport{
		reason:  reason,
		message: "container ended without a terminal report",
	})
}

// failEntry settles a task as failed on the daemon's behalf: the write-ahead
// report entry and the terminal write are committed before any forward
// (markTerminal is the single write path, §4.2), then the C10 report, then
// the Job deletion. The verdict also starts the drain, so the credential and
// the entry stay until the Job terminates or the upper bound expires
// (ADR-017). A report that does not land stays in the durable queue, so the
// Job stays until a drain lands it (§10) — deleting it would let a Job
// deletion stand in for a delivered report.
func (s *Scheduler) failEntry(ctx context.Context, e registry.TaskEntry, report failReport) error {
	// Every synthetic terminal goes through here (compensation, boot bound,
	// unconfirmed delivery, refused start). None of them may be emitted while
	// the queue's carrier list is outstanding: a pending report the rebuild
	// has not seen owns the verdict (§4.3).
	if !s.pendingReports.Rebuilt() {
		return nil
	}
	_, body, err := s.settleFailure(ctx, e, report)
	if err != nil {
		return err
	}
	if err := s.reportFail(ctx, e, body); err != nil {
		return nil
	}
	e, ok := s.reg.Get(e.TaskID)
	if !ok {
		return nil
	}
	return s.deleteJob(ctx, e)
}

// settleFailure writes the failed verdict Foreman judged itself: the
// compensation event, the terminal write and the decided-body registration,
// all before any forward and independent of its result (§4.4, ADR-031). The
// returned body is the instance's decided body — the report's own encoding,
// unless a body was already registered, in which case the first decision
// wins.
func (s *Scheduler) settleFailure(ctx context.Context, e registry.TaskEntry, report failReport) (registry.TaskEntry, []byte, error) {
	body := report.encode()
	// §10 write-ahead: durable before the verdict write, exactly like an
	// inbound terminal report — a crash cannot leave a failure verdict whose
	// report only lived in the process. The attribution rides the queue entry:
	// the rebuilt entry has no in-memory verdict, and the replay must restore
	// failure_reason from the entry, never from the body (§4.1).
	s.enqueuePending(ctx, e, EPFail, report.reason, body)
	settled, first, err := s.reg.MarkTerminal(e.TaskID, registry.ResultFailed, report.reason, s.now())
	if err != nil {
		return e, body, err
	}
	if first {
		s.log.ErrorContext(ctx, "task.failed_compensated",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"failure_reason", report.reason)
		s.metrics.TaskTerminal(string(registry.ResultFailed))
		if registered, ok := s.reg.RegisterDecidedReport(e.TaskID, body); ok {
			settled = registered
		}
	}
	if decided, ok := s.reg.DecidedReport(e.TaskID); ok {
		body = decided
	}
	return settled, body, nil
}

// SettleTerminal converges an entry the server already holds as terminal
// (C13 completed/failed/cancelled, or a rebuilt entry whose deletion is
// still pending): the entry is marked terminal and the Job is deleted
// (§4.2) — the credential and the entry then wait for the Job's pods
// to terminate or for the drain upper bound (ADR-017). A terminal report
// still waiting in the durable queue holds the Job back until it lands
// (§10): the recovery loop drains the queue every round and the
// round that saw it delivered deletes the Job.
//
// The task's lock serializes this write with every other settlement path, so
// a C13 answer can never reclassify an entry a concurrent report decided
// (§4.2: terminal is irreversible).
func (s *Scheduler) SettleTerminal(ctx context.Context, taskID, result string) error {
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
	return s.settleTerminalLocked(ctx, taskID, result)
}

// settleTerminalLocked is SettleTerminal for callers that already hold the
// task's lock: the S8 answer round (OnStartRejected) and the Job compensation
// (FailJob).
func (s *Scheduler) settleTerminalLocked(ctx context.Context, taskID, result string) error {
	defer s.SyncInflight()
	// §4.3: an uncompleted carrier list is not an empty queue. Converging an
	// entry now could delete the Job of a task whose queued report the
	// rebuild has not seen yet.
	if !s.pendingReports.Rebuilt() {
		return nil
	}
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	if s.pendingReports.Queued(taskID) {
		// The partition-and-queue form (§4.6, §10): the objects wait for the
		// report — a deleted Job must never stand in for a delivered one.
		return nil
	}
	_, first, err := s.reg.MarkTerminal(taskID, registry.Result(result), "", s.now())
	if err != nil {
		return err
	}
	if first {
		s.metrics.TaskTerminal(result)
		s.log.InfoContext(ctx, "task.terminal",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"result", result)
	}
	return s.deleteJob(ctx, e)
}

// AdoptRunning records that the server reports the task post-delivery (C13),
// e.g. after a restart rebuilt the entry: the Job is kept, lease renewals
// stop (started_at is set) and the daemon's reports are accepted
// (§4.3: the adopt branch). The observed status rides on the event so an
// adoption is never read back as "running". Adoption is an observation, not
// a report: only started_at moves, so the projection keeps the rebuild's
// initial phase (ADR-026 决策结果 1 / 后果).
//
// The write takes the task lock: started_at is the fact the discard row is
// decided on, so it must be ordered against DiscardTask's re-read and
// deletion (§4.2: one decisive fact per transition).
func (s *Scheduler) AdoptRunning(ctx context.Context, taskID, status string) error {
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
	return s.adoptRunningLocked(ctx, taskID, status)
}

// adoptRunningLocked is AdoptRunning for callers that already hold the task
// lock. A terminal entry is never reopened (MarkStartFact refuses it).
func (s *Scheduler) adoptRunningLocked(ctx context.Context, taskID, status string) error {
	e, ok := s.reg.MarkStartFact(taskID, s.now())
	if !ok {
		return nil
	}
	s.log.InfoContext(ctx, "task.adopted",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID, "status", status)
	return nil
}

// DiscardTask removes the objects and the mapping of a task that will be
// re-run, without a terminal report and without entering drain (§4.3). The
// row's 成立面 is re-read from the live entry under the task lock (ADR-026
// 决策 5): terminal, started, or payload-bearing entries are kept. C13 is
// re-read before the deletion, so a stale pre-delivery snapshot never deletes
// a Job the server now holds. The predecessor's digest is revoked first
// (ADR-017); a rebuilt entry has none and logs cred.revoke_unavailable. A
// failed deletion keeps the entry as the only retry handle (§6.4).
func (s *Scheduler) DiscardTask(ctx context.Context, taskID string) error {
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	if e.IsTerminal() || !e.StartedAt.IsZero() || len(e.Payload) != 0 {
		return nil
	}
	st, err := s.server.TaskStatus(ctx, taskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		return s.settleVanishedLocked(ctx, taskID)
	case err != nil:
		// Truth unknown: keep the entry as the retry handle.
		return fmt.Errorf("check status of task %s: %w", taskID, err)
	}
	s.reg.SetLastStatusSeen(taskID, st)
	switch {
	case st == "running" || st == "waiting_local_directory":
		// The daemon holds the task: adopt, never discard.
		return s.adoptRunningLocked(ctx, taskID, st)
	case registry.IsTerminalResult(st):
		return s.settleTerminalLocked(ctx, taskID, st)
	case st == "queued" || st == "dispatched":
		return s.discardTaskLocked(ctx, e)
	default:
		s.logUnclassifiedStatus(ctx, e, st)
		return nil
	}
}

// discardTaskLocked removes the objects and the mapping. The caller holds the
// task lock, which is what makes re-reading the entry here a decision on the
// live state rather than on a round snapshot.
func (s *Scheduler) discardTaskLocked(ctx context.Context, e registry.TaskEntry) error {
	defer s.SyncInflight()
	s.revokeCredential(ctx, e)
	if err := s.deleteObjects(ctx, e); err != nil {
		return err
	}
	return s.reg.Delete(e.TaskID)
}

// OnTaskVanished settles a task the server deleted (§4.6, C13/C4 404): the
// entry is marked terminal with an empty result — the only source of result=""
// — and the Job goes; the credential and the entry stay until the Job
// terminates or the drain upper bound expires (ADR-017). No report is sent.
//
// It runs under the task lock on a live re-read, and the 404 is confirmed
// once more before the deletion: a stale observation never settles a live
// task. A decided entry keeps its verdict and only retries the Job deletion.
func (s *Scheduler) OnTaskVanished(ctx context.Context, taskID string) error {
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
	return s.onTaskVanishedLocked(ctx, taskID)
}

// onTaskVanishedLocked is OnTaskVanished for callers that already hold the
// task lock. The 404 is confirmed once more before the deletion: a vanished
// verdict deletes the Job and drops the credential's use, so a stale 404 must
// not be acted on (§4.2). A failed confirmation is not a verdict — the entry
// is kept and the next round retries.
func (s *Scheduler) onTaskVanishedLocked(ctx context.Context, taskID string) error {
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	if e.IsTerminal() {
		return s.deleteJob(ctx, e)
	}
	st, err := s.server.TaskStatus(ctx, taskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		return s.settleVanishedLocked(ctx, taskID)
	case err != nil:
		return fmt.Errorf("check status of task %s: %w", taskID, err)
	default:
		// The task still exists: the vanish verdict was not a fact.
		s.reg.SetLastStatusSeen(taskID, st)
		return nil
	}
}

// settleVanishedLocked writes the empty-result verdict of a confirmed
// vanished task and deletes its Job. The caller holds the task lock.
func (s *Scheduler) settleVanishedLocked(ctx context.Context, taskID string) error {
	defer s.SyncInflight()
	e, ok := s.reg.Get(taskID)
	if !ok {
		return nil
	}
	// A decided entry keeps its verdict: the in-place settlement only writes
	// the empty result when this path is the first to decide (§4.2, §4.4).
	_, first, err := s.reg.MarkTerminal(taskID, "", "", s.now())
	if err != nil {
		return err
	}
	if first {
		s.log.InfoContext(ctx, "task.vanished",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID)
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
//
// The caller holds the task lock (OnClaim's duplicate-claim round), so the
// entry re-read here is the live one: a start fact established since the
// caller's snapshot (S8 during the C13 round trip) makes the entry adopted and
// keeps it (ADR-026 决策 5; ADR-017 names this row replace_pre_start, whose
// 成立面 is pre-start). It reports whether the replacement may proceed; false
// means the live entry carries a start fact and was kept — the claim must be
// dropped instead. An entry already gone, or a failed deletion, still lets the
// replacement proceed: the new claim cleans up leftovers idempotently.
func (s *Scheduler) discardQuietly(ctx context.Context, e registry.TaskEntry) bool {
	fresh, ok := s.reg.Get(e.TaskID)
	if !ok {
		return true
	}
	if !fresh.StartedAt.IsZero() {
		return false
	}
	if err := s.discardTaskLocked(ctx, fresh); err != nil {
		s.log.WarnContext(ctx, "task.discard_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"err", err)
	}
	return true
}

// reclaimObjectless closes an entry whose instance never had K8s objects. The
// revoke still runs first (it is in-process and the digest may exist in the
// set); the Secret and Job steps are skipped because nothing was ever created
// under names that may not even be legal.
func (s *Scheduler) reclaimObjectless(ctx context.Context, e registry.TaskEntry) error {
	s.revokeCredential(ctx, e)
	drainSeconds := s.drainSeconds(e.TaskID)
	if err := s.reg.Delete(e.TaskID); err != nil {
		return err
	}
	s.metrics.CredReclaimed(JobTerminationReasonNoObjects, drainSeconds)
	s.log.InfoContext(ctx, "cred.reclaimed",
		"task_id", e.TaskID, "job_name", e.JobName, "reason", JobTerminationReasonNoObjects,
		"drain_seconds", drainSeconds,
		"pod_terminated_at", "", "pod_terminated_at_source", PodTerminatedAtSourceObserved,
		"orphan_pods", 0)
	return nil
}

// OnJobTerminated is the credential-reclaim entry point — the only one
// (ADR-017). It runs the teardown in its fixed order: ① revoke the digest
// (in-process, no K8s dependency) → ② delete the Secret → ③ drop the entry.
// A Secret deletion that fails keeps the entry in drain for the next round:
// the entry is the only retry handle, and dropping it first would leave an
// orphan Secret with no sweep face. Idempotent; a no-op for an entry that is
// not draining. The verdict-time Job deletion is caught up outside that
// sequence, under the §10 report gate — see the body.
//
// podTerminatedAt is the termination fact the caller measured (zero when the
// evidence carries none), orphanPods the pods still alive at reclaim time
// (instance-filtered); both are evidence fields of the cred.reclaimed event
// (§9.1). The clock domain the value belongs to is
// derived here and declared alongside it: the drain upper bound closes the
// window without any termination fact (empty value, observed domain), a
// kubelet-reported finish is the node clock (k8s domain), and a fact that
// exists only as the observation moment is Foreman's clock (observed domain).
func (s *Scheduler) OnJobTerminated(ctx context.Context, jobName, reason string, podTerminatedAt time.Time, orphanPods int) error {
	defer s.SyncInflight()
	// The caller holds only the Job name, so the key is resolved outside the
	// lock and the entry re-read under it: a replacement claim may have taken
	// the same display name, and the successor's credential Secret and entry
	// must never be reclaimed on the predecessor's termination evidence.
	e, ok := s.reg.ByJob(jobName)
	if !ok {
		return nil
	}
	if s.beforeJobLock != nil {
		s.beforeJobLock()
	}
	unlock := s.taskMu.Lock(e.TaskID)
	defer unlock()
	e, ok = s.reg.ByJob(jobName)
	if !ok || !e.IsDraining() {
		return nil
	}
	if e.FailureReason == FailureReasonInvalidTaskID {
		// The instance never produced an object: there is no Secret to delete,
		// no Job to catch up, and the identity-derived names can be illegal
		// object names (the task id itself is unusable). Waiting for pod
		// evidence would only issue rejected reads until the drain bound.
		return s.reclaimObjectless(ctx, e)
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
	if err := s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(e.DaemonID)); err != nil {
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
	// while a terminal report is queued (§10 — a deleted Job must
	// never stand in for a delivered report), so a drain that closed inside
	// that window would otherwise leave the Job with no deleter at all. The
	// catch-up runs after the reclaim sequence, under the same gate, and is
	// deliberately not part of it: a failure neither stalls the credential's
	// bound nor keeps the entry — the template's ttlSecondsAfterFinished owns
	// the leftover Job (§8.1).
	if !s.pendingReports.Queued(e.TaskID) {
		if err := s.deleteJob(ctx, e); err != nil {
			// The entry is gone, so nothing retries this deletion: the
			// counter is the only alarm that the Job will live until the
			// template's ttlSecondsAfterFinished reclaims it.
			s.metrics.JobDeleteFailed("catch_up")
		}
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
// fact: the pods the Job controller labelled with this Job's name,
// restricted to the current instance — a pod created before
// this Job instance belongs to a reused name's predecessor, whose disposal is
// the revoke-and-discard path, never this Job's credential verdict. since is
// the Job's apiserver creationTimestamp, so both sides of the comparison are
// apiserver timestamps (§4.1).
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
// records as the observation moment in the observed domain (§10: the
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

// SweepOrphanSecrets runs the two Secret-domain sweeps of §6.5.
//
// The credential domain is selected by the absence of a role label ∧ the
// task-id label ∧ the name fm-<task-id>-cred; its criterion is two-sided: no
// registry entry for the task-id (any state) AND no task Job carrying it;
// either half alone deletes a Secret still needed (CreateSecret → CreateJob
// window, or a Job that boots job_auth_failed). A Secret whose name is not
// fm-<task-id>-cred is not a design product and is skipped — deleting by
// label would hit a same-named in-flight credential.
//
// The carrier domain is selected by the carrier role label ∧ managed-by; its
// criterion is "the entry settled" — the queue no longer holds the task. The
// task-id label and the Job's existence do not participate: a carrier must
// outlive its Job until the report is settled (§6.5).
//
// The Job's presence is answered by listing task Jobs and reading their
// foreman.tsic.top/task-id labels, never by deriving a name: the display name
// is the template's business, so a name built from the task id would resolve
// to nothing for every custom format and delete a live Job's credential
// (ADR-034 decision 9 — the label is the only locator).
func (s *Scheduler) SweepOrphanSecrets(ctx context.Context) error {
	secrets, err := s.jobs.ListSecrets(ctx)
	if err != nil {
		return fmt.Errorf("list credential secrets: %w", err)
	}
	jobs, err := s.jobs.ListJobs(ctx)
	if err != nil {
		// A list error is not a NotFound: the criterion demands an explicit
		// absence, so no Secret is touched this round.
		return fmt.Errorf("list task jobs: %w", err)
	}
	withJob := make(map[string]struct{}, len(jobs))
	for i := range jobs {
		if taskID := jobs[i].Labels[registry.LabelTaskID]; taskID != "" {
			withJob[taskID] = struct{}{}
		}
	}
	var firstErr error
	for _, secret := range secrets {
		// Ownership exclusion precedes the criterion: a role-labelled Secret
		// belongs to another domain, and the carrier carries a task-id label
		// (§6.5).
		if secret.Labels[registry.LabelRole] != "" {
			continue
		}
		taskID := secret.Labels[registry.LabelTaskID]
		if taskID == "" {
			continue
		}
		if secret.Name != jobbuilder.CredentialSecretName(jobbuilder.DaemonIDFor(taskID)) {
			continue
		}
		if _, ok := s.reg.Get(taskID); ok {
			continue
		}
		if _, ok := withJob[taskID]; ok {
			continue
		}
		if err := s.jobs.DeleteSecret(ctx, secret.Name); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// job_name is derived for attribution only: the Job is gone, so there
		// is no object to read a name from and the identity name is the one
		// value the sweep can name (§9.1: cred.orphan_secret_removed).
		s.log.InfoContext(ctx, "cred.orphan_secret_removed",
			"task_id", taskID, "job_name", jobbuilder.DaemonIDFor(taskID), "secret", secret.Name)
	}
	for _, secret := range secrets {
		// Carrier domain: selected by the carrier role label ∧ managed-by.
		// The name form and the task-id label are not selectors (§6.5).
		if secret.Labels[registry.LabelRole] != registry.RolePendingReport {
			continue
		}
		if secret.Labels[registry.LabelManagedBy] != registry.ManagedByForeman {
			continue
		}
		taskID := secret.Labels[registry.LabelTaskID]
		if taskID == "" {
			// No task identity: no settlement judgement, so no deletion.
			continue
		}
		if s.pendingReports.Queued(taskID) {
			// Unsettled carriers are never swept — the retention bound does
			// not apply to entries already queued (§6.5, §4.6).
			continue
		}
		if err := s.jobs.DeleteSecret(ctx, secret.Name); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		s.log.InfoContext(ctx, "pending.carrier_removed",
			"task_id", taskID, "secret", secret.Name)
	}
	return firstErr
}

// Reconcile converges the live index with the server. With a recovery
// reconciler wired it delegates; otherwise it runs the built-in startup
// convergence (§4.3: the rebuild disposition table) over every live entry.
// The per-node soft cap and the inflight gauge are refreshed first: both are
// derived state of the live index and must not outlive the load they describe.
func (s *Scheduler) Reconcile(ctx context.Context) error {
	s.RefreshNodeSaturation(ctx)
	s.SyncInflight()
	// Round-level action, independent of the entry list (§6.5: the probe
	// residual sweep): probe remnants are found by their labels, not by the
	// session table, which is empty after a restart.
	if err := s.SweepStaleProbes(ctx); err != nil {
		s.log.WarnContext(ctx, "probe sweep failed", "err", err)
	}
	if s.reconciler != nil {
		return s.reconciler.Reconcile(ctx)
	}
	// Without the recovery loop this path is the only convergence driver:
	// the queue must be rebuilt before any entry is settled, or a failed
	// carrier list would read as an empty queue (§4.3).
	if err := s.pendingReports.Restore(ctx); err != nil {
		return err
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
		// The task-Job predicate is managed-by ∧ task-id (§3): a
		// catalog-probe Job carries no task-id and must never become a
		// payload-less pseudo entry.
		if _, ok := job.GetLabels()[registry.LabelTaskID]; !ok {
			s.log.DebugContext(ctx, "skip non-task job", "job_name", job.GetName())
			continue
		}
		e, err := registry.TaskEntryFromJob(job)
		if err != nil {
			s.log.ErrorContext(ctx, "skip malformed job", "job_name", job.GetName(), "err", err)
			continue
		}
		// A live entry is this process's claim and carries what the rebuilt
		// projection cannot restore: the payload, the token digest and the
		// boot/lease deadlines. PutIfAbsent checks and inserts in one locked
		// step, so a claim landing on either side of it is never replaced
		// (§4.3).
		inserted, err := s.reg.PutIfAbsent(e)
		if err != nil {
			s.log.ErrorContext(ctx, "restore entry failed",
				"task_id", e.TaskID, "job_name", e.JobName, "err", err)
			continue
		}
		if !inserted {
			s.log.DebugContext(ctx, "keep live entry", "task_id", e.TaskID, "job_name", e.JobName)
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
	// The same recovery rebuilds the pending queue from its carriers
	// (§4.3). A list failure is not fatal here: the queue stays unrestored
	// and the recovery round retries it, settling nothing until it succeeds.
	if err := s.pendingReports.Restore(ctx); err != nil {
		s.log.ErrorContext(ctx, "pending.restore_failed", "err", err)
	}
	return nil
}

// convergeEntry settles one rebuilt or lingering entry against the server's
// status (C13): a post-delivery status adopts the entry (Job kept, start fact
// recorded), queued/dispatched releases it for server redispatch, terminal
// cleans up, 404 cleans up without a report. Every registry write is an
// in-lock transition — a snapshot Put after the network round-trip could
// clobber a concurrent daemon callback.
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
	case st == "running" || st == "waiting_local_directory":
		// The task is in the daemon's hands: keep the Job and accept its
		// reports. AdoptRunning records started_at once; it only gates the
		// boot timeout, so the rebuild-time approximation is safe. A miss
		// means the entry turned terminal or was deleted mid-round — benign.
		return s.AdoptRunning(ctx, e.TaskID, st)
	case st == "queued" || st == "dispatched":
		// ADR-026 决策 3/5: the discard row is a rebuilt entry — payload lost
		// with the restart and no start fact. Payload availability is stable
		// for a live entry; the start fact is re-read from the live entry at
		// the deletion surface (DiscardTask holds the task lock), so a
		// concurrent S8 transition wins over this round's snapshot. This path
		// deliberately does not enter drain — the task will be re-run
		// (ADR-017).
		if len(e.Payload) != 0 {
			return nil
		}
		return s.DiscardTask(ctx, e.TaskID)
	case registry.IsTerminalResult(st):
		return s.SettleTerminal(ctx, e.TaskID, st)
	default:
		s.logUnclassifiedStatus(ctx, e, st)
		return nil
	}
}

// resolveRuntimeIdentity turns a claim's real runtime id into the declared
// identity the entry carries (§2.3). A missing resolver or an unresolvable id
// yields the empty identity: S-4 must refuse such a Job rather than answer for
// an identity the process never learned.
func (s *Scheduler) resolveRuntimeIdentity(runtimeID string) string {
	if s.runtimeIdentity == nil || runtimeID == "" {
		return ""
	}
	identity, ok := s.runtimeIdentity(runtimeID)
	if !ok {
		return ""
	}
	return identity
}

// failClaim settles a task whose Job could not be created
// (pending → terminal, result=failed) and tells the server. The verdict's
// write-ahead report stays queued when the forward does not land, and the
// terminal entry stays until a round drains it (§10).
func (s *Scheduler) failClaim(ctx context.Context, e registry.TaskEntry, cause error) {
	defer s.SyncInflight()
	// An invalid task ID is not a create failure: the Job must not be
	// retried, the operator has to fix the task (§4.4).
	reason := FailureReasonJobCreateFailed
	if errors.Is(cause, jobbuilder.ErrInvalidTaskID) {
		reason = FailureReasonInvalidTaskID
	}
	_, body, err := s.settleFailure(ctx, e, failReport{reason: reason, message: cause.Error()})
	if err != nil {
		s.log.ErrorContext(ctx, "mark terminal failed", "task_id", e.TaskID, "err", err)
	}
	// A report whose forward does not land goes to the durable queue
	// (场景 #10); the entry stays terminal until a round drains it.
	_ = s.reportFail(ctx, e, body)
}

// terminalReportDelivered reports whether a terminal forward reached a state
// with nothing left to retry: the server wrote the verdict (2xx), no longer
// holds the task (404), or already holds a verdict (409). The inbound report
// path, the compensation forward, the queue sender and the queue drain all use
// this one predicate — a 409 must never mean "delivered" on one path and
// "permanent failure" on another.
func terminalReportDelivered(code int) bool {
	return (code >= 200 && code < 300) || code == http.StatusNotFound || code == http.StatusConflict
}

// permanentTerminalRejection reports a 4xx that retrying cannot fix (§1.5):
// every 4xx except 408 and 429. 404 and 409 are not rejections — they are the
// task-vanished and already-settled faces terminalReportDelivered owns. Every
// terminal path (inbound report, compensation forward, queue replay) uses this
// one boundary, so a permanent refusal can never stay queued on one of them.
func permanentTerminalRejection(code int) bool {
	return code >= 400 && code < 500 &&
		code != http.StatusRequestTimeout && code != http.StatusTooManyRequests &&
		code != http.StatusNotFound && code != http.StatusConflict
}

// ErrUnnameableClaim marks a claim with no readable task id. Nothing can be
// registered or settled for it, and the caller must not log it per attempt:
// the scheduler itself counts it and logs at a bounded rate, while the
// server's lease expiry is the only convergence.
var ErrUnnameableClaim = errors.New("claim payload has no readable task id")

// claimID reads just the task id from a claim payload whose full head parse
// failed. An id that cannot be read leaves nothing to settle.
func claimID(task json.RawMessage) string {
	var head struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(task, &head); err != nil {
		return ""
	}
	return head.ID
}

// settleMalformedClaim dead-letters a claim whose id is readable but whose
// rest is not: the entry is registered without any object and settled with the
// documented invalid-task-id attribution, so the server stops re-dispatching
// it. Without this the same claim would be re-delivered on every lease expiry
// forever, since no entry exists for any other path to converge it.
func (s *Scheduler) settleMalformedClaim(ctx context.Context, id string, task json.RawMessage, parseErr error) error {
	unlock := s.taskMu.Lock(id)
	defer unlock()
	cause := fmt.Errorf("%w: malformed claim payload: %v", jobbuilder.ErrInvalidTaskID, parseErr)
	if existing, ok := s.reg.Get(id); ok {
		if !existing.IsTerminal() {
			s.failClaim(ctx, existing, cause)
		}
		return nil
	}
	if s.reg.IsDone(id) {
		s.metrics.DuplicateDispatch()
		s.log.WarnContext(ctx, "duplicate_dispatch",
			"task_id", id, "old_job_name", "", "action", "drop_terminal_done")
		return nil
	}
	daemonID := jobbuilder.DaemonIDFor(id)
	e := registry.TaskEntry{
		TaskID:       id,
		JobNamespace: s.cfg.JobNamespace,
		// No object exists (or ever will); the identity name is the key the
		// reclaim path's Job-name lookup needs.
		JobName:      daemonID,
		DaemonID:     daemonID,
		JobRuntimeID: jobRuntimeID(daemonID),
		State:        registry.StatePending,
		Attempt:      1,
		ClaimedAt:    s.now(),
		Payload:      append(json.RawMessage(nil), task...),
	}
	if err := s.reg.Put(e); err != nil {
		return err
	}
	s.failClaim(ctx, e, cause)
	return nil
}

// noteUnnameableClaim counts a claim with no readable id and logs it at a
// bounded rate: the server's lease/expiry is the only convergence, and the
// loop would otherwise repeat the error at claim cadence.
func (s *Scheduler) noteUnnameableClaim(ctx context.Context, err error, size int) {
	s.metrics.ClaimError("malformed_claim")
	s.malformedClaimMu.Lock()
	due := s.now().Sub(s.malformedClaimLoggedAt) >= malformedClaimLogInterval
	if due {
		s.malformedClaimLoggedAt = s.now()
	}
	s.malformedClaimMu.Unlock()
	if !due {
		return
	}
	s.log.ErrorContext(ctx, "claim.malformed",
		"err", err, "body_bytes", size,
		"note", "no readable task id: the server's lease expires and re-dispatches")
}

// reportFail forwards a synthetic fail report (C10) for a task the daemon
// could not settle itself. Its write-ahead entry was queued by the verdict
// (settleFailure), so a forward that does not land leaves it for the recovery
// drain (场景 #10). A 2xx/404/409 settles the report; a permanent 4xx is not
// retried, so its queue entry goes too. body is the instance's decided body
// (§4.4): when one is registered it wins over the body this call synthesized,
// so a replay can never displace the verdict.
func (s *Scheduler) reportFail(ctx context.Context, e registry.TaskEntry, body []byte) error {
	if decided, ok := s.reg.DecidedReport(e.TaskID); ok {
		body = decided
	}
	code, _, err := s.server.Forward(ctx, EPFail, e.TaskID, body)
	if err == nil {
		switch {
		case terminalReportDelivered(code):
			s.pendingReports.Drop(ctx, e.TaskID)
			return nil
		case permanentTerminalRejection(code):
			// Permanent (§1.5): a replay would only be refused again.
			s.pendingReports.Drop(ctx, e.TaskID)
		}
	}
	if err == nil {
		err = fmt.Errorf("server returned %d", code)
	}
	s.log.ErrorContext(ctx, "task.forward_failed",
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"endpoint", string(EPFail), "status", code, "err", err)
	return err
}

// DrainPendingReports re-delivers the durable terminal-report queue through
// the single terminal face (ADR-031) and reports how many entries are still
// undelivered. The recovery loop calls it once per round, so a restored
// Foreman catches up within the contract's two-round bound (§10).
func (s *Scheduler) DrainPendingReports(ctx context.Context) (int, error) {
	return s.pendingReports.Drain(ctx, s)
}

// RestorePendingReports rebuilds the queue from its carriers (§4.3). It is
// idempotent: once the list succeeded every later round is a no-op. An error
// means the queue is not rebuilt — the recovery loop must settle nothing that
// round (a failed list is not an empty queue).
func (s *Scheduler) RestorePendingReports(ctx context.Context) error {
	return s.pendingReports.Restore(ctx)
}

// DeliverQueued re-delivers one queued terminal report through the same
// terminal face every other forward uses (ADR-031's single entry): the
// instance's decided body wins, the 404 in-place decision runs here, and a
// 2xx deletes the Job. It is the sender the queue's Drain calls, so a replay
// is never an inbound report — it does not enter the attribution gate and
// cannot emit a second attribution decision.
func (s *Scheduler) DeliverQueued(ctx context.Context, ep Endpoint, taskID string, body []byte, failureReason string) (int, []byte, error) {
	e, ok := s.reg.Get(taskID)
	if !ok {
		// The credential was reclaimed before the report landed: the entry is
		// gone, but the report still goes — a terminal callback is never
		// dropped (§10). No objects are left to clean up.
		code, resp, err := s.server.Forward(ctx, ep, taskID, body)
		if err == nil && ep.IsTerminal() && permanentTerminalRejection(code) {
			// A replay of a permanently refused report would only be refused
			// again: the queue entry goes now instead of at the TTL.
			s.pendingReports.Drop(ctx, taskID)
			s.log.ErrorContext(ctx, "task.forward_dropped",
				"task_id", taskID, "endpoint", string(ep), "status", code,
				"reason", "permanent_4xx")
		}
		return code, resp, err
	}
	if decided, ok := s.reg.DecidedReport(taskID); ok {
		body = decided
	}
	code, resp, err := s.server.Forward(ctx, ep, taskID, body)
	if err != nil {
		return code, resp, err
	}
	if !ep.IsTerminal() {
		return code, resp, nil
	}
	switch {
	case terminalReportDelivered(code):
		if code == 404 {
			// §4.4's in-place decision: the server no longer holds the task.
			// The verdict is re-read under the task lock after the forward: the
			// replay can stay in flight for the retry budget, and a settlement
			// path may have decided the instance meanwhile — a decided instance
			// keeps its result (§4.2, §4.4). Only an instance that never
			// reached a verdict settles with no result, and the Job deletion
			// uses the fresh entry.
			unlock := s.taskMu.Lock(taskID)
			fresh, ok := s.reg.Get(taskID)
			if !ok {
				// Reclaimed while the replay was in flight: the pre-forward
				// snapshot is the only Job handle left, and the deletion is
				// idempotent.
				unlock()
				s.deleteJob(ctx, e)
				return code, resp, nil
			}
			if !fresh.IsTerminal() && len(fresh.DecidedReport) == 0 {
				if _, _, err := s.reg.MarkTerminal(taskID, "", "", s.now()); err != nil {
					s.log.ErrorContext(ctx, "task.settle_failed",
						"task_id", taskID, "job_name", fresh.JobName, "daemon_id", fresh.DaemonID,
						"reason", "server has no record of the task", "err", err)
				}
			}
			unlock()
			s.deleteJob(ctx, fresh)
			return code, resp, nil
		}
		// 2xx and 409: the verdict is written on the server side. 409 means it
		// was already there, so nothing is replayed again. A rebuilt entry
		// carries no verdict, so the replay's endpoint supplies it — without
		// this the later missing-Job path would synthesize a failure for a
		// completed task (§4.3).
		s.settleReplayed(ctx, taskID, ep, failureReason)
		s.deleteJob(ctx, e)
	case permanentTerminalRejection(code):
		// §1.5: a permanent 4xx is never retried. The queue entry goes so the
		// drain cannot replay it to the TTL (and SettleTerminal's Queued gate
		// cannot keep the Job around for it); the Job is torn down like every
		// other terminal path.
		s.pendingReports.Drop(ctx, taskID)
		s.log.ErrorContext(ctx, "task.forward_dropped",
			"task_id", taskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"endpoint", string(ep), "status", code, "reason", "permanent_4xx")
		s.deleteJob(ctx, e)
	}
	return code, resp, nil
}

// settleReplayed records the verdict a successful replay establishes on a
// rebuilt entry. In the same-process form the entry is already terminal (the
// write-ahead enqueue preceded the verdict), so this is a no-op; after a
// restart the rebuilt entry carries no verdict and the queued report is its
// only terminal source (§4.3). A decided entry keeps its own result (§4.4).
// failureReason is the attribution the queued entry was decided with: a
// synthesized failure replayed across a restart must project the same
// failure_reason, never an empty one (§4.1).
func (s *Scheduler) settleReplayed(ctx context.Context, taskID string, ep Endpoint, failureReason string) {
	unlock := s.taskMu.Lock(taskID)
	defer unlock()
	fresh, ok := s.reg.Get(taskID)
	if !ok || fresh.IsTerminal() || len(fresh.DecidedReport) > 0 {
		return
	}
	result := registry.Result(ep.TerminalResult())
	reason := ""
	if result == registry.ResultFailed {
		reason = failureReason
	}
	_, first, err := s.reg.MarkTerminal(taskID, result, reason, s.now())
	if err != nil {
		s.log.ErrorContext(ctx, "task.settle_failed",
			"task_id", taskID, "job_name", fresh.JobName, "daemon_id", fresh.DaemonID,
			"reason", "queued report delivered", "err", err)
		return
	}
	if first {
		s.metrics.TaskTerminal(string(result))
	}
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
	if err := s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(e.DaemonID)); err != nil {
		s.log.ErrorContext(ctx, "job.delete_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "err", err)
		errs = append(errs, fmt.Errorf("delete secret %s: %w", jobbuilder.CredentialSecretName(e.DaemonID), err))
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
		AgentName:       e.AgentName,
		ProjectID:       e.ProjectID,
		ProjectSource:   e.ProjectSource,
	}
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
