package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// The S14 attribution gate and its single evidence function (§4.4, ADR-031):
// the platform's forced termination wins over the same instance's daemon
// failure self-report, and the decision is made before the report is
// forwarded. The convergence path calls the same judgement through
// PlatformTermination, so the fact has exactly one determination program.

// defaultAttributionWait is the gate path's budget (ADR-031): the evidence
// reads, the platform-fact wait, the task lock, the C13 re-check and the
// caller's terminal-write lock all draw on this one bound, so the S14 round
// ends by its start plus this bound instead of stacking a full wait, a full
// C13 timeout or a full lease wait. It is an implementation constant — no
// env carries it: the lower bound must cover the control plane's
// write-then-observe latency, and the upper bound must stay under the Job
// daemon's 30s control-plane timeout.
const defaultAttributionWait = 15 * time.Second

// attributionPollInterval is the cadence at which the gate re-reads the
// platform evidence while it waits.
const attributionPollInterval = 250 * time.Millisecond

// attributionWriteReserveMax caps the share of attributionWait the gate leaves
// unspent. At the deployed budget the reserve is the same order as the shared
// store's own call bound, so one lease acquisition fits even behind a slow
// store; a budget below four times the cap keeps a quarter of itself.
const attributionWriteReserveMax = 2 * time.Second

// attributionWriteReserve is the part of attributionWait the gate does not
// spend: the caller's terminal write takes the task lease on what is left. A
// gate that exhausted its share — an expired wait, a truncated read, a C13
// that never answered — therefore still writes the verdict and its decision
// event instead of failing closed on an expired deadline (§10, ADR-031).
func (s *Scheduler) attributionWriteReserve() time.Duration {
	reserve := s.attributionWait / 4
	if reserve > attributionWriteReserveMax {
		reserve = attributionWriteReserveMax
	}
	return reserve
}

// Platform-termination fact sources (ADR-031): a troubleshooting fingerprint
// only — the pod read is the defensive fallback, never a second authority.
const (
	FactSourceJob = "job"
	FactSourcePod = "pod"
)

// task.attribution_decided decision values (§4.4, ADR-031). platform is the
// gate path's pass condition; daemon_self_report and c13_settled are legal
// branches that leave the attribution with the daemon; degraded is the
// visible failure face of an expired wait; read_error marks a read that could
// not produce a fact.
const (
	DecisionPlatform         = "platform"
	DecisionDaemonSelfReport = "daemon_self_report"
	DecisionC13Settled       = "c13_settled"
	DecisionDegraded         = "degraded"
	DecisionReadError        = "read_error"
)

// jobDeadlineExceededReason is the Job condition reason the k8s Job
// controller stamps when activeDeadlineSeconds terminated the Job, and the
// container-termination reason a deadline kill can carry.
const jobDeadlineExceededReason = "DeadlineExceeded"

// deadlineCondition reports the Job condition that carries the deadline
// termination fact: the controller publishes FailureTarget when it decides to
// fail the overdue Job and replaces it with Failed once the pods are gone, so
// only Failed misses the kill whose pod was collected first (§4.4).
func deadlineCondition(cond batchv1.JobCondition) bool {
	if cond.Status != corev1.ConditionTrue || cond.Reason != jobDeadlineExceededReason {
		return false
	}
	return cond.Type == batchv1.JobFailureTarget || cond.Type == batchv1.JobFailed
}

// PlatformTermination applies §4.4's single judgement of the platform-forced
// termination fact in the fixed order ADR-031 pins: ① the instance Job's
// deadline condition (FailureTarget or Failed) carries the deadline reason;
// ② only on a miss, the instance pod's container termination reason. A nil
// Job or a missing pod is a miss, never an error — the reader either produced
// objects or failed on its own. since filters the pods to the current Job
// instance (the jobPods filter's basis), so a reused name's predecessor pod
// cannot decide this instance's fact.
func PlatformTermination(job *batchv1.Job, pods []corev1.Pod, since time.Time) (source string, hit bool) {
	if job != nil {
		for _, cond := range job.Status.Conditions {
			if deadlineCondition(cond) {
				return FactSourceJob, true
			}
		}
	}
	for i := range pods {
		if !since.IsZero() && pods[i].CreationTimestamp.Time.Before(since) {
			continue
		}
		if PodDeadlineTerminated(&pods[i]) {
			return FactSourcePod, true
		}
	}
	return "", false
}

// PodDeadlineTerminated reports a pod whose termination evidence names the
// deadline: the container's terminated reason, or the pod-level reason. It is
// the pod half of PlatformTermination, exported so the watcher's notification
// filter reads the same shape.
func PodDeadlineTerminated(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Reason == jobDeadlineExceededReason {
			return true
		}
	}
	return pod.Status.Reason == jobDeadlineExceededReason
}

// platformEvidence is one read of the instance's platform evidence: the fact
// plus the two wait signals ADR-031 lets decide whether the gate waits.
type platformEvidence struct {
	source string
	hit    bool
	// deleting: a pod of the instance already carries a platform-initiated
	// deletion (deletionTimestamp set). Never a "pod terminated" reading —
	// that shape is shared by ordinary self-failures and would pull them into
	// the wait face, diluting the degradation signal.
	deleting bool
	// overdue: the Job declares a deadline budget and the instance has passed
	// its apiserver creation instant plus that budget. The start point is the
	// apiserver's fact (the Job's creationTimestamp), never the arrival time
	// of the start report.
	overdue bool
}

// waiting reports whether either wait signal holds. They only decide whether
// to wait; they are never a verdict.
func (ev platformEvidence) waiting() bool { return ev.deleting || ev.overdue }

// readPlatformEvidence reads the instance's Job and pods once and derives
// both the fact and the wait signals from that read.
func (s *Scheduler) readPlatformEvidence(ctx context.Context, e registry.TaskEntry) (platformEvidence, error) {
	job, err := s.jobs.GetJob(ctx, e.JobName)
	if err != nil {
		return platformEvidence{}, fmt.Errorf("get job %s: %w", e.JobName, err)
	}
	pods, err := s.jobPods(ctx, e.JobName, e.JobCreatedAt)
	if err != nil {
		return platformEvidence{}, fmt.Errorf("list pods of job %s: %w", e.JobName, err)
	}
	ev := platformEvidence{}
	ev.source, ev.hit = PlatformTermination(job, pods, e.JobCreatedAt)
	for i := range pods {
		if !pods[i].DeletionTimestamp.IsZero() {
			ev.deleting = true
			break
		}
	}
	if job != nil {
		if budget := job.Spec.ActiveDeadlineSeconds; budget != nil && *budget > 0 {
			start := job.CreationTimestamp.Time
			if !start.IsZero() && s.now().After(start.Add(time.Duration(*budget)*time.Second)) {
				ev.overdue = true
			}
		}
	}
	return ev, nil
}

// awaitPlatformFact waits for the fact to become visible within the gate
// path's remaining budget, re-reading the evidence on attributionPollInterval.
// The bound and the overdue signal are read from the same clock — the
// scheduler's — so the two halves of one wait decision never disagree; the
// poll cadence stays a real timer, since it decides when to look again, not
// when the wait ends.
func (s *Scheduler) awaitPlatformFact(ctx context.Context, e registry.TaskEntry, deadline time.Time) (platformEvidence, error) {
	for {
		remaining := deadline.Sub(s.now())
		if remaining <= 0 {
			return platformEvidence{}, nil
		}
		wait := attributionPollInterval
		if wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return platformEvidence{}, ctx.Err()
		case <-timer.C:
		}
		ev, err := s.readPlatformEvidence(ctx, e)
		if err != nil || ev.hit {
			return ev, err
		}
	}
}

// taskStatusWithin is C13 under the gate path's remaining budget: the read
// spends what the evidence reads, the wait and the task lock left of
// attributionWait, never a fresh control-plane timeout (§10, ADR-031). An
// exhausted budget makes no call, so the caller takes its undecidable branch.
func (s *Scheduler) taskStatusWithin(ctx context.Context, taskID string) (string, error) {
	deadline, ok := ctx.Deadline()
	if ok && !time.Now().Before(deadline) {
		return "", fmt.Errorf("C13 %s: attribution budget exhausted", taskID)
	}
	return s.server.TaskStatus(ctx, taskID)
}

// lockTaskWithin takes the task lock within what is left of the gate path's
// budget: the caller's terminal write of an undecided report is the last
// stage that draws on attributionWait, so a held lease cannot put the S14
// response back on the lease's TTL (§10, ADR-031). A zero deadline is a
// report the gate never ran for; it keeps the caller's own bound.
func (s *Scheduler) lockTaskWithin(ctx context.Context, taskID string, deadline time.Time) (func(), error) {
	if deadline.IsZero() {
		return s.lockTaskFor(ctx, taskID)
	}
	lockCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return s.lockTaskFor(lockCtx, taskID)
}

// gateVerdict is the attribution gate's outcome for its caller. It keeps the
// gate's own settlement apart from a decision another path landed while the
// gate was reading or waiting: the caller must not write a second terminal
// verdict in either shape, and only gateUndecided leaves the decision to the
// inbound report (§4.4, ADR-031).
type gateVerdict int

const (
	// gateUndecided: the gate left the terminal decision to this report.
	gateUndecided gateVerdict = iota
	// gateSettled: the gate settled the instance with the platform verdict.
	gateSettled
	// gateDecidedElsewhere: another path decided the instance while the gate
	// was running; its decided body is the one to forward.
	gateDecidedElsewhere
)

// gateDecided re-reads the instance under the task lock and reports whether
// another path already decided it. Every non-settling exit of the gate runs
// it first: the wait opens a window in which the convergence path may settle
// the instance, and a decided instance is neither rewritten nor given a
// second decision event (§4.4). body is replaced by the registered decided
// body when one exists; otherwise the caller's body stands.
func (s *Scheduler) gateDecided(e registry.TaskEntry, body []byte) (registry.TaskEntry, []byte, bool) {
	unlock := s.taskMu.Lock(e.TaskID)
	defer unlock()
	if fresh, ok := s.reg.Get(e.TaskID); ok {
		e = fresh
	}
	if decided, ok := s.reg.DecidedReport(e.TaskID); ok {
		return e, decided, true
	}
	if e.IsTerminal() {
		return e, body, true
	}
	return e, body, false
}

// gateDecision is one attribution decision a gate round leaves for its caller
// to apply: the values task.attribution_decided carries (§9.1, ADR-031). The
// path that applies the decision emits it, under the lock that applies it, so
// the event never names a decision another path superseded.
type gateDecision struct {
	decision    string
	factSource  string
	errorSource string
}

// gateUndecidedExit is the gate's non-settling exit: another path's decision
// wins and emits nothing; otherwise the decision travels back to the caller,
// which emits it when — and only when — its own terminal write is the one that
// decides.
func (s *Scheduler) gateUndecidedExit(e registry.TaskEntry, body []byte, decision, factSource, errorSource string) (registry.TaskEntry, []byte, gateVerdict, gateDecision) {
	fresh, decidedBody, decided := s.gateDecided(e, body)
	if decided {
		return fresh, decidedBody, gateDecidedElsewhere, gateDecision{}
	}
	return fresh, body, gateUndecided, gateDecision{decision: decision, factSource: factSource, errorSource: errorSource}
}

// attributionGate is §4.4's gate, the S14 branch's first step (ADR-031). It
// faces only a not-yet-decided inbound daemon failure self-report; a decided
// instance and the forwards produced by a verdict (the convergence
// compensation, a queue replay) never enter it. The returned entry is the
// entry after the gate — settled with the compensation body when the platform
// fact decided the failure — the returned body is what the caller forwards,
// and the last result is the gate's verdict: only gateUndecided leaves the
// terminal write to the caller, and the pending decision rides back with it so
// the caller announces it at that write. gateDeadline is the round's wall-clock
// budget, derived by the caller so its terminal-write lock draws on the same
// span (§10, ADR-031).
func (s *Scheduler) attributionGate(ctx context.Context, e registry.TaskEntry, body []byte, sessionID string, gateDeadline time.Time) (registry.TaskEntry, []byte, gateVerdict, gateDecision) {
	// One budget covers the whole gate path (§10, ADR-031): the reads, the
	// wait, the task lock and C13 all draw on it, so no stage opens a fresh
	// control-plane timeout behind another. The wait reads the bound on the
	// scheduler's clock — the overdue signal's source — and the context
	// carries that same span on the wall clock, which is what cuts a slow
	// read or a held lock off mid-flight. The gate stops one reserve short of
	// the round's budget: the caller's terminal write takes the lease on the
	// difference, so the exits that spend this share still land the verdict.
	reserve := s.attributionWriteReserve()
	deadline := s.now().Add(s.attributionWait - reserve)
	gateCtx, cancel := context.WithDeadline(ctx, gateDeadline.Add(-reserve))
	defer cancel()
	ev, err := s.readPlatformEvidence(gateCtx, e)
	if err != nil {
		// A read that failed — the budget cutting it off is one such read —
		// is not a teardown sign: the report is forwarded as it arrived, with
		// the failure recorded.
		return s.gateUndecidedExit(e, body, DecisionReadError, "", "platform_fact")
	}
	if !ev.hit {
		if !ev.waiting() {
			// Neither wait signal holds: the attribution stays with the daemon
			// and the report is forwarded as it arrived.
			return s.gateUndecidedExit(e, body, DecisionDaemonSelfReport, "", "")
		}
		ev, err = s.awaitPlatformFact(gateCtx, e, deadline)
		if err != nil {
			if ctx.Err() != nil {
				return e, body, gateUndecided, gateDecision{}
			}
			if errors.Is(err, context.DeadlineExceeded) {
				// The budget ran out mid-read: the bound's own expiry, not a
				// read failure, so it takes the degraded face below.
				return s.gateUndecidedExit(e, body, DecisionDegraded, "", "")
			}
			return s.gateUndecidedExit(e, body, DecisionReadError, "", "platform_fact")
		}
		if !ev.hit {
			// The bound expired without a fact: the self-report is released
			// and the degradation is visible (the design's failure face).
			return s.gateUndecidedExit(e, body, DecisionDegraded, "", "")
		}
	}
	// The platform fact holds: the server's verdict and the decision run under
	// the task's lock, so no settlement path can decide this instance while
	// the gate is deciding on its behalf, and an instance another path decided
	// during the wait is never rewritten (§4.4). The lock draws on the same
	// budget, so a holder cannot keep the gate open for the rest of its lease.
	unlock, lockErr := s.lockTaskFor(gateCtx, e.TaskID)
	if lockErr != nil {
		// The shared task lock is unreachable within the budget: never decide
		// on a snapshot another instance may be changing. This exit writes no
		// verdict and holds no lock, so the caller forwards the instance's
		// decided body when one is registered, queues this report and answers
		// inside the same bound — it must not take the lease again.
		return e, body, gateDecidedElsewhere, gateDecision{}
	}
	defer unlock()
	if fresh, ok := s.reg.Get(e.TaskID); ok {
		e = fresh
	}
	if e.IsTerminal() || len(e.DecidedReport) > 0 {
		if decided, ok := s.reg.DecidedReport(e.TaskID); ok {
			return e, decided, gateDecidedElsewhere, gateDecision{}
		}
		return e, body, gateDecidedElsewhere, gateDecision{}
	}
	// The server's own verdict is consulted before Foreman decides on the
	// daemon's behalf (§4.4: 补偿前必查 C13), spending only what the gate
	// path has left of its budget — never a fresh control-plane timeout.
	// These exits leave the terminal write to the caller, so their decision
	// rides back unannounced.
	st, err := s.taskStatusWithin(gateCtx, e.TaskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		return e, body, gateUndecided, gateDecision{decision: DecisionC13Settled}
	case err != nil:
		return e, body, gateUndecided, gateDecision{decision: DecisionReadError, errorSource: "c13"}
	}
	s.reg.SetLastStatusSeen(e.TaskID, st)
	if registry.IsTerminalResult(st) {
		return e, body, gateUndecided, gateDecision{decision: DecisionC13Settled}
	}
	// The fact suppresses this instance's failure self-report: the verdict is
	// the compensation body, registered before the forward, session value
	// included.
	settled, decided, err := s.settleFailure(ctx, e, failReport{
		reason:    FailureReasonJobDeadline,
		message:   "task terminated by the platform before the daemon's failure report",
		sessionID: sessionID,
	})
	if err != nil {
		s.log.ErrorContext(ctx, "task.settle_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"reason", FailureReasonJobDeadline, "err", err)
		// The platform verdict never landed: the daemon report decides after
		// all, and the event names that decision at the caller's write.
		return e, body, gateUndecided, gateDecision{decision: DecisionDaemonSelfReport}
	}
	// The settlement landed inside this lock, so the event names the verdict
	// that stands (§9.1).
	s.attributionDecided(ctx, e, DecisionPlatform, ev.source, "")
	return settled, decided, gateSettled, gateDecision{}
}

// attributionDecided emits the gate's decision event (§9.1, ADR-031). It is
// the gate's own event face only: a replay or a convergence compensation
// never emits it. Both callers emit it under the lock that applies the
// verdict — the gate once its own settlement landed, OnReport at the terminal
// write it leaves to the report — so the event never names a decision a
// failed write or another path left without a verdict.
func (s *Scheduler) attributionDecided(ctx context.Context, e registry.TaskEntry, decision, factSource, errorSource string) {
	fields := []any{
		"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
		"decision", decision,
	}
	if factSource != "" {
		fields = append(fields, "fact_source", factSource)
	}
	if errorSource != "" {
		fields = append(fields, "error_source", errorSource)
	}
	if decision == DecisionDegraded || decision == DecisionReadError {
		s.log.WarnContext(ctx, "task.attribution_decided", fields...)
		return
	}
	s.log.InfoContext(ctx, "task.attribution_decided", fields...)
}
