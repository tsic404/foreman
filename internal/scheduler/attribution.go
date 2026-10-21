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

// defaultAttributionWait is the gate's wait upper bound (ADR-031): how long
// the platform termination fact is awaited before the daemon's self-report is
// released. It is an implementation constant — not a configuration item, and
// no env carries it: the lower bound must cover the control plane's
// write-then-observe latency, the upper bound must stay under the Job
// daemon's 30s control-plane timeout so a gated response never lands in the
// daemon's retry face.
const defaultAttributionWait = 15 * time.Second

// attributionPollInterval is the cadence at which the gate re-reads the
// platform evidence while it waits.
const attributionPollInterval = 250 * time.Millisecond

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

// jobDeadlineExceededReason is the Job Failed condition reason the k8s Job
// controller stamps when activeDeadlineSeconds terminated the Job, and the
// container-termination reason a deadline kill can carry.
const jobDeadlineExceededReason = "DeadlineExceeded"

// PlatformTermination applies §4.4's single judgement of the platform-forced
// termination fact in the fixed order ADR-031 pins: ① the instance Job's
// Failed condition carries the deadline reason; ② only on a miss, the
// instance pod's container termination reason. A nil Job or a missing pod is
// a miss, never an error — the reader either produced objects or failed on
// its own. since filters the pods to the current Job instance (the jobPods
// filter's basis), so a reused name's predecessor pod cannot decide this
// instance's fact.
func PlatformTermination(job *batchv1.Job, pods []corev1.Pod, since time.Time) (source string, hit bool) {
	if job != nil {
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue &&
				cond.Reason == jobDeadlineExceededReason {
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

// awaitPlatformFact waits up to attributionWait for the fact to become
// visible, re-reading the evidence on attributionPollInterval. The bound runs
// on the wall clock: it bounds this HTTP round, not a virtual one.
func (s *Scheduler) awaitPlatformFact(ctx context.Context, e registry.TaskEntry) (platformEvidence, error) {
	deadline := time.Now().Add(s.attributionWait)
	for {
		remaining := time.Until(deadline)
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

// gateUndecidedExit is the gate's non-settling exit: another path's decision
// wins and emits nothing; otherwise the decision event is emitted and the
// caller keeps the terminal decision.
func (s *Scheduler) gateUndecidedExit(ctx context.Context, e registry.TaskEntry, body []byte, decision, factSource, errorSource string) (registry.TaskEntry, []byte, gateVerdict) {
	fresh, decidedBody, decided := s.gateDecided(e, body)
	if decided {
		return fresh, decidedBody, gateDecidedElsewhere
	}
	s.attributionDecided(ctx, fresh, decision, factSource, errorSource)
	return fresh, body, gateUndecided
}

// attributionGate is §4.4's gate, the S14 branch's first step (ADR-031). It
// faces only a not-yet-decided inbound daemon failure self-report; a decided
// instance and the forwards produced by a verdict (the convergence
// compensation, a queue replay) never enter it. The returned entry is the
// entry after the gate — settled with the compensation body when the platform
// fact decided the failure — the returned body is what the caller forwards,
// and the last result is the gate's verdict: only gateUndecided leaves the
// terminal write to the caller.
func (s *Scheduler) attributionGate(ctx context.Context, e registry.TaskEntry, body []byte, sessionID string) (registry.TaskEntry, []byte, gateVerdict) {
	ev, err := s.readPlatformEvidence(ctx, e)
	if err != nil {
		// A read that failed is not a teardown sign: the report is forwarded
		// as it arrived, with the failure recorded.
		return s.gateUndecidedExit(ctx, e, body, DecisionReadError, "", "platform_fact")
	}
	if !ev.hit {
		if !ev.waiting() {
			// Neither wait signal holds: the attribution stays with the daemon
			// and the report is forwarded as it arrived.
			return s.gateUndecidedExit(ctx, e, body, DecisionDaemonSelfReport, "", "")
		}
		ev, err = s.awaitPlatformFact(ctx, e)
		if err != nil {
			if ctx.Err() != nil {
				return e, body, gateUndecided
			}
			return s.gateUndecidedExit(ctx, e, body, DecisionReadError, "", "platform_fact")
		}
		if !ev.hit {
			// The bound expired without a fact: the self-report is released
			// and the degradation is visible (the design's failure face).
			return s.gateUndecidedExit(ctx, e, body, DecisionDegraded, "", "")
		}
	}
	// The platform fact holds: the server's verdict and the decision run under
	// the task's lock, so no settlement path can decide this instance while
	// the gate is deciding on its behalf, and an instance another path decided
	// during the wait is never rewritten (§4.4).
	unlock, lockErr := s.lockTaskFor(ctx, e.TaskID)
	if lockErr != nil {
		// The shared task lock is unreachable: never decide on a snapshot
		// another instance may be changing; the report is forwarded as-is and
		// the shared CAS keeps the terminal write single.
		return e, body, gateDecidedElsewhere
	}
	defer unlock()
	if fresh, ok := s.reg.Get(e.TaskID); ok {
		e = fresh
	}
	if e.IsTerminal() || len(e.DecidedReport) > 0 {
		if decided, ok := s.reg.DecidedReport(e.TaskID); ok {
			return e, decided, gateDecidedElsewhere
		}
		return e, body, gateDecidedElsewhere
	}
	// The server's own verdict is consulted before Foreman decides on the
	// daemon's behalf (§4.4: 补偿前必查 C13).
	st, err := s.server.TaskStatus(ctx, e.TaskID)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		s.attributionDecided(ctx, e, DecisionC13Settled, "", "")
		return e, body, gateUndecided
	case err != nil:
		s.attributionDecided(ctx, e, DecisionReadError, "", "c13")
		return e, body, gateUndecided
	}
	s.reg.SetLastStatusSeen(e.TaskID, st)
	if registry.IsTerminalResult(st) {
		s.attributionDecided(ctx, e, DecisionC13Settled, "", "")
		return e, body, gateUndecided
	}
	// The fact suppresses this instance's failure self-report: the verdict is
	// the compensation body, registered before the forward, session value
	// included.
	s.attributionDecided(ctx, e, DecisionPlatform, ev.source, "")
	settled, decided, err := s.settleFailure(ctx, e, failReport{
		reason:    FailureReasonJobDeadline,
		message:   "task terminated by the platform before the daemon's failure report",
		sessionID: sessionID,
	})
	if err != nil {
		s.log.ErrorContext(ctx, "task.settle_failed",
			"task_id", e.TaskID, "job_name", e.JobName, "daemon_id", e.DaemonID,
			"reason", FailureReasonJobDeadline, "err", err)
		return e, body, gateUndecided
	}
	return settled, decided, gateSettled
}

// attributionDecided emits the gate's decision event (§9.1, ADR-031). It is
// the gate's own event face only: a replay or a convergence compensation
// never emits it.
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
