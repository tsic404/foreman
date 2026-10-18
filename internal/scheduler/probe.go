package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// ProbeHandle identifies one catalog-probe run. Callers hand it
// back to AwaitProbeRelayed/DeleteProbe verbatim; they never build or parse it.
type ProbeHandle struct {
	RequestID  string // the probe Job name's suffix (C2 ack id, or the minted startup id)
	JobName    string // fm-catalog-<requestID>
	JobRuntime string // uuid5(NS_JOB_RUNTIME, JobName), minted at Start
}

// Probe failure classes of the startup probe: the reason values are fixed by
// §10, and every one has its own observable landing point.
const (
	ProbeFailureJobCreateFailed     = "job_create_failed"
	ProbeFailurePodNotStarted       = "pod_not_started"
	ProbeFailureDaemonNotRegistered = "daemon_not_registered"
	ProbeFailureDiscoveryNotLanded  = "discovery_not_landed"
)

// ErrStartupProbeUnregistered is the fail-fast verdict: no runtime registered
// inside the startup window, so Foreman must exit non-zero (C-1, AC-36).
var ErrStartupProbeUnregistered = errors.New("startup probe registered no runtime within the window")

// StartupProbeFailure carries the fixed reason class of a failed startup probe,
// so the exit line reports the same reason the event does (§9.1
// pins the field set and requires the same reason on the exit record).
type StartupProbeFailure struct {
	JobName string
	Reason  string
}

func (e *StartupProbeFailure) Error() string {
	return fmt.Sprintf("%v: %s (reason %s)", ErrStartupProbeUnregistered, e.JobName, e.Reason)
}

// Unwrap keeps errors.Is(err, ErrStartupProbeUnregistered) working.
func (e *StartupProbeFailure) Unwrap() error { return ErrStartupProbeUnregistered }

// probePollInterval is how often the startup orchestration re-reads the probe
// session and the probe's pod state.
const probePollInterval = 500 * time.Millisecond

// relayPollInterval is how often AwaitProbeRelayed re-reads the session's
// result face; the wait is a shared-state poll, not a new channel.
const relayPollInterval = 100 * time.Millisecond

// probePodPollInterval is how often a running probe's pod is inspected for its
// landing node (the session's node_name producer).
const probePodPollInterval = time.Second

// WithProbeRegistry wires the catalog-probe session table: probe identity,
// command delivery and result routing all read it.
func WithProbeRegistry(probes *registry.ProbeRegistry) Option {
	return func(s *Scheduler) { s.probes = probes }
}

// probeJobName is the probe Job name of one request id (§3).
func probeJobName(requestID string) string { return "fm-catalog-" + requestID }

// StartProbe builds one catalog-probe Job: it mints the job runtime id, issues
// the probe Job Token, writes the session, then creates the Secret and the Job
// in that fixed order — the session must exist before the pod can register, and
// the Secret before the Job that mounts it.
//
// realRuntimeID is empty in startup-discovery mode, where the probe's own
// minted command id (== requestID) is what S5 delivers.
func (s *Scheduler) StartProbe(ctx context.Context, requestID, realRuntimeID string, budget time.Duration) (ProbeHandle, error) {
	if s.probes == nil {
		return ProbeHandle{}, errors.New("probe session table is not wired")
	}
	if requestID == "" {
		return ProbeHandle{}, errors.New("probe request id is required")
	}
	jobName := probeJobName(requestID)
	jobRuntime := jobRuntimeID(jobName)
	session := registry.ProbeSession{
		JobName:       jobName,
		SelfMintedID:  requestID,
		JobRuntime:    jobRuntime,
		RealRuntimeID: realRuntimeID,
		Deadline:      s.now().Add(budget),
	}
	if realRuntimeID != "" {
		// Command-driven probe: the id came from the server's ack and is
		// relayed verbatim; the minted id is not used.
		session.SelfMintedID = ""
		session.RequestID = requestID
	}
	job, cred, tokenHash, err := s.builder.BuildCatalogProbe(requestID, jobRuntime, s.cfg.WorkspaceID, budget)
	if err != nil {
		return ProbeHandle{}, fmt.Errorf("build catalog probe %s: %w", jobName, err)
	}
	session.TokenHash = tokenHash
	if err := s.probes.PutProbe(session); err != nil {
		return ProbeHandle{}, fmt.Errorf("record probe session %s: %w", jobName, err)
	}
	handle := ProbeHandle{RequestID: requestID, JobName: jobName, JobRuntime: jobRuntime}
	if err := s.jobs.CreateSecret(ctx, cred); err != nil {
		s.rollbackProbe(ctx, handle)
		return ProbeHandle{}, fmt.Errorf("create probe secret %s: %w", cred.Name, err)
	}
	if _, err := s.jobs.CreateJob(ctx, job); err != nil {
		s.rollbackProbe(ctx, handle)
		return ProbeHandle{}, fmt.Errorf("create probe job %s: %w", jobName, err)
	}
	s.log.InfoContext(ctx, "probe.started", "job_name", jobName, "job_runtime_id", jobRuntime, "budget", budget)
	go s.trackProbeNode(context.WithoutCancel(ctx), handle, session.Deadline)
	return handle, nil
}

// rollbackProbe removes what StartProbe built so far, in the teardown order
// (session first, then Secret, then Job); a failed rollback leaves objects for
// the sweep to find.
func (s *Scheduler) rollbackProbe(ctx context.Context, h ProbeHandle) {
	if err := s.DeleteProbe(ctx, h); err != nil {
		s.log.WarnContext(ctx, "probe rollback incomplete", "job_name", h.JobName, "err", err)
	}
}

// trackProbeNode backfills the session's node_name from the probe pod: the
// fake server cannot know it (it never touches K8s), and the value is what the
// relay event and AwaitProbeRelayed report.
func (s *Scheduler) trackProbeNode(ctx context.Context, h ProbeHandle, deadline time.Time) {
	for {
		pods, err := s.jobs.ListPods(ctx, jobNameSelector(h.JobName))
		if err == nil {
			for i := range pods {
				if node := pods[i].Spec.NodeName; node != "" {
					s.probes.UpdateProbe(h.JobName, func(session *registry.ProbeSession) {
						session.NodeName = node
					})
					return
				}
			}
		}
		if ctx.Err() != nil || !s.now().Before(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(probePodPollInterval):
		}
	}
}

// AwaitProbeRelayed waits until the probe's result has landed (startup
// discovery) or been forwarded (command driven), or the deadline passes. The
// completion signal is the session's RelayedAt — the result path writes it, not
// the container log.
func (s *Scheduler) AwaitProbeRelayed(ctx context.Context, h ProbeHandle, deadline time.Time) (nodeName string, relayed bool, relayedCode int, err error) {
	for {
		session, ok := s.probes.ProbeByJob(h.JobName)
		if !ok {
			// The session is gone: the probe was torn down or swept, so no
			// result can arrive any more.
			return "", false, 0, nil
		}
		if !session.RelayedAt.IsZero() {
			return session.NodeName, true, session.RelayedCode, nil
		}
		if !s.now().Before(deadline) {
			return session.NodeName, false, 0, nil
		}
		select {
		case <-ctx.Done():
			return session.NodeName, false, 0, ctx.Err()
		case <-time.After(relayPollInterval):
		}
	}
}

// DeleteProbe tears one probe down: the session goes first and unconditionally
// (it is the credential's validity face), then the Secret, then the Job, all
// with the Background propagation policy. Every step is attempted; the first
// failure is returned.
func (s *Scheduler) DeleteProbe(ctx context.Context, h ProbeHandle) error {
	var errs []error
	if err := s.probes.DeleteProbe(h.JobName); err != nil {
		errs = append(errs, fmt.Errorf("remove probe session %s: %w", h.JobName, err))
	}
	if err := s.jobs.DeleteSecret(ctx, credName(h.JobName)); err != nil {
		errs = append(errs, fmt.Errorf("delete probe secret %s: %w", credName(h.JobName), err))
	}
	if err := s.jobs.DeleteJob(ctx, h.JobName); err != nil {
		errs = append(errs, fmt.Errorf("delete probe job %s: %w", h.JobName, err))
	}
	if err := errors.Join(errs...); err != nil {
		s.log.WarnContext(ctx, "probe teardown incomplete", "job_name", h.JobName, "err", err)
		return err
	}
	return nil
}

// SweepStaleProbes clears probe remnants by their labels, independent of the
// session table (which is not rebuilt after a restart). Two discovery faces
// share one grace period and one ownership exclusion: anything carrying a
// task-id belongs to the task face and is never touched here
// (§6.5: the probe residual sweep).
func (s *Scheduler) SweepStaleProbes(ctx context.Context) error {
	cutoff := s.now().Add(-s.staleProbeGrace())
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	jobs, err := s.jobs.ListJobs(ctx)
	if err != nil {
		return err
	}
	for i := range jobs {
		job := jobs[i]
		if !isProbeObject(job.GetLabels()) {
			continue
		}
		if job.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		s.log.InfoContext(ctx, "probe.swept", "job_name", job.GetName(), "age_seconds", s.now().Sub(job.CreationTimestamp.Time).Seconds())
		if derr := s.jobs.DeleteSecret(ctx, credName(job.GetName())); derr != nil {
			record(derr)
		}
		if derr := s.jobs.DeleteJob(ctx, job.GetName()); derr != nil {
			record(derr)
		}
		s.probes.DeleteProbe(job.GetName())
	}
	secrets, err := s.jobs.ListSecrets(ctx)
	if err != nil {
		return err
	}
	for i := range secrets {
		secret := secrets[i]
		if !isProbeObject(secret.Labels) || !strings.HasSuffix(secret.Name, "-cred") {
			continue
		}
		jobName := strings.TrimSuffix(secret.Name, "-cred")
		job, jerr := s.jobs.GetJob(ctx, jobName)
		if jerr != nil || job != nil {
			// An API error is not a NotFound, and a live Job owns its Secret:
			// the "Secret created, Job not yet" window must survive.
			continue
		}
		if secret.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		s.log.InfoContext(ctx, "probe.swept", "secret", secret.Name, "job_name", jobName,
			"age_seconds", s.now().Sub(secret.CreationTimestamp.Time).Seconds())
		if derr := s.jobs.DeleteSecret(ctx, secret.Name); derr != nil {
			record(derr)
		}
		s.probes.DeleteProbe(jobName)
	}
	return firstErr
}

// staleProbeGrace is the probe remnant grace: one catalog budget plus a
// reconciliation round on each side, measured from creationTimestamp.
func (s *Scheduler) staleProbeGrace() time.Duration {
	return s.cfg.ModelCatalogTimeout + 2*s.cfg.ReconcileInterval
}

// isProbeObject is the probe domain predicate: the role label present and the
// task-id label absent (the task face owns anything carrying a task-id).
func isProbeObject(labels map[string]string) bool {
	if labels == nil {
		return false
	}
	if labels[registry.LabelRole] != registry.RoleCatalogProbe {
		return false
	}
	_, hasTask := labels[registry.LabelTaskID]
	return !hasTask
}

// RunStartupProbe is the startup orchestration of ADR-025: build one probe,
// wait for its registration (the truth source C1 declares), wait for the
// discovery to land when the catalog route is on, clean up, and hand the
// discovery back for C1.
//
// Only "no runtime registered inside the window" is fatal — the discovery not
// landing is a visible failure event that does not stop startup (C-1, §10).
func (s *Scheduler) RunStartupProbe(ctx context.Context) (registry.Discovery, error) {
	window := s.cfg.StartupProbeTimeout
	minted, err := mintedProbeID()
	if err != nil {
		return registry.Discovery{}, fmt.Errorf("mint startup probe id: %w", err)
	}
	handle, err := s.StartProbe(ctx, minted, "", window)
	if err != nil {
		s.reportProbeFailure(ctx, probeJobName(minted), ProbeFailureJobCreateFailed, "", "", false)
		return registry.Discovery{}, fmt.Errorf("startup probe: %w", err)
	}
	deadline := s.now().Add(window)
	readRegistration := func() (registry.Discovery, bool) {
		session, ok := s.probes.ProbeByJob(handle.JobName)
		if !ok || session.RegisteredAt.IsZero() {
			return registry.Discovery{}, false
		}
		return registry.Discovery{
			CLIVersion:     session.CLIVersion,
			Runtimes:       session.Runtimes,
			FailedProfiles: session.FailedProfiles,
		}, true
	}
	discovery := registry.Discovery{}
	podStatus, jobEvent := "", ""
	podStarted := false
	registered := false
	for s.now().Before(deadline) {
		if found, ok := readRegistration(); ok {
			registered, discovery = true, found
			break
		}
		status, started, event := s.probeObservation(ctx, handle.JobName)
		podStatus, jobEvent = status, event
		podStarted = podStarted || started
		select {
		case <-ctx.Done():
			s.teardownStartupProbe(ctx, handle)
			return registry.Discovery{}, ctx.Err()
		case <-time.After(probePollInterval):
		}
	}
	if !registered {
		// The window's last poll interval still counts: a registration landing
		// in it is "a runtime registered within the window" (§10).
		if found, ok := readRegistration(); ok {
			registered, discovery = true, found
		}
	}
	if !registered {
		// The third class is "pod up but the daemon never registered", which
		// covers a pod that started and then exited (the template-credential
		// failure shape); only a pod that never ran is the second class.
		reason := ProbeFailureDaemonNotRegistered
		if !podStarted {
			reason = ProbeFailurePodNotStarted
		}
		sessionExists := false
		if _, ok := s.probes.ProbeByJob(handle.JobName); ok {
			sessionExists = true
		}
		s.reportProbeFailure(ctx, handle.JobName, reason, podStatus, jobEvent, sessionExists)
		s.teardownStartupProbe(ctx, handle)
		return registry.Discovery{}, &StartupProbeFailure{JobName: handle.JobName, Reason: reason}
	}
	// The registration is in: when the catalog route is on, the startup
	// discovery still has to land in its slot before the probe is torn down.
	// With the route off, registration alone completes the orchestration —
	// the catalog face is a supported deployment degradation, not a failure.
	if s.cfg.ModelCatalogEnabled {
	waitLanding:
		for s.now().Before(deadline) {
			session, ok := s.probes.ProbeByJob(handle.JobName)
			if !ok || !session.RelayedAt.IsZero() {
				break
			}
			select {
			case <-ctx.Done():
				break waitLanding
			case <-time.After(relayPollInterval):
			}
		}
		if session, ok := s.probes.ProbeByJob(handle.JobName); ok && session.RelayedAt.IsZero() {
			s.reportProbeFailure(ctx, handle.JobName, ProbeFailureDiscoveryNotLanded, podStatus, jobEvent, true)
		}
	}
	s.teardownStartupProbe(ctx, handle)
	return discovery, nil
}

func (s *Scheduler) teardownStartupProbe(ctx context.Context, h ProbeHandle) {
	if err := s.DeleteProbe(context.WithoutCancel(ctx), h); err != nil {
		s.log.WarnContext(ctx, "startup probe teardown incomplete", "job_name", h.JobName, "err", err)
	}
}

// reportProbeFailure emits the startup probe failure event; the exit path
// carries the same reason in its error line (§10, §9.1).
func (s *Scheduler) reportProbeFailure(ctx context.Context, jobName, reason, podStatus, jobEvent string, sessionExists bool) {
	s.log.ErrorContext(ctx, "startup.probe_failed",
		"job_name", jobName, "reason", reason, "pod_status", podStatus,
		"job_event", jobEvent, "session_exists", sessionExists)
}

// probeObservation summarizes the probe's pod and Job state for the failure
// event: the pod's phase (with a container waiting reason when present), whether
// the pod ever ran a container, and the Job's own failure condition.
func (s *Scheduler) probeObservation(ctx context.Context, jobName string) (podStatus string, started bool, jobEvent string) {
	if pods, err := s.jobs.ListPods(ctx, jobNameSelector(jobName)); err == nil && len(pods) > 0 {
		pod := pods[0]
		for i := range pods {
			if pods[i].CreationTimestamp.After(pod.CreationTimestamp.Time) {
				pod = pods[i]
			}
		}
		podStatus = string(pod.Status.Phase)
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
				podStatus += "/" + status.State.Waiting.Reason
				break
			}
		}
		started = podStarted(pod)
	}
	job, err := s.jobs.GetJob(ctx, jobName)
	if err != nil {
		return podStatus, started, "job lookup failed"
	}
	if job == nil {
		return podStatus, started, "job not found"
	}
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		jobEvent = string(condition.Type)
		if condition.Reason != "" {
			jobEvent += "/" + condition.Reason
		}
	}
	return podStatus, started, jobEvent
}

// podStarted reports whether the probe pod ever ran a container: a pod that
// started and exited still means "the daemon did not register", while a pod
// stuck in Pending/Init never got that far.
func podStarted(pod corev1.Pod) bool {
	switch pod.Status.Phase {
	case corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
		return true
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if status.State.Running != nil || status.State.Terminated != nil || status.LastTerminationState.Terminated != nil {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Running != nil || status.State.Terminated != nil || status.LastTerminationState.Terminated != nil {
			return true
		}
		if status.Started != nil && *status.Started {
			return true
		}
	}
	return false
}

// mintedProbeID is the startup discovery's self-minted command id: it must not
// share the server's request-id shape, so it carries an explicit prefix.
func mintedProbeID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "startup-" + hex.EncodeToString(raw[:]), nil
}
