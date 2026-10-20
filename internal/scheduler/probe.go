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

	"github.com/tsic404/foreman/internal/jobbuilder"
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

// probeRegistrationSettle is how long the orchestration keeps collecting
// registrations after the last one arrived. One probe daemon registers once per
// workspace it serves (§1.2); those registrations are separate HTTP calls, so
// tearing the probe down on the first one would cut the scope short. The
// settle is an implementation constant, not a deployment knob, and stays far
// below the probe window.
const probeRegistrationSettle = time.Second

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

// StartProbe builds one catalog-probe Job: it mints the job runtime id, issues
// the probe Job Token, writes the session, then creates the Secret and the Job
// in that fixed order — the session must exist before the pod can register, and
// the Secret before the Job that mounts it.
//
// mode fixes the session's command face (registry.ProbeMode): a startup probe
// mints its own command id, a command-driven probe relays the C2 ack's id
// verbatim (realRuntimeID names the runtime it belongs to), and a
// registration-discovery probe owns no command at all. An id that does not
// belong to the mode is rejected rather than silently ignored.
func (s *Scheduler) StartProbe(ctx context.Context, requestID, realRuntimeID, workspaceID string, mode registry.ProbeMode, budget time.Duration) (ProbeHandle, error) {
	if s.probes == nil {
		return ProbeHandle{}, errors.New("probe session table is not wired")
	}
	if requestID == "" {
		return ProbeHandle{}, errors.New("probe request id is required")
	}
	jobName := jobbuilder.ProbeJobName(requestID)
	jobRuntime := jobRuntimeID(jobName)
	session := registry.ProbeSession{
		JobName:    jobName,
		Mode:       mode,
		JobRuntime: jobRuntime,
		Deadline:   s.now().Add(budget),
	}
	switch mode {
	case registry.ProbeStartupDiscovery:
		if realRuntimeID != "" {
			return ProbeHandle{}, errors.New("startup-discovery probe carries no real runtime id")
		}
		session.SelfMintedID = requestID
	case registry.ProbeCommandDriven:
		if realRuntimeID == "" {
			return ProbeHandle{}, errors.New("command-driven probe requires a real runtime id")
		}
		if workspaceID == "" {
			return ProbeHandle{}, errors.New("command-driven probe requires a workspace id")
		}
		session.RequestID = requestID
		session.RealRuntimeID = realRuntimeID
		// A catalog probe serves one workspace (the request's); the startup
		// and discovery probes carry no pin and span the configured set.
		session.WorkspaceID = workspaceID
	case registry.ProbeRegisterOnly:
		if realRuntimeID != "" {
			return ProbeHandle{}, errors.New("registration-only probe carries no real runtime id")
		}
	default:
		return ProbeHandle{}, fmt.Errorf("unknown probe mode %d", mode)
	}
	tokenWorkspace := workspaceID
	if tokenWorkspace == "" {
		tokenWorkspace = s.probeTokenWorkspace()
	}
	if tokenWorkspace == "" {
		return ProbeHandle{}, errors.New("no workspace configured for the probe credential")
	}
	job, cred, tokenHash, err := s.builder.BuildCatalogProbe(requestID, jobRuntime, tokenWorkspace, budget)
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
	s.log.InfoContext(ctx, "probe.started", "job_name", jobName, "job_runtime_id", jobRuntime, "workspace_id", tokenWorkspace, "budget", budget)
	trackCtx, stopTracking := context.WithDeadline(s.trackerContext(ctx), session.Deadline)
	go func() {
		defer stopTracking()
		s.trackProbeNode(trackCtx, handle, session.Deadline)
	}()
	return handle, nil
}

// trackerContext is what a detached probe tracker runs under: the scheduler's
// process context once Run has started, so shutdown cancels it; before that
// (tests calling StartProbe directly) the caller's values without its
// cancellation, with the probe budget still bounding the loop.
func (s *Scheduler) trackerContext(caller context.Context) context.Context {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.lifecycle != nil {
		return s.lifecycle
	}
	return context.WithoutCancel(caller)
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
	if err := s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(h.JobName)); err != nil {
		errs = append(errs, fmt.Errorf("delete probe secret %s: %w", jobbuilder.CredentialSecretName(h.JobName), err))
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
		if workspaceID := job.GetLabels()[registry.LabelWorkspaceID]; workspaceID != "" && !s.workspaceConfigured(workspaceID) {
			// A probe of a workspace this deployment does not serve is
			// outside its sweep domain (§6.5 按 workspace 分区).
			continue
		}
		if job.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		s.log.InfoContext(ctx, "probe.swept", "job_name", job.GetName(), "age_seconds", s.now().Sub(job.CreationTimestamp.Time).Seconds())
		if derr := s.jobs.DeleteSecret(ctx, jobbuilder.CredentialSecretName(job.GetName())); derr != nil {
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
		if workspaceID := secret.Labels[registry.LabelWorkspaceID]; workspaceID != "" && !s.workspaceConfigured(workspaceID) {
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
// discovery to land when the catalog route is on, clean up, and hand each
// workspace's declaration back for C1. One probe serves the whole deployment:
// the template's daemon decides which workspaces it registers to (§1.2).
//
// Only "no runtime registered inside the window" is fatal — the discovery not
// landing is a visible failure event that does not stop startup (C-1, §10).
func (s *Scheduler) RunStartupProbe(ctx context.Context) (map[string]registry.Discovery, error) {
	return s.runProbe(ctx, true, "startup")
}

// RunDiscoveryProbe runs one registration-discovery probe for one workspace
// (§1.6): the same shape, the same window upper bound and the same cleanup as
// the startup probe, but registration alone is the completion signal — no
// model catalog discovery is performed (the profile list is what the probe's
// C1 body declares). A failure is returned to the caller; it never exits the
// process.
func (s *Scheduler) RunDiscoveryProbe(ctx context.Context, workspaceID string) (registry.Discovery, error) {
	discoveries, err := s.runProbe(ctx, false, "discovery")
	if err != nil {
		return registry.Discovery{}, err
	}
	discovery, ok := discoveries[workspaceID]
	if !ok {
		return registry.Discovery{}, fmt.Errorf("discovery probe did not register workspace %s", workspaceID)
	}
	return discovery, nil
}

// probeTokenWorkspace is the workspace the probe Job Token is bound to when
// the probe itself is not pinned: the first configured workspace. The token is
// only an admission credential for the probe Job; its S-1 answer is the
// configured set and its registrations tell which workspaces it actually
// serves.
func (s *Scheduler) probeTokenWorkspace() string {
	if ids := s.WorkspaceIDs(); len(ids) > 0 {
		return ids[0]
	}
	return s.cfg.WorkspaceID
}

// runProbe is the shared orchestration of the startup probe and the
// registration-discovery probe. waitCatalog selects the startup path's extra
// wait for the model catalog result (§1.3); the discovery probe stops at
// registration and never owns a catalog command (§1.6). The result is keyed
// by workspace: each declaration covers exactly the workspaces the probe
// registered to, and a configured workspace with no registration is reported
// (never silently served).
func (s *Scheduler) runProbe(ctx context.Context, waitCatalog bool, kind string) (map[string]registry.Discovery, error) {
	window := s.cfg.StartupProbeTimeout
	minted, err := mintedProbeID()
	if err != nil {
		return nil, fmt.Errorf("mint %s probe id: %w", kind, err)
	}
	mode := registry.ProbeRegisterOnly
	if waitCatalog {
		mode = registry.ProbeStartupDiscovery
	}
	handle, err := s.StartProbe(ctx, minted, "", "", mode, window)
	if err != nil {
		s.reportProbeFailure(ctx, kind, jobbuilder.ProbeJobName(minted), ProbeFailureJobCreateFailed, "", "", false)
		return nil, fmt.Errorf("%s probe: %w", kind, err)
	}
	deadline := s.now().Add(window)
	readRegistration := func() (registry.ProbeSession, bool) {
		session, ok := s.probes.ProbeByJob(handle.JobName)
		if !ok || session.RegisteredAt.IsZero() {
			return registry.ProbeSession{}, false
		}
		return session, true
	}
	session := registry.ProbeSession{}
	podStatus, jobEvent := "", ""
	podStarted := false
	registered := false
	for s.now().Before(deadline) {
		if found, ok := readRegistration(); ok {
			registered, session = true, found
			break
		}
		status, started, event := s.probeObservation(ctx, handle.JobName)
		podStatus, jobEvent = status, event
		podStarted = podStarted || started
		select {
		case <-ctx.Done():
			s.teardownStartupProbe(ctx, handle)
			return nil, ctx.Err()
		case <-time.After(probePollInterval):
		}
	}
	if !registered {
		// The window's last poll interval still counts: a registration landing
		// in it is "a runtime registered within the window" (§10).
		if found, ok := readRegistration(); ok {
			registered, session = true, found
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
		s.reportProbeFailure(ctx, kind, handle.JobName, reason, podStatus, jobEvent, sessionExists)
		s.teardownStartupProbe(ctx, handle)
		return nil, &StartupProbeFailure{JobName: handle.JobName, Reason: reason}
	}
	// The startup path's extra completion signal: when the catalog route is
	// on, the startup discovery still has to land in its slot before the probe
	// is torn down. With the route off — and for every discovery probe —
	// registration alone completes the orchestration.
	// One probe may register to several workspaces (§1.2). The first
	// registration is the completion signal, but the daemon's remaining
	// registrations arrive as separate calls: collect them until the session
	// has been quiet for the settle window (or the probe window ends), then
	// read the final registration set.
	settleUntil := s.now().Add(probeRegistrationSettle)
	lastRegistrations := len(session.Registrations)
settle:
	for s.now().Before(deadline) {
		select {
		case <-ctx.Done():
			break settle
		case <-time.After(probePollInterval):
		}
		current, ok := s.probes.ProbeByJob(handle.JobName)
		if !ok {
			break
		}
		if len(current.Registrations) > lastRegistrations {
			lastRegistrations = len(current.Registrations)
			settleUntil = s.now().Add(probeRegistrationSettle)
		}
		session = current
		if !s.now().Before(settleUntil) {
			break
		}
	}

	if waitCatalog && s.cfg.ModelCatalogEnabled {
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
			s.reportProbeFailure(ctx, kind, handle.JobName, ProbeFailureDiscoveryNotLanded, podStatus, jobEvent, true)
		}
	}
	s.teardownStartupProbe(ctx, handle)
	discoveries := make(map[string]registry.Discovery, len(session.Registrations))
	for _, workspaceID := range s.WorkspaceIDs() {
		registration, ok := session.Registration(workspaceID)
		if !ok {
			s.log.WarnContext(ctx, "probe.workspace_not_registered", "job_name", handle.JobName,
				"workspace_id", workspaceID, "kind", kind,
				"detail", "configured workspace was not registered by the probe; it is not served")
			continue
		}
		discoveries[workspaceID] = registration.Discovery()
	}
	if len(discoveries) == 0 && len(session.Registrations) == 0 && !session.RegisteredAt.IsZero() {
		// Legacy session shape (no per-workspace map): the single captured
		// registration belongs to the probe's own credential workspace.
		discoveries[s.probeTokenWorkspace()] = registry.Discovery{
			CLIVersion:     session.CLIVersion,
			Runtimes:       session.Runtimes,
			FailedProfiles: session.FailedProfiles,
		}
	}
	if len(discoveries) == 0 && len(session.Registrations) > 0 {
		// The probe registered to workspaces the deployment does not
		// configure: no declaration is servable.
		for workspaceID, registration := range session.Registrations {
			s.log.WarnContext(ctx, "probe.workspace_not_configured", "job_name", handle.JobName,
				"workspace_id", workspaceID, "kind", kind,
				"detail", "probe registered a workspace this deployment does not configure")
			discoveries[workspaceID] = registration.Discovery()
		}
	}
	return discoveries, nil
}

func (s *Scheduler) teardownStartupProbe(ctx context.Context, h ProbeHandle) {
	if err := s.DeleteProbe(context.WithoutCancel(ctx), h); err != nil {
		s.log.WarnContext(ctx, "startup probe teardown incomplete", "job_name", h.JobName, "err", err)
	}
}

// reportProbeFailure emits the startup probe failure event; the exit path
// carries the same reason in its error line (§10, §9.1).
func (s *Scheduler) reportProbeFailure(ctx context.Context, kind, jobName, reason, podStatus, jobEvent string, sessionExists bool) {
	event := "startup.probe_failed"
	if kind != "startup" {
		event = "discovery.probe_failed"
	}
	s.log.ErrorContext(ctx, event,
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
