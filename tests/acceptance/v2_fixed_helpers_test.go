package acceptance

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/release"
)

type v2Cache struct {
	mu        sync.Mutex
	requested []attempt.CLI
	fetched   []attempt.CLI
	present   map[string]bool
	artifacts map[string]attempt.Artifact
	err       error
	failKind  string
}

func (c *v2Cache) Ensure(_ context.Context, cli attempt.CLI) (attempt.Artifact, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requested = append(c.requested, cli)
	if c.err != nil && (c.failKind == "" || c.failKind == cli.Kind) {
		return attempt.Artifact{}, c.err
	}
	artifact, ok := c.artifacts[cli.Kind]
	if !ok {
		return attempt.Artifact{}, errors.New("cache miss")
	}
	if !c.present[cli.Kind] {
		c.fetched = append(c.fetched, cli)
	}
	return artifact, nil
}

func (c *v2Cache) calls() []attempt.CLI {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]attempt.CLI(nil), c.requested...)
}

func (c *v2Cache) fetches() []attempt.CLI {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]attempt.CLI(nil), c.fetched...)
}

type v2Remote struct {
	mu         sync.Mutex
	readyErr   error
	closeErr   error
	session    attempt.Session
	close      attempt.CloseEvidence
	readyCalls int
	closeCalls int
	readyEnvs  []string
	closed     []string
}

func (r *v2Remote) Ready(_ context.Context, binding attempt.Binding) (attempt.Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readyCalls++
	r.readyEnvs = append(r.readyEnvs, binding.EnvironmentID)
	if r.readyErr != nil {
		return attempt.Session{}, r.readyErr
	}
	return r.session, nil
}

func (r *v2Remote) Close(_ context.Context, session attempt.Session) (attempt.CloseEvidence, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeCalls++
	r.closed = append(r.closed, session.ID)
	return r.close, r.closeErr
}

type v2Registrar struct {
	registration attempt.Registration
	err          error
	calls        int
}

func (r *v2Registrar) Register(_ context.Context, binding attempt.Binding, _ attempt.Session) (attempt.Registration, error) {
	r.calls++
	if r.err != nil {
		return attempt.Registration{}, r.err
	}
	registration := r.registration
	if registration.DaemonID == "" {
		registration.DaemonID = binding.DaemonID
	}
	if registration.RuntimeID == "" {
		registration.RuntimeID = binding.JobRuntimeID
	}
	if registration.Version == "" {
		if cli, ok := binding.Multica(); ok {
			registration.Version = cli.Version
		}
	}
	return registration, nil
}

type v2Runner struct {
	result   attempt.RunResult
	err      error
	calls    int
	envs     []string
	sessions []string
}

func (r *v2Runner) Run(_ context.Context, binding attempt.Binding, session attempt.Session, _ attempt.Artifact) (attempt.RunResult, error) {
	r.calls++
	r.envs = append(r.envs, binding.EnvironmentID)
	r.sessions = append(r.sessions, session.ID)
	return r.result, r.err
}

type v2Slots struct {
	mu        sync.Mutex
	status    attempt.SlotStatus
	releases  []bool
	claims    []string
	begins    int
	binds     []attempt.Session
	starts    int
	statusErr error
	err       error
	startErr  error
}

func newV2Slots() *v2Slots {
	return &v2Slots{status: attempt.SlotStatus{Exists: true, State: attempt.SlotActive}}
}

func (s *v2Slots) Status(context.Context, attempt.Binding) (attempt.SlotStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statusErr != nil {
		return attempt.SlotStatus{}, s.statusErr
	}
	return s.status, nil
}

func (s *v2Slots) Begin(_ context.Context, _ attempt.Binding, claimID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if !s.status.Exists {
		return errors.New("slot is not recorded")
	}
	if s.status.State == attempt.SlotIsolated {
		return errors.New("slot is isolated")
	}
	if s.status.Phase == attempt.SlotPhaseIdle && s.status.ClaimID == "" {
		s.status.ClaimID = claimID
		s.status.Phase = attempt.SlotPhaseOpening
	} else if s.status.Phase == attempt.SlotPhaseOpening && s.status.ClaimID == claimID {
		// A retry of the same begin is idempotent.
	} else {
		return errors.New("slot is owned by another attempt execution")
	}
	s.begins++
	s.claims = append(s.claims, claimID)
	return nil
}

func (s *v2Slots) TakeOver(_ context.Context, _ attempt.Binding, claimID, expectedClaimID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.status.ClaimID != expectedClaimID {
		return errors.New("slot is owned by another attempt execution")
	}
	s.status.ClaimID = claimID
	s.claims = append(s.claims, claimID)
	return nil
}

func (s *v2Slots) Bind(_ context.Context, _ attempt.Binding, claimID string, session attempt.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.status.ClaimID != claimID {
		return errors.New("slot is owned by another attempt execution")
	}
	if s.status.Phase == attempt.SlotPhaseConnected {
		if s.status.SessionID == session.ID {
			return nil
		}
		return errors.New("slot is already bound to another remote session")
	}
	if s.status.Phase != attempt.SlotPhaseOpening {
		return errors.New("slot phase transition is not allowed")
	}
	s.binds = append(s.binds, session)
	s.status.Phase = attempt.SlotPhaseConnected
	s.status.SessionID = session.ID
	return nil
}

func (s *v2Slots) MarkStarted(_ context.Context, _ attempt.Binding, claimID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startErr != nil {
		return s.startErr
	}
	if s.err != nil {
		return s.err
	}
	if s.status.ClaimID != claimID {
		return errors.New("slot is owned by another attempt execution")
	}
	switch s.status.Phase {
	case attempt.SlotPhaseConnected:
		s.status.Phase = attempt.SlotPhaseStarted
		s.status.Started = true
	case attempt.SlotPhaseStarted:
		return nil
	default:
		return errors.New("slot phase transition is not allowed")
	}
	s.starts++
	s.claims = append(s.claims, claimID)
	return nil
}

func (s *v2Slots) Release(_ context.Context, _ attempt.Binding, claimID string, confirmed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases = append(s.releases, confirmed)
	if s.err != nil {
		return s.err
	}
	if s.status.Exists && s.status.ClaimID != claimID {
		return errors.New("slot is owned by another attempt execution")
	}
	if confirmed {
		s.status = attempt.SlotStatus{Exists: false}
		return nil
	}
	s.status.State = attempt.SlotIsolated
	return nil
}

func (s *v2Slots) released() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.releases...)
}

func v2ReadySession(binding attempt.Binding) attempt.Session {
	return attempt.Session{
		ID:            "session-" + binding.EnvironmentID,
		EnvironmentID: binding.EnvironmentID,
		Generation:    binding.EnvironmentGen,
		Endpoint:      binding.Endpoint,
	}
}

func v2FixedBinding() attempt.Binding {
	return attempt.Binding{
		AttemptID: "task-v2-1", TaskID: "task-v2-1", SourceID: "src-a",
		WorkspaceID: "ws-1", AgentID: "agent-1", DaemonID: "fm-task-v2-1",
		JobRuntimeID: "runtime-job-1", EnvironmentID: "build-a",
		Endpoint: "build-a.example.test:7443", TLSRef: "ref/build-a",
		StateRoot: "/srv/rsbx/build-a", EnvironmentGen: "tree-1",
		SlotID: "task-v2-1",
		Image:  "ghcr.io/tsic404/rsbx", ImageDigest: "sha256:" + strings.Repeat("1", 64),
		Platform: "linux/amd64", ProxyURL: "http://foreman.foreman.svc.cluster.local:8080",
		CLIs: []attempt.CLI{
			{Kind: attempt.KindMultica, Version: "0.6.1", Platform: "linux/amd64",
				Digest:      "sha256:" + strings.Repeat("2", 64),
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/multica/0.6.1/linux/amd64"},
			{Kind: attempt.KindAgent, Backend: "codex", Version: "0.167.1", Platform: "linux/amd64",
				Digest:      "sha256:" + strings.Repeat("3", 64),
				DownloadURL: "http://foreman.foreman.svc.cluster.local:8080/cli/agent/0.167.1/linux/amd64?backend=codex"},
		},
	}
}

// v2Artifacts is the exact local set a cache hit answers: one file per CLI
// kind, matching the frozen version/platform/digest.
func v2Artifacts(binding attempt.Binding) map[string]attempt.Artifact {
	out := map[string]attempt.Artifact{}
	for _, cli := range binding.CLIs {
		out[cli.Kind] = attempt.Artifact{
			Kind: cli.Kind, Backend: cli.Backend, Version: cli.Version,
			Platform: cli.Platform, Digest: cli.Digest,
			Path: "/run/foreman/clis/" + cli.Kind,
		}
	}
	return out
}

// v2FixedSupervisor wires the Job-side supervisor to the scripted seams and
// the durable record at storePath.
func v2FixedSupervisor(t *testing.T, storePath string, cache *v2Cache, remote *v2Remote, registrar *v2Registrar, runner *v2Runner, slots *v2Slots) *attempt.Supervisor {
	t.Helper()
	store, err := attempt.NewFileStore(storePath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	return &attempt.Supervisor{
		Cache: cache, Remote: remote, Registrar: registrar,
		Runner: runner, Slots: slots, Store: store,
	}
}

func v2StorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "attempt-state.json")
}

type v2DownloadVerifier struct {
	claims auth.Claims
}

func (v v2DownloadVerifier) Verify(string, auth.RequestScope) (auth.Claims, error) {
	return v.claims, nil
}

type v2DownloadBindings struct {
	binding attempt.Binding
}

func (b v2DownloadBindings) FrozenAttempt(sourceID, workspaceID, taskID string) (attempt.Binding, bool) {
	if b.binding.TaskID != taskID || b.binding.SourceID != sourceID || b.binding.WorkspaceID != workspaceID {
		return attempt.Binding{}, false
	}
	return b.binding, true
}

type v2PublishedCLI struct {
	kind     release.Kind
	adapter  string
	version  string
	platform string
	digest   string
	body     []byte
}

type v2DownloadReleases struct {
	server   string
	releases []v2PublishedCLI
	reads    atomic.Int32
}

func (r *v2DownloadReleases) VerifiedRelease(_ context.Context, serverID string, kind release.Kind, adapter, version, platform string) (string, bool) {
	if serverID != r.server {
		return "", false
	}
	for _, published := range r.releases {
		if published.kind != kind {
			continue
		}
		if kind == release.KindAgent && !strings.EqualFold(published.adapter, adapter) {
			continue
		}
		if published.version != version || !strings.EqualFold(published.platform, platform) {
			continue
		}
		return published.digest, true
	}
	return "", false
}

func (r *v2DownloadReleases) VerifiedArtifactByDomain(_ context.Context, serverID, digest string) ([]byte, error) {
	r.reads.Add(1)
	if serverID != r.server {
		return nil, errors.New("unknown artifact")
	}
	for _, published := range r.releases {
		if published.digest == digest {
			return published.body, nil
		}
	}
	return nil, errors.New("unknown artifact")
}
