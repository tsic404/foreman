package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/registry"
)

// projectStub is the platform-side project seam: it answers both sources from
// fixed tables and counts the reverse lookups, so "the lookup did not happen"
// is an assertion and not an absence of evidence.
type projectStub struct {
	mu          sync.Mutex
	payloadName string
	byIssue     map[string]string
	err         error
	block       bool
	calls       []string
}

func (s *projectStub) FromPayload(json.RawMessage) string { return s.payloadName }

func (s *projectStub) ByIssue(ctx context.Context, issueID string) (string, error) {
	s.mu.Lock()
	s.calls = append(s.calls, issueID)
	block, err, name := s.block, s.err, s.byIssue[issueID]
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if err != nil {
		return "", err
	}
	return name, nil
}

func (s *projectStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// projectAwareBuilder is the fixture builder plus the one seam method that
// makes the claim path ask for a project identity at all.
type projectAwareBuilder struct{ inner *fakeBuilder }

func (b projectAwareBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	return b.inner.Build(e)
}

func (b projectAwareBuilder) ResolveName(e jobbuilder.TaskEntry) (string, error) {
	return b.inner.ResolveName(e)
}

func (projectAwareBuilder) RequiresProjectIdentity(jobbuilder.TaskEntry) bool { return true }

func (b projectAwareBuilder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	return b.inner.BuildCatalogProbe(requestID, jobRuntimeID, workspaceID, budget)
}

// projectFixture wires a scheduler whose builder declares a project-reading
// axis and whose project seam is the stub.
func projectFixture(t *testing.T, stub *projectStub, timeout time.Duration) (*fixture, *strings.Builder) {
	t.Helper()
	f := newFixture(t)
	logs := &strings.Builder{}
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Project = stub
	cfg.ProjectLookupTimeout = timeout
	sched, err := New(cfg, f.reg, f.jobs, projectAwareBuilder{inner: f.builder}, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithPendingReports(f.pending),
		WithRevoker(f.revoker),
		WithProbeRegistry(registry.NewProbeRegistry()),
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.sched = sched
	return f, logs
}

// projectClaim is a claim payload whose issue id is the reverse lookup's input.
func projectClaim(id, issueID string) json.RawMessage {
	return json.RawMessage(`{
		"id": "` + id + `", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "real-runtime", "issue_id": "` + issueID + `",
		"issue_identifier": "MULTI-1", "agent": {"name": "omp"}
	}`)
}

// TestProjectIdentityPayloadSource: a payload carrying the structured project
// key is the first source — the platform is never asked, so the reverse lookup
// cannot fail a claim that already has the answer.
func TestProjectIdentityPayloadSource(t *testing.T) {
	stub := &projectStub{payloadName: "foreman-payload", byIssue: map[string]string{"issue-1": "foreman-api"}}
	f, logs := projectFixture(t, stub, time.Second)

	if err := f.sched.OnClaim(context.Background(), projectClaim("task-1", "issue-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	entry, ok := f.reg.Get("task-1")
	if !ok {
		t.Fatal("entry missing after the claim")
	}
	if entry.ProjectName != "foreman-payload" || entry.ProjectSource != jobbuilder.ProjectSourcePayload {
		t.Errorf("project = %q/%q, want foreman-payload/payload", entry.ProjectName, entry.ProjectSource)
	}
	if stub.callCount() != 0 {
		t.Errorf("reverse lookups = %d, want none for a payload-sourced task", stub.callCount())
	}
	if strings.Contains(logs.String(), "project.lookup_failed") {
		t.Errorf("the payload branch must not log a lookup failure:\n%s", logs.String())
	}
}

// TestProjectIdentityAPISourceAndCache: without the structured key the claim
// falls back to the read-only lookup — once per issue id, cached afterwards.
func TestProjectIdentityAPISourceAndCache(t *testing.T) {
	stub := &projectStub{byIssue: map[string]string{"issue-1": "foreman-api"}}
	f, _ := projectFixture(t, stub, time.Second)

	for _, taskID := range []string{"task-1", "task-2"} {
		if err := f.sched.OnClaim(context.Background(), projectClaim(taskID, "issue-1")); err != nil {
			t.Fatalf("OnClaim(%s): %v", taskID, err)
		}
		entry, ok := f.reg.Get(taskID)
		if !ok {
			t.Fatalf("entry %s missing", taskID)
		}
		if entry.ProjectName != "foreman-api" || entry.ProjectSource != jobbuilder.ProjectSourceAPI {
			t.Errorf("entry %s project = %q/%q, want foreman-api/api", taskID, entry.ProjectName, entry.ProjectSource)
		}
	}
	if stub.callCount() != 1 {
		t.Errorf("reverse lookups = %d, want one (the second task is a cache hit)", stub.callCount())
	}
}

// TestProjectIdentityLookupFailureContinues: a failing or timing-out lookup is
// visible (warn) and degrades the project tiers — it never fails the task.
func TestProjectIdentityLookupFailureContinues(t *testing.T) {
	cases := map[string]*projectStub{
		"error":     {err: errors.New("platform down")},
		"no result": {byIssue: map[string]string{}},
	}
	for name, stub := range cases {
		t.Run(name, func(t *testing.T) {
			f, logs := projectFixture(t, stub, time.Second)
			if err := f.sched.OnClaim(context.Background(), projectClaim("task-1", "issue-1")); err != nil {
				t.Fatalf("OnClaim: %v", err)
			}
			entry, ok := f.reg.Get("task-1")
			if !ok {
				t.Fatal("entry missing: the claim must not fail on a lookup problem")
			}
			if entry.ProjectName != "" || entry.ProjectSource != jobbuilder.ProjectSourceNone {
				t.Errorf("project = %q/%q, want empty/none", entry.ProjectName, entry.ProjectSource)
			}
			if name == "no result" {
				// "No project" is a legal answer, not a failure event.
				if strings.Contains(logs.String(), "project.lookup_failed") {
					t.Errorf("an empty answer is not a lookup failure:\n%s", logs.String())
				}
				return
			}
			if !strings.Contains(logs.String(), "project.lookup_failed") {
				t.Errorf("a failing lookup must warn:\n%s", logs.String())
			}
			if !strings.Contains(logs.String(), `"issue_id":"issue-1"`) {
				t.Errorf("the warn must name the issue:\n%s", logs.String())
			}
		})
	}
}

// TestProjectIdentityLookupTimeoutIsBounded: a lookup that ignores the clock is
// cut off by the configured bound, so the claim → Job budget holds.
func TestProjectIdentityLookupTimeoutIsBounded(t *testing.T) {
	stub := &projectStub{block: true}
	f, logs := projectFixture(t, stub, 50*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- f.sched.OnClaim(context.Background(), projectClaim("task-1", "issue-1")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("OnClaim: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnClaim did not return: the lookup is not bounded")
	}
	if !strings.Contains(logs.String(), "project.lookup_failed") {
		t.Errorf("an expired lookup must warn:\n%s", logs.String())
	}
}

// TestProjectLookupSkippedWhenNoAxisNeedsIt: with a builder that declares no
// project-reading tier, the claim never calls the platform.
func TestProjectLookupSkippedWhenNoAxisNeedsIt(t *testing.T) {
	stub := &projectStub{byIssue: map[string]string{"issue-1": "foreman-api"}}
	f := newFixture(t)
	cfg, err := LoadConfig(testEnv(nil))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Project = stub
	sched, err := New(cfg, f.reg, f.jobs, f.builder, f.server, f.metrics,
		WithClock(func() time.Time { return f.now }),
		WithPendingReports(f.pending),
		WithRevoker(f.revoker),
		WithProbeRegistry(registry.NewProbeRegistry()),
		WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.sched = sched

	if err := f.sched.OnClaim(context.Background(), projectClaim("task-1", "issue-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	if stub.callCount() != 0 {
		t.Errorf("reverse lookups = %d, want none when no tier reads the project side", stub.callCount())
	}
}
