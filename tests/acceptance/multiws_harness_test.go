// Multi-workspace acceptance harness (ADR-047 / ADR-049): two or more
// workspace identities, one fake client per identity, one shared scheduler and
// one Job-facing fake server. It is the fixture the TC-tech-multiws-* scenarios
// drive; it deliberately mirrors cmd/foreman's wiring (one client per
// (credential, daemon identity) pair, one claim budget per workspace, a
// workspace-routed server seam). newUserScopedWS builds the discovery form on
// top of it (C-17 → manifest → derived identities).
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/workspace"
)

// multiWS is the multi-workspace fixture.
type multiWS struct {
	t        *testing.T
	reg      *registry.Registry
	probes   *registry.ProbeRegistry
	issuer   *auth.Issuer
	jobs     *jobClient
	builder  *multiBuilder
	metrics  *recordingMetrics
	logs     *syncBuffer
	pending  *scheduler.PendingReports
	sched    *scheduler.Scheduler
	schedCfg scheduler.Config
	clients  map[string]*proxy.Client
	set      *proxy.ClientSet
	upstream map[string]*upstreamStub
	handler  http.Handler
	jobsHTTP *httptest.Server
	server   *multiServer
}

// multiBuilder is the multi-workspace scheduler's Job seam: the identity and
// workspace faces are real (labels + Job Token workspace), the shape is the
// canned one.
type multiBuilder struct {
	issuer *auth.Issuer
	mu     sync.Mutex
	tokens map[string]string // Job name -> Job Token
}

func (b *multiBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	name := "fm-" + e.TaskID
	token, err := b.issuer.Issue(name, e.TaskID, e.WorkspaceID, auth.DefaultJobTokenTTL)
	if err != nil {
		return nil, nil, "", err
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			registry.LabelWorkspaceID: e.WorkspaceID,
			registry.LabelTaskID:      e.TaskID,
			registry.LabelManagedBy:   registry.ManagedByForeman,
		},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:   name + "-cred",
		Labels: map[string]string{registry.LabelWorkspaceID: e.WorkspaceID},
	}}
	b.mu.Lock()
	if b.tokens == nil {
		b.tokens = map[string]string{}
	}
	b.tokens[name] = token
	b.mu.Unlock()
	return job, secret, b.issuer.Hash(token), nil
}

func (b *multiBuilder) ResolveName(e jobbuilder.TaskEntry) (string, error) {
	return "fm-" + e.TaskID, nil
}

func (b *multiBuilder) RequiresProjectIdentity(jobbuilder.TaskEntry) bool { return false }

func (b *multiBuilder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	name := "fm-catalog-" + requestID
	token, err := b.issuer.IssueProbe(name, requestID, workspaceID, budget+2*time.Minute)
	if err != nil {
		return nil, nil, "", err
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			registry.LabelRole:        registry.RoleCatalogProbe,
			registry.LabelWorkspaceID: workspaceID,
		},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:   name + "-cred",
		Labels: map[string]string{registry.LabelWorkspaceID: workspaceID},
	}}
	b.mu.Lock()
	if b.tokens == nil {
		b.tokens = map[string]string{}
	}
	b.tokens[name] = token
	b.mu.Unlock()
	return job, secret, b.issuer.Hash(token), nil
}

func (b *multiBuilder) tokenFor(jobName string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens[jobName]
}

// multiServer routes the scheduler's server calls by entry workspace: each
// call uses that workspace's credential (§1.4 失败粒度含 workspace).
type multiServer struct {
	reg     *registry.Registry
	clients map[string]*proxy.Client
}

func (m *multiServer) clientFor(taskID string) (*proxy.Client, bool) {
	e, ok := m.reg.Get(taskID)
	if !ok {
		return nil, false
	}
	c, ok := m.clients[e.WorkspaceID]
	return c, ok
}

func (m *multiServer) TaskStatus(ctx context.Context, taskID string) (string, error) {
	c, ok := m.clientFor(taskID)
	if !ok {
		return "", scheduler.ErrTaskNotFound
	}
	return c.TaskStatus(ctx, taskID)
}

func (m *multiServer) Forward(ctx context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error) {
	c, ok := m.clientFor(taskID)
	if !ok {
		return 0, nil, scheduler.ErrTaskNotFound
	}
	return c.Forward(ctx, ep, taskID, body)
}

// ForwardInWorkspace is the entry-gone replay path: the queue entry's
// workspace selects the credential (§10).
func (m *multiServer) ForwardInWorkspace(ctx context.Context, ep scheduler.Endpoint, taskID, workspaceID string, body []byte) (int, []byte, error) {
	c, ok := m.clients[workspaceID]
	if !ok {
		return 0, nil, scheduler.ErrTaskNotFound
	}
	return c.Forward(ctx, ep, taskID, body)
}

// newMultiWS builds the fixture for ids. Every workspace gets its own upstream
// stub and its own proxy client; the scheduler, registry, probe table and Job
// client are shared, exactly as one deployment shares them.
func newMultiWS(t *testing.T, ids ...string) *multiWS {
	t.Helper()
	return newMultiWSWithURLs(t, ids, nil)
}

// wsIdentity is one deployment identity the fixture wires: the workspace, its
// daemon identity, and the credential its requests carry.
type wsIdentity struct {
	workspaceID string
	daemonID    string
	token       string
	name        string
}

// newMultiWSWithURLs builds the fixture while letting a scenario replace one
// workspace's upstream address (credential-failure scenarios). A workspace
// without an override gets a fresh stub. This is the workspace-scoped
// credential form: one credential per workspace.
func newMultiWSWithURLs(t *testing.T, ids []string, urls map[string]string) *multiWS {
	t.Helper()
	identities := make([]wsIdentity, 0, len(ids))
	for _, id := range ids {
		identities = append(identities, wsIdentity{
			workspaceID: id,
			daemonID:    "foreman-" + id,
			token:       "mdt_" + id,
			name:        id,
		})
	}
	return newMultiWSFromIdentities(t, identities, urls)
}

// newMultiWSFromIdentities builds the fixture around a given identity set.
// Every workspace gets its own upstream stub and its own proxy client; the
// scheduler, registry, probe table and Job client are shared, exactly as one
// deployment shares them.
func newMultiWSFromIdentities(t *testing.T, identities []wsIdentity, urls map[string]string) *multiWS {
	t.Helper()
	if len(identities) < 1 {
		t.Fatalf("newMultiWS needs at least one workspace")
	}
	upstream := make(map[string]*upstreamStub, len(identities))
	clients := make(map[string]*proxy.Client, len(identities))
	clientList := make([]*proxy.Client, 0, len(identities))
	logs := &syncBuffer{}
	metrics := &recordingMetrics{}
	reg := registry.New(time.Now, registry.WithDeliveryConfirmTimeout(300*time.Millisecond))
	probes := registry.NewProbeRegistry()

	ids := make([]string, 0, len(identities))
	for _, identity := range identities {
		id := identity.workspaceID
		ids = append(ids, id)
		serverURL := urls[id]
		if serverURL == "" {
			stub := newUpstreamStub(t)
			stub.setWorkspaceID(id)
			upstream[id] = stub
			serverURL = stub.server.URL
		}
		cfg := proxy.Config{
			ServerURL:             serverURL,
			WorkspaceID:           id,
			DaemonID:              identity.daemonID,
			Version:               "0.1.0",
			ClaimInterval:         time.Second,
			ClaimIntervalIdle:     time.Second,
			LeaseRefreshInterval:  time.Second,
			ModelCatalogEnabled:   false,
			ModelCatalogTimeout:   45 * time.Second,
			ModelCatalogCacheTTL:  6 * time.Hour,
			WorkspaceStateTimeout: 2 * time.Second,
			SkillCacheFreshTTL:    10 * time.Minute,
			SkillCacheRetention:   time.Hour,
			SkillCacheMaxEntries:  16,
			SkillCacheMaxBytes:    1 << 20,
		}
		client := proxy.NewClient(cfg, identity.token,
			proxy.WithMetrics(metrics),
			proxy.WithDiscoveryProbe(proxy.DiscoveryProbeFunc(func(context.Context, string) (registry.Discovery, error) {
				return registry.Discovery{}, nil
			})),
			proxy.WithTaskRuntime(func(taskID string) string {
				e, ok := reg.Get(taskID)
				if !ok {
					return ""
				}
				return e.RealRuntimeID
			}),
		)
		client.SetWorkspaceName(identity.name)
		clients[id] = client
		clientList = append(clientList, client)
	}
	clientSet, err := proxy.NewClientSet(clientList...)
	if err != nil {
		t.Fatalf("NewClientSet: %v", err)
	}
	issuer, err := proxy.JobTokenIssuer(testKey, reg, probes, auth.DefaultJobTokenTTL, nil)
	if err != nil {
		t.Fatalf("JobTokenIssuer: %v", err)
	}
	jobs := newJobClient()
	bld := &multiBuilder{issuer: issuer}
	pending, err := scheduler.NewPendingReports(t.TempDir(),
		scheduler.WithPendingReportsGauge(metrics.PendingReports),
		scheduler.WithPendingReportsCarrier(scheduler.NewSecretCarrierStore(jobs)))
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	if err := pending.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	schedCfg := scheduler.Config{
		JobNamespace:                "multica-agents",
		Workspaces:                  append([]string(nil), ids...),
		WorkspaceID:                 ids[0],
		MaxInflightJobs:             10,
		MaxInflightJobsPerWorkspace: 0,
		ClaimBatchMax:               32,
		JobBootTimeout:              300 * time.Second,
		JobDrainTimeout:             120 * time.Second,
		DeliveryConfirmTimeout:      300 * time.Millisecond,
		StartupProbeTimeout:         3 * time.Second,
		ModelCatalogEnabled:         false,
		ModelCatalogTimeout:         45 * time.Second,
		ReconcileInterval:           30 * time.Second,
	}
	server := &multiServer{reg: reg, clients: clients}
	sched, err := scheduler.New(schedCfg, reg, jobs, bld, server, metrics,
		scheduler.WithPendingReports(pending),
		scheduler.WithRevoker(noRevoker{}),
		scheduler.WithProbeRegistry(probes),
		scheduler.WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
		scheduler.WithRuntimeIdentity(clientSet.RuntimeIdentity),
	)
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	fakeServer := proxy.NewFakeServer(issuer, reg, sched, clientList[0], nil, metrics,
		proxy.WithProbeRegistry(probes),
		proxy.WithClientSet(clientSet),
		proxy.WithServerLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	httpServer := httptest.NewServer(fakeServer)
	t.Cleanup(httpServer.Close)

	return &multiWS{
		t: t, reg: reg, probes: probes, issuer: issuer, jobs: jobs, builder: bld,
		metrics: metrics, logs: logs, pending: pending, sched: sched, schedCfg: schedCfg,
		clients: clients, set: clientSet, upstream: upstream,
		handler: fakeServer, jobsHTTP: httpServer, server: server,
	}
}

// rebuildScheduler replaces the scheduler with one built from a mutated
// configuration; every other component stays shared, as in a deployment.
func (m *multiWS) rebuildScheduler(mutate func(*scheduler.Config)) {
	m.t.Helper()
	cfg := m.schedCfg
	mutate(&cfg)
	sched, err := scheduler.New(cfg, m.reg, m.jobs, m.builder, m.server, m.metrics,
		scheduler.WithPendingReports(m.pending),
		scheduler.WithRevoker(noRevoker{}),
		scheduler.WithProbeRegistry(m.probes),
		scheduler.WithLogger(slog.New(slog.NewJSONHandler(m.logs, nil))),
		scheduler.WithRuntimeIdentity(m.set.RuntimeIdentity),
	)
	if err != nil {
		m.t.Fatalf("scheduler.New: %v", err)
	}
	m.sched = sched
	m.schedCfg = cfg
}

// claim drives one claim under workspaceID. runtimeID is the dispatch record
// the server would have attached.
func (m *multiWS) claim(workspaceID, taskID, runtimeID string) registry.TaskEntry {
	m.t.Helper()
	payload := map[string]any{
		"id":           taskID,
		"agent_id":     "agent-" + workspaceID,
		"workspace_id": workspaceID,
		"runtime_id":   runtimeID,
		"issue_id":     "issue-" + taskID,
	}
	body, _ := json.Marshal(payload)
	if err := m.sched.OnClaimFor(m.t.Context(), workspaceID, body); err != nil {
		m.t.Fatalf("OnClaimFor(%s, %s): %v", workspaceID, taskID, err)
	}
	e, ok := m.reg.Get(taskID)
	if !ok {
		m.t.Fatalf("entry %s missing after claim", taskID)
	}
	return e
}

// call performs one Job-facing request.
func (m *multiWS) call(method, path, token string, body []byte) (int, []byte) {
	m.t.Helper()
	req, err := http.NewRequest(method, m.jobsHTTP.URL+path, bytes.NewReader(body))
	if err != nil {
		m.t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		m.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		m.t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}

// tasksByWorkspace drives the ops projection for one workspace.
func (m *multiWS) tasksByWorkspace(workspaceID string) []registry.TaskEntry {
	m.t.Helper()
	entries := m.reg.List()
	out := entries[:0]
	for _, entry := range entries {
		if entry.WorkspaceID == workspaceID {
			out = append(out, entry)
		}
	}
	return out
}

// multiWSTemplate is the fixture template for TC-02/TC-08: workspace
// placeholders appear in labels, env values (one string repeating the
// placeholder), a list element, and a volume path, beside a fixed literal that
// must survive verbatim.
const multiWSTemplate = `apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  labels:
    foreman.tsic.top/workspace-id: "<workspace_id>"
spec:
  template:
    metadata:
      labels:
        foreman.tsic.top/workspace-id: "<workspace_id>"
        pair: "<workspace_id>|<workspace_id>"
        literal: "/fixed/state"
    spec:
      containers:
        - name: agent
          image: <job-image>
          args: ["--ws=<workspace_id>"]
          env:
            - name: MULTICA_WORKSPACES_ROOT
              value: "/state/workspaces"
            - name: WS_TWICE
              value: "<workspace_id>/<workspace_id>"
        - name: other
          image: plain
      volumes:
        - name: home
          hostPath: { path: /state/<workspace_id>, type: DirectoryOrCreate }
        - name: fixed
          hostPath: { path: /fixed/state, type: DirectoryOrCreate }
`

// newRealMultiBuilder builds the production jobbuilder over a fixture
// template and an optional partitioned policy, with the real issuer.
func newRealMultiBuilder(t *testing.T, m *multiWS, workspaceIDs []string, policyYAML string) *jobbuilder.Builder {
	t.Helper()
	dir := t.TempDir()
	templatePath := filepath.Join(dir, "job-overlay.yaml")
	if err := os.WriteFile(templatePath, []byte(multiWSTemplate), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	policyPath := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(policyPath, []byte(policyYAML), 0o644); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	cfg := jobbuilder.Config{
		JobNamespace:                "multica-agents",
		JobImage:                    harnessJobImage,
		JobTokenTTL:                 auth.DefaultJobTokenTTL,
		ServerURL:                   "http://foreman.foreman.svc.cluster.local:8080",
		JobDrainTimeout:             120 * time.Second,
		ProbeHeartbeatInterval:      15 * time.Second,
		TemplatePath:                templatePath,
		PolicyPath:                  policyPath,
		PolicyNameResolutionTimeout: 5 * time.Second,
		WorkspaceIDs:                workspaceIDs,
		Issuer:                      m.issuer,
		Nodes:                       m.reg,
	}
	builder, err := jobbuilder.NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	return builder
}

// taskEntryFor is one task entry for a real-builder render.
func taskEntryFor(taskID, workspaceID, agentName string) jobbuilder.TaskEntry {
	return jobbuilder.TaskEntry{
		TaskID:       taskID,
		AgentID:      "agent-" + taskID,
		WorkspaceID:  workspaceID,
		JobRuntimeID: "runtime-" + taskID,
		ClaimedAt:    time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC),
		Attempt:      1,
		AgentName:    agentName,
	}
}

// discoveryStub answers C-17 for the user-scoped credential form and records
// every call. The answer and its status are programmable, so the discovery
// scenarios can drive the visible set, an auth rejection and an unreachable
// upstream.
type discoveryStub struct {
	mu      sync.Mutex
	visible []workspace.Ref
	status  int
	calls   int
	server  *httptest.Server
}

func newDiscoveryStub(t *testing.T, visible []workspace.Ref) *discoveryStub {
	t.Helper()
	stub := &discoveryStub{visible: append([]workspace.Ref(nil), visible...), status: http.StatusOK}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/daemon/workspaces", func(w http.ResponseWriter, _ *http.Request) {
		stub.mu.Lock()
		visible, status := append([]workspace.Ref(nil), stub.visible...), stub.status
		stub.calls++
		stub.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"rejected"}`))
			return
		}
		answer := make([]map[string]string, 0, len(visible))
		for _, ref := range visible {
			answer = append(answer, map[string]string{"id": ref.ID, "name": ref.Name})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	})
	stub.server = httptest.NewServer(mux)
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *discoveryStub) setStatus(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

func (s *discoveryStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// newUserScopedWS builds the user-scoped credential form fixture: one C-17
// discovery against stub, the manifest applied, then one derived identity per
// discovered workspace sharing the single credential — the same wiring
// cmd/foreman performs at startup (ADR-049).
func newUserScopedWS(t *testing.T, stub *discoveryStub, manifest []string) (*multiWS, proxy.Discovery) {
	t.Helper()
	deployment := workspace.Deployment{
		Scope:        workspace.ScopeUser,
		UserToken:    "mul_user",
		BaseDaemonID: "foreman",
		Manifest:     manifest,
	}
	discoveryClient := proxy.NewClient(proxy.Config{ServerURL: stub.server.URL, Version: "0.1.0"}, deployment.UserToken)
	discovered, err := discoveryClient.DiscoverWorkspaces(t.Context(), manifest)
	if err != nil {
		t.Fatalf("DiscoverWorkspaces: %v", err)
	}
	identities := deployment.Identities(discovered.Set)
	names := make(map[string]string, len(discovered.Set))
	for _, ref := range discovered.Set {
		names[ref.ID] = ref.Name
	}
	wsIdentities := make([]wsIdentity, 0, len(identities))
	for _, identity := range identities {
		wsIdentities = append(wsIdentities, wsIdentity{
			workspaceID: identity.WorkspaceID,
			daemonID:    identity.DaemonID,
			token:       identity.Token,
			name:        names[identity.WorkspaceID],
		})
	}
	return newMultiWSFromIdentities(t, wsIdentities, nil), discovered
}
