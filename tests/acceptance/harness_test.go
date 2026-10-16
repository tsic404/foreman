// Package acceptance holds the executable form of the design repo's
// technical test cases: each file names the TC it implements and drives the
// real Foreman components (proxy, scheduler, auth, registry) against a stub
// upstream and in-memory Kubernetes fakes. The design repo states the
// scenario semantics; the steps, fixtures and assertions live here.
package acceptance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

var testKey = bytes.Repeat([]byte{0x24}, 32)

// harnessJobImage is the image reference the real builder renders into a Job:
// the <job-image> placeholder's value in this suite.
const harnessJobImage = "ghcr.io/tsic404/foreman-job:test"

// Named aliases keep the scenario files free of repeated import qualifiers.
type schedulerConfig = scheduler.Config
type batchv1Job = batchv1.Job

// yamlDocuments reads one repo file and returns its YAML documents, so a test
// can assert against a manifest the deployment actually applies.
func yamlDocuments(t *testing.T, path string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	var docs [][]byte
	for {
		doc, err := reader.Read()
		if err != nil {
			break
		}
		if len(bytes.TrimSpace(doc)) > 0 {
			docs = append(docs, doc)
		}
	}
	return docs
}

// createdJob returns the Job the harness recorded under name.
func (h *harness) createdJob(t *testing.T, name string) *batchv1Job {
	t.Helper()
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	for _, job := range h.jobs.createdJobs {
		if job.Name == name {
			return job
		}
	}
	t.Fatalf("Job %s not recorded", name)
	return nil
}

// createdSecret returns the Secret the harness recorded under name.
func (h *harness) createdSecret(t *testing.T, name string) *corev1.Secret {
	t.Helper()
	h.jobs.mu.Lock()
	defer h.jobs.mu.Unlock()
	for _, secret := range h.jobs.createdSecrets {
		if secret.Name == name {
			return secret
		}
	}
	t.Fatalf("Secret %s not recorded", name)
	return nil
}

// declaredRuntimes is the startup discovery the acceptance scenarios declare:
// two providers, exactly what a template with two CLI providers reports.
func declaredRuntimes() registry.Discovery {
	return registry.Discovery{
		CLIVersion: "0.6.1",
		Runtimes: []registry.DiscoveredRuntime{
			{Name: "foreman-omp", Type: "omp", Version: "0.6.1"},
			{Name: "foreman-zz", Type: "zz", Version: "3.1.4"},
		},
		FailedProfiles: []json.RawMessage{json.RawMessage(`{"profile_id":"pf","command_name":"zz","reason":"x"}`)},
	}
}

// upstreamRequest is one call the stub upstream observed.
type upstreamRequest struct {
	Method string
	Path   string
	Body   []byte
	Header http.Header
}

// ackRuntimeIDShape is how the stub shapes the heartbeat ack's runtime_id: the
// real server's ack does not reliably echo it, and the scenarios must hold
// whatever it says.
type ackRuntimeIDShape int

const (
	// ackEchoesRuntimeID echoes the heartbeat's runtime id (the healthy shape).
	ackEchoesRuntimeID ackRuntimeIDShape = iota
	// ackOmitsRuntimeID leaves the field out.
	ackOmitsRuntimeID
	// ackForeignRuntimeID names another runtime.
	ackForeignRuntimeID
)

// modelCommand is one caller-facing model-list request
// (POST /api/runtimes/{rid}/models): the stub plays the server's own request
// record — pending until the C21 result lands, delivered at most once through a
// heartbeat ack (the server delivers a request exactly once).
type modelCommand struct {
	id        string
	runtimeID string
	delivered bool
	completed bool
}

// upstreamStub is the server side of C1–C21 as the scenarios need it.
type upstreamStub struct {
	mu sync.Mutex
	// registerIDs maps a declared provider type to the runtime id the server
	// answers with; a missing entry means the provider is not registered.
	registerIDs map[string]string
	// heartbeats maps a real runtime id to the status the heartbeat answers
	// with (default 200).
	heartbeats map[string]int
	// modelsStatus is the C21 answer (default 200).
	modelsStatus int
	// models tracks the caller-facing model-list commands by request id.
	models map[string]*modelCommand
	// modelSeq numbers the minted request ids (the real ones are 32 hex digits).
	modelSeq int
	// ackShape shapes the heartbeat ack's runtime_id field.
	ackShape ackRuntimeIDShape
	requests []upstreamRequest
	server   *httptest.Server
}

func newUpstreamStub(t *testing.T) *upstreamStub {
	t.Helper()
	stub := &upstreamStub{
		registerIDs:  map[string]string{},
		heartbeats:   map[string]int{},
		modelsStatus: 200,
		models:       map[string]*modelCommand{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, "/api/daemon/register", body)
		var req struct {
			Runtimes []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"runtimes"`
		}
		_ = json.Unmarshal(body, &req)
		answers := make([]map[string]string, 0, len(req.Runtimes))
		for _, rt := range req.Runtimes {
			id, ok := stub.idFor(rt.Type)
			if !ok {
				continue
			}
			answers = append(answers, map[string]string{
				"id": id, "name": rt.Name, "provider": rt.Type, "status": "online",
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"runtimes": answers, "repos": []any{}, "repos_version": "", "settings": nil,
		})
	})
	mux.HandleFunc("POST /api/daemon/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, "/api/daemon/heartbeat", body)
		var req struct {
			RuntimeID string `json:"runtime_id"`
		}
		_ = json.Unmarshal(body, &req)
		status := stub.statusFor(req.RuntimeID)
		if status == http.StatusNotFound {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"runtime not found"}`))
			return
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
			return
		}
		ack := map[string]any{"status": "ok"}
		switch stub.shapeOfAck() {
		case ackEchoesRuntimeID:
			ack["runtime_id"] = req.RuntimeID
		case ackForeignRuntimeID:
			ack["runtime_id"] = "rid-foreign"
		}
		if cmd := stub.deliverModelCommand(req.RuntimeID); cmd != nil {
			ack["pending_model_list"] = map[string]string{"id": cmd.id}
		}
		writeJSON(w, status, ack)
	})
	mux.HandleFunc("POST /api/runtimes/{rid}/models", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		cmd := stub.newModelCommand(r.PathValue("rid"))
		writeJSON(w, http.StatusOK, map[string]string{"id": cmd.id, "status": "pending"})
	})
	mux.HandleFunc("POST /api/daemon/tasks/claim", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, "/api/daemon/tasks/claim", body)
		writeJSON(w, http.StatusOK, map[string]any{"tasks": []any{}})
	})
	// C4: the lease keeper's renewal. Without this route the stub's 404 would
	// read as "the server deleted the task" and settle every pre-start entry
	// as vanished.
	mux.HandleFunc("POST /api/daemon/runtimes/{rid}/tasks/{tid}/prepare-lease", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, r.URL.Path, body)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /api/daemon/deregister", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, "/api/daemon/deregister", body)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /api/daemon/runtimes/{rid}/models/{req}/result", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, r.URL.Path, body)
		stub.completeModelCommand(r.PathValue("req"))
		writeJSON(w, stub.modelsStatus, map[string]string{"status": "ok"})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	stub.server = server
	return stub
}

func (s *upstreamStub) record(r *http.Request, path string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, upstreamRequest{
		Method: r.Method, Path: path, Body: body, Header: r.Header.Clone(),
	})
}

func (s *upstreamStub) idFor(provider string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.registerIDs[provider]
	return id, ok
}

func (s *upstreamStub) statusFor(runtimeID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.heartbeats[runtimeID]
	if !ok {
		return http.StatusOK
	}
	return status
}

func (s *upstreamStub) calls(method, path string) []upstreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found []upstreamRequest
	for _, req := range s.requests {
		if req.Method == method && req.Path == path {
			found = append(found, req)
		}
	}
	return found
}

func (s *upstreamStub) pathCalls(prefix string) []upstreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found []upstreamRequest
	for _, req := range s.requests {
		if len(req.Path) >= len(prefix) && req.Path[:len(prefix)] == prefix {
			found = append(found, req)
		}
	}
	return found
}

// setAckShape shapes the heartbeat ack's runtime_id field.
func (s *upstreamStub) setAckShape(shape ackRuntimeIDShape) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackShape = shape
}

func (s *upstreamStub) shapeOfAck() ackRuntimeIDShape {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackShape
}

// newModelCommand records one caller-facing command and mints its request id.
func (s *upstreamStub) newModelCommand(runtimeID string) *modelCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelSeq++
	cmd := &modelCommand{id: fmt.Sprintf("%032x", s.modelSeq), runtimeID: runtimeID}
	s.models[cmd.id] = cmd
	return cmd
}

// deliverModelCommand hands the runtime's pending command to one ack — the
// server delivers a request exactly once.
func (s *upstreamStub) deliverModelCommand(runtimeID string) *modelCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cmd := range s.models {
		if cmd.runtimeID == runtimeID && !cmd.delivered {
			cmd.delivered = true
			return cmd
		}
	}
	return nil
}

// completeModelCommand records the C21 result the server accepted.
func (s *upstreamStub) completeModelCommand(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cmd, ok := s.models[requestID]; ok {
		cmd.completed = true
	}
}

func (s *upstreamStub) modelCommandState(requestID string) (delivered, completed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.models[requestID]
	if !ok {
		return false, false
	}
	return cmd.delivered, cmd.completed
}

// requestModelList is the caller-facing entry point: one model-list command for
// the runtime, answered with the request id the ack will carry.
func (h *harness) requestModelList(t *testing.T, runtimeID string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		h.upstream.server.URL+"/api/runtimes/"+runtimeID+"/models", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /api/runtimes/%s/models: %v", runtimeID, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/runtimes/%s/models = %d: %s", runtimeID, resp.StatusCode, raw)
	}
	var answer struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil || answer.ID == "" {
		t.Fatalf("model-list answer %s: %v", raw, err)
	}
	return answer.ID
}

// waitModelCompleted waits for the server's request record to turn completed —
// the observable end of the caller's request (the alternative is the 60s window
// timeout).
func (h *harness) waitModelCompleted(t *testing.T, requestID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, completed := h.upstream.modelCommandState(requestID); completed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("model-list request %s never completed (no C21 result reached the upstream)", requestID)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

// jobClient is the Kubernetes surface the scenarios need: it records objects
// and can answer List/Get from programmed state.
type jobClient struct {
	mu sync.Mutex

	createdJobs    []*batchv1.Job
	createdSecrets []*corev1.Secret
	deletedJobs    []string
	deletedSecrets []string
	createJobErr   error
	// onCreateJob runs before a create is answered (scenario setup hooks).
	onCreateJob func(job *batchv1.Job)

	existingJobs map[string]*batchv1.Job
	jobs         []batchv1.Job
	secrets      []corev1.Secret
	pods         []corev1.Pod
}

func newJobClient() *jobClient {
	return &jobClient{existingJobs: map[string]*batchv1.Job{}}
}

func (c *jobClient) CreateJob(_ context.Context, job *batchv1.Job) (*batchv1.Job, error) {
	if c.onCreateJob != nil {
		c.onCreateJob(job)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.createJobErr != nil {
		return nil, c.createJobErr
	}
	c.createdJobs = append(c.createdJobs, job)
	created := job.DeepCopy()
	created.CreationTimestamp = metav1.NewTime(time.Now())
	// The list face the startup rebuild reads: a created Job is visible to
	// ListJobs, exactly as the apiserver's object is.
	c.jobs = append(c.jobs, *created)
	return created, nil
}

func (c *jobClient) DeleteJob(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletedJobs = append(c.deletedJobs, name)
	// A deleted object leaves the list face too, or a "delete then restart"
	// scenario would rebuild a ghost entry from it.
	kept := c.jobs[:0]
	for _, job := range c.jobs {
		if job.Name != name {
			kept = append(kept, job)
		}
	}
	c.jobs = kept
	return nil
}

func (c *jobClient) GetJob(_ context.Context, name string) (*batchv1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	job, ok := c.existingJobs[name]
	if !ok {
		return nil, nil
	}
	return job.DeepCopy(), nil
}

func (c *jobClient) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createdSecrets = append(c.createdSecrets, secret)
	return nil
}

func (c *jobClient) DeleteSecret(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletedSecrets = append(c.deletedSecrets, name)
	return nil
}

func (c *jobClient) ListJobs(context.Context) ([]batchv1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]batchv1.Job(nil), c.jobs...), nil
}

func (c *jobClient) ListSecrets(context.Context) ([]corev1.Secret, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]corev1.Secret(nil), c.secrets...), nil
}

func (c *jobClient) ListPods(context.Context, string) ([]corev1.Pod, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]corev1.Pod(nil), c.pods...), nil
}

func (c *jobClient) WatchJobs(context.Context, scheduler.JobEventHandler) error { return nil }

func (c *jobClient) jobCreated(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, job := range c.createdJobs {
		if job.Name == name {
			return true
		}
	}
	return false
}

// jobDeleted reports whether the Job object was deleted.
func (c *jobClient) jobDeleted(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, deleted := range c.deletedJobs {
		if deleted == name {
			return true
		}
	}
	return false
}

func (c *jobClient) probeJobsCreated() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var names []string
	for _, job := range c.createdJobs {
		if job.Labels[registry.LabelRole] == registry.RoleCatalogProbe {
			names = append(names, job.Name)
		}
	}
	return names
}

// builder is the jobbuilder seam: it renders canned objects and issues the
// probe Job Token with the real issuer, so probe requests authenticate against
// the real auth module.
type builder struct {
	issuer   *auth.Issuer
	probes   *registry.ProbeRegistry
	probeErr error

	mu     sync.Mutex
	tokens map[string]string // probe Job name -> issued Job Token
}

func (b *builder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	name := "fm-" + e.TaskID
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-cred"}}
	return job, secret, "hash-" + name, nil
}

func (b *builder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	if b.probeErr != nil {
		return nil, nil, "", b.probeErr
	}
	name := "fm-catalog-" + requestID
	token, err := b.issuer.IssueProbe(name, requestID, workspaceID, budget+2*time.Minute)
	if err != nil {
		return nil, nil, "", err
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{registry.LabelRole: registry.RoleCatalogProbe},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-cred"}}
	b.mu.Lock()
	if b.tokens == nil {
		b.tokens = map[string]string{}
	}
	b.tokens[name] = token
	b.mu.Unlock()
	return job, secret, b.issuer.Hash(token), nil
}

// probeToken returns the Job Token issued for a probe Job.
func (b *builder) probeToken(jobName string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens[jobName]
}

// realBuilder drives the production jobbuilder over the deployment's template,
// so a scenario exercises the real probe render path and its credential.
type realBuilder struct {
	inner *jobbuilder.Builder
	jobs  *jobClient
}

func (b realBuilder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	return b.inner.Build(e)
}

func (b realBuilder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	return b.inner.BuildCatalogProbe(requestID, jobRuntimeID, workspaceID, budget)
}

// probeToken reads the Job Token the real builder wrote into the probe's
// credential Secret.
func (b realBuilder) probeToken(jobName string) string {
	b.jobs.mu.Lock()
	defer b.jobs.mu.Unlock()
	for _, secret := range b.jobs.createdSecrets {
		if secret.Name != jobName+"-cred" {
			continue
		}
		raw := secret.Data["config.json"]
		if len(raw) == 0 {
			raw = []byte(secret.StringData["config.json"])
		}
		var payload struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		return payload.Token
	}
	return ""
}

// deployedTemplatePath writes the ConfigMap's job-overlay.yaml content — the
// reference template the Deployment mounts — to a file and returns its path,
// so the real builder runs through the same required-template gate a cluster
// deployment does (ADR-030).
func deployedTemplatePath(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "25-job-template.yaml"))
	if err != nil {
		t.Fatalf("read deploy/25-job-template.yaml: %v", err)
	}
	jsonBytes, err := utilyaml.ToJSON(raw)
	if err != nil {
		t.Fatalf("decode deploy/25-job-template.yaml: %v", err)
	}
	var configMap corev1.ConfigMap
	if err := json.Unmarshal(jsonBytes, &configMap); err != nil {
		t.Fatalf("decode the template ConfigMap: %v", err)
	}
	content := configMap.Data["job-overlay.yaml"]
	if content == "" {
		t.Fatal("the template ConfigMap carries no job-overlay.yaml key")
	}
	path := filepath.Join(t.TempDir(), "job-overlay.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write template: %v", err)
	}
	return path
}

// useRealBuilder swaps the canned builder for the production one, driven by the
// template the Deployment publishes.
func (h *harness) useRealBuilder(t *testing.T) {
	t.Helper()
	h.useRealBuilderWithTemplate(t, deployedTemplatePath(t))
}

// useRealBuilderWithTemplate swaps the canned builder for the production one
// over a test-owned template file (the custom-template window).
func (h *harness) useRealBuilderWithTemplate(t *testing.T, templatePath string) {
	t.Helper()
	jbCfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("jobbuilder.LoadConfig: %v", err)
	}
	jbCfg.TemplatePath = templatePath
	jbCfg.JobNamespace = "multica-agents"
	jbCfg.JobImage = harnessJobImage
	jbCfg.Issuer = h.issuer
	jbCfg.Nodes = h.reg
	jbCfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	inner, err := jobbuilder.NewBuilder(jbCfg)
	if err != nil {
		t.Fatalf("jobbuilder.NewBuilder: %v", err)
	}
	h.builder = realBuilder{inner: inner, jobs: h.jobs}
	h.rebuildScheduler(t, func(*scheduler.Config) {})
}

// schedulerServerClient is the scheduler's server seam.
type schedulerServerClient struct {
	mu       sync.Mutex
	statuses map[string]string
	forwards []upstreamRequest
}

func (c *schedulerServerClient) TaskStatus(_ context.Context, taskID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.statuses == nil {
		return "dispatched", nil
	}
	status, ok := c.statuses[taskID]
	if !ok {
		return "", scheduler.ErrTaskNotFound
	}
	return status, nil
}

// setStatus programs the C13 answer for one task: once any task has one, an
// unprogrammed task reads as deleted (ErrTaskNotFound).
func (c *schedulerServerClient) setStatus(taskID, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.statuses == nil {
		c.statuses = map[string]string{}
	}
	c.statuses[taskID] = status
}

func (c *schedulerServerClient) Forward(_ context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forwards = append(c.forwards, upstreamRequest{Method: string(ep), Path: taskID, Body: body})
	return 200, []byte(`{"status":"ok"}`), nil
}

func (c *schedulerServerClient) forwarded(ep scheduler.Endpoint) []upstreamRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	var found []upstreamRequest
	for _, req := range c.forwards {
		if req.Method == string(ep) {
			found = append(found, req)
		}
	}
	return found
}

// recordingMetrics implements proxy.Metrics and the scheduler's metric seam,
// recording the catalog outcomes and the S6 delivery face.
type recordingMetrics struct {
	mu            sync.Mutex
	catalogCodes  []string
	catalogSecs   []float64
	deliveryKinds []string
	unconfirmed   int
}

func (m *recordingMetrics) Forward(string, int, time.Duration) {}
func (m *recordingMetrics) ClaimError(string)                  {}
func (m *recordingMetrics) AuthFailure(string)                 {}
func (m *recordingMetrics) WSConnect(string)                   {}
func (m *recordingMetrics) WSDisconnect(string)                {}
func (m *recordingMetrics) WSReconnect(string)                 {}

// The scheduler's own seam: recorded only where a scenario reads it, no-ops
// otherwise (the scheduler must never change behaviour because a metric
// changed).
func (m *recordingMetrics) TaskClaimed()                  {}
func (m *recordingMetrics) TaskTerminal(string)           {}
func (m *recordingMetrics) JobBootSeconds(float64)        {}
func (m *recordingMetrics) JobCreateSeconds(float64)      {}
func (m *recordingMetrics) InflightJobs(int)              {}
func (m *recordingMetrics) DuplicateDispatch()            {}
func (m *recordingMetrics) CredReclaimed(string, float64) {}

func (m *recordingMetrics) ModelCatalogRelay(code string, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.catalogCodes = append(m.catalogCodes, code)
	m.catalogSecs = append(m.catalogSecs, seconds)
}

func (m *recordingMetrics) DeliveryClaim(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveryKinds = append(m.deliveryKinds, result)
}

func (m *recordingMetrics) DeliveryLatency(float64) {}

func (m *recordingMetrics) DeliveryUnconfirmed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unconfirmed++
}

func (m *recordingMetrics) codes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.catalogCodes...)
}

// seconds returns the recorded catalog durations, in observation order.
func (m *recordingMetrics) seconds() []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]float64(nil), m.catalogSecs...)
}

// deliveryClaimCount counts the recorded S6 claim outcomes per result.
func (m *recordingMetrics) deliveryClaimCount(result string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, recorded := range m.deliveryKinds {
		if recorded == result {
			n++
		}
	}
	return n
}

// deliveryUnconfirmedCount reports how many entries the confirmation window
// judged undelivered.
func (m *recordingMetrics) deliveryUnconfirmedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.unconfirmed
}

// probeBuilder is the jobbuilder seam plus the test-only credential reader.
type probeBuilder interface {
	scheduler.JobBuilder
	probeToken(jobName string) string
}

// harness wires the real components against the stubs.
type harness struct {
	t        *testing.T
	cfg      proxy.Config
	reg      *registry.Registry
	probes   *registry.ProbeRegistry
	startup  *registry.StartupCatalog
	slots    *proxy.CatalogSlots
	jobs     *jobClient
	builder  probeBuilder
	server   *schedulerServerClient
	metrics  *recordingMetrics
	issuer   *auth.Issuer
	client   *proxy.Client
	sched    *scheduler.Scheduler
	upstream *upstreamStub
	handler  http.Handler
	jobsHTTP *httptest.Server
	logs     *bytes.Buffer

	schedCfg scheduler.Config
}

// Delivery confirmation window and lease cadence the scenarios run with: the
// window is short enough for a bounded wait in a test, and the keeper cadence
// sits far below it (production defaults are 30s / 15s).
const (
	deliveryConfirmWindow = 300 * time.Millisecond
	leaseKeeperInterval   = 20 * time.Millisecond
)

func newHarness(t *testing.T, catalogEnabled bool) *harness {
	t.Helper()
	return newHarnessWithWindow(t, catalogEnabled, deliveryConfirmWindow)
}

// newHarnessWithWindow is newHarness with the S6 confirmation window chosen by
// the scenario. Zero turns the re-delivery exit off — the form whose scenario
// is "delivered exactly once" (ADR-032 / AC-46) — and the registry and the
// scheduler config must agree on it, as they do in a deployment.
func newHarnessWithWindow(t *testing.T, catalogEnabled bool, window time.Duration) *harness {
	t.Helper()
	upstream := newUpstreamStub(t)
	reg := registry.New(time.Now, registry.WithDeliveryConfirmTimeout(window))
	probes := registry.NewProbeRegistry()
	startup := registry.NewStartupCatalog()
	slots := proxy.NewCatalogSlots()
	logs := &bytes.Buffer{}

	cfg := proxy.Config{
		ServerURL:            upstream.server.URL,
		WorkspaceID:          "ws-1",
		DaemonID:             "foreman",
		Version:              "0.1.0",
		ClaimInterval:        time.Second,
		ClaimIntervalIdle:    time.Second,
		LeaseRefreshInterval: time.Second,
		WSEnabled:            false,
		ModelCatalogEnabled:  catalogEnabled,
		ModelCatalogTimeout:  45 * time.Second,
		ModelCatalogCacheTTL: 6 * time.Hour,
	}
	client := proxy.NewClient(cfg, "mdt_test",
		proxy.WithStartupCatalog(startup),
		proxy.WithCatalogSlots(slots),
		proxy.WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
		proxy.WithTaskRuntime(func(taskID string) string {
			entry, ok := reg.Get(taskID)
			if !ok {
				return ""
			}
			return entry.RealRuntimeID
		}),
	)
	issuer, err := proxy.JobTokenIssuer(testKey, reg, probes, auth.DefaultJobTokenTTL)
	if err != nil {
		t.Fatalf("JobTokenIssuer: %v", err)
	}
	jobs := newJobClient()
	bld := &builder{issuer: issuer, probes: probes}
	server := &schedulerServerClient{}
	metrics := &recordingMetrics{}
	schedCfg := scheduler.Config{
		JobNamespace:           "multica-agents",
		WorkspaceID:            "ws-1",
		MaxInflightJobs:        10,
		ClaimBatchMax:          32,
		JobBootTimeout:         300 * time.Second,
		JobDrainTimeout:        120 * time.Second,
		DeliveryConfirmTimeout: window,
		StartupProbeTimeout:    3 * time.Second,
		ModelCatalogEnabled:    catalogEnabled,
		ModelCatalogTimeout:    45 * time.Second,
		ReconcileInterval:      30 * time.Second,
	}
	sched, err := scheduler.New(schedCfg, reg, jobs, bld, server, metrics,
		scheduler.WithPendingReports(noReports{}),
		scheduler.WithRevoker(noRevoker{}),
		scheduler.WithProbeRegistry(probes),
		scheduler.WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	)
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	handler := proxy.NewFakeServer(issuer, reg, sched, client, nil, metrics, proxy.WithProbeRegistry(probes))
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	return &harness{
		t: t, cfg: cfg, reg: reg, probes: probes, startup: startup, slots: slots,
		jobs: jobs, builder: bld, server: server, metrics: metrics, issuer: issuer,
		client: client, sched: sched, upstream: upstream, handler: handler,
		jobsHTTP: httpServer, logs: logs, schedCfg: schedCfg,
	}
}

// rebuildScheduler replaces the scheduler with one built from a mutated
// config; the seams (registry, job client, builder, probe table) stay.
func (h *harness) rebuildScheduler(t *testing.T, mutate func(*scheduler.Config)) {
	t.Helper()
	cfg := h.schedCfg
	mutate(&cfg)
	sched, err := scheduler.New(cfg, h.reg, h.jobs, h.builder, h.server, h.metrics,
		scheduler.WithPendingReports(noReports{}),
		scheduler.WithRevoker(noRevoker{}),
		scheduler.WithProbeRegistry(h.probes),
		scheduler.WithLogger(slog.New(slog.NewJSONHandler(h.logs, nil))),
	)
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	h.sched = sched
	h.schedCfg = cfg
}

// startLeaseKeeper runs the production lease keeper from the call onward for
// the rest of the test, so a scenario can observe a pre-start bound firing
// (delivery confirmation window / boot deadline) without waiting out the
// production cadence. When it starts is part of a scenario's construction:
// starting it before the state a scenario is about to build turns that state
// into a race against the keeper's rounds. It is called once per scenario, and
// it is built from the scheduler in place at that moment.
func (h *harness) startLeaseKeeper(t *testing.T) {
	t.Helper()
	keeper := proxy.NewLeaseKeeper(h.client, h.reg, h.sched, leaseKeeperInterval)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = keeper.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// declare performs C1 with the scenario's discovery.
func (h *harness) declare(d registry.Discovery) {
	h.t.Helper()
	if err := h.client.RegisterWithDiscovery(context.Background(), d); err != nil {
		h.t.Fatalf("RegisterWithDiscovery: %v", err)
	}
	// Drop the appearance notifications: a scenario asserting on the change
	// channel means the moves that happen after the declaration.
	for {
		select {
		case <-h.client.Changes():
			continue
		default:
		}
		break
	}
}

// call performs one Job-side request against the fake server.
func (h *harness) call(method, path, token string, body []byte) (int, []byte) {
	h.t.Helper()
	req, err := http.NewRequest(method, h.jobsHTTP.URL+path, bytes.NewReader(body))
	if err != nil {
		h.t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}

// waitCatalogMetrics waits until the reporter has recorded n catalog outcomes:
// the relay metric follows the C21 answer (the probe path observes the session
// one poll interval later).
func (h *harness) waitCatalogMetrics(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.metrics.codes()) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newReporter builds the catalog reporter the catalog scenarios drive.
func (h *harness) newReporter(t *testing.T) *proxy.ModelCatalogReporter {
	t.Helper()
	return proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(&writeBuffer{}, nil))))
}

// serveProbeDaemon plays the probe pod's daemon: register (S4), take the command
// from the S5 ack, then report it (S27). It runs off the CreateJob call so the
// scheduler's start path is not held up by the HTTP round trips; the returned
// channel yields the command id the daemon was handed.
func (h *harness) serveProbeDaemon(t *testing.T, resultBody string) <-chan string {
	t.Helper()
	seen := make(chan string, 1)
	h.jobs.onCreateJob = func(job *batchv1Job) {
		name := job.Name
		go func() {
			token := h.builder.probeToken(name)
			if token == "" {
				return
			}
			code, body, err := h.probeCall("POST", "/api/daemon/register", token, []byte(`{
				"daemon_id": "`+name+`",
				"cli_version": "0.6.1",
				"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
			}`))
			if err != nil || code != 200 {
				t.Errorf("probe S4 = %d: %s (%v)", code, body, err)
				return
			}
			session, ok := h.probes.ProbeByJob(name)
			if !ok {
				t.Errorf("probe session %s missing at S4", name)
				return
			}
			code, body, err = h.probeCall("POST", "/api/daemon/heartbeat", token,
				[]byte(`{"runtime_id": "`+session.JobRuntime+`"}`))
			if err != nil || code != 200 {
				t.Errorf("probe S5 = %d: %s (%v)", code, body, err)
				return
			}
			var ack struct {
				PendingModelList *struct {
					ID string `json:"id"`
				} `json:"pending_model_list"`
			}
			if err := json.Unmarshal(body, &ack); err != nil || ack.PendingModelList == nil {
				t.Errorf("probe S5 ack carries no command: %s (%v)", body, err)
				return
			}
			seen <- ack.PendingModelList.ID
			code, body, err = h.probeCall("POST",
				"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+ack.PendingModelList.ID+"/result",
				token, []byte(resultBody))
			if err != nil || code != 200 {
				t.Errorf("probe S27 = %d: %s (%v)", code, body, err)
			}
		}()
	}
	return seen
}

// probeCall performs one Job-side request from the probe daemon's own
// goroutine: t.Fatalf must stay on the test goroutine, so a transport failure
// comes back as an error and the caller reports it with t.Errorf.
func (h *harness) probeCall(method, path, token string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(method, h.jobsHTTP.URL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, raw, nil
}

// events returns the parsed scheduler log lines carrying one msg.
func (h *harness) events(msg string) []map[string]any {
	var found []map[string]any
	for _, line := range bytes.Split(h.logs.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			continue
		}
		if fields["msg"] == msg {
			found = append(found, fields)
		}
	}
	return found
}

type noReports struct{}

func (noReports) Enqueue(string, scheduler.Endpoint, []byte) error { return nil }
func (noReports) Queued(string) bool                               { return false }

type noRevoker struct{}

func (noRevoker) Revoke(string) {}
