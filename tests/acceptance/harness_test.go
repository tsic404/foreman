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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coder/websocket"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/coordination"
	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
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
	// At is when the request arrived: the wire observation a renewal-cadence
	// scenario reads its intervals off.
	At time.Time
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
	// deliveredAt anchors the server's window at delivery (§1.3「窗口自交付起算」).
	deliveredAt time.Time
	// body is the result the record holds, bodyWrites how many C21 forwards it
	// adopted: a repeat that adopts the same bytes again is idempotent, a forward
	// past the window adopts nothing (§1.3).
	body       []byte
	bodyWrites int
	// dropNextResponse makes the next C21 answer vanish on the wire — the
	// result landed but its response never reached Foreman (C-21 / §1.3).
	dropNextResponse bool
}

// queuedTask is one task the stub server holds on its side of the claim
// contract: queued until a C3 claim hands it out, dispatched afterwards. The
// server keeps the excess queued — Foreman only ever sees what it claimed.
type queuedTask struct {
	id      string
	payload json.RawMessage
	status  string
}

// The server-side task statuses the claim contract moves through.
const (
	taskStatusQueued     = "queued"
	taskStatusDispatched = "dispatched"
)

// forwardProbeObservation is one S29 fidelity probe's upstream view: what the
// proxy forwarded, with the body digested rather than held, so a payload larger
// than any proxy-side buffer can be compared without buffering it here.
type forwardProbeObservation struct {
	Method   string
	Path     string
	RawQuery string
	Header   http.Header
	Host     string
	BodyLen  int64
	BodySHA  string
}

// forwardPattern yields a deterministic byte stream of any length without
// holding it in memory (the S29 payload probes' content).
type forwardPattern struct{ off int64 }

func (p *forwardPattern) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte(p.off*31 + p.off>>7)
		p.off++
	}
	return len(b), nil
}

// forwardDigest is the digest forwardPattern's first n bytes hash to.
func forwardDigest(t *testing.T, n int64) string {
	t.Helper()
	sum := sha256.New()
	if _, err := io.CopyN(sum, &forwardPattern{}, n); err != nil {
		t.Fatalf("digest the probe pattern: %v", err)
	}
	return hex.EncodeToString(sum.Sum(nil))
}

// forwardProbeAnswer is the non-JSON verdict body the fidelity probe returns:
// bytes a passthrough must not re-encode or re-type.
var forwardProbeAnswer = []byte{0x00, 0x01, 0xfe, 0xff, 'o', 'k'}

// forwardProbeBulkBytes is the download probe's size: past the bound the local
// daemon face used to impose on every body, so a proxy-side buffer cannot hold
// it.
const forwardProbeBulkBytes = 33<<20 + 7

// upstreamStub is the server side of C1–C21 as the scenarios need it.
type upstreamStub struct {
	mu sync.Mutex
	// workspaceID is the workspace the C-17 answer names (a multi-workspace
	// fixture gives each stub its own). workspacesBody, when non-nil, is the
	// answer verbatim — a scenario judging a malformed entry (an id with an
	// empty or whitespace-only display name) programs it.
	workspaceID    string
	workspacesBody []byte
	// registerIDs maps a declared provider type to the runtime id the server
	// answers with; a missing entry means the provider is not registered.
	// profileIDs holds the profile identities, keyed by RuntimeIdentityKey.
	registerIDs map[string]string
	profileIDs  map[string]string
	// registerRepos / registerReposVersion / registerSettings are the C-1
	// workspace state seed; a scenario replaces them to drive AC-58.
	registerRepos        json.RawMessage
	registerReposVersion string
	registerSettings     json.RawMessage
	// registerStatus/registerError program a failing C-1 (the refresh
	// sequence's commit failure). profilesStatus/profilesBody are the C-18
	// answer; reposStatus/reposBody the C-22 answer (default 404 keeps the
	// pre-C-22 fixture shape). The counters are what a refresh scenario
	// asserts against.
	registerStatus int
	registerError  string
	profilesStatus int
	profilesBody   []byte
	profilesGate   chan struct{}
	profilesCalls  int
	reposStatus    int
	reposBody      []byte
	reposGate      chan struct{}
	reposCalls     int
	// heartbeats maps a real runtime id to the status the heartbeat answers
	// with (default 200).
	heartbeats map[string]int
	// modelsStatus is the C21 answer (default 200).
	modelsStatus int
	// models tracks the caller-facing model-list commands by request id.
	models map[string]*modelCommand
	// modelWindow is the server's request window, anchored at delivery: a C21
	// forward past it is answered like any other and adopts nothing. Zero means
	// the window is not modelled (every forward adopts).
	modelWindow time.Duration
	// modelSeq numbers the minted request ids (the real ones are 32 hex digits).
	modelSeq int
	// queued is the server-side task queue C3 serves. Empty by default.
	queued []*queuedTask
	// skillStatus and skillBody are the C19 resolve answer (default 200 with
	// an empty bundle list); skillGate, when set, holds every answer until it
	// closes, so a scenario can make a resolve genuinely in flight.
	skillStatus int
	skillBody   []byte
	skillGate   chan struct{}
	// ackShape shapes the heartbeat ack's runtime_id field.
	ackShape ackRuntimeIDShape
	// taskStatuses is the C13 answer per task (an absent key is a 404, the
	// shape a task the server no longer holds produces).
	taskStatuses map[string]string
	// lifecycleCodes programs the C5–C12 answer per endpoint (absent = 200):
	// the permanent-rejection face a terminal report can meet.
	lifecycleCodes map[string]int
	// lifecycleGates parks the next C5–C12 forward for an endpoint after it
	// is recorded, so a scenario can read the write-ahead window (queue depth
	// and carrier) while the report is still in flight.
	lifecycleGates map[string]chan struct{}
	// lifecycleDrops makes the next C5–C12 forward for an endpoint fail at the
	// transport: the connection is closed without a response head. One-shot,
	// so the retry that follows takes the programmed answer.
	lifecycleDrops map[string]bool
	// gcStatus/gcBody are the S-20 answer (default 404).
	gcStatus int
	gcBody   []byte
	requests []upstreamRequest
	server   *httptest.Server
	// forwardProbes records the S29 fidelity probe's upstream view.
	forwardProbes []forwardProbeObservation
	// forwardStreamed / forwardGate program the streaming probe: the answer's
	// first chunk signals the channel, the rest waits for the gate.
	forwardStreamed chan struct{}
	forwardGate     chan struct{}
	// wsDials counts the C-20 handshake attempts. wsStatus programs the answer
	// (0 accepts, any other value is written as the rejection status — 403
	// drives the §1.5 config-error face). wsConns is the live connection per
	// runtime id, the push face a scenario drives.
	wsDials  int
	wsStatus int
	wsConns  map[string]*websocket.Conn
}

func newUpstreamStub(t *testing.T) *upstreamStub {
	t.Helper()
	stub := &upstreamStub{
		registerIDs:          map[string]string{},
		profileIDs:           map[string]string{},
		registerRepos:        json.RawMessage(`[]`),
		registerReposVersion: "",
		heartbeats:           map[string]int{},
		modelsStatus:         200,
		models:               map[string]*modelCommand{},
		skillStatus:          200,
		skillBody:            []byte(`{"bundles":[]}`),
		taskStatuses:         map[string]string{},
		lifecycleCodes:       map[string]int{},
		lifecycleGates:       map[string]chan struct{}{},
		lifecycleDrops:       map[string]bool{},
		gcStatus:             http.StatusNotFound,
		gcBody:               []byte(`{"error":"not found"}`),
		registerStatus:       http.StatusOK,
		profilesStatus:       http.StatusOK,
		profilesBody:         []byte(`{"workspace_id":"ws-1","runtime_profiles":[]}`),
		reposStatus:          http.StatusNotFound,
		reposBody:            []byte(`{"error":"not found"}`),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, "/api/daemon/register", body)
		var req struct {
			Runtimes []struct {
				Name      string `json:"name"`
				Type      string `json:"type"`
				ProfileID string `json:"profile_id"`
			} `json:"runtimes"`
		}
		_ = json.Unmarshal(body, &req)
		stub.mu.Lock()
		repos, version, settings := stub.registerRepos, stub.registerReposVersion, stub.registerSettings
		status, errBody := stub.registerStatus, stub.registerError
		stub.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(errBody))
			return
		}
		answers := make([]map[string]any, 0, len(req.Runtimes))
		for _, rt := range req.Runtimes {
			id, ok := stub.idFor(rt.Type, rt.ProfileID)
			if !ok {
				continue
			}
			answer := map[string]any{
				"id": id, "name": rt.Name, "provider": rt.Type, "status": "online",
				"profile_id": nil,
			}
			if rt.ProfileID != "" {
				answer["profile_id"] = rt.ProfileID
			}
			answers = append(answers, answer)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"runtimes": answers, "repos": repos, "repos_version": version, "settings": settings,
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
		var req struct {
			MaxTasks int `json:"max_tasks"`
		}
		_ = json.Unmarshal(body, &req)
		writeJSON(w, http.StatusOK, map[string]any{"tasks": stub.claim(req.MaxTasks)})
	})
	// C5–C12: the lifecycle forwards the scheduler sends through the
	// production client. The stub answers 200 and records the call; a
	// scenario wiring the real client reads the forward metric off these,
	// and one judging a permanent rejection programs the answer and can hold
	// the call in flight.
	for _, ep := range []string{
		"start", "progress", "messages", "usage", "complete", "fail",
		"cancel-ack", "session", "wait-local-directory",
	} {
		mux.HandleFunc("POST /api/daemon/tasks/{tid}/"+ep, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			stub.record(r, r.URL.Path, body)
			gate, status, drop := stub.lifecycleAnswer(ep)
			if gate != nil {
				<-gate
			}
			if drop {
				// Transport failure: the request landed but the answer never
				// starts, the shape the client reports as an error rather than
				// a status code.
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					return
				}
				_ = conn.Close()
				return
			}
			if status == 0 {
				status = http.StatusOK
			}
			writeJSON(w, status, map[string]string{"status": "ok"})
		})
	}
	// C4: the lease keeper's renewal. Without this route the stub's 404 would
	// read as "the server deleted the task" and settle every pre-start entry
	// as vanished.
	mux.HandleFunc("POST /api/daemon/runtimes/{rid}/tasks/{tid}/prepare-lease", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, r.URL.Path, body)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	// C19: the skill-bundle resolve Foreman forwards on a cache miss. The
	// answer is programmable, and the call is recorded so a scenario can count
	// upstream touches.
	mux.HandleFunc("POST /api/daemon/runtimes/{rid}/tasks/{tid}/skill-bundles/resolve", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, r.URL.Path, body)
		status, resp, gate := stub.skillAnswer()
		if gate != nil {
			<-gate
		}
		w.WriteHeader(status)
		_, _ = w.Write(resp)
	})
	mux.HandleFunc("POST /api/daemon/deregister", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, "/api/daemon/deregister", body)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /api/daemon/runtimes/{rid}/models/{req}/result", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, r.URL.Path, body)
		requestID := r.PathValue("req")
		stub.acceptModelResult(requestID, body)
		if stub.takeDroppedModelResponse(requestID) {
			// The result landed but its answer never reaches Foreman: closing
			// the connection is how a lost response looks to the caller.
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				_ = conn.Close()
				return
			}
			writeJSON(w, http.StatusBadGateway, map[string]string{"status": "error"})
			return
		}
		writeJSON(w, stub.modelsStatus, map[string]string{"status": "ok"})
	})
	// The claim loop's preflight (C17/C18/C16). A scenario driving the
	// production loop needs these to answer as the server does before C3 runs.
	mux.HandleFunc("GET /api/daemon/workspaces", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, "/api/daemon/workspaces", nil)
		stub.mu.Lock()
		// bytes.Clone keeps a programmed empty answer non-nil, so it cannot fall back.
		id, body := stub.workspaceID, bytes.Clone(stub.workspacesBody)
		stub.mu.Unlock()
		if body != nil {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			return
		}
		if id == "" {
			id = "ws-1"
		}
		writeJSON(w, http.StatusOK, []map[string]string{{"id": id, "name": "foreman"}})
	})
	mux.HandleFunc("GET /api/daemon/workspaces/{wid}/runtime-profiles", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		stub.mu.Lock()
		status, body, gate := stub.profilesStatus, append([]byte(nil), stub.profilesBody...), stub.profilesGate
		stub.profilesCalls++
		stub.mu.Unlock()
		if gate != nil {
			<-gate
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	// C-22: the S-2 refresh face. The default 404 keeps the pre-C-22 fixtures
	// replaying their C-1 seed; a workspace-state scenario programs this.
	mux.HandleFunc("GET /api/daemon/workspaces/{wid}/repos", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		stub.mu.Lock()
		status, body, gate := stub.reposStatus, append([]byte(nil), stub.reposBody...), stub.reposGate
		stub.reposCalls++
		stub.mu.Unlock()
		if gate != nil {
			<-gate
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	mux.HandleFunc("POST /api/tokens/current/renew", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, "/api/tokens/current/renew", body)
		writeJSON(w, http.StatusOK, map[string]string{"token": "mdt_test"})
	})
	// C-20: the server side of the subscription. The default accepts the
	// handshake; a scenario programs a rejection status (403) to drive the
	// §1.5 config-error face, and pushes frames through the live connection.
	mux.HandleFunc("GET /api/daemon/ws", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer mdt_test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		stub.mu.Lock()
		stub.wsDials++
		status := stub.wsStatus
		stub.mu.Unlock()
		if status != 0 && status != http.StatusSwitchingProtocols {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"rejected"}`))
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		runtimeID := r.URL.Query().Get("runtime_ids")
		stub.mu.Lock()
		if stub.wsConns == nil {
			stub.wsConns = map[string]*websocket.Conn{}
		}
		stub.wsConns[runtimeID] = conn
		stub.mu.Unlock()
		_, _, _ = conn.Read(r.Context()) // hold the stream until the peer leaves
	})
	// C13: the daemon's status poll. An unprogrammed task is a 404 — the same
	// answer the server gives for a task it no longer holds.
	mux.HandleFunc("GET /api/daemon/tasks/{tid}/status", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		stub.mu.Lock()
		status, ok := stub.taskStatuses[r.PathValue("tid")]
		stub.mu.Unlock()
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": status})
	})
	// S-20: the gc-check query the daemon's GC path calls. Programmable so a
	// carrier scenario can assert status and bytes travel verbatim.
	mux.HandleFunc("GET /api/daemon/tasks/{tid}/gc-check", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		stub.mu.Lock()
		status, body := stub.gcStatus, append([]byte(nil), stub.gcBody...)
		stub.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	// S-23: the caller-credential forward paths. The answer is fixed so the
	// census scenario can assert the upstream's own status, header and body
	// reach the daemon unchanged.
	faceAnswer := func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.record(r, r.URL.Path, body)
		w.Header().Set("X-Face-Upstream", "kept")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(`{"credential_header":"x","credential":"v","x_future_field":true}`))
	}
	mux.HandleFunc("GET /api/daemon/tasks/{tid}/plugin-hooks", faceAnswer)
	mux.HandleFunc("POST /api/daemon/tasks/{tid}/plugin-hooks", faceAnswer)
	mux.HandleFunc("GET /api/daemon/tasks/{tid}/plugin-mcp/{cid}/credential", faceAnswer)
	mux.HandleFunc("GET /api/daemon/tasks/{tid}/remote-mcp/{cid}/credential", faceAnswer)
	// S29 fidelity probes: the upstream observes what arrived (body digested,
	// never held) and answers with a non-JSON verdict the proxy must replay
	// unchanged.
	mux.HandleFunc("POST /api/forward-probe", func(w http.ResponseWriter, r *http.Request) {
		stub.observeForward(r)
		h := w.Header()
		h.Set("Content-Type", "application/octet-stream")
		h.Set("X-Probe-Verdict", "kept")
		h.Add("X-Probe-Multi", "a")
		h.Add("X-Probe-Multi", "b")
		h.Add("Set-Cookie", "a=1")
		h.Add("Set-Cookie", "b=2")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(forwardProbeAnswer)
	})
	// The bulk download probe: the body is generated, so its size is bounded by
	// the caller's read, not by the stub's memory.
	mux.HandleFunc("GET /api/forward-probe-bulk", func(w http.ResponseWriter, r *http.Request) {
		stub.observeForward(r)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(forwardProbeBulkBytes, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.CopyN(w, &forwardPattern{}, forwardProbeBulkBytes)
	})
	// The streaming probe: the head and a first chunk go out, then the answer
	// waits for the scenario's release. A proxy that buffers the whole body
	// cannot deliver that chunk before the release.
	mux.HandleFunc("GET /api/forward-probe-stream", func(w http.ResponseWriter, r *http.Request) {
		stub.observeForward(r)
		stub.mu.Lock()
		streamed, gate := stub.forwardStreamed, stub.forwardGate
		stub.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("f"), 16<<10))
		w.(http.Flusher).Flush()
		if streamed != nil {
			close(streamed)
		}
		if gate != nil {
			<-gate
		}
		_, _ = w.Write([]byte("-last"))
	})
	// The S29 failure probes (TC-tech-forward-08): a head that never arrives, an
	// upstream 5xx, and a head whose body is cut short. Each records the request
	// so a scenario can count what one caller request produced upstream.
	mux.HandleFunc("GET /api/forward-probe-refused", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_ = conn.Close()
	})
	mux.HandleFunc("GET /api/forward-probe-5xx", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"upstream overloaded"}`))
	})
	mux.HandleFunc("GET /api/forward-probe-truncated", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r, r.URL.Path, nil)
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		// One chunk, then the connection: no terminating chunk and no declared
		// length, so the caller-side framing is chunked — the shape in which a
		// buffered byte is lost rather than merely cut short.
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nshort\r\n")
		_ = buf.Flush()
		_ = conn.Close()
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	stub.server = server
	return stub
}

// all snapshots every call the stub observed, in arrival order. It is the
// denominator of "this route never reached the upstream".
func (s *upstreamStub) all() []upstreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]upstreamRequest(nil), s.requests...)
}

// setTaskStatus programs the C13 answer for one task.
func (s *upstreamStub) setTaskStatus(taskID, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.taskStatuses[taskID] = status
}

// setLifecycleStatus programs the C5–C12 answer for one endpoint (0 restores
// 200). The permanent-rejection scenario answers a terminal report here.
func (s *upstreamStub) setLifecycleStatus(ep string, code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lifecycleCodes[ep] = code
}

// holdLifecycle parks the next C5–C12 forward for ep once it is recorded; the
// scenario releases the returned channel. It is how the write-ahead window is
// read from outside the scheduler.
func (s *upstreamStub) holdLifecycle(ep string) chan struct{} {
	gate := make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lifecycleGates[ep] = gate
	return gate
}

// lifecycleAnswer takes the pending gate, the programmed status and the
// one-shot drop flag for ep in one locked step.
func (s *upstreamStub) lifecycleAnswer(ep string) (chan struct{}, int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gate := s.lifecycleGates[ep]
	delete(s.lifecycleGates, ep)
	drop := s.lifecycleDrops[ep]
	delete(s.lifecycleDrops, ep)
	return gate, s.lifecycleCodes[ep], drop
}

// dropNextLifecycle makes the next C5–C12 forward for ep fail before the
// response head: the stub records the request, then closes the connection.
// It is the client-side failure the upstream status cannot express.
func (s *upstreamStub) dropNextLifecycle(ep string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lifecycleDrops[ep] = true
}

// setGCCheck programs the S-20 answer.
func (s *upstreamStub) setGCCheck(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcStatus, s.gcBody = status, []byte(body)
}

func (s *upstreamStub) record(r *http.Request, path string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, upstreamRequest{
		Method: r.Method, Path: path, Body: body, Header: r.Header.Clone(), At: time.Now(),
	})
}

// wsDialCount counts the C-20 handshake attempts the stub observed.
func (s *upstreamStub) wsDialCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wsDials
}

// setWSStatus programs the WS handshake answer: 0 accepts, any other value is
// the rejection status (403 = the §1.5 credential/identity mismatch).
func (s *upstreamStub) setWSStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wsStatus = code
}

// pushTaskAvailable sends daemon:task_available over the runtime's live
// connection, the C-20 push the claim loop's wakeup rides on.
func (s *upstreamStub) pushTaskAvailable(t *testing.T, runtimeID, taskID string) {
	t.Helper()
	s.mu.Lock()
	conn := s.wsConns[runtimeID]
	s.mu.Unlock()
	if conn == nil {
		t.Fatalf("no WS connection for runtime %q", runtimeID)
	}
	raw, err := json.Marshal(map[string]any{
		"type":    "daemon:task_available",
		"payload": map[string]string{"task_id": taskID},
	})
	if err != nil {
		t.Fatalf("marshal task_available: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatalf("push task_available: %v", err)
	}
}

// observeForward records one fidelity probe's upstream view; the body is read
// through a digest, so the probe never depends on the payload fitting memory.
func (s *upstreamStub) observeForward(r *http.Request) {
	sum := sha256.New()
	n, _ := io.Copy(sum, r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forwardProbes = append(s.forwardProbes, forwardProbeObservation{
		Method: r.Method, Path: r.URL.Path, RawQuery: r.URL.RawQuery,
		Header: r.Header.Clone(), Host: r.Host,
		BodyLen: n, BodySHA: hex.EncodeToString(sum.Sum(nil)),
	})
}

// forwardObservations returns the fidelity probes the upstream saw, in order.
func (s *upstreamStub) forwardObservations() []forwardProbeObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]forwardProbeObservation(nil), s.forwardProbes...)
}

// forwardStreamGate programs the streaming probe and returns the first-chunk
// signal plus the release function (idempotent, so a failing test can clean up
// without a double close).
func (s *upstreamStub) forwardStreamGate() (<-chan struct{}, func()) {
	streamed := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	s.mu.Lock()
	s.forwardStreamed, s.forwardGate = streamed, gate
	s.mu.Unlock()
	return streamed, release
}

func (s *upstreamStub) idFor(provider, profileID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.profileIDs[registry.RuntimeIdentityKey(provider, profileID)]; ok {
		return id, true
	}
	id, ok := s.registerIDs[provider]
	return id, ok
}

// setRuntimeID maps one identity to the runtime id the C1 answer carries.
// setWorkspaceID makes the C-17 answer name this workspace.
func (s *upstreamStub) setWorkspaceID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workspaceID = id
}

// setWorkspacesAnswer programs the C-17 answer verbatim; the scenario owns the
// entry shapes (an empty or whitespace-only display name is a malformed one).
func (s *upstreamStub) setWorkspacesAnswer(body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workspacesBody = []byte(body)
}

func (s *upstreamStub) setRuntimeID(provider, profileID, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if profileID == "" {
		s.registerIDs[provider] = id
		return
	}
	s.profileIDs[registry.RuntimeIdentityKey(provider, profileID)] = id
}

// setProfiles programs the C-18 answer; a gate installed by gateProfiles holds
// every answer until it closes, so a refresh can be observed in flight.
func (s *upstreamStub) setProfiles(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profilesStatus, s.profilesBody = status, []byte(body)
}

// gateProfiles holds every C-18 answer until the returned function is called:
// it is how a scenario makes one refresh be in flight while events pile up.
func (s *upstreamStub) gateProfiles() (release func()) {
	gate := make(chan struct{})
	s.mu.Lock()
	s.profilesGate = gate
	s.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// profileCalls reads the C-18 call count.
func (s *upstreamStub) profileCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.profilesCalls
}

// setRepos programs the C-22 answer; gate (when non-nil) holds every answer
// until it closes, so a concurrency scenario can make one refresh in flight.
func (s *upstreamStub) setRepos(status int, body string, gate chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reposStatus, s.reposBody, s.reposGate = status, []byte(body), gate
}

// reposCallCount reads the C-22 call count.
func (s *upstreamStub) reposCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reposCalls
}

// setRegisterStatus programs a failing C-1 answer (the refresh commit path).
func (s *upstreamStub) setRegisterStatus(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registerStatus, s.registerError = status, body
}

// setRegisterWorkspace programs the C-1 workspace state seed.
func (s *upstreamStub) setRegisterWorkspace(repos, version, settings string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registerRepos = json.RawMessage(repos)
	s.registerReposVersion = version
	if settings == "" {
		s.registerSettings = nil
		return
	}
	s.registerSettings = json.RawMessage(settings)
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

// enqueueTask puts one task on the stub server's queue in the queued state.
// The payload carries the fields the scheduler indexes.
func (s *upstreamStub) enqueueTask(taskID string) {
	s.enqueuePayload(taskID, "agent-1", "ws-1", "rid-zz")
}

// enqueueTaskFor puts one task on a workspace-scoped stub's queue: the payload
// names the workspace and the runtime that workspace's claim carries, so the
// scheduler registers the entry under the expected identity.
func (s *upstreamStub) enqueueTaskFor(taskID, workspaceID string) {
	s.enqueuePayload(taskID, "agent-"+workspaceID, workspaceID, "rid-"+workspaceID)
}

func (s *upstreamStub) enqueuePayload(taskID, agentID, workspaceID, runtimeID string) {
	payload, _ := json.Marshal(map[string]any{
		"id": taskID, "agent_id": agentID, "workspace_id": workspaceID,
		"runtime_id": runtimeID, "agent": map[string]any{"model": "k3"},
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queued = append(s.queued, &queuedTask{
		id: taskID, payload: payload, status: taskStatusQueued,
	})
}

// claim serves up to maxTasks queued tasks and flips each to dispatched: the
// server's own queued → dispatched transition on C-3 (§1.2). Tasks beyond the
// batch stay queued — they are the server's queue, not Foreman's.
func (s *upstreamStub) claim(maxTasks int) []json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxTasks < 0 {
		// A negative batch asks for nothing; make() would panic on it.
		maxTasks = 0
	}
	served := make([]json.RawMessage, 0, maxTasks)
	for _, task := range s.queued {
		if len(served) >= maxTasks {
			break
		}
		if task.status != taskStatusQueued {
			continue
		}
		task.status = taskStatusDispatched
		served = append(served, task.payload)
	}
	return served
}

// requeueTask puts a dispatched task back on the stub server's queue: the
// redispatch the server performs once its lease on the task has lapsed. The
// lease magnitude is the server's own clock, so the fixture plays its decision
// rather than waiting it out. Reports false when the task is not the stub's or
// is not dispatched.
func (s *upstreamStub) requeueTask(taskID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.queued {
		if task.id != taskID {
			continue
		}
		if task.status != taskStatusDispatched {
			return false
		}
		task.status = taskStatusQueued
		return true
	}
	return false
}

// taskStatus is the stub server's status for one enqueued task ("" when the
// task was never enqueued).
func (s *upstreamStub) taskStatus(taskID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.queued {
		if task.id == taskID {
			return task.status
		}
	}
	return ""
}

// tasksInStatus counts the stub server's tasks in one status.
func (s *upstreamStub) tasksInStatus(status string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, task := range s.queued {
		if task.status == status {
			n++
		}
	}
	return n
}

// setSkillAnswer programs the C19 answer the stub serves for every resolve.
func (s *upstreamStub) setSkillAnswer(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.skillStatus = status
	s.skillBody = []byte(body)
}

// skillAnswer reads the programmed C19 answer and the gate currently holding
// it (nil when the answers flow straight through).
func (s *upstreamStub) skillAnswer() (int, []byte, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.skillStatus
	if status == 0 {
		status = http.StatusOK
	}
	return status, append([]byte(nil), s.skillBody...), s.skillGate
}

// gateSkill holds every later C19 answer until the returned release is called:
// a scenario that needs a resolve genuinely in flight releases it explicitly.
func (s *upstreamStub) gateSkill() (release func()) {
	gate := make(chan struct{})
	s.mu.Lock()
	s.skillGate = gate
	s.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// skillCalls counts the C19 resolves the upstream observed.
func (s *upstreamStub) skillCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, req := range s.requests {
		if strings.HasSuffix(req.Path, "/skill-bundles/resolve") {
			n++
		}
	}
	return n
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
// server delivers a request exactly once, and the delivery anchors its window.
func (s *upstreamStub) deliverModelCommand(runtimeID string) *modelCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cmd := range s.models {
		if cmd.runtimeID == runtimeID && !cmd.delivered {
			cmd.delivered = true
			cmd.deliveredAt = time.Now()
			return cmd
		}
	}
	return nil
}

// acceptModelResult plays the server taking one C21 forward: inside the window
// the record adopts the body (counting the write, so a repeat stays visible),
// past it the forward is answered like any other and changes nothing (§1.3
// 「越过窗口后的转发不改写终态与缓存」).
func (s *upstreamStub) acceptModelResult(requestID string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.models[requestID]
	if !ok {
		return
	}
	if s.modelWindow > 0 && !cmd.deliveredAt.IsZero() && time.Since(cmd.deliveredAt) > s.modelWindow {
		return
	}
	cmd.body = append([]byte(nil), body...)
	cmd.bodyWrites++
	cmd.completed = true
}

// modelCommandState reports the delivery and completion faces of one request.
func (s *upstreamStub) modelCommandState(requestID string) (delivered, completed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.models[requestID]
	if !ok {
		return false, false
	}
	return cmd.delivered, cmd.completed
}

// modelCommandVerdict reads the server's request record back: "completed" once
// a result landed, "timeout" once the window closed without one, "pending"
// while it is still open ("" for an unknown request).
func (s *upstreamStub) modelCommandVerdict(requestID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.models[requestID]
	if !ok {
		return ""
	}
	switch {
	case cmd.completed:
		return "completed"
	case cmd.delivered && s.modelWindow > 0 && time.Since(cmd.deliveredAt) > s.modelWindow:
		return "timeout"
	default:
		return "pending"
	}
}

// modelCommandRecord reads back what the server's record holds and how many C21
// forwards it adopted.
func (s *upstreamStub) modelCommandRecord(requestID string) ([]byte, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.models[requestID]
	if !ok {
		return nil, 0
	}
	return append([]byte(nil), cmd.body...), cmd.bodyWrites
}

// setModelWindow programs the server's request window (zero leaves it
// unmodelled).
func (s *upstreamStub) setModelWindow(window time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelWindow = window
}

// markModelDelivered plays the server handing one request out through the
// runtime's ack — the step a heartbeat-stream scenario gets for free, and one a
// scenario driving OnAck directly has to state.
func (s *upstreamStub) markModelDelivered(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cmd, ok := s.models[requestID]; ok {
		cmd.delivered = true
		cmd.deliveredAt = time.Now()
	}
}

// programDroppedModelResponse programs the next C21 answer for one request to
// vanish on the wire.
func (s *upstreamStub) programDroppedModelResponse(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cmd, ok := s.models[requestID]; ok {
		cmd.dropNextResponse = true
	}
}

// takeDroppedModelResponse consumes the programmed lost answer.
func (s *upstreamStub) takeDroppedModelResponse(requestID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.models[requestID]
	if !ok || !cmd.dropNextResponse {
		return false
	}
	cmd.dropNextResponse = false
	return true
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

	createdJobs     []*batchv1.Job
	createdSecrets  []*corev1.Secret
	deletedJobs     []string
	deletedSecrets  []string
	getJobs         []string
	createJobErr    error
	createSecretErr error
	// createCarrierErr refuses only the pending-report carrier writes, leaving
	// the credential Secret face working.
	createCarrierErr error
	// deleteSecretErr makes the next Secret deletion fail (the reclaim's
	// retry face): the entry must survive as the retry handle.
	deleteSecretErr error
	// onCreateJob runs before a create is answered (scenario setup hooks).
	onCreateJob func(job *batchv1.Job)

	// deleteJobErr makes the next Job deletion fail: a terminating
	// predecessor keeps its name without the apiserver having removed it.
	deleteJobErr error
	// nameHeld makes CreateJob answer AlreadyExists for a name, as the
	// apiserver does while a predecessor still holds it.
	nameHeld map[string]bool

	existingJobs map[string]*batchv1.Job
	jobs         []batchv1.Job
	secrets      []corev1.Secret
	pods         []corev1.Pod

	// listSecretsErr makes the Secret list face fail — the carrier list
	// window a restart scenario has to survive (§4.3).
	listSecretsErr error
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
	if c.nameHeld[job.Name] {
		return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "jobs"}, job.Name)
	}
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
	if c.deleteJobErr != nil {
		return c.deleteJobErr
	}
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

// setDeleteSecretError programs the next Secret deletion to fail (nil clears
// it): the reclaim's retry face.
func (c *jobClient) setDeleteSecretError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteSecretErr = err
}

// setDeleteJobError programs Job deletion to fail (nil clears it): a
// terminating predecessor that keeps its name.
func (c *jobClient) setDeleteJobError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteJobErr = err
}

// setNameHeld makes CreateJob answer AlreadyExists for name (false clears it),
// as the apiserver does while a predecessor holds the name.
func (c *jobClient) setNameHeld(name string, held bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nameHeld == nil {
		c.nameHeld = map[string]bool{}
	}
	c.nameHeld[name] = held
}

func (c *jobClient) GetJob(_ context.Context, name string) (*batchv1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getJobs = append(c.getJobs, name)
	job, ok := c.existingJobs[name]
	if !ok {
		return nil, nil
	}
	return job.DeepCopy(), nil
}

func (c *jobClient) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// The injected failure models the apiserver refusing the claim chain's
	// credential Secret; the pending-report carrier is the queue's durability
	// face, not an object of that chain, so it keeps working.
	if c.createSecretErr != nil && secret.Labels[registry.LabelRole] != registry.RolePendingReport {
		return c.createSecretErr
	}
	// The carrier is the queue's durability face (§4.6): its write can fail on
	// its own while the credential Secrets stay writable, which is the window
	// the unbacked-entry scenario judges.
	if c.createCarrierErr != nil && secret.Labels[registry.LabelRole] == registry.RolePendingReport {
		return c.createCarrierErr
	}
	c.createdSecrets = append(c.createdSecrets, secret)
	// The list face sees the object too, exactly as the apiserver's does: a
	// carrier written before a restart is what the rebuilt queue lists.
	c.secrets = append(c.secrets, *secret.DeepCopy())
	return nil
}

func (c *jobClient) DeleteSecret(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deleteSecretErr != nil {
		return c.deleteSecretErr
	}
	c.deletedSecrets = append(c.deletedSecrets, name)
	// As on the Job side: a deleted object leaves the list face, or a sweep that
	// reads the list again would find what it just removed.
	kept := c.secrets[:0]
	for _, secret := range c.secrets {
		if secret.Name != name {
			kept = append(kept, secret)
		}
	}
	c.secrets = kept
	return nil
}

func (c *jobClient) ListJobs(context.Context) ([]batchv1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]batchv1.Job(nil), c.jobs...), nil
}

// ListJobsByTask is the orphan sweep's per-task presence face: the label pair
// identifies the task's Job, never a name composed from the task id.
func (c *jobClient) ListJobsByTask(_ context.Context, workspaceID, taskID string) ([]batchv1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []batchv1.Job
	for _, job := range c.jobs {
		if job.Labels[registry.LabelTaskID] != taskID {
			continue
		}
		if job.Labels[registry.LabelWorkspaceID] != workspaceID {
			continue
		}
		out = append(out, job)
	}
	return out, nil
}

func (c *jobClient) ListSecrets(context.Context) ([]corev1.Secret, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listSecretsErr != nil {
		return nil, c.listSecretsErr
	}
	return append([]corev1.Secret(nil), c.secrets...), nil
}

// setListSecretsErr programs the Secret list face (nil restores it).
func (c *jobClient) setListSecretsErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listSecretsErr = err
}

// setCreateCarrierErr programs the pending-report carrier write (nil restores
// it): the queue's durability face fails while the credential Secrets stay
// writable.
func (c *jobClient) setCreateCarrierErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createCarrierErr = err
}

// carrierSecrets lists the pending-report carriers on the cluster face.
func (c *jobClient) carrierSecrets() []corev1.Secret {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []corev1.Secret
	for _, secret := range c.secrets {
		if secret.Labels[registry.LabelRole] == registry.RolePendingReport {
			out = append(out, secret)
		}
	}
	return out
}

func (c *jobClient) ListPods(context.Context, string) ([]corev1.Pod, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]corev1.Pod(nil), c.pods...), nil
}

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
// jobCreatedCount counts how many create calls targeted name.
func (c *jobClient) jobCreatedCount(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, job := range c.createdJobs {
		if job.Name == name {
			n++
		}
	}
	return n
}

// deletedCount counts how many delete calls targeted name.
func (c *jobClient) deletedCount(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, deleted := range c.deletedJobs {
		if deleted == name {
			n++
		}
	}
	return n
}

// liveJobs counts the Job objects currently held.
func (c *jobClient) liveJobs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.jobs)
}

// prepareLeaseCalls counts the C-4 prepare-lease renewals the stub received.
func (s *upstreamStub) prepareLeaseCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, req := range s.requests {
		if req.Method == "POST" && strings.HasSuffix(req.Path, "/prepare-lease") {
			n++
		}
	}
	return n
}

// queuedCount reports the stub server's queue length.
func (s *upstreamStub) queuedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queued)
}

// heartbeatRuntimeIDs counts the heartbeat requests per runtime id.
func (s *upstreamStub) heartbeatRuntimeIDs() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for _, req := range s.requests {
		if req.Method != "POST" || req.Path != "/api/daemon/heartbeat" {
			continue
		}
		var body struct {
			RuntimeID string `json:"runtime_id"`
		}
		if json.Unmarshal(req.Body, &body) == nil && body.RuntimeID != "" {
			out[body.RuntimeID]++
		}
	}
	return out
}

// liveJobCount counts the Job objects currently held for name.
func (c *jobClient) liveJobCount(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, job := range c.jobs {
		if job.Name == name {
			n++
		}
	}
	return n
}

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

// hasSecret reports whether the Secret still exists on the live list face.
func (c *jobClient) hasSecret(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, secret := range c.secrets {
		if secret.Name == name {
			return true
		}
	}
	return false
}

// secretDeleted reports whether the Secret object was deleted.
func (c *jobClient) secretDeleted(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, deleted := range c.deletedSecrets {
		if deleted == name {
			return true
		}
	}
	return false
}

// deletedSecretNames snapshots the Secret deletions in order.
func (c *jobClient) deletedSecretNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deletedSecrets...)
}

// deletedJobNames snapshots the Job deletions in order.
func (c *jobClient) deletedJobNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deletedJobs...)
}

// programListed stages the list faces (ListJobs/ListSecrets): the remnant forms
// a sweep scenario needs, which the fake client cannot reach by creating
// objects alone.
func (c *jobClient) programListed(jobs []batchv1.Job, secrets []corev1.Secret) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobs = jobs
	c.secrets = secrets
}

// programGetJob stages the GetJob face: the sweep's "does this Job still
// exist" question answers from here.
func (c *jobClient) programGetJob(job *batchv1.Job) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.existingJobs[job.Name] = job
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
// Job Tokens with the real issuer — the task Job's credential and the probe's
// alike — so probe requests authenticate against the real auth module and a
// task entry records the digest the daemon face adjudicates (ADR-017).
type builder struct {
	issuer   *auth.Issuer
	probes   *registry.ProbeRegistry
	probeErr error

	mu     sync.Mutex
	tokens map[string]string // Job name -> issued Job Token
}

// Build renders the task's objects and issues its Job Token, returning the
// digest the entry carries. It mirrors the production builder's credential
// behaviour (ADR-017's token_hash write is OnClaim's, from this return).
func (b *builder) Build(e jobbuilder.TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	name := "fm-" + e.TaskID
	token, err := b.issuer.Issue(name, e.TaskID, e.WorkspaceID, auth.DefaultJobTokenTTL)
	if err != nil {
		return nil, nil, "", err
	}
	// The canned object carries the per-task identity keys the K8s rebuild
	// reads (§3 判定式), so a scenario judging the rebuild exercises the same
	// object shape the production builder writes.
	labels := map[string]string{
		registry.LabelManagedBy:   registry.ManagedByForeman,
		registry.LabelTaskID:      e.TaskID,
		registry.LabelWorkspaceID: e.WorkspaceID,
	}
	if e.AgentID != "" {
		labels[registry.LabelAgentID] = e.AgentID
	}
	annos := map[string]string{
		registry.AnnoDaemonID:     "fm-" + e.TaskID,
		registry.AnnoJobRuntimeID: e.JobRuntimeID,
		registry.AnnoAttempt:      strconv.Itoa(e.Attempt),
	}
	if !e.ClaimedAt.IsZero() {
		annos[registry.AnnoClaimedAt] = e.ClaimedAt.UTC().Format(time.RFC3339)
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: annos}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-cred"}}
	b.mu.Lock()
	if b.tokens == nil {
		b.tokens = map[string]string{}
	}
	b.tokens[name] = token
	b.mu.Unlock()
	return job, secret, b.issuer.Hash(token), nil
}

// ResolveName mirrors the fallback name the canned builder renders.
func (b *builder) ResolveName(e jobbuilder.TaskEntry) (string, error) { return "fm-" + e.TaskID, nil }

// RequiresProjectIdentity keeps the canned builder's claims out of the
// platform lookup; the real builder answers from the policy table.
func (b *builder) RequiresProjectIdentity(jobbuilder.TaskEntry) bool { return false }

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

// probeToken returns the Job Token issued for a Job (task or probe).
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

// ResolveName forwards the production name composition, so a scenario can see
// the display name the template expresses without building the objects.
func (b realBuilder) ResolveName(e jobbuilder.TaskEntry) (string, error) {
	return b.inner.ResolveName(e)
}

// RequiresProjectIdentity forwards the policy table's answer.
func (b realBuilder) RequiresProjectIdentity(e jobbuilder.TaskEntry) bool {
	return b.inner.RequiresProjectIdentity(e)
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
	h.useRealBuilderWithPolicy(t, templatePath, "", nil)
}

// projectIDFor is the fixture platform's project identifier for one policy
// project name: opaque-looking, and deliberately different from the name, so a
// runtime match through the name is a detectible failure (ADR-048 decision 3).
func projectIDFor(name string) string { return "pid-" + name }

// projectNameResolver is the fixture platform's policy name resolution: a
// project key resolves to its identifier, an agent key reports the documented
// unavailable conclusion (the workspace has no visibility-independent
// agent-name query — docs/upstream-anchors.md). Unavailable only warns, so a
// boot with this resolver proceeds for both sections.
type projectNameResolver struct{}

func (projectNameResolver) ResolveName(_ context.Context, _, kind, name string) (jobbuilder.NameResolution, error) {
	if kind != jobbuilder.PolicyKeyProject {
		return jobbuilder.NameResolution{Conclusion: jobbuilder.NameUnavailable}, nil
	}
	return jobbuilder.NameResolution{Conclusion: jobbuilder.NameResolved, Identifier: projectIDFor(name)}, nil
}

// payloadProjectIdentity is the fixture's claim-time project seam: it reads
// the payload's top-level project_id and performs no reverse lookup. A
// scenario that judges the lookup installs the production adapter through
// rebuildScheduler.
type payloadProjectIdentity struct{}

func (payloadProjectIdentity) FromPayload(task json.RawMessage) string {
	var head struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(task, &head); err != nil {
		return ""
	}
	return strings.TrimSpace(head.ProjectID)
}

func (payloadProjectIdentity) ByIssue(context.Context, string, string) (string, error) {
	return "", nil
}

// countingProjectIdentity is payloadProjectIdentity with a reverse-lookup
// counter, so "the lookup did not happen" is an assertion.
type countingProjectIdentity struct {
	mu    sync.Mutex
	calls []string
}

func (c *countingProjectIdentity) FromPayload(task json.RawMessage) string {
	var head struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(task, &head); err != nil {
		return ""
	}
	return strings.TrimSpace(head.ProjectID)
}

func (c *countingProjectIdentity) ByIssue(_ context.Context, _, issueID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, issueID)
	return "", nil
}

func (c *countingProjectIdentity) lookupCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

// useRealBuilderWithPolicy is useRealBuilderWithTemplate plus the policy file
// (02-contracts.md §8.5): an empty policyPath leaves the field at
// the mounted path, which does not exist under test — the legal default of an
// absent file (three axes None, warn). mutate receives the loaded config, so a
// scenario can inject the platform name-resolution seam or any other knob.
func (h *harness) useRealBuilderWithPolicy(t *testing.T, templatePath, policyPath string, mutate func(*jobbuilder.Config)) {
	t.Helper()
	jbCfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("jobbuilder.LoadConfig: %v", err)
	}
	jbCfg.TemplatePath = templatePath
	if policyPath != "" {
		jbCfg.PolicyPath = policyPath
	}
	jbCfg.JobNamespace = "multica-agents"
	jbCfg.JobImage = harnessJobImage
	if h.jobImage != "" {
		jbCfg.JobImage = h.jobImage
	}
	jbCfg.Issuer = h.issuer
	jbCfg.Nodes = h.reg
	jbCfg.Metrics = h.metrics
	jbCfg.Logger = slog.New(slog.NewJSONHandler(h.logs, nil))
	// The fixture platform answers the startup name resolution with one
	// identifier per configured project name; a scenario that judges the
	// resolution installs its own resolver through mutate.
	jbCfg.NameResolver = projectNameResolver{}
	if mutate != nil {
		mutate(&jbCfg)
	}
	inner, err := jobbuilder.NewBuilder(jbCfg)
	if err != nil {
		t.Fatalf("jobbuilder.NewBuilder: %v", err)
	}
	h.builder = realBuilder{inner: inner, jobs: h.jobs}
	h.rebuildScheduler(t, func(*scheduler.Config) {})
}

// writePolicyFile writes a policy.yaml where the builder reads it.
func writePolicyFile(t *testing.T, policy string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	return path
}

// lateProbeRunner breaks the client/scheduler construction cycle: the client
// needs the §1.6 discovery-probe runner, and the scheduler needs the client as
// its server seam.
type lateProbeRunner struct {
	sched *scheduler.Scheduler
}

func (l *lateProbeRunner) RunDiscoveryProbe(ctx context.Context, workspaceID string) (registry.Discovery, error) {
	if l.sched == nil {
		return registry.Discovery{}, errors.New("scheduler not wired")
	}
	return l.sched.RunDiscoveryProbe(ctx, workspaceID)
}

// schedulerServerClient is the scheduler's server seam.
type schedulerServerClient struct {
	mu       sync.Mutex
	statuses map[string]string
	forwards []upstreamRequest
	// forwardCode programs the C5–C12 answer (0 = 200); accepted counts the
	// writes the stub took (2xx) per endpoint, so a scenario can judge
	// "exactly once".
	forwardCode int
	accepted    map[scheduler.Endpoint]int
	// appliedTerminal is the first accepted terminal callback per task. The
	// server applies that one and ignores later callbacks for the same task
	// (§4.4: the terminal verdict is irreversible); a scenario judging "one
	// write" reads this set, not the wire-call count, because a daemon retry
	// is a legitimate second call that carries the same decided body.
	appliedTerminal map[string]upstreamRequest
	// onStatus fires after the C13 answer is read and the client lock is
	// released, before the answer is returned. It lets a scenario land a
	// transition inside the round trip (an S8 start whose MarkStarted must
	// beat the caller's next deletion decision).
	onStatus func()
}

func (c *schedulerServerClient) TaskStatus(_ context.Context, taskID string) (string, error) {
	c.mu.Lock()
	status, err := c.statusAnswerLocked(taskID)
	hook := c.onStatus
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return status, err
}

func (c *schedulerServerClient) statusAnswerLocked(taskID string) (string, error) {
	if c.statuses == nil {
		return "dispatched", nil
	}
	status, ok := c.statuses[taskID]
	if !ok {
		return "", scheduler.ErrTaskNotFound
	}
	return status, nil
}

// setStatusHook programs the post-answer hook (nil clears it).
func (c *schedulerServerClient) setStatusHook(h func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onStatus = h
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
	code := c.forwardCode
	if code == 0 {
		code = 200
	}
	if code >= 200 && code < 300 {
		if c.accepted == nil {
			c.accepted = map[scheduler.Endpoint]int{}
		}
		c.accepted[ep]++
		if ep.IsTerminal() {
			if c.appliedTerminal == nil {
				c.appliedTerminal = map[string]upstreamRequest{}
			}
			if _, applied := c.appliedTerminal[taskID]; !applied {
				c.appliedTerminal[taskID] = upstreamRequest{
					Method: string(ep), Path: taskID, Body: append([]byte(nil), body...),
				}
			}
		}
	}
	return code, []byte(`{"status":"ok"}`), nil
}

// ForwardInWorkspace is the single-workspace fixture's routing face: one
// credential serves every task, so the persisted workspace only names it.
func (c *schedulerServerClient) ForwardInWorkspace(ctx context.Context, ep scheduler.Endpoint, taskID, _ string, body []byte) (int, []byte, error) {
	return c.Forward(ctx, ep, taskID, body)
}

// appliedTerminalWrites returns the terminal write the stub applied for the
// task: the first accepted callback, later ones being the server's idempotent
// no-ops.
func (c *schedulerServerClient) appliedTerminalWrites(taskID string) []upstreamRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	write, ok := c.appliedTerminal[taskID]
	if !ok {
		return nil
	}
	return []upstreamRequest{write}
}

// setForwardCode programs the C5–C12 answer (0 restores 200).
func (c *schedulerServerClient) setForwardCode(code int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forwardCode = code
}

// acceptedFor counts the writes the stub took (2xx) for ep.
func (c *schedulerServerClient) acceptedFor(ep scheduler.Endpoint) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accepted[ep]
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

// metricForward is one forwarded lifecycle call: the endpoint and the
// status the upstream answered with (the two label values of
// foreman_forward_total).
type metricForward struct {
	endpoint string
	code     int
}

// tokenUsageRecord is one usage-report entry as the scheduler seam sees it
// (the two label values of foreman_token_usage_total plus the four token
// counters).
type tokenUsageRecord struct {
	provider   string
	model      string
	input      int64
	output     int64
	cacheRead  int64
	cacheWrite int64
}

// recordingMetrics implements proxy.Metrics and the scheduler's metric seam,
// recording the catalog outcomes, the daemon face's auth failures, the S6
// delivery face, the skill-cache resolve faces and the task-driven §9.2
// families the observability scenarios judge.
type recordingMetrics struct {
	mu                sync.Mutex
	catalogCodes      []string
	catalogSecs       []float64
	deliveryKinds     []string
	deliveryLatencies []float64
	unconfirmed       int
	authReasons       []string
	duplicates        int
	skillResults      []string
	skillEvicts       []string
	skillEntries      int
	skillBytes        int64
	profileResults    []string
	workspaceResults  []string
	taskResults       []string
	policyHits        map[string]int
	nameConclusions   []string
	pendingReports    int
	taskClaims        int
	claimErrors       []string
	jobCreateSamples  []float64
	jobBootSamples    []float64
	inflight          []int
	credReclaims      map[string]int
	drainSamples      []float64
	jobDeleteFailed   []string
	forwards          []metricForward
	tokenUsage        []tokenUsageRecord
	terminalResults   []string
	wsConnected       map[string]int
	wsReconnects      map[string]int
}

// Forward records one lifecycle forward by endpoint and status code
// (foreman_forward_total / foreman_forward_latency_seconds).
func (m *recordingMetrics) Forward(endpoint string, code int, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forwards = append(m.forwards, metricForward{endpoint: endpoint, code: code})
}

// ClaimError records one failed claim cycle by status or reason.
func (m *recordingMetrics) ClaimError(code string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimErrors = append(m.claimErrors, code)
}

// TokenUsage records one usage-report entry by provider and model
// (foreman_token_usage_total).
func (m *recordingMetrics) TokenUsage(provider, model string, input, output, cacheRead, cacheWrite int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokenUsage = append(m.tokenUsage, tokenUsageRecord{
		provider: provider, model: model,
		input: input, output: output, cacheRead: cacheRead, cacheWrite: cacheWrite,
	})
}

// WSConnect/WSDisconnect track the foreman_ws_connected gauge by side;
// WSReconnect counts foreman_ws_reconnects_total.
func (m *recordingMetrics) WSConnect(side string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wsConnected == nil {
		m.wsConnected = map[string]int{}
	}
	m.wsConnected[side]++
}

func (m *recordingMetrics) WSDisconnect(side string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wsConnected == nil {
		m.wsConnected = map[string]int{}
	}
	m.wsConnected[side]--
}

func (m *recordingMetrics) WSReconnect(side string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wsReconnects == nil {
		m.wsReconnects = map[string]int{}
	}
	m.wsReconnects[side]++
}

// wsConnectedCount is the gauge reading for one side.
func (m *recordingMetrics) wsConnectedCount(side string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wsConnected[side]
}

// wsReconnectCount is the reconnect counter for one side.
func (m *recordingMetrics) wsReconnectCount(side string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wsReconnects[side]
}

// AuthFailure records the reason the daemon face rejected a request
// (foreman_auth_failures_total{reason}).
func (m *recordingMetrics) AuthFailure(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.authReasons = append(m.authReasons, reason)
}

// The scheduler's own seam. Recording is passive: the scheduler must never
// change behaviour because a metric changed.
func (m *recordingMetrics) TaskClaimed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskClaims++
}

func (m *recordingMetrics) JobBootSeconds(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobBootSamples = append(m.jobBootSamples, seconds)
}

func (m *recordingMetrics) JobCreateSeconds(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobCreateSamples = append(m.jobCreateSamples, seconds)
}

func (m *recordingMetrics) InflightJobs(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inflight = append(m.inflight, n)
}

func (m *recordingMetrics) CredReclaimed(reason string, drainSeconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.credReclaims == nil {
		m.credReclaims = map[string]int{}
	}
	m.credReclaims[reason]++
	m.drainSamples = append(m.drainSamples, drainSeconds)
}

func (m *recordingMetrics) JobDeleteFailed(stage string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobDeleteFailed = append(m.jobDeleteFailed, stage)
}

// TaskTerminal records the result label of every counted terminal transition
// (foreman_tasks_total{result}).
func (m *recordingMetrics) TaskTerminal(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskResults = append(m.taskResults, result)
	m.terminalResults = append(m.terminalResults, result)
}

// terminalCount is the number of counted terminal transitions with result.
func (m *recordingMetrics) terminalCount(result string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, got := range m.terminalResults {
		if got == result {
			n++
		}
	}
	return n
}

// DuplicateDispatch counts a server redispatch of a known task
// (foreman_duplicate_dispatch_total).
func (m *recordingMetrics) DuplicateDispatch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.duplicates++
}

// PendingReports records the terminal-report queue depth (the gauge the
// recovery loop keeps current).
func (m *recordingMetrics) PendingReports(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingReports = n
}

func (m *recordingMetrics) pendingReportsDepth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pendingReports
}

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

func (m *recordingMetrics) DeliveryLatency(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveryLatencies = append(m.deliveryLatencies, seconds)
}

func (m *recordingMetrics) QueueOp(string)                  {}
func (m *recordingMetrics) QueueDepth(int)                  {}
func (m *recordingMetrics) SharedSlots(string, int)         {}
func (m *recordingMetrics) TaskLockWait(float64)            {}
func (m *recordingMetrics) ReconcileResult(string, float64) {}
func (m *recordingMetrics) StateError(string)               {}

func (m *recordingMetrics) DeliveryUnconfirmed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unconfirmed++
}

// SkillCache records one resolve face (§9.2).
func (m *recordingMetrics) SkillCache(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skillResults = append(m.skillResults, result)
}

// SkillCacheSize records the store's current size (§9.2).
func (m *recordingMetrics) SkillCacheSize(entries int, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skillEntries = entries
	m.skillBytes = bytes
}

// SkillCacheEviction records one reclaimed entry by reason (§9.2).
func (m *recordingMetrics) SkillCacheEviction(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skillEvicts = append(m.skillEvicts, reason)
}

// RuntimeProfileRefresh records one §1.6 refresh outcome (§9.2).
func (m *recordingMetrics) RuntimeProfileRefresh(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.profileResults = append(m.profileResults, result)
}

// WorkspaceStateRefresh records one C-22 outcome (§9.2).
func (m *recordingMetrics) WorkspaceStateRefresh(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workspaceResults = append(m.workspaceResults, result)
}

// PolicyHit records one policy axis evaluation by axis and source (§9.2).
func (m *recordingMetrics) PolicyHit(axis, source string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.policyHits == nil {
		m.policyHits = map[string]int{}
	}
	m.policyHits[axis+"/"+source]++
}

// PolicyNameResolution records one startup key's conclusion (§9.2).
func (m *recordingMetrics) PolicyNameResolution(conclusion string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nameConclusions = append(m.nameConclusions, conclusion)
}

// policyHitCount counts the axis evaluations recorded under one axis/source.
func (m *recordingMetrics) policyHitCount(axis, source string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.policyHits[axis+"/"+source]
}

// nameResolutionCount counts the startup conclusions recorded under one value.
func (m *recordingMetrics) nameResolutionCount(conclusion string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, recorded := range m.nameConclusions {
		if recorded == conclusion {
			n++
		}
	}
	return n
}

// taskResultCount counts the terminal results recorded under one value.
func (m *recordingMetrics) taskResultCount(result string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, recorded := range m.taskResults {
		if recorded == result {
			n++
		}
	}
	return n
}

// skillResultCount counts the recorded resolve faces per result.
func (m *recordingMetrics) skillResultCount(result string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, recorded := range m.skillResults {
		if recorded == result {
			n++
		}
	}
	return n
}

// skillEvictionCount counts the recorded reclaims per reason.
func (m *recordingMetrics) skillEvictionCount(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, recorded := range m.skillEvicts {
		if recorded == reason {
			n++
		}
	}
	return n
}

// skillSize is the last size the store published.
func (m *recordingMetrics) skillSize() (entries int, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.skillEntries, m.skillBytes
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

// deliveryLatencySamples returns the recorded S6 delivery spans in seconds,
// in observation order.
func (m *recordingMetrics) deliveryLatencySamples() []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]float64(nil), m.deliveryLatencies...)
}

// deliveryUnconfirmedCount reports how many entries the confirmation window
// judged undelivered.
func (m *recordingMetrics) deliveryUnconfirmedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.unconfirmed
}

// duplicateDispatchCount reports how many redispatches of a known task the
// scheduler counted.
func (m *recordingMetrics) duplicateDispatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.duplicates
}

// authFailureCount counts the daemon-face rejections recorded under one reason.
func (m *recordingMetrics) authFailureCount(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, recorded := range m.authReasons {
		if recorded == reason {
			n++
		}
	}
	return n
}

// authFailureTotal counts every daemon-face rejection
// (foreman_auth_failures_total across reasons).
func (m *recordingMetrics) authFailureTotal() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.authReasons)
}

// taskClaimCount counts the claims the scheduler accepted
// (foreman_task_claims_total).
func (m *recordingMetrics) taskClaimCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taskClaims
}

// claimErrorCount counts the failed claim cycles
// (foreman_claim_errors_total).
func (m *recordingMetrics) claimErrorCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.claimErrors)
}

// jobCreateSampleCount and jobBootSampleCount count the latency
// observations of the two histograms (foreman_job_create_seconds /
// foreman_job_boot_seconds).
func (m *recordingMetrics) jobCreateSampleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.jobCreateSamples)
}

func (m *recordingMetrics) jobBootSampleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.jobBootSamples)
}

// inflightLast is the current in-flight gauge reading
// (foreman_inflight_jobs).
func (m *recordingMetrics) inflightLast() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.inflight) == 0 {
		return -1
	}
	return m.inflight[len(m.inflight)-1]
}

// inflightSaw reports whether the gauge ever read n.
func (m *recordingMetrics) inflightSaw(n int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, got := range m.inflight {
		if got == n {
			return true
		}
	}
	return false
}

// credReclaimCount counts the credential reclaims recorded under one reason
// (foreman_cred_reclaim_total).
func (m *recordingMetrics) credReclaimCount(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.credReclaims[reason]
}

// drainSampleCount counts the drain-duration observations
// (foreman_job_drain_seconds).
func (m *recordingMetrics) drainSampleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.drainSamples)
}

// jobDeleteFailureCount counts the Job deletions that failed with no retry
// handle (foreman_job_delete_failed_total).
func (m *recordingMetrics) jobDeleteFailureCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.jobDeleteFailed)
}

// forwardCount counts the forwards recorded under one endpoint and status
// code (foreman_forward_total).
func (m *recordingMetrics) forwardCount(endpoint string, code int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, call := range m.forwards {
		if call.endpoint == endpoint && call.code == code {
			n++
		}
	}
	return n
}

// tokenUsageFor sums the usage-report entries recorded under one
// provider/model label pair (foreman_token_usage_total).
func (m *recordingMetrics) tokenUsageFor(provider, model string) (input, output, cacheRead, cacheWrite int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, got := range m.tokenUsage {
		if got.provider == provider && got.model == model {
			input += got.input
			output += got.output
			cacheRead += got.cacheRead
			cacheWrite += got.cacheWrite
		}
	}
	return input, output, cacheRead, cacheWrite
}

// tokenUsageSnapshot returns the recorded usage entries under the lock.
func (m *recordingMetrics) tokenUsageSnapshot() []tokenUsageRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]tokenUsageRecord(nil), m.tokenUsage...)
}

// probeBuilder is the jobbuilder seam plus the test-only credential reader.
type probeBuilder interface {
	scheduler.JobBuilder
	probeToken(jobName string) string
}

// syncBuffer is the log sink a scenario reads while the production loops log
// into it from their own goroutines (claim loop, lease keeper, probe server):
// a plain bytes.Buffer would be read while it is written.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// Bytes snapshots the lines written so far.
func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// String snapshots the lines written so far.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// harness wires the real components against the stubs.
type harness struct {
	t       *testing.T
	cfg     proxy.Config
	reg     *registry.Registry
	probes  *registry.ProbeRegistry
	startup *registry.StartupCatalog
	slots   *proxy.CatalogSlots
	jobs    *jobClient
	builder probeBuilder
	server  *schedulerServerClient
	// serverClient is the seam the scheduler forwards through. It is the stub
	// above by default; a scenario that judges the production forward metric
	// wires the real proxy client instead.
	serverClient scheduler.ServerClient
	metrics      *recordingMetrics
	issuer       *auth.Issuer
	client       *proxy.Client
	sched        *scheduler.Scheduler
	pending      *scheduler.PendingReports
	upstream     *upstreamStub
	handler      http.Handler
	jobsHTTP     *httptest.Server
	logs         *syncBuffer
	// handlerLogs captures the fake server's own event lines (the job-facing
	// face), separate from the scheduler's.
	handlerLogs *syncBuffer
	// bundles is the S26 resolve cache the fake server answers from; skillClock
	// is its clock seam, which a scenario steps to age entries.
	bundles    *proxy.SkillCache
	skillClock *steppedClock
	// schedClock is the scheduler's one clock seam: buildHarnessWith seeds it
	// from harnessOptions.clock, and rebuildScheduler carries it forward. The
	// delivery-latency scenarios inject a fixed clock here to sample the
	// claim -> commit span.
	schedClock func() time.Time

	// jobImage overrides the reference template's <job-image> value: a
	// scenario that asserts the deployment's own reference sets it before
	// swapping in the real builder.
	jobImage string

	schedCfg scheduler.Config
	// Stage-two coordination face (ADR-046 决策 1-9): the shared store, this
	// instance's identity and the leader lease a scenario drives explicitly.
	coord      state.CoordStore
	instanceID string
	leaderCfg  coordination.Config
	// leaderMu guards the lease handles: the claim and heartbeat gates read
	// them from their own goroutines while a scenario starts, crashes or
	// stops the lease from the test goroutine.
	leaderMu   sync.Mutex
	leader     *coordination.Leader
	leaderStop context.CancelFunc
	leaderDone chan struct{}
	// revoker is the scheduler's credential-revocation seam. Nil leaves it
	// inert (noRevoker): a scenario that judges the revocation set wires the
	// shared issuer here and rebuilds the scheduler, as a deployment wires it.
	revoker scheduler.Revoker
}

// Delivery confirmation window and lease cadence the scenarios run with: the
// window is short enough for a bounded wait in a test, and the keeper cadence
// sits far below it (production defaults are 30s / 15s).
const (
	deliveryConfirmWindow = 300 * time.Millisecond
	leaseKeeperInterval   = 20 * time.Millisecond
)

// Recheck window the recovery scenarios run with. The window is a contract
// constant (10s, §4.6) that the reconciler takes from recovery.Config, so a
// scenario injects a short one and waits a few of it: the window's semantics
// run on the real reconciler, the suite does not pay the contract's wall
// clock.
const recheckWindow = 100 * time.Millisecond

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
	return newHarnessWithSkill(t, catalogEnabled, window, nil)
}

// Skill-bundle cache windows and bounds the scenarios run with: short enough
// that one clock step crosses them, and shaped like a deployment's knobs.
const (
	skillFreshTTL        = 10 * time.Minute
	skillDowngradeWindow = time.Hour
	skillRetention       = 2 * time.Hour
	skillMaxEntries      = 4
	skillMaxBytes        = 1 << 20
)

// newHarnessWithSkill is newHarnessWithWindow with the resolve cache's §1.8
// configuration chosen by the scenario (nil keeps the defaults above): a
// capacity or window scenario needs its own bounds, and the cache captures them
// at construction.
func newHarnessWithSkill(t *testing.T, catalogEnabled bool, window time.Duration, skill func(*proxy.Config)) *harness {
	t.Helper()
	return newHarnessWithOptions(t, catalogEnabled, window, skill, false)
}

// newHarnessWithRealServer is newHarness with the production proxy client as
// the scheduler's server seam instead of the recording stub. A scenario that
// judges the forward metric must drive the client that records it, not a
// stand-in for it.
func newHarnessWithRealServer(t *testing.T, catalogEnabled bool) *harness {
	t.Helper()
	return newHarnessWithOptions(t, catalogEnabled, deliveryConfirmWindow, nil, true)
}

// newHarnessWithWS is newHarness with FOREMAN_WS_ENABLED on: the Job-side
// S-30 route is mounted and the config says so, so a scenario can drive the
// subscriber and the daemon handshake against the same fixture.
func newHarnessWithWS(t *testing.T, catalogEnabled bool) *harness {
	t.Helper()
	return buildHarness(t, catalogEnabled, deliveryConfirmWindow, nil, false, true)
}

func newHarnessWithOptions(t *testing.T, catalogEnabled bool, window time.Duration, skill func(*proxy.Config), realServer bool) *harness {
	t.Helper()
	return buildHarness(t, catalogEnabled, window, skill, realServer, false)
}

// newHarnessWithSharedState is newHarness with the stage-one shared state
// face wired into the registry and the issuer (ADR-046): the harness mirrors
// cmd/foreman's wiring, so a scenario can drive the restart-persistence
// semantics against a real store (miniredis) instead of stubs.
func newHarnessWithSharedState(t *testing.T, catalogEnabled bool, store state.Store) *harness {
	t.Helper()
	return buildHarnessWith(t, catalogEnabled, deliveryConfirmWindow, nil, false, false, harnessOptions{store: store})
}

// harnessOptions are the fixture knobs a scenario needs beyond the shared
// shape: extra production seams on the proxy client (the §10 terminal retry
// budget), on the pending queue (the retention bound), and the stage-one
// shared state face (ADR-046). Zero values keep the deployment defaults.
type harnessOptions struct {
	clientOpts  []proxy.Option
	pendingOpts []scheduler.PendingReportsOption
	// store is the shared state face the registry and the issuer persist
	// through; nil keeps the pre-stage-one in-memory shape.
	store state.Store
	// coord is the stage-two coordination face; nil keeps the scheduler on the
	// stage-one in-memory delivery/counter path.
	coord      state.CoordStore
	instanceID string
	// serverURL points this instance's proxy client at another instance's
	// upstream stub: the two-instance fixtures share one server-side queue.
	serverURL string
	// jobs lets a second instance share the first instance's fake cluster.
	jobs *jobClient
	// grace is the shared-state outage window written to the issuer.
	grace time.Duration
	// mutateSched adjusts the scheduler config before construction.
	mutateSched func(*scheduler.Config)
	// clock seeds the harness's scheduler clock (h.schedClock) at construction,
	// the same seam rebuildScheduler carries forward. The fake server is built
	// around this scheduler, so S6 delivery (and the claim it commits) reads
	// this clock, not wall time.
	clock     func() time.Time
	leaderCfg coordination.Config
}

func buildHarness(t *testing.T, catalogEnabled bool, window time.Duration, skill func(*proxy.Config), realServer, wsEnabled bool) *harness {
	t.Helper()
	return buildHarnessWith(t, catalogEnabled, window, skill, realServer, wsEnabled, harnessOptions{})
}

func buildHarnessWith(t *testing.T, catalogEnabled bool, window time.Duration, skill func(*proxy.Config), realServer, wsEnabled bool, opts harnessOptions) *harness {
	upstream := newUpstreamStub(t)
	reg := registry.New(time.Now, registry.WithDeliveryConfirmTimeout(window), registry.WithStateStore(opts.store))
	t.Cleanup(func() { reg.Close(context.Background()) })
	probes := registry.NewProbeRegistry()
	startup := registry.NewStartupCatalog()
	slots := proxy.NewCatalogSlots()
	logs := &syncBuffer{}
	handlerLogs := &syncBuffer{}

	cfg := proxy.Config{
		ServerURL:            upstream.server.URL,
		WorkspaceID:          "ws-1",
		DaemonID:             "foreman",
		Version:              "0.1.0",
		ClaimInterval:        time.Second,
		ClaimIntervalIdle:    time.Second,
		LeaseRefreshInterval: time.Second,
		WSEnabled:            wsEnabled,
		ModelCatalogEnabled:  catalogEnabled,
		ModelCatalogTimeout:  45 * time.Second,
		ModelCatalogCacheTTL: 6 * time.Hour,
		// Bounded C-22 refresh: long enough for a controlled gate, short
		// enough that a failing scenario never stalls the suite.
		WorkspaceStateTimeout: 2 * time.Second,

		SkillCacheFreshTTL:        skillFreshTTL,
		SkillCacheDowngradeWindow: skillDowngradeWindow,
		SkillCacheRetention:       skillRetention,
		SkillCacheMaxEntries:      skillMaxEntries,
		SkillCacheMaxBytes:        skillMaxBytes,
	}
	if opts.serverURL != "" {
		cfg.ServerURL = opts.serverURL
	}
	if skill != nil {
		skill(&cfg)
	}
	probeRunner := &lateProbeRunner{}
	metrics := &recordingMetrics{}
	client := proxy.NewClient(cfg, "mdt_test",
		append([]proxy.Option{
			proxy.WithStartupCatalog(startup),
			proxy.WithCatalogSlots(slots),
			proxy.WithDiscoveryProbe(probeRunner),
			proxy.WithMetrics(metrics),
			proxy.WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
			proxy.WithTaskRuntime(func(taskID string) string {
				entry, ok := reg.Get(taskID)
				if !ok {
					return ""
				}
				return entry.RealRuntimeID
			}),
		}, opts.clientOpts...)...,
	)
	if opts.coord != nil {
		probes.AttachShared(opts.coord)
		slots.AttachShared(opts.coord)
	}
	issuerOpts := []auth.Option{}
	if opts.coord != nil {
		grace := opts.grace
		if grace == 0 {
			grace = 30 * time.Second
		}
		issuerOpts = append(issuerOpts, auth.WithSharedHealth(opts.coord, grace))
	}
	issuer, err := proxy.JobTokenIssuer(testKey, reg, probes, auth.DefaultJobTokenTTL, opts.store, issuerOpts...)
	if err != nil {
		t.Fatalf("JobTokenIssuer: %v", err)
	}
	jobs := opts.jobs
	if jobs == nil {
		jobs = newJobClient()
	}
	bld := &builder{issuer: issuer, probes: probes}
	server := &schedulerServerClient{}
	var serverClient scheduler.ServerClient = server
	if realServer {
		serverClient = client
	}
	// The terminal-report queue is the production one: the write-ahead
	// discipline and the gauge are part of the face the scenarios judge.
	pending, err := scheduler.NewPendingReports(t.TempDir(),
		append([]scheduler.PendingReportsOption{
			scheduler.WithPendingReportsGauge(metrics.PendingReports),
			scheduler.WithPendingReportsCarrier(scheduler.NewSecretCarrierStore(jobs)),
		}, opts.pendingOpts...)...)
	if err != nil {
		t.Fatalf("NewPendingReports: %v", err)
	}
	// Production's startup rebuild restores the queue before any settlement
	// (§4.3); the harness mirrors that so the settlement gates see a completed
	// carrier list.
	if err := pending.Restore(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
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
		Project:                payloadProjectIdentity{},
	}
	if opts.mutateSched != nil {
		opts.mutateSched(&schedCfg)
	}
	h := &harness{coord: opts.coord, instanceID: opts.instanceID, leaderCfg: opts.leaderCfg, schedClock: opts.clock}
	schedOpts := []scheduler.Option{
		scheduler.WithPendingReports(pending),
		scheduler.WithRevoker(noRevoker{}),
		scheduler.WithProbeRegistry(probes),
		scheduler.WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
		scheduler.WithRuntimeIdentity(client.RuntimeIdentity),
	}
	if h.schedClock != nil {
		schedOpts = append(schedOpts, scheduler.WithClock(h.schedClock))
	}
	if opts.coord != nil {
		instanceID := opts.instanceID
		if instanceID == "" {
			instanceID = "instance-1"
		}
		lockTTL := opts.leaderCfg.TaskLockTTL
		if lockTTL == 0 {
			lockTTL = 30 * time.Second
		}
		schedOpts = append(schedOpts,
			scheduler.WithSharedStore(opts.coord, instanceID, lockTTL),
			scheduler.WithLeaderGate(h.leaderHeld),
		)
	}
	sched, err := scheduler.New(schedCfg, reg, jobs, bld, serverClient, metrics, schedOpts...)
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	probeRunner.sched = sched
	// The resolve cache gets its own clock: a scenario ages entries by stepping
	// it instead of waiting the windows out.
	skillClock := &steppedClock{}
	bundles := proxy.NewSkillCache(cfg, client, metrics,
		proxy.WithSkillCacheClock(skillClock.now),
		proxy.WithSkillCacheLogger(slog.New(slog.NewJSONHandler(handlerLogs, nil))))
	// The S-30 route exists only with WS enabled, exactly as in cmd/foreman.
	var wsHandler *proxy.WSHandler
	if wsEnabled {
		wsHandler = proxy.NewWSHandler(issuer, reg, metrics)
	}
	handler := proxy.NewFakeServer(issuer, reg, sched, client, wsHandler, metrics, proxy.WithProbeRegistry(probes),
		proxy.WithSkillBundles(bundles),
		proxy.WithServerLogger(slog.New(slog.NewJSONHandler(handlerLogs, nil)).With("component", "fake-server")))
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	h.t = t
	h.cfg = cfg
	h.reg = reg
	h.probes = probes
	h.startup = startup
	h.slots = slots
	h.jobs = jobs
	h.builder = bld
	h.server = server
	h.serverClient = serverClient
	h.metrics = metrics
	h.issuer = issuer
	h.client = client
	h.sched = sched
	h.pending = pending
	h.upstream = upstream
	h.handler = handler
	h.jobsHTTP = httpServer
	h.logs = logs
	h.handlerLogs = handlerLogs
	h.bundles = bundles
	h.skillClock = skillClock
	h.schedCfg = schedCfg
	return h
}

// rebuildScheduler replaces the scheduler with one built from a mutated
// config; the seams (registry, job client, builder, probe table) stay.
func (h *harness) rebuildScheduler(t *testing.T, mutate func(*scheduler.Config)) {
	t.Helper()
	h.rebuildSchedulerWithLevel(t, slog.LevelInfo, mutate)
}

// rebuildSchedulerWithLevel replaces the scheduler with one whose logger emits
// at level: the job.create_step trace is the debug-only face, so a scenario
// judging it has to run the handler at debug.
func (h *harness) rebuildSchedulerWithLevel(t *testing.T, level slog.Level, mutate func(*scheduler.Config)) {
	t.Helper()
	cfg := h.schedCfg
	mutate(&cfg)
	opts := []scheduler.Option{
		scheduler.WithPendingReports(h.pending),
		scheduler.WithRevoker(credentialRevoker(h.revoker)),
		scheduler.WithProbeRegistry(h.probes),
		scheduler.WithLogger(slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: level}))),
		scheduler.WithRuntimeIdentity(h.client.RuntimeIdentity),
	}
	if h.schedClock != nil {
		opts = append(opts, scheduler.WithClock(h.schedClock))
	}
	if h.coord != nil {
		instanceID := h.instanceID
		if instanceID == "" {
			instanceID = "instance-1"
		}
		lockTTL := h.leaderCfg.TaskLockTTL
		if lockTTL == 0 {
			lockTTL = 30 * time.Second
		}
		opts = append(opts,
			scheduler.WithSharedStore(h.coord, instanceID, lockTTL),
			scheduler.WithLeaderGate(h.leaderHeld),
		)
	}
	sched, err := scheduler.New(cfg, h.reg, h.jobs, h.builder, h.serverClient, h.metrics, opts...)
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
// channel yields the command id of the first probe the daemon serves (later
// probes report without waiting for a reader — a scenario that watches one
// probe is not made to drain every one).
func (h *harness) serveProbeDaemon(t *testing.T, resultBody string) <-chan string {
	t.Helper()
	seen := make(chan string, 1)
	h.jobs.onCreateJob = func(job *batchv1Job) {
		name := job.Name
		go func() {
			command := h.probeTakeCommand(t, name)
			if command == "" {
				return
			}
			select {
			case seen <- command:
			default:
			}
			session, ok := h.probes.ProbeByJob(name)
			if !ok {
				t.Errorf("probe session %s is gone before its result", name)
				return
			}
			code, body, err := h.probeCall("POST",
				"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+command+"/result",
				h.builder.probeToken(name), []byte(resultBody))
			if err != nil || code != 200 {
				t.Errorf("probe S27 = %d: %s (%v)", code, body, err)
			}
		}()
	}
	return seen
}

// probeTakeCommand plays the probe daemon's start-up: S4 registration, then the
// command id the S5 ack hands it. It returns "" when a round trip failed — the
// daemon runs off the test goroutine, so failures are reported with t.Errorf.
func (h *harness) probeTakeCommand(t *testing.T, name string) string {
	t.Helper()
	token := h.builder.probeToken(name)
	if token == "" {
		t.Errorf("probe Job %s has no Job Token", name)
		return ""
	}
	code, body, err := h.probeCall("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "`+name+`",
		"cli_version": "0.6.1",
		"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
	}`))
	if err != nil || code != 200 {
		t.Errorf("probe S4 = %d: %s (%v)", code, body, err)
		return ""
	}
	session, ok := h.probes.ProbeByJob(name)
	if !ok {
		t.Errorf("probe session %s missing at S4", name)
		return ""
	}
	code, body, err = h.probeCall("POST", "/api/daemon/heartbeat", token,
		[]byte(`{"runtime_id": "`+session.JobRuntime+`"}`))
	if err != nil || code != 200 {
		t.Errorf("probe S5 = %d: %s (%v)", code, body, err)
		return ""
	}
	var ack struct {
		PendingModelList *struct {
			ID string `json:"id"`
		} `json:"pending_model_list"`
	}
	if err := json.Unmarshal(body, &ack); err != nil || ack.PendingModelList == nil {
		t.Errorf("probe S5 ack carries no command: %s (%v)", body, err)
		return ""
	}
	return ack.PendingModelList.ID
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

// handlerEvents is events for the fake server's own logger, whose lines land in
// a separate buffer from the scheduler's.
func (h *harness) handlerEvents(msg string) []map[string]any {
	var found []map[string]any
	for _, line := range bytes.Split(h.handlerLogs.Bytes(), []byte("\n")) {
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

type noRevoker struct{}

func (noRevoker) Revoke(string, string) {}

// credentialRevoker is the scheduler's revoker: the scenario's when it wired
// one, the inert one otherwise.
func credentialRevoker(r scheduler.Revoker) scheduler.Revoker {
	if r != nil {
		return r
	}
	return noRevoker{}
}

// steppedClock is the reporter's clock seam with a scenario-controlled offset:
// a catalog scenario ages one request's budget without waiting it out, and the
// span the reporter records stays exactly reproducible.
type steppedClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *steppedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

// advance moves the clock forward; the reporter reads the offset on its next
// now() call.
func (c *steppedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// fixedClock is a scenario clock that does not follow wall time: every read is
// the base instant plus the steps taken. A span between two reads is therefore
// exactly the stepped amount — steppedClock cannot serve that role, because its
// wall-clock base keeps moving between the claim and the delivery.
type fixedClock struct {
	mu     sync.Mutex
	base   time.Time
	offset time.Duration
}

func newFixedClock() *fixedClock {
	return &fixedClock{base: time.Now()}
}

func (c *fixedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base.Add(c.offset)
}

func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// gatingBuilder holds probe builds until the scenario releases them, so the
// state a queued-request scenario needs — a probe genuinely in flight — is
// deterministic instead of a race against the probe's build and relay.
type gatingBuilder struct {
	probeBuilder
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatingBuilder(inner probeBuilder) *gatingBuilder {
	return &gatingBuilder{probeBuilder: inner, started: make(chan struct{}, 1), release: make(chan struct{})}
}

// BuildCatalogProbe signals the first build and waits for the release; once
// released, every later build passes straight through.
func (b *gatingBuilder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-b.release
	return b.probeBuilder.BuildCatalogProbe(requestID, jobRuntimeID, workspaceID, budget)
}

// releaseAll lets every held build through; safe to call more than once (a
// scenario registers it as cleanup so a failed assertion cannot strand one).
func (b *gatingBuilder) releaseAll() {
	b.once.Do(func() { close(b.release) })
}

// gateProbes installs the gating builder on the scheduler, so a scenario can
// hold one probe open. The scheduler is rebuilt because it captured the builder
// at construction.
func (h *harness) gateProbes(t *testing.T) *gatingBuilder {
	t.Helper()
	gated := newGatingBuilder(h.builder)
	h.builder = gated
	h.rebuildScheduler(t, func(*scheduler.Config) {})
	return gated
}

// runHeartbeatStream runs the production C2 stream against the stub upstream
// for the rest of the test: the ack it receives is what hands a model-list
// command to the reporter.
func (h *harness) runHeartbeatStream(t *testing.T, reporter *proxy.ModelCatalogReporter) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = proxy.NewHeartbeatLoop(h.client, reporter).Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// waitFor polls cond until it holds or the timeout passes: the catalog
// outcomes land one poll interval after the relay, not on the call's return.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// currentLeader returns the lease loop this harness runs, nil when none is.
func (h *harness) currentLeader() *coordination.Leader {
	h.leaderMu.Lock()
	defer h.leaderMu.Unlock()
	return h.leader
}

// leaderHeld is the gate the claim loop, the heartbeat loop and the control
// plane take: true while this instance's lease is held.
func (h *harness) leaderHeld() bool {
	h.leaderMu.Lock()
	defer h.leaderMu.Unlock()
	return h.leader != nil && h.leader.Held()
}

// startLeader runs this instance's control-plane lease until stopLeader (or
// the test's cleanup). OnAcquire mirrors production: takeover reconciliation
// before the control plane serves.
func (h *harness) startLeader(t *testing.T) {
	t.Helper()
	h.runLeader(t)
	leader := h.currentLeader()
	if leader == nil {
		t.Fatal("startLeader: no lease loop was started")
	}
	if !leader.WaitHeld(context.Background(), 10*time.Second) {
		t.Fatal("leader lease was not acquired")
	}
}

// runLeader starts this instance's lease loop without waiting for ownership
// (the follower side of a takeover scenario).
func (h *harness) runLeader(t *testing.T) {
	t.Helper()
	if h.coord == nil {
		t.Fatal("runLeader: no shared coordination face")
	}
	cfg := h.leaderCfg
	if cfg.LeaseTTL == 0 {
		cfg = coordination.Config{
			LeaseTTL:       600 * time.Millisecond,
			TakeoverWindow: 50 * time.Millisecond,
			InstanceTTL:    5 * time.Second,
			TaskLockTTL:    30 * time.Second,
		}
	}
	instanceID := h.instanceID
	if instanceID == "" {
		instanceID = "instance-1"
	}
	leader := coordination.New(h.coord, instanceID, cfg,
		coordination.WithLogger(slog.New(slog.NewJSONHandler(h.logs, nil))),
		coordination.WithOnAcquire(h.sched.OnTakeover))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = leader.Run(ctx)
	}()
	h.leaderMu.Lock()
	h.leader = leader
	h.leaderStop = cancel
	h.leaderDone = done
	h.leaderMu.Unlock()
	t.Cleanup(func() { h.stopLeader(t) })
}

// crashLeader kills this instance's control plane without releasing the
// lease: the survivor must wait out the lease TTL (a real process death).
func (h *harness) crashLeader(t *testing.T) {
	t.Helper()
	if leader := h.currentLeader(); leader != nil {
		leader.Crash()
	}
	h.stopLeader(t)
}

// stopLeader releases the lease and parks this instance's control plane. The
// handles are cleared first so the gates close before the loop is awaited and
// a second call (cleanup after an explicit stop) is a no-op.
func (h *harness) stopLeader(t *testing.T) {
	t.Helper()
	h.leaderMu.Lock()
	stop, done := h.leaderStop, h.leaderDone
	h.leaderStop, h.leaderDone, h.leader = nil, nil, nil
	h.leaderMu.Unlock()
	if stop == nil {
		return
	}
	stop()
	<-done
}

// queueDepth reads the shared queue's marker count.
func (h *harness) queueDepth(t *testing.T) int64 {
	t.Helper()
	n, err := h.coord.QueueDepth(context.Background())
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	return n
}

// twoInstances is the stage-two fixture: two full instances sharing one
// miniredis store, one fake K8s cluster and one upstream stub (the leader's).
type twoInstances struct {
	t     *testing.T
	mr    *miniredis.Miniredis
	store state.Store
	coord state.CoordStore
	// stateEvents records the shared-state availability transitions the
	// production event hook emits (§9.1).
	stateEvents *syncBuffer
	a, b        *harness
}

// twoInstanceOptions are the fixture knobs of the two-instance world.
type twoInstanceOptions struct {
	deliveryWindow   time.Duration
	grace            time.Duration
	bootTimeout      time.Duration
	maxInflight      int
	maxInflightPerWS int
	leaderCfg        coordination.Config
	// wsEnabled mounts the Job-side S-30 route (the subscription gate's
	// evidence needs it).
	wsEnabled bool
	// workspaces overrides the scheduler's configured workspace set (the
	// zero-row counter reconciliation needs a workspace with no live row).
	workspaces []string
	// wrap decorates the shared store both instances share: a scenario fails
	// one command while the rest of the store stays healthy.
	wrap func(store state.Store, coord state.CoordStore) (state.Store, state.CoordStore)
}

func newTwoInstances(t *testing.T, opts twoInstanceOptions) *twoInstances {
	t.Helper()
	if opts.deliveryWindow == 0 {
		opts.deliveryWindow = 300 * time.Millisecond
	}
	if opts.grace == 0 {
		opts.grace = 30 * time.Second
	}
	if opts.bootTimeout == 0 {
		opts.bootTimeout = 300 * time.Second
	}
	if opts.maxInflight == 0 {
		opts.maxInflight = 10
	}
	if opts.leaderCfg.LeaseTTL == 0 {
		opts.leaderCfg = coordination.Config{
			LeaseTTL:       600 * time.Millisecond,
			TakeoverWindow: 50 * time.Millisecond,
			InstanceTTL:    5 * time.Second,
			TaskLockTTL:    30 * time.Second,
		}
	}
	mr := miniredis.RunT(t)
	// A real store expires keys on its own clock (the lease takeover depends on
	// it); miniredis leaves TTLs frozen unless ClockTTL is on.
	mr.ClockTTL(true)
	stateEvents := &syncBuffer{}
	st, err := state.New(state.Config{
		URL:         "redis://" + mr.Addr(),
		DialTimeout: time.Second,
		EntryTTL:    48 * time.Hour,
		Grace:       opts.grace,
	}, state.WithEventHook(func(event string, downFor time.Duration, err error) {
		detail := event
		if event == "recovered" {
			detail = event + " " + downFor.String()
		}
		_, _ = stateEvents.Write([]byte(detail + "\n"))
	}))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	coord := st.(state.CoordStore)
	if opts.wrap != nil {
		st, coord = opts.wrap(st, coord)
	}
	mutate := func(c *scheduler.Config) {
		c.JobBootTimeout = opts.bootTimeout
		c.DeliveryConfirmTimeout = opts.deliveryWindow
		c.MaxInflightJobs = opts.maxInflight
		c.MaxInflightJobsPerWorkspace = opts.maxInflightPerWS
		if len(opts.workspaces) > 0 {
			c.Workspaces = opts.workspaces
		}
	}
	a := buildHarnessWith(t, false, opts.deliveryWindow, nil, false, opts.wsEnabled, harnessOptions{
		store: st, coord: coord, instanceID: "instance-a", grace: opts.grace,
		mutateSched: mutate, leaderCfg: opts.leaderCfg,
	})
	b := buildHarnessWith(t, false, opts.deliveryWindow, nil, false, opts.wsEnabled, harnessOptions{
		store: st, coord: coord, instanceID: "instance-b", grace: opts.grace,
		mutateSched: mutate, leaderCfg: opts.leaderCfg,
		serverURL: a.upstream.server.URL, jobs: a.jobs,
	})
	// One server-side declaration serves both clients.
	a.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	return &twoInstances{t: t, mr: mr, store: st, coord: coord, stateEvents: stateEvents, a: a, b: b}
}

// declare performs C1 for one instance through the shared upstream stub.
func (tw *twoInstances) declare(t *testing.T, h *harness) {
	t.Helper()
	h.declare(declaredRuntimes())
}

// clientList renders the harness's client as the bootstrap's client slice.
func (h *harness) clientList() []*proxy.Client { return []*proxy.Client{h.client} }

// autoRegisterStartupProbe answers the startup probe's S-4 through the
// harness's own Job face when the probe Job is created: the probe daemon
// registers its declared runtime, which is the probe's completion signal.
func (h *harness) autoRegisterStartupProbe(t *testing.T, workspaceID string) {
	t.Helper()
	h.jobs.onCreateJob = func(job *batchv1.Job) {
		go func(name string) {
			token := h.builder.probeToken(name)
			if token == "" {
				return
			}
			body, err := json.Marshal(map[string]any{
				"daemon_id":    name,
				"workspace_id": workspaceID,
				"cli_version":  "0.6.1",
				"runtimes": []map[string]string{{
					"name": "foreman-job", "type": "omp", "version": "0.6.1",
				}},
			})
			if err != nil {
				t.Errorf("marshal probe registration: %v", err)
				return
			}
			if code, resp := h.call("POST", "/api/daemon/register", token, body); code != 200 {
				t.Errorf("probe S-4 = %d: %s", code, resp)
			}
		}(job.Name)
	}
}

// startAssemblyControlPlane builds and starts the loop set through the
// production builder (proxy.NewControlPlane) with the same gate/publisher
// wiring cmd/foreman applies. It returns a stop function.
func (h *harness) startAssemblyControlPlane(t *testing.T, gate func() bool, publisher func(context.Context, *proxy.Client) error) func() {
	t.Helper()
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.client.CatalogSlots(), h.metrics)
	plane := proxy.NewControlPlane(proxy.ControlPlaneOptions{
		ProxyConfig: h.cfg,
		Client:      h.client,
		Scheduler:   h.sched,
		Registry:    h.reg,
		LeaseHooks:  h.sched,
		Reporter:    reporter,
		Metrics:     h.metrics,
		Logger:      slog.New(slog.NewJSONHandler(h.logs, nil)),
		WorkspaceID: "ws-1",
		WSEnabled:   h.cfg.WSEnabled,
	})
	if gate != nil {
		plane.SetLeaderGate(gate)
	}
	if publisher != nil {
		plane.SetDeclarationPublisher(publisher)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	for _, component := range plane.Components() {
		wg.Add(1)
		go func(run func(context.Context) error) {
			defer wg.Done()
			_ = run(ctx)
		}(component.Run)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		wg.Wait()
	}
	t.Cleanup(stop)
	return stop
}

// TestUpstreamStubC17AnswerVerbatim pins the C-17 fixture's programming
// contract: an answer set by setWorkspacesAnswer reaches the caller byte for
// byte, and only an unset one falls back to the default entry. A zero-length
// answer is a programmed answer — a fallback there would let a scenario assert
// against an answer it never programmed and still pass.
func TestUpstreamStubC17AnswerVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name    string
		program func(*upstreamStub)
		want    string
	}{
		{"unset falls back to the default entry", func(*upstreamStub) {}, `[{"id":"ws-1","name":"foreman"}]`},
		{"empty answer is delivered empty", func(s *upstreamStub) { s.setWorkspacesAnswer("") }, ""},
		{"answer is delivered verbatim", func(s *upstreamStub) { s.setWorkspacesAnswer(`[{"id":"ws-b","name":""}]`) }, `[{"id":"ws-b","name":""}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := newUpstreamStub(t)
			tc.program(stub)

			resp, err := http.Get(stub.server.URL + "/api/daemon/workspaces")
			if err != nil {
				t.Fatalf("GET C-17 answer: %v", err)
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read C-17 answer: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("C-17 answer = %q, want %q", got, tc.want)
			}
		})
	}
}
