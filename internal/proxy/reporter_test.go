package proxy

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// fakeProbe is the scheduler's probe face as the reporter sees it: it records
// the calls and answers from a programmable result.
type fakeProbe struct {
	mu       sync.Mutex
	started  []string // request ids in StartProbe order
	runtimes []string // the real runtime id each StartProbe was addressed to
	deleted  []string
	budgets  []time.Duration

	// gate blocks StartProbe until the test releases it (single-flight tests).
	gate     chan struct{}
	result   probeResult
	startErr error
}

type probeResult struct {
	node    string
	relayed bool
	code    int
}

func (f *fakeProbe) StartProbe(_ context.Context, requestID, realRuntimeID string, budget time.Duration) (scheduler.ProbeHandle, error) {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return scheduler.ProbeHandle{}, f.startErr
	}
	f.started = append(f.started, requestID)
	f.runtimes = append(f.runtimes, realRuntimeID)
	f.budgets = append(f.budgets, budget)
	jobName := "fm-catalog-" + requestID
	return scheduler.ProbeHandle{RequestID: requestID, JobName: jobName, JobRuntime: "jr-" + requestID}, nil
}

func (f *fakeProbe) AwaitProbeRelayed(_ context.Context, h scheduler.ProbeHandle, _ time.Time) (string, bool, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.result.node, f.result.relayed, f.result.code, nil
}

func (f *fakeProbe) DeleteProbe(_ context.Context, h scheduler.ProbeHandle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, h.JobName)
	return nil
}

func (f *fakeProbe) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.started)
}

func (f *fakeProbe) startedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.started...)
}

// lockedBuffer is the harness log sink. The reporter logs from the goroutine
// that answers a request (the replay and the relay budget run outside the
// heartbeat round), while the test reads the same buffer, so both go through
// the mutex.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// c21Recorder collects the C21 requests the stub upstream received. The answer
// path is concurrent with the test's polling for the same reason, so the
// recorder owns the slice.
type c21Recorder struct {
	mu    sync.Mutex
	calls []recordedRequest
}

func (r *c21Recorder) add(path string, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedRequest{Path: path, Body: body})
}

func (r *c21Recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *c21Recorder) snapshot() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedRequest(nil), r.calls...)
}

// reporterHarness wires the reporter to a stub upstream (the C21 endpoint),
// the probe fake and a log buffer.
type reporterHarness struct {
	reporter *ModelCatalogReporter
	slots    *CatalogSlots
	probes   *fakeProbe
	metrics  *fakeMetrics
	client   *Client
	stub     *stubUpstream
	logs     *lockedBuffer
	now      time.Time
}

func newReporterHarness(t *testing.T, cfg Config) *reporterHarness {
	t.Helper()
	srv, stub := newStubUpstream(t)
	h := &reporterHarness{
		slots:   NewCatalogSlots(),
		probes:  &fakeProbe{},
		metrics: &fakeMetrics{},
		stub:    stub,
		logs:    &lockedBuffer{},
		now:     time.Now(),
	}
	cfg.ServerURL = srv.URL
	h.client = NewClient(cfg, "mdt_test")
	h.reporter = NewModelCatalogReporter(
		cfg,
		h.client,
		h.probes,
		h.slots,
		h.metrics,
		WithReporterClock(func() time.Time { return h.now }),
		WithReporterLogger(slog.New(slog.NewTextHandler(h.logs, nil))),
	)
	return h
}

// recordC21 makes the stub answer and record the C21 path.
func (h *reporterHarness) recordC21(t *testing.T, status int) *c21Recorder {
	t.Helper()
	rec := &c21Recorder{}
	h.stub.on("POST", "/api/daemon/runtimes/rid-a/models/req-1/result", func(w http.ResponseWriter, r *http.Request, body []byte) {
		rec.add(r.URL.Path, body)
		writeJSONStub(w, status, map[string]string{"status": "ok"})
	})
	return rec
}

func TestReporterReplaysCachedBodyVerbatim(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	body := []byte(`{"status":"completed","models":[{"id":"m1"}]}`)
	h.slots.Put("rid-a", body, h.now.Add(-time.Minute))
	calls := h.recordC21(t, 200)

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	// The replay is an independent answer path: it leaves the heartbeat round,
	// so the assertions wait for it to land.
	waitForCondition(t, "the replay to reach the server", func() bool { return calls.len() == 1 })

	if h.probes.startCount() != 0 {
		t.Fatal("a cached slot must never start a probe")
	}
	if calls.len() != 1 {
		t.Fatalf("C21 calls = %d, want 1", calls.len())
	}
	if got := calls.snapshot(); string(got[0].Body) != string(body) {
		t.Errorf("replayed body = %s, want it verbatim", got[0].Body)
	}
	if got := h.metrics.catalogCodeCount("200"); got != 1 {
		t.Errorf("relay metric code 200 = %d, want 1", got)
	}
	if !strings.Contains(h.logs.String(), "model.catalog_relayed") {
		t.Errorf("relayed event missing: %s", h.logs.String())
	}
}

func TestReporterReplayRetriesTransient5xx(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	h.client.relayWaits = []time.Duration{0, 0, 0}
	h.slots.Put("rid-a", []byte(`{"models":[]}`), h.now)
	var attempts atomic.Int32
	h.stub.on("POST", "/api/daemon/runtimes/rid-a/models/req-1/result", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		n := attempts.Add(1)
		if n < 2 {
			writeJSONStub(w, 503, map[string]string{"status": "boom"})
			return
		}
		writeJSONStub(w, 200, map[string]string{"status": "ok"})
	})

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the replay to retry and succeed", func() bool { return attempts.Load() == 2 })

	if attempts.Load() != 2 {
		t.Fatalf("upstream attempts = %d, want the replay to retry a 5xx", attempts.Load())
	}
	if got := h.metrics.catalogCodeCount("200"); got != 1 {
		t.Errorf("relay metric code 200 = %d, want 1", got)
	}
}

func TestReporterReplayExhaustionIsVisible(t *testing.T) {
	cfg := testConfig("http://unused")
	cfg.ModelCatalogTimeout = 200 * time.Millisecond
	h := newReporterHarness(t, cfg)
	h.client.relayWaits = []time.Duration{0, 0, 0}
	h.slots.Put("rid-a", []byte(`{"models":[]}`), h.now)
	var attempts atomic.Int32
	h.stub.on("POST", "/api/daemon/runtimes/rid-a/models/req-1/result", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		attempts.Add(1)
		writeJSONStub(w, 503, map[string]string{"status": "boom"})
	})

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the replay budget to run out", func() bool { return h.metrics.catalogCodeCount("none") > 0 })

	if attempts.Load() < 2 {
		t.Errorf("upstream attempts = %d, want the transport errors retried", attempts.Load())
	}
	if got := h.metrics.catalogCodeCount("none"); got != 1 {
		t.Errorf("abandoned relay metric = %d, want 1", got)
	}
	if !strings.Contains(h.logs.String(), "catalog relay exhausted") {
		t.Errorf("the exhausted relay must be visible: %s", h.logs.String())
	}
}

// TestReporterReplayDoesNotBlockTheHeartbeatRound pins the C2 cadence: the ack
// is consumed in-round, but a replay that has to retry must not hold the
// heartbeat goroutine for its whole budget (§1.1 C2「每 15s」).
func TestReporterReplayDoesNotBlockTheHeartbeatRound(t *testing.T) {
	cfg := testConfig("http://unused")
	cfg.ModelCatalogTimeout = 30 * time.Second
	h := newReporterHarness(t, cfg)
	h.client.relayWaits = []time.Duration{time.Second, time.Second}
	h.slots.Put("rid-a", []byte(`{"models":[]}`), h.now)
	h.stub.on("POST", "/api/daemon/runtimes/rid-a/models/req-1/result", func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(503) // keeps the replay busy retrying
	})

	begin := time.Now()
	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	if elapsed := time.Since(begin); elapsed > 100*time.Millisecond {
		t.Fatalf("OnAck took %s: the replay must not run inside the heartbeat round", elapsed)
	}
}

func TestReporterReplayIgnoresInFlightProbe(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	// One probe is stuck in flight for rid-b; rid-a's cache must answer anyway.
	h.probes.gate = make(chan struct{})
	h.probes.result = probeResult{relayed: true, code: 200}
	h.reporter.Submit(t.Context(), "rid-b", "req-b1", h.now)
	h.slots.Put("rid-a", []byte(`{"models":[]}`), h.now)
	calls := h.recordC21(t, 200)

	begin := time.Now()
	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("OnAck blocked %s: a replay must not queue behind another runtime's probe", elapsed)
	}
	waitForCondition(t, "the replay to reach the server", func() bool { return calls.len() == 1 })
	if calls.len() != 1 {
		t.Fatalf("C21 calls = %d, want 1", calls.len())
	}
	close(h.probes.gate)
}

func TestReporterStaleCacheStillAnswers(t *testing.T) {
	cfg := testConfig("http://unused")
	cfg.ModelCatalogCacheTTL = time.Minute
	h := newReporterHarness(t, cfg)
	h.slots.Put("rid-a", []byte(`{"models":[]}`), h.now.Add(-2*time.Hour))
	calls := h.recordC21(t, 200)

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the stale replay to answer", func() bool { return calls.len() == 1 })

	if calls.len() != 1 {
		t.Fatalf("a stale cache must still answer (TTL is a warning surface)")
	}
	if !strings.Contains(h.logs.String(), "model.catalog_stale") {
		t.Errorf("staleness warning missing: %s", h.logs.String())
	}
}

func TestReporterDisabledDoesNotConsume(t *testing.T) {
	cfg := testConfig("http://unused")
	cfg.ModelCatalogEnabled = false
	h := newReporterHarness(t, cfg)
	h.slots.Put("rid-a", []byte(`{"models":[]}`), h.now)
	calls := h.recordC21(t, 200)

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")

	if calls.len() != 0 {
		t.Error("a disabled catalog route must not relay, not even from cache")
	}
	if h.probes.startCount() != 0 {
		t.Error("a disabled catalog route must not start a probe")
	}
	if got := h.metrics.catalogCodeCount("none"); got != 1 {
		t.Errorf("abandoned relay metric = %d, want 1", got)
	}
	logs := h.logs.String()
	if !strings.Contains(logs, "model.catalog_relay_failed") || !strings.Contains(logs, "catalog relay disabled") {
		t.Errorf("the disabled path must be visible: %s", logs)
	}
}

func TestReporterEmptySlotStartsProbeAndRecordsResult(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	h.probes.result = probeResult{node: "node-a", relayed: true, code: 200}

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the probe to finish", func() bool {
		h.probes.mu.Lock()
		defer h.probes.mu.Unlock()
		return len(h.probes.deleted) == 1
	})

	if got := h.probes.startedIDs(); len(got) != 1 || got[0] != "req-1" {
		t.Fatalf("started = %v, want the ack's request id", got)
	}
	if h.probes.runtimes[0] != "rid-a" {
		t.Errorf("probe bound to runtime %q, want the ack's runtime", h.probes.runtimes[0])
	}
	logs := h.logs.String()
	if !strings.Contains(logs, "model.catalog_requested") || !strings.Contains(logs, "model.catalog_relayed") {
		t.Errorf("requested/relayed events missing: %s", logs)
	}
	if got := h.metrics.catalogCodeCount("200"); got != 1 {
		t.Errorf("relay metric = %d, want 1", got)
	}
}

func TestReporterUnrelayedProbeCountsAsAbandoned(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	h.probes.result = probeResult{relayed: false}

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the probe to finish", func() bool {
		h.probes.mu.Lock()
		defer h.probes.mu.Unlock()
		return len(h.probes.deleted) == 1
	})

	if got := h.metrics.catalogCodeCount("none"); got != 1 {
		t.Errorf("abandoned relay metric = %d, want 1", got)
	}
	if !strings.Contains(h.logs.String(), "model.catalog_relay_failed") {
		t.Errorf("failure event missing: %s", h.logs.String())
	}
}

// TestReporterExhaustedProbeRelayIsVisible pins the "转发耗尽" face: an S27
// that used up its retry budget reports code 0, and the reporter must surface
// it as a failed relay rather than a relayed one.
func TestReporterExhaustedProbeRelayIsVisible(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	h.probes.result = probeResult{node: "node-a", relayed: true, code: 0}

	h.reporter.OnAck(t.Context(), "rid-a", "req-1")
	waitForCondition(t, "the probe to finish", func() bool {
		h.probes.mu.Lock()
		defer h.probes.mu.Unlock()
		return len(h.probes.deleted) == 1
	})

	if got := h.metrics.catalogCodeCount("none"); got != 1 {
		t.Errorf("abandoned relay metric = %d, want 1", got)
	}
	if !strings.Contains(h.logs.String(), "catalog relay exhausted") {
		t.Errorf("the exhausted relay must be visible: %s", h.logs.String())
	}
	if strings.Contains(h.logs.String(), "model.catalog_relayed") {
		t.Errorf("an exhausted relay is not a relayed one: %s", h.logs.String())
	}
}

func TestReporterSupersedesQueuedRequest(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	h.probes.gate = make(chan struct{})
	h.probes.result = probeResult{relayed: true, code: 200}

	h.reporter.Submit(t.Context(), "rid-a", "req-1", h.now)
	h.reporter.Submit(t.Context(), "rid-a", "req-2", h.now)
	h.reporter.Submit(t.Context(), "rid-a", "req-3", h.now)
	close(h.probes.gate)
	waitForCondition(t, "the first probe to finish", func() bool {
		h.probes.mu.Lock()
		defer h.probes.mu.Unlock()
		return len(h.probes.started) == 2
	})

	if got := h.probes.startedIDs(); got[0] != "req-1" || got[1] != "req-3" {
		t.Fatalf("started = %v, want req-1 then the superseding req-3", got)
	}
	logs := h.logs.String()
	if !strings.Contains(logs, "superseded by a newer request") {
		t.Errorf("the superseded request must stay visible: %s", logs)
	}
	// The superseded request never produced a relay.
	for _, id := range []string{"req-1", "req-3"} {
		if !strings.Contains(logs, "model.catalog_relayed") || !strings.Contains(logs, id) {
			t.Errorf("req %s: relayed event missing: %s", id, logs)
		}
	}
}

func TestReporterInsufficientBudgetDoesNotStartProbe(t *testing.T) {
	cfg := testConfig("http://unused")
	cfg.ModelCatalogTimeout = 45 * time.Second
	h := newReporterHarness(t, cfg)
	consumed := h.now.Add(-40 * time.Second)

	h.reporter.Submit(t.Context(), "rid-a", "req-1", consumed)
	waitForCondition(t, "the budget check", func() bool {
		return strings.Contains(h.logs.String(), "insufficient budget after queueing")
	})

	if h.probes.startCount() != 0 {
		t.Error("a request below the start threshold must not start a probe")
	}
	if got := h.metrics.catalogCodeCount("none"); got != 1 {
		t.Errorf("abandoned relay metric = %d, want 1", got)
	}
}

func TestReporterSingleFlightIsPerRuntime(t *testing.T) {
	cfg := testConfig("http://unused")
	h := newReporterHarness(t, cfg)
	h.probes.gate = make(chan struct{})
	h.probes.result = probeResult{relayed: true, code: 200}

	h.reporter.Submit(t.Context(), "rid-a", "req-a", h.now)
	h.reporter.Submit(t.Context(), "rid-b", "req-b", h.now)
	close(h.probes.gate)
	waitForCondition(t, "both runtimes' probes to start", func() bool { return h.probes.startCount() == 2 })

	started := h.probes.startedIDs()
	if len(started) != 2 || started[0] == started[1] {
		t.Fatalf("started = %v, want one probe per runtime", started)
	}
}

// waitForCondition polls cond until it holds (bounded, test-only).
func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}
