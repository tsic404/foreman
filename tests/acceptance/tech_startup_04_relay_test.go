// TC-tech-startup-04 exercises the command-driven catalog path end to end with
// the production components: a C2 ack's pending_model_list starts one probe Job
// (real jobbuilder), the probe daemon registers, receives the command through
// its S5 ack, reports through S27, and Foreman relays the result verbatim as
// C21. A slot with a cache answers without a probe at all — including when the
// cache is past its TTL — and two commands on one slot get identical bytes. The
// only fake is the Kubernetes API.
package acceptance

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestStartup04CommandDrivenRelayEndToEnd(t *testing.T) {
	h := newHarness(t, true)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	const resultBody = `{"status":"completed","models":[{"id":"m1"}]}`
	commandSeen := h.serveProbeDaemon(t, resultBody)

	logs := &writeBuffer{}
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	)
	reporter.OnAck(t.Context(), "rid-omp", "req-cmd")

	select {
	case id := <-commandSeen:
		if id != "req-cmd" {
			t.Fatalf("probe was handed command %q, want the ack's id verbatim", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the probe daemon never received a command (S5 delivery)")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.upstream.pathCalls("/api/daemon/runtimes/rid-omp/models/req-cmd/result")) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	relayed := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/req-cmd/result")
	if len(relayed) != 1 {
		t.Fatalf("C21 calls = %d, want 1 (the probe's result relayed verbatim)", len(relayed))
	}
	if string(relayed[0].Body) != resultBody {
		t.Errorf("C21 body = %s, want the probe's bytes verbatim", relayed[0].Body)
	}
	// The reporter observes the result through the session, so the metric and
	// the relayed event follow the relay within one poll interval.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if codes := h.metrics.codes(); len(codes) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "200" {
		t.Errorf("relay metric codes = %v, want one 200 (the C21 answer)", codes)
	}
	if event := findJSONEvent(t, logs.Bytes(), "model.catalog_relayed"); event == nil {
		t.Errorf("relayed event missing: %s", logs.String())
	} else if event["request_id"] != "req-cmd" {
		t.Errorf("relayed event = %+v", event)
	}
	// Single flight: the probe is torn down before the next request is served.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.probes.ListProbes()) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if probes := h.probes.ListProbes(); len(probes) != 0 {
		t.Errorf("the probe session must be removed at teardown: %+v", probes)
	}
}

// TestStartup04CachedSlotAnswersThroughTheHeartbeatStream drives the command
// path through the real C2 stream: the ack carries the server's pending request,
// the slot of the *heartbeat's* runtime answers it, and the caller's request
// completes without any probe Job. The ack body's runtime_id is shaped like the
// real server's (absent, or naming another runtime) — the command must not be
// re-keyed by it.
func TestStartup04CachedSlotAnswersThroughTheHeartbeatStream(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape ackRuntimeIDShape
	}{
		{"ack without runtime_id", ackOmitsRuntimeID},
		{"ack with a foreign runtime_id", ackForeignRuntimeID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, true)
			h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
			h.declare(declaredRuntimes())
			h.upstream.setAckShape(tc.shape)
			const cached = `{"status":"completed","models":[{"id":"m1"}]}`
			h.slots.Put("rid-omp", []byte(cached), time.Now())
			reporter := h.newReporter(t)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// The command is pending before the first heartbeat, so the stream's
			// first ack delivers it.
			id := h.requestModelList(t, "rid-omp")
			go func() { _ = proxy.NewHeartbeatLoop(h.client, reporter).Run(ctx) }()

			h.waitModelCompleted(t, id)
			relayed := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/"+id+"/result")
			if len(relayed) != 1 {
				t.Fatalf("C21 calls = %d, want the cached body replayed exactly once", len(relayed))
			}
			if string(relayed[0].Body) != cached {
				t.Errorf("C21 body = %s, want the cached bytes verbatim", relayed[0].Body)
			}
			if jobs := h.jobs.probeJobsCreated(); len(jobs) != 0 {
				t.Errorf("probe Jobs = %v, want none for a cached slot", jobs)
			}
			h.waitCatalogMetrics(t, 1)
			if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "200" {
				t.Errorf("relay metric codes = %v, want one 200 (the replay's C21 answer)", codes)
			}
			if secs := h.metrics.seconds(); len(secs) != 1 || secs[0] > 1 {
				t.Errorf("catalog seconds = %v, want the replay's near-zero duration (no probe round trip)", secs)
			}
		})
	}
}

// TestStartup04EmptySlotRelaysThroughTheHeartbeatStream is the other half: an
// empty slot falls back to a probe Job built in *command-driven* mode, and the
// probe's result reaches the upstream as C21. A startup-discovery probe would
// absorb the result locally (S27) and the caller's request would run into the
// server's 60s window instead.
func TestStartup04EmptySlotRelaysThroughTheHeartbeatStream(t *testing.T) {
	h := newHarness(t, true)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setAckShape(ackForeignRuntimeID)
	const resultBody = `{"status":"completed","models":[{"id":"m1"}]}`
	commands := h.serveProbeDaemon(t, resultBody)
	reporter := h.newReporter(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := h.requestModelList(t, "rid-omp")
	go func() { _ = proxy.NewHeartbeatLoop(h.client, reporter).Run(ctx) }()

	select {
	case got := <-commands:
		if got != id {
			t.Fatalf("probe was handed command %q, want the ack's id verbatim", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the probe daemon never received a command (S5 delivery)")
	}
	h.waitModelCompleted(t, id)
	relayed := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/"+id+"/result")
	if len(relayed) != 1 || string(relayed[0].Body) != resultBody {
		t.Fatalf("C21 calls = %d (%s), want the probe's bytes relayed verbatim once", len(relayed), relayed)
	}
	// The probe session must be command-driven and bound to the heartbeat's
	// runtime: a startup-discovery session would absorb the result locally.
	jobs := h.jobs.probeJobsCreated()
	if len(jobs) != 1 {
		t.Fatalf("probe Jobs = %v, want exactly one", jobs)
	}
	session, ok := h.probes.ProbeByJob(jobs[0])
	if !ok {
		t.Fatalf("probe session %s missing", jobs[0])
	}
	if session.SelfMintedID != "" || session.RequestID != id || session.RealRuntimeID != "rid-omp" {
		t.Fatalf("probe session = {self_minted:%q request:%q real_runtime:%q}, want a command-driven session bound to rid-omp",
			session.SelfMintedID, session.RequestID, session.RealRuntimeID)
	}
	h.waitCatalogMetrics(t, 1)
	if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "200" {
		t.Errorf("relay metric codes = %v, want one 200 (the relayed C21 answer)", codes)
	}
}

// TestStartup04StaleCacheStillAnswersWhileTheProbeSlotIsBusy pins AC-39's stale
// branch: a cache past its TTL is still answered — warned about, never dropped —
// and the answer never joins the runtime's single-flight queue, the seat a
// command's own probe is holding while the stale command arrives.
func TestStartup04StaleCacheStillAnswersWhileTheProbeSlotIsBusy(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	gated := h.gateProbes(t)
	t.Cleanup(gated.releaseAll)
	const probeBody = `{"status":"completed","models":[{"id":"probe"}]}`
	h.serveProbeDaemon(t, probeBody)

	// The threshold the slot has to cross, crossed by construction: the landing
	// time sits two TTLs in the past.
	h.cfg.ModelCatalogCacheTTL = time.Minute
	logs := &writeBuffer{}
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(logs, nil))))

	// One command of this runtime occupies the single-flight seat: its probe
	// stays held inside the build for as long as the assertions below need.
	occupying := h.requestModelList(t, "rid-omp")
	reporter.OnAck(t.Context(), "rid-omp", occupying)
	select {
	case <-gated.started:
	case <-time.After(3 * time.Second):
		t.Fatal("the occupying command never started its probe")
	}

	const cached = `{"status":"completed","models":[{"id":"cached"}]}`
	h.slots.Put("rid-omp", []byte(cached), time.Now().Add(-2*h.cfg.ModelCatalogCacheTTL))
	stale := h.requestModelList(t, "rid-omp")
	reporter.OnAck(t.Context(), "rid-omp", stale)

	// Answered while the probe is still held: a request that had entered the
	// single-flight queue could not complete before the gate opens.
	waitFor(t, "the stale command's replay", 3*time.Second, func() bool {
		return h.upstream.modelCommandVerdict(stale) == "completed"
	})
	relayed := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/"+stale+"/result")
	if len(relayed) != 1 {
		t.Fatalf("C21 calls for the stale command = %d, want exactly one replay", len(relayed))
	}
	if string(relayed[0].Body) != cached {
		t.Errorf("C21 body = %s, want the cached bytes verbatim", relayed[0].Body)
	}
	if body, writes := h.upstream.modelCommandRecord(stale); string(body) != cached || writes != 1 {
		t.Errorf("server record for the stale command = %q after %d writes, want the cache body once", body, writes)
	}
	// The metric lands after the C21 answer, written by the replay goroutine
	// that logs it: waiting here is the edge the log reads below need, since
	// that goroutine runs on past the ack's return (the other drivers read the
	// reporter's log the same way).
	h.waitCatalogMetrics(t, 1)
	if codes, seconds := h.metrics.codes(), h.metrics.seconds(); len(codes) != 1 || codes[0] != "200" || seconds[0] > 1 {
		t.Errorf("relay metric = %v over %v, want one 200 within the replay's near-zero span", codes, seconds)
	}
	// The warning names the crossed threshold: the slot's own age against the
	// configured TTL, not a test-supplied staleness flag.
	warn := reporterEvent(t, logs.Bytes(), "model.catalog_stale", stale)
	if warn == nil {
		t.Fatalf("staleness warning for %s missing: %s", stale, logs.String())
	}
	if age, _ := warn["age_seconds"].(float64); age < (2 * h.cfg.ModelCatalogCacheTTL).Seconds() {
		t.Errorf("staleness age = %v, want the slot's past landing time", warn["age_seconds"])
	}
	if ttl, _ := warn["ttl_seconds"].(float64); ttl != h.cfg.ModelCatalogCacheTTL.Seconds() {
		t.Errorf("staleness ttl = %v, want the configured threshold", warn["ttl_seconds"])
	}
	// The replay is the cache path, not the probe path: no probe was built for
	// it, and the probe path's own start event does not name it.
	if event := reporterEvent(t, logs.Bytes(), "model.catalog_requested", stale); event != nil {
		t.Errorf("the stale command reached the probe path: %+v", event)
	}
	if jobs := h.jobs.probeJobsCreated(); len(jobs) != 0 {
		t.Errorf("probe Jobs = %v, want none while the occupying build is held", jobs)
	}

	// The seat the stale command never took still belongs to the probe path:
	// released, the occupying command runs its probe and relays the probe's own
	// bytes.
	gated.releaseAll()
	waitFor(t, "the occupying command's relay", 5*time.Second, func() bool {
		return h.upstream.modelCommandVerdict(occupying) == "completed"
	})
	jobs := h.jobs.probeJobsCreated()
	if len(jobs) != 1 || jobs[0] != "fm-catalog-"+occupying {
		t.Errorf("probe Jobs = %v, want only the occupying command's", jobs)
	}
	occupyingCalls := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/"+occupying+"/result")
	if len(occupyingCalls) != 1 || string(occupyingCalls[0].Body) != probeBody {
		t.Errorf("C21 calls for the occupying command = %d (%s), want the probe's bytes", len(occupyingCalls), occupyingCalls)
	}
}

// TestStartup04CachedSlotAnswersTwoCommandsVerbatim pins the other observable
// AC-39 writes for the cached branch: two commands on one slot come back with
// identical result bodies — the answer is the cache's bytes, whoever asks —
// while each request id travels in its own C21 path.
func TestStartup04CachedSlotAnswersTwoCommandsVerbatim(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	const cached = `{"status":"completed","models":[{"id":"m1"},{"id":"m2"}]}`
	h.slots.Put("rid-omp", []byte(cached), time.Now())
	reporter := h.newReporter(t)

	// Both commands go to the reporter's ack entry point: the heartbeat stream
	// delivers one command per 15s round, which cannot make two answers
	// back-to-back.
	first := h.requestModelList(t, "rid-omp")
	second := h.requestModelList(t, "rid-omp")
	reporter.OnAck(t.Context(), "rid-omp", first)
	reporter.OnAck(t.Context(), "rid-omp", second)

	for _, id := range []string{first, second} {
		h.waitModelCompleted(t, id)
	}
	firstBody, firstWrites := h.upstream.modelCommandRecord(first)
	secondBody, secondWrites := h.upstream.modelCommandRecord(second)
	if !bytes.Equal(firstBody, secondBody) {
		t.Errorf("result bodies differ across the two commands: %q vs %q", firstBody, secondBody)
	}
	if string(firstBody) != cached || string(secondBody) != cached {
		t.Errorf("result bodies = %q / %q, want the cached bytes verbatim", firstBody, secondBody)
	}
	if firstWrites != 1 || secondWrites != 1 {
		t.Errorf("record writes = %d / %d, want each command's result adopted exactly once", firstWrites, secondWrites)
	}
	for _, id := range []string{first, second} {
		relayed := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/"+id+"/result")
		if len(relayed) != 1 || string(relayed[0].Body) != cached {
			t.Errorf("C21 calls for %s = %d (%s), want the cached bytes replayed once under its own request id", id, len(relayed), relayed)
		}
	}
	if jobs := h.jobs.probeJobsCreated(); len(jobs) != 0 {
		t.Errorf("probe Jobs = %v, want none for a cached slot", jobs)
	}
	h.waitCatalogMetrics(t, 2)
	if codes := h.metrics.codes(); len(codes) != 2 || codes[0] != "200" || codes[1] != "200" {
		t.Errorf("relay metric codes = %v, want two 200 (one replay per command)", codes)
	}
}

// writeBuffer is a tiny io.Writer for the reporter's logger. The reporter and
// the command loop can log from different goroutines, so the buffer carries
// its own lock instead of relying on the producers' ordering.
type writeBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *writeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *writeBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...)
}

func (b *writeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}

var _ io.Writer = (*writeBuffer)(nil)
