// TC-tech-startup-04 exercises the command-driven catalog path end to end with
// the production components: a C2 ack's pending_model_list starts one probe Job
// (real jobbuilder), the probe daemon registers, receives the command through
// its S5 ack, reports through S27, and Foreman relays the result verbatim as
// C21. The only fake is the Kubernetes API.
package acceptance

import (
	"context"
	"io"
	"log/slog"
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

// writeBuffer is a tiny io.Writer for the reporter's logger.
type writeBuffer struct{ data []byte }

func (b *writeBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *writeBuffer) Bytes() []byte { return b.data }

func (b *writeBuffer) String() string { return string(b.data) }

var _ io.Writer = (*writeBuffer)(nil)
