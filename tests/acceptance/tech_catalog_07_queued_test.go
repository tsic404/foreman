// TC-tech-catalog-07 / AC-31: the queued side of one runtime's single-flight
// slot. A second command waits behind the probe in flight, and when its own
// budget has run out by the time the slot frees, no probe is started for it and
// the server's window converges on its own (03-acceptance.md「模型目录」).
//
// The design registers this branch as not deterministically constructible on
// the available infrastructure (a real cluster cannot be made to reach the
// budget threshold on demand), so this case runs the alternative judgement the
// entry registers: no probe started, no result forward, the server's record
// left to time out — driven through the reporter's clock seam instead of a real
// 45s window.
//
// The same slot's other queued branch — a waiting request replaced by a newer
// one without ever starting — is covered here too: it has no TC of its own, and
// its only observable is the abandoned-relay event and its non-zero span.
package acceptance

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
)

func TestCatalog07QueuedBudgetExhaustedSkipsTheProbe(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setModelWindow(200 * time.Millisecond)
	gated := h.gateProbes(t)
	t.Cleanup(gated.releaseAll)
	commands := h.serveProbeDaemon(t, `{"status":"completed","models":[{"id":"m1"}]}`)

	clock := &steppedClock{}
	logs := &writeBuffer{}
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterClock(clock.now),
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(logs, nil))))

	occupying := h.requestModelList(t, "rid-omp")
	queued := h.requestModelList(t, "rid-omp")
	h.upstream.markModelDelivered(occupying)
	h.upstream.markModelDelivered(queued)

	// The first command occupies the slot: its probe is held open inside the
	// build, so the second command below is queued behind a live occupancy.
	reporter.OnAck(t.Context(), "rid-omp", occupying)
	select {
	case <-gated.started:
	case <-time.After(3 * time.Second):
		t.Fatal("the occupying command never started its probe")
	}
	reporter.OnAck(t.Context(), "rid-omp", queued)
	// The waiting command ages past the probe-start threshold while it waits.
	clock.advance(40 * time.Second)
	gated.releaseAll()

	select {
	case handed := <-commands:
		if handed != occupying {
			t.Fatalf("probe was handed %q, want the occupying command %q", handed, occupying)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the occupying probe never received its command")
	}
	h.waitModelCompleted(t, occupying)

	// No probe for the queued command: it never starts, and nothing is ever
	// forwarded for it.
	waitFor(t, "the queued command's budget verdict", 3*time.Second, func() bool {
		return len(h.metrics.codes()) >= 2
	})
	jobs := h.jobs.probeJobsCreated()
	if len(jobs) != 1 || jobs[0] != "fm-catalog-"+occupying {
		t.Errorf("probe Jobs = %v, want only the occupying command's", jobs)
	}
	if calls := h.upstream.pathCalls("/api/daemon/runtimes/rid-omp/models/" + queued); len(calls) != 0 {
		t.Errorf("result forwards for the queued command = %v, want none", calls)
	}
	body, writes := h.upstream.modelCommandRecord(queued)
	if len(body) != 0 || writes != 0 {
		t.Errorf("server record for the queued command = %q after %d writes, want nothing", body, writes)
	}
	waitFor(t, "the queued command's window timeout", 2*time.Second, func() bool {
		return h.upstream.modelCommandVerdict(queued) == "timeout"
	})

	// The Foreman-side face: the abandoned relay is recorded with the span it
	// waited (non-zero), and the reason names the budget.
	noneSeconds := noneSpans(t, h.metrics)
	if len(noneSeconds) != 1 {
		t.Fatalf("abandoned relay spans = %v, want exactly one for the queued command", noneSeconds)
	}
	if noneSeconds[0] <= 0 {
		t.Errorf("abandoned relay span = %v, want the queued wait (non-zero)", noneSeconds[0])
	}
	event := reporterEvent(t, logs.Bytes(), "model.catalog_relay_failed", queued)
	if event == nil {
		t.Fatalf("failure event for %s missing: %s", queued, logs.String())
	}
	if reason, _ := event["reason"].(string); !strings.Contains(reason, "insufficient budget") {
		t.Errorf("failure reason = %q, want the exhausted queued budget", reason)
	}
}

func TestCatalog07QueuedRequestSupersededNeverStarts(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	// Wide enough that the promoted command's own forward lands inside it: this
	// case is about the superseded request, not about the window's edge.
	h.upstream.setModelWindow(2 * time.Second)
	gated := h.gateProbes(t)
	t.Cleanup(gated.releaseAll)
	h.serveProbeDaemon(t, `{"status":"completed","models":[{"id":"m1"}]}`)

	clock := &steppedClock{}
	logs := &writeBuffer{}
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterClock(clock.now),
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(logs, nil))))

	occupying := h.requestModelList(t, "rid-omp")
	waiting := h.requestModelList(t, "rid-omp")
	newer := h.requestModelList(t, "rid-omp")
	for _, id := range []string{occupying, waiting, newer} {
		h.upstream.markModelDelivered(id)
	}

	reporter.OnAck(t.Context(), "rid-omp", occupying)
	select {
	case <-gated.started:
	case <-time.After(3 * time.Second):
		t.Fatal("the occupying command never started its probe")
	}
	reporter.OnAck(t.Context(), "rid-omp", waiting)
	clock.advance(2 * time.Second)
	// The newer command replaces the waiting one: the waiting request is
	// abandoned without ever holding the slot.
	reporter.OnAck(t.Context(), "rid-omp", newer)

	noneSeconds := noneSpans(t, h.metrics)
	if len(noneSeconds) != 1 {
		t.Fatalf("abandoned relay spans = %v, want exactly one for the superseded request", noneSeconds)
	}
	if noneSeconds[0] < 2 {
		t.Errorf("abandoned relay span = %v, want the superseded wait (>= 2s, non-zero)", noneSeconds[0])
	}
	event := reporterEvent(t, logs.Bytes(), "model.catalog_relay_failed", waiting)
	if event == nil {
		t.Fatalf("failure event for %s missing: %s", waiting, logs.String())
	}
	if reason, _ := event["reason"].(string); !strings.Contains(reason, "superseded") {
		t.Errorf("failure reason = %q, want the superseded marker", reason)
	}

	// The superseded request never reaches a probe: once the slot frees, the
	// promotion belongs to the newer command.
	gated.releaseAll()
	waitFor(t, "the newer command's relay", 5*time.Second, func() bool {
		return h.upstream.modelCommandVerdict(newer) == "completed"
	})
	jobs := h.jobs.probeJobsCreated()
	if len(jobs) != 2 || jobs[0] != "fm-catalog-"+occupying || jobs[1] != "fm-catalog-"+newer {
		t.Errorf("probe Jobs = %v, want the occupying and the newer command only", jobs)
	}
	if calls := h.upstream.pathCalls("/api/daemon/runtimes/rid-omp/models/" + waiting); len(calls) != 0 {
		t.Errorf("result forwards for the superseded request = %v, want none", calls)
	}
	body, writes := h.upstream.modelCommandRecord(waiting)
	if len(body) != 0 || writes != 0 {
		t.Errorf("server record for the superseded request = %q after %d writes, want nothing", body, writes)
	}
	waitFor(t, "the superseded request's window timeout", 4*time.Second, func() bool {
		return h.upstream.modelCommandVerdict(waiting) == "timeout"
	})
}

// noneSpans returns the spans of the abandoned-relay metric entries, in
// observation order.
func noneSpans(t *testing.T, metrics *recordingMetrics) []float64 {
	t.Helper()
	var spans []float64
	codes, seconds := metrics.codes(), metrics.seconds()
	if len(codes) != len(seconds) {
		t.Fatalf("metric faces disagree: %v codes, %v seconds", codes, seconds)
	}
	for i, code := range codes {
		if code == "none" {
			spans = append(spans, seconds[i])
		}
	}
	return spans
}

// reporterEvent returns the first event of one msg carrying a request id, so a
// scenario can pick one request's line out of a shared log stream.
func reporterEvent(t *testing.T, raw []byte, msg, requestID string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, msg) {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			continue
		}
		if fields["msg"] == msg && fields["request_id"] == requestID {
			return fields
		}
	}
	return nil
}
