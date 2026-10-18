// TC-tech-catalog-02 / AC-31: a probe that comes up and takes its command but
// never reports inside its own budget produces *no* result forward at all —
// Foreman assembles no failure body — and the server converges on its own
// window timeout instead. The failure is visible on the Foreman side as an
// event and as the abandoned-relay metric (ADR-019 决策结果 4/6, confirmation 5;
// 03-acceptance.md「模型目录」).
package acceptance

import (
	"log/slog"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/tsic404/foreman/internal/proxy"
)

// stallProbeDaemon plays a probe whose daemon registers and takes the command
// from its S5 ack but never posts the result: the「未在预算内上报」form. The
// returned channel yields the command id the ack handed it.
func (h *harness) stallProbeDaemon(t *testing.T) <-chan string {
	t.Helper()
	seen := make(chan string, 1)
	h.jobs.onCreateJob = func(job *batchv1.Job) {
		name := job.Name
		go func() {
			if command := h.probeTakeCommand(t, name); command != "" {
				seen <- command
			}
		}()
	}
	return seen
}

func TestCatalog02ProbeSilentWithinBudgetRelaysNothing(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	// The server's window and Foreman's own budget are both a scenario fixture:
	// the window is anchored at delivery, the budget at the ack's consumption.
	h.upstream.setModelWindow(200 * time.Millisecond)
	commands := h.stallProbeDaemon(t)

	cfg := h.cfg
	cfg.ModelCatalogTimeout = 800 * time.Millisecond
	logs := &writeBuffer{}
	reporter := proxy.NewModelCatalogReporter(cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	// The command must be on the server before the stream's first heartbeat:
	// the ack is its only delivery channel and the next one is a full C2
	// cadence away, so a request that lands after that beat would reach the
	// probe far outside this scenario's budget (§1.2 C2; §1.3 交付).
	id := h.requestModelList(t, "rid-omp")
	h.runHeartbeatStream(t, reporter)

	// The handover is polled, not waited out on a bare timer: the ack lands one
	// round after the stream starts, which is a scheduling step, not a deadline.
	var handed string
	waitFor(t, "the probe's S5 handover", 3*time.Second, func() bool {
		select {
		case handed = <-commands:
			return true
		default:
			return false
		}
	})
	if handed != id {
		t.Fatalf("probe was handed command %q, want %q verbatim", handed, id)
	}

	// Foreman gives up at its own budget and reports the abandoned relay.
	waitFor(t, "the abandoned relay", 3*time.Second, func() bool { return len(h.metrics.codes()) > 0 })
	if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "none" {
		t.Errorf("relay metric codes = %v, want one none (no relay happened)", codes)
	}
	event := findJSONEvent(t, logs.Bytes(), "model.catalog_relay_failed")
	if event == nil {
		t.Fatalf("failure event missing: %s", logs.String())
	}
	if event["request_id"] != id {
		t.Errorf("failure event = %+v, want request_id %q", event, id)
	}

	// No failure body is assembled: nothing reaches the server for this
	// request, and its record keeps nothing.
	if calls := h.upstream.pathCalls("/api/daemon/runtimes/rid-omp/models/" + id); len(calls) != 0 {
		t.Errorf("result forwards = %v, want none (a failure body is never assembled)", calls)
	}
	body, writes := h.upstream.modelCommandRecord(id)
	if len(body) != 0 || writes != 0 {
		t.Errorf("server record = %q after %d writes, want nothing", body, writes)
	}

	// The convergence face is the server's own window: the request times out
	// there, with no write of any kind.
	waitFor(t, "the server window to close", 2*time.Second, func() bool {
		return h.upstream.modelCommandVerdict(id) == "timeout"
	})

	// The probe's own objects do not outlive the attempt: the session goes
	// first, then the credential Secret, then the Job. All three are the wait's
	// condition — the session empties before the other two land, so stopping
	// there would judge the teardown mid-flight.
	jobs := h.jobs.probeJobsCreated()
	if len(jobs) != 1 {
		t.Fatalf("probe Jobs = %v, want exactly one for the command", jobs)
	}
	waitFor(t, "the probe teardown", 3*time.Second, func() bool {
		return len(h.probes.ListProbes()) == 0 && h.jobs.secretDeleted(jobs[0]+"-cred") && h.jobs.jobDeleted(jobs[0])
	})
}
