// TC-tech-catalog-01 / AC-31 (first clause): a model-list command for a real
// runtime whose slot is empty starts exactly one probe Job, the probe's own
// daemon is handed the command id verbatim, its result reaches the server byte
// for byte on the *real* runtime's path, and the probe's credential Secret and
// Job are torn down afterwards. The probe runs the deployment template's
// official daemon, so what lands is the node's own discovery output — Foreman
// assembles nothing (ADR-019 confirmation 1/2/3/6; 03-acceptance.md「模型目录」).
package acceptance

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

func TestCatalog01EmptySlotRelaysProbeResultVerbatim(t *testing.T) {
	h := newHarness(t, true)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	// The scenario's precondition: the runtime has no cached catalog, so the
	// command falls back to a fresh probe (§1.1 C21 第 12 条).
	if _, cached := h.slots.Get("rid-omp"); cached {
		t.Fatal("the runtime's catalog slot must start empty")
	}
	// A body only a byte-for-byte relay reproduces: re-encoding it would
	// normalize the spacing and the key order.
	const resultBody = `{"status":"completed","models":[{"id":"m1","selector":"omp-default"}],  "trailing":[1,2,3]}`
	commands := h.serveProbeDaemon(t, resultBody)

	logs := &writeBuffer{}
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, h.metrics,
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	h.runHeartbeatStream(t, reporter)

	id := h.requestModelList(t, "rid-omp")
	var handed string
	select {
	case handed = <-commands:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe daemon never received a command (S5 delivery)")
	}
	if handed != id {
		t.Fatalf("probe was handed command %q, want the ack's id %q verbatim", handed, id)
	}

	// One probe Job for the command, rendered from the deployment template: its
	// container runs the official daemon under the probe's own profile.
	jobs := h.jobs.probeJobsCreated()
	if len(jobs) != 1 {
		t.Fatalf("probe Jobs = %v, want exactly one for the command", jobs)
	}
	if _, ok := h.probes.ProbeByJob(jobs[0]); !ok {
		t.Errorf("probe session %s is missing while the probe is up", jobs[0])
	}
	job := h.createdJob(t, jobs[0])
	if job.Labels[registry.LabelRole] != registry.RoleCatalogProbe {
		t.Errorf("probe labels = %v, want the catalog-probe role", job.Labels)
	}
	if _, hasTask := job.Labels[registry.LabelTaskID]; hasTask {
		t.Error("a probe Job must never carry a task-id label")
	}
	agent := job.Spec.Template.Spec.Containers[0]
	if got := strings.Join(agent.Command, " "); got != "/usr/bin/dumb-init -- /usr/local/bin/multica" {
		t.Errorf("probe container command = %q, want the template's official daemon invocation", got)
	}
	if got := strings.Join(agent.Args, " "); got != "daemon start --foreground --profile foreman-catalog" {
		t.Errorf("probe container args = %q, want the daemon started under the probe profile", got)
	}

	// The relay: the probe's bytes and the command id verbatim, on the real
	// runtime's path — the probe's own runtime never appears in a C21 path.
	h.waitModelCompleted(t, id)
	relayed := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/"+id+"/result")
	if len(relayed) != 1 {
		t.Fatalf("C21 calls = %d, want exactly one relay", len(relayed))
	}
	if string(relayed[0].Body) != resultBody {
		t.Errorf("C21 body = %s, want the probe's bytes verbatim", relayed[0].Body)
	}
	for _, call := range h.upstream.pathCalls("/api/daemon/runtimes/") {
		if strings.HasSuffix(call.Path, "/models/"+id+"/result") && call.Path != relayed[0].Path {
			t.Errorf("result call %s, want the real runtime's path %s", call.Path, relayed[0].Path)
		}
	}
	// What the server holds is what the probe reported: nothing was assembled.
	body, writes := h.upstream.modelCommandRecord(id)
	if string(body) != resultBody || writes != 1 {
		t.Errorf("server record = %q adopted %d times, want the probe's bytes adopted once", body, writes)
	}
	if verdict := h.upstream.modelCommandVerdict(id); verdict != "completed" {
		t.Errorf("server verdict = %q, want completed", verdict)
	}

	// The Foreman-side landing face: the relayed event and its metric carry the
	// request id and the C21 status.
	h.waitCatalogMetrics(t, 1)
	if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "200" {
		t.Errorf("relay metric codes = %v, want one 200", codes)
	}
	if event := findJSONEvent(t, logs.Bytes(), "model.catalog_relayed"); event == nil {
		t.Errorf("relayed event missing: %s", logs.String())
	} else if event["request_id"] != id {
		t.Errorf("relayed event = %+v", event)
	}

	// Teardown: the session, the credential Secret and the Job all go.
	waitFor(t, "probe teardown", 3*time.Second, func() bool {
		return len(h.probes.ListProbes()) == 0 && h.jobs.secretDeleted(jobs[0]+"-cred") && h.jobs.jobDeleted(jobs[0])
	})
}
