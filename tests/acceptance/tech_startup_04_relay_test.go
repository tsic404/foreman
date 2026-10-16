// TC-tech-startup-04 exercises the command-driven catalog path end to end with
// the production components: a C2 ack's pending_model_list starts one probe Job
// (real jobbuilder), the probe daemon registers, receives the command through
// its S5 ack, reports through S27, and Foreman relays the result verbatim as
// C21. The only fake is the Kubernetes API.
package acceptance

import (
	"encoding/json"
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
	commandSeen := make(chan string, 1)
	// The probe pod's daemon: register (S4), take the command from the S5 ack,
	// then report it (S27). It runs off the CreateJob call so the scheduler's
	// start path is not held up by the HTTP round trips.
	h.jobs.onCreateJob = func(job *batchv1Job) {
		name := job.Name
		go func() {
			token := h.builder.probeToken(name)
			if token == "" {
				return
			}
			code, body := h.call("POST", "/api/daemon/register", token, []byte(`{
				"daemon_id": "`+name+`",
				"cli_version": "0.6.1",
				"runtimes": [{"name": "foreman-omp", "type": "omp", "version": "0.6.1"}]
			}`))
			if code != 200 {
				t.Errorf("probe S4 = %d: %s", code, body)
				return
			}
			session, ok := h.probes.ProbeByJob(name)
			if !ok {
				t.Errorf("probe session %s missing at S4", name)
				return
			}
			code, body = h.call("POST", "/api/daemon/heartbeat", token,
				[]byte(`{"runtime_id": "`+session.JobRuntime+`"}`))
			if code != 200 {
				t.Errorf("probe S5 = %d: %s", code, body)
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
			commandSeen <- ack.PendingModelList.ID
			code, body = h.call("POST",
				"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+ack.PendingModelList.ID+"/result",
				token, []byte(resultBody))
			if code != 200 {
				t.Errorf("probe S27 = %d: %s", code, body)
			}
		}()
	}

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

// writeBuffer is a tiny io.Writer for the reporter's logger.
type writeBuffer struct{ data []byte }

func (b *writeBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *writeBuffer) Bytes() []byte { return b.data }

func (b *writeBuffer) String() string { return string(b.data) }

var _ io.Writer = (*writeBuffer)(nil)
