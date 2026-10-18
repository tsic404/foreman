// TC-tech-catalog-03 / AC-31: repeats on the result path are idempotent, and a
// forward that lands past the server's window is answered like any success but
// changes nothing — "转发成功" is not "目录已更新", so the judgement reads the
// server's request record back (02-contracts.md §1.3, AC-31 第二子句;
// 03-acceptance.md「模型目录」).
package acceptance

import (
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
)

// delayedProbeDaemon plays a probe whose daemon registers, takes its command and
// reports it only after delay — the late-forward form. The returned channel
// yields the code the daemon's own S27 answer carried.
func (h *harness) delayedProbeDaemon(t *testing.T, resultBody string, delay time.Duration) <-chan int {
	t.Helper()
	codes := make(chan int, 1)
	h.jobs.onCreateJob = func(job *batchv1.Job) {
		name := job.Name
		go func() {
			command := h.probeTakeCommand(t, name)
			if command == "" {
				return
			}
			session, ok := h.probes.ProbeByJob(name)
			if !ok {
				t.Errorf("probe session %s is gone before its result", name)
				return
			}
			time.Sleep(delay)
			code, body, err := h.probeCall("POST",
				"/api/daemon/runtimes/"+session.JobRuntime+"/models/"+command+"/result",
				h.builder.probeToken(name), []byte(resultBody))
			if err != nil {
				t.Errorf("probe S27: %v", err)
				return
			}
			codes <- code
			_ = body
		}()
	}
	return codes
}

// TestCatalog03DuplicateForwardIsIdempotent drives the C21 retry rule: the
// server took the result but its answer was lost, so Foreman forwards the same
// bytes again. The repeat is accepted like the first, and the record holds the
// same result — nothing is rejected as a duplicate (§1.1 C21 重试规则).
func TestCatalog03DuplicateForwardIsIdempotent(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	const resultBody = `{"status":"completed","models":[{"id":"m1","selector":"omp-default"}]}`
	h.serveProbeDaemon(t, resultBody)
	reporter := h.newReporter(t)

	id := h.requestModelList(t, "rid-omp")
	// The first answer is the one that never arrives: program it before the ack
	// hands the command out.
	h.upstream.programDroppedModelResponse(id)
	h.runHeartbeatStream(t, reporter)

	relayPath := "/api/daemon/runtimes/rid-omp/models/" + id + "/result"
	waitFor(t, "the repeat forward", 5*time.Second, func() bool {
		return len(h.upstream.calls("POST", relayPath)) == 2
	})
	relayed := h.upstream.calls("POST", relayPath)
	if string(relayed[0].Body) != resultBody || string(relayed[1].Body) != resultBody {
		t.Errorf("C21 bodies = %s / %s, want the probe's bytes on both attempts", relayed[0].Body, relayed[1].Body)
	}
	body, writes := h.upstream.modelCommandRecord(id)
	if string(body) != resultBody || writes != 2 {
		t.Errorf("server record = %q after %d writes, want the repeat adopted like the first", body, writes)
	}
	if verdict := h.upstream.modelCommandVerdict(id); verdict != "completed" {
		t.Errorf("server verdict = %q, want completed", verdict)
	}

	// The relay ends as a success, not as an abandoned one: the repeat is what
	// carried it.
	h.waitCatalogMetrics(t, 1)
	if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "200" {
		t.Errorf("relay metric codes = %v, want one 200", codes)
	}
}

// TestCatalog03LateForwardIsIgnored reports after the server's window has
// closed: the forward is answered like any other success, but the record keeps
// nothing — a scenario reading only "转发成功" would call this a landing.
func TestCatalog03LateForwardIsIgnored(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	h.upstream.setModelWindow(200 * time.Millisecond)

	const resultBody = `{"status":"completed","models":[{"id":"m1"}]}`
	reportCodes := h.delayedProbeDaemon(t, resultBody, 400*time.Millisecond)
	reporter := h.newReporter(t)

	id := h.requestModelList(t, "rid-omp")
	h.runHeartbeatStream(t, reporter)

	select {
	case code := <-reportCodes:
		if code != 200 {
			t.Fatalf("the daemon's S27 answer = %d, want the late forward answered like a success", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the probe daemon never reported")
	}
	relayed := h.upstream.calls("POST", "/api/daemon/runtimes/rid-omp/models/"+id+"/result")
	if len(relayed) != 1 || string(relayed[0].Body) != resultBody {
		t.Fatalf("C21 calls = %d (%s), want the late forward once, bytes verbatim", len(relayed), relayed)
	}

	// Read the server's record back: the forward changed nothing there.
	body, writes := h.upstream.modelCommandRecord(id)
	if len(body) != 0 || writes != 0 {
		t.Errorf("server record = %q after %d writes, want the late forward to change nothing", body, writes)
	}
	if verdict := h.upstream.modelCommandVerdict(id); verdict != "timeout" {
		t.Errorf("server verdict = %q, want the window's timeout", verdict)
	}

	// Foreman records the status the forward was answered with — its own metric
	// can never claim the catalog was updated.
	h.waitCatalogMetrics(t, 1)
	if codes := h.metrics.codes(); len(codes) != 1 || codes[0] != "200" {
		t.Errorf("relay metric codes = %v, want the C21 status", codes)
	}
}
