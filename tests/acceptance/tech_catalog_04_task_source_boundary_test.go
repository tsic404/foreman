// TC-tech-catalog-04 / AC-32: a task Job is never a model-catalog source
// (01-scope.md F9). Its heartbeat ack carries the two acknowledgment fields and
// no pending field of any name (02-contracts.md §2.2 S-5); its own Job Token on
// the result path is answered 404 and nothing is forwarded (§2.2 S-27); the
// real runtime's catalog slot reads back field for field unchanged (§1.3;
// 03-acceptance.md「模型目录」).
package acceptance

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestCatalog04TaskJobCannotFeedTheCatalog(t *testing.T) {
	h := newHarness(t, true)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	// A task in flight under rid-zz: that real runtime's catalog is the face
	// the F9 negative is about.
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	entry, ok := h.reg.Get("task-1")
	if !ok {
		t.Fatal("the claimed entry is missing")
	}
	token := h.builder.probeToken("fm-task-1")
	if token == "" {
		t.Fatal("the task Job has no Job Token")
	}
	registerTaskDaemon(t, h, "task-1", token)

	// The server holds a pending catalog request for the real runtime, and the
	// runtime's slot already carries a catalog: both are faces the task Job
	// must not reach.
	command := h.upstream.newModelCommand("rid-zz")
	const catalogBody = `{"models":[{"id":"m1","selector":"zz-default"}],"version":"v1"}`
	landedAt := time.Now().Add(-time.Minute).UTC()
	h.slots.Put("rid-zz", []byte(catalogBody), landedAt)
	before, ok := h.slots.Get("rid-zz")
	if !ok {
		t.Fatal("the real runtime's slot is missing after Put")
	}

	// ① The task Job's heartbeat ack: the two acknowledgment fields and nothing
	// else — no pending field of any name, so the daemon has no catalog request
	// to run.
	code, raw := h.call("POST", "/api/daemon/heartbeat", token,
		[]byte(`{"runtime_id":"`+entry.JobRuntimeID+`"}`))
	if code != 200 {
		t.Fatalf("task S5 = %d: %s", code, raw)
	}
	var ack map[string]json.RawMessage
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatalf("decode the task ack %s: %v", raw, err)
	}
	wantKeys := map[string]bool{"status": true, "runtime_id": true}
	for key := range ack {
		if !wantKeys[key] {
			t.Errorf("task ack carries %q: %s, want only status and runtime_id", key, raw)
		}
	}
	if len(ack) != len(wantKeys) {
		t.Errorf("task ack = %s, want both status and runtime_id", raw)
	}
	if delivered, completed := h.upstream.modelCommandState(command.id); delivered || completed {
		t.Errorf("server request %s = delivered %v / completed %v, want it untouched: a task Job is no delivery channel",
			command.id, delivered, completed)
	}
	if probes := h.jobs.probeJobsCreated(); len(probes) != 0 {
		t.Errorf("probe Jobs = %v, want none for a task Job's heartbeat", probes)
	}

	// ② The task Job's own token on the result path: 404, and nothing is
	// forwarded upstream.
	code, raw = h.call("POST",
		"/api/daemon/runtimes/"+entry.JobRuntimeID+"/models/"+command.id+"/result",
		token, []byte(`{"status":"completed","models":[{"id":"forged"}]}`))
	if code != 404 {
		t.Errorf("task S27 = %d: %s, want 404 (F9: a task Job is never a catalog source)", code, raw)
	}
	if calls := h.upstream.pathCalls("/api/daemon/runtimes/rid-zz/models/"); len(calls) != 0 {
		t.Errorf("C21 calls = %v, want none from a task Job", calls)
	}

	// The real runtime's catalog reads back field for field unchanged.
	after, ok := h.slots.Get("rid-zz")
	if !ok {
		t.Fatal("the real runtime's slot disappeared")
	}
	if !bytes.Equal(after.Body, before.Body) || !after.LandedAt.Equal(before.LandedAt) {
		t.Errorf("slot = %q @ %v, want %q @ %v unchanged", after.Body, after.LandedAt, before.Body, before.LandedAt)
	}
	var beforeFields, afterFields any
	if err := json.Unmarshal(before.Body, &beforeFields); err != nil {
		t.Fatalf("decode the slot before: %v", err)
	}
	if err := json.Unmarshal(after.Body, &afterFields); err != nil {
		t.Fatalf("decode the slot after: %v", err)
	}
	if !reflect.DeepEqual(beforeFields, afterFields) {
		t.Errorf("catalog fields changed: %v -> %v", beforeFields, afterFields)
	}
}
