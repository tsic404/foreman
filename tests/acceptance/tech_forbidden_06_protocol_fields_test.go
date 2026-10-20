// TC-tech-forbidden-06 (F6): Foreman depends on no protocol field that does
// not exist. A task Job's heartbeat ack carries no pending/poll/delay hint; the
// delivered claim payload adds no top-level prompt or model key the payload did
// not carry; and the S-4 answer's runtime entry keeps exactly the contract's
// field set. The positive path still works with none of the invented fields
// present.
package acceptance

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestForbidden06NoDependencyOnAbsentProtocolFields(t *testing.T) {
	h := newDeliveryHarness(t)
	if err := h.sched.OnClaim(t.Context(), deliveryPayload("task-1")); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	token := issueJobToken(t, h, "task-1")

	// S-4's answer: one runtime entry with exactly the contract's fields.
	code, registerBody := h.call("POST", "/api/daemon/register", token, []byte(`{
		"daemon_id": "fm-task-1",
		"runtimes": [{"name": "foreman-zz", "type": "zz", "version": "3.1.4"}]
	}`))
	if code != http.StatusOK {
		t.Fatalf("S4 = %d (%s), want 200", code, registerBody)
	}
	var register struct {
		Runtimes []map[string]json.RawMessage `json:"runtimes"`
	}
	if err := json.Unmarshal(registerBody, &register); err != nil || len(register.Runtimes) != 1 {
		t.Fatalf("S4 body = %s (%v)", registerBody, err)
	}
	wantRuntimeKeys := map[string]bool{"id": true, "name": true, "provider": true, "status": true, "profile_id": true}
	for key := range register.Runtimes[0] {
		if !wantRuntimeKeys[key] {
			t.Errorf("S4 runtime entry invents field %q", key)
		}
	}
	for key := range wantRuntimeKeys {
		if _, ok := register.Runtimes[0][key]; !ok {
			t.Errorf("S4 runtime entry lost field %q", key)
		}
	}

	// S-5's ack: no pending, polling or delayed-task hint for a task Job.
	code, ackBody := h.call("POST", "/api/daemon/heartbeat", token, []byte(`{"runtime_id":"`+runtimeIDOf(t, h, "task-1")+`"}`))
	if code != http.StatusOK {
		t.Fatalf("S5 = %d (%s), want 200", code, ackBody)
	}
	var ack map[string]json.RawMessage
	if err := json.Unmarshal(ackBody, &ack); err != nil {
		t.Fatalf("S5 body = %s (%v)", ackBody, err)
	}
	if len(ack) != 2 {
		t.Errorf("S5 ack keys = %v, want exactly status and runtime_id", ack)
	}
	for key := range ack {
		lower := strings.ToLower(key)
		for _, hint := range []string{"pending", "poll", "delay", "wakeup"} {
			if strings.Contains(lower, hint) {
				t.Errorf("S5 ack carries the hint field %q, which the task face must not emit", key)
			}
		}
	}

	// S-6's payload: no top-level prompt/model/thinking key is synthesized.
	delivered := claimTask(t, h, "task-1", token)
	var answer struct {
		Tasks []map[string]json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(delivered, &answer); err != nil || len(answer.Tasks) != 1 {
		t.Fatalf("S6 = %s (%v)", delivered, err)
	}
	for _, invented := range []string{"prompt", "model", "thinking", "system_prompt"} {
		if _, ok := answer.Tasks[0][invented]; ok {
			t.Errorf("delivered payload invents top-level %q", invented)
		}
	}
	if _, ok := answer.Tasks[0]["agent"]; !ok {
		t.Error("delivered payload lost the agent object")
	}
}

// runtimeIDOf returns the entry's job runtime id.
func runtimeIDOf(t *testing.T, h *harness, taskID string) string {
	t.Helper()
	entry, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("entry %s missing", taskID)
	}
	return entry.JobRuntimeID
}
