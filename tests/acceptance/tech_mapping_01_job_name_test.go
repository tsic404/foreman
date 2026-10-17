// TC-tech-mapping-01 implements AC-04's identity face after the Job-name
// change (contract §3.1「任务 Job 名」/ §1.3): the name is a one-way
// derivation from the claim payload, the projection's job_name / daemon_id
// are that same name, the credential Secret is its `-cred`, and a task's
// objects are located by the task-id label — never by the name. It also
// covers the upgrade-window rule: a leftover Job of the same task under a
// name the current rule would not produce is cleared before the new Job is
// created.
package acceptance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
)

const mappingTaskID = "0199c0e4-1234-7abc-8def-550f91a80824"

// mappingClaim is the claim payload of the naming scenarios: an issue-triggered
// task that names its agent, so both readable segments carry values.
func mappingClaim(taskID string) []byte {
	return []byte(`{
		"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
		"issue_id": "issue-9", "issue_identifier": "TSI-9999",
		"runtime_id": "rid-omp", "agent": {"name": "Lynx", "model": "k3"}
	}`)
}

// TestMapping01JobNameDerivedAndProjected is AC-04's core assertion: the Job
// name, the daemon identity and the projection all carry the §3.1 name, and
// the credential Secret is derived from it — not from the task id.
func TestMapping01JobNameDerivedAndProjected(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	if err := h.sched.OnClaim(t.Context(), mappingClaim(mappingTaskID)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	const wantName = "fm-tsi-9999-lynx-550f91a80824"
	if !h.jobs.jobCreated(wantName) {
		t.Fatalf("created jobs do not contain %q", wantName)
	}
	if want := registry.CredName(wantName); !h.jobs.secretCreated(want) {
		t.Errorf("credential Secret %q was not created", want)
	}

	entry, ok := h.reg.Get(mappingTaskID)
	if !ok {
		t.Fatalf("task %s is not registered", mappingTaskID)
	}
	if entry.JobName != wantName || entry.DaemonID != wantName {
		t.Errorf("entry job_name/daemon_id = %q/%q, want %q for both", entry.JobName, entry.DaemonID, wantName)
	}
	if entry.JobRuntimeID == "" {
		t.Error("job_runtime_id must be derived from the Job name")
	}

	ops := httptest.NewServer(observability.NewOpsHandler(observability.NewMetrics(), h.reg, nil, observability.OpsConfig{}))
	t.Cleanup(ops.Close)

	code, body := httpGet(t, ops.URL+"/foreman/tasks/"+mappingTaskID)
	if code != http.StatusOK {
		t.Fatalf("GET /foreman/tasks/%s = %d (%s), want 200", mappingTaskID, code, body)
	}
	var projected map[string]any
	if err := json.Unmarshal(body, &projected); err != nil {
		t.Fatalf("decode projection: %v (%s)", err, body)
	}
	if projected["job_name"] != wantName || projected["daemon_id"] != wantName {
		t.Errorf("projection job_name/daemon_id = %v/%v, want %q",
			projected["job_name"], projected["daemon_id"], wantName)
	}
	if projected["issue_identifier"] != "TSI-9999" {
		t.Errorf("projection issue_identifier = %v, want TSI-9999", projected["issue_identifier"])
	}
}

// TestMapping01NameFallsBackWithoutContext pins the two fallback segments: a
// claim with neither an issue identifier nor an agent name still yields a
// legal, derivable name — the task is never refused for a missing context.
func TestMapping01NameFallsBackWithoutContext(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	const taskID = "0199c0e4-1234-7abc-8def-000000000001"
	payload := []byte(`{
		"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "agent": {"model": "k3"}
	}`)
	if err := h.sched.OnClaim(t.Context(), payload); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	const wantName = "fm-task-agent-000000000001"
	if !h.jobs.jobCreated(wantName) {
		t.Fatalf("created jobs do not contain %q", wantName)
	}
	entry, ok := h.reg.Get(taskID)
	if !ok || entry.JobName != wantName {
		t.Fatalf("entry job_name = %q, want %q", entry.JobName, wantName)
	}
}

// TestMapping01ForeignNamedJobOfSameTaskIsPurged covers the upgrade window
// (contract §1.3「旧名残留」): the previous version's name form cannot be
// reached by name any more, so the claim clears it by the task-id label —
// otherwise the task would end up with two Jobs. A Job of a *different* task
// is untouched.
func TestMapping01ForeignNamedJobOfSameTaskIsPurged(t *testing.T) {
	h := newHarness(t, false)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	oldName := "fm-" + mappingTaskID // the previous version's name form
	h.jobs.jobs = append(h.jobs.jobs, batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:   oldName,
		Labels: map[string]string{registry.LabelTaskID: mappingTaskID},
	}})
	h.jobs.existingJobs[oldName] = &h.jobs.jobs[len(h.jobs.jobs)-1]

	otherName := "fm-0199c0e4-1234-7abc-8def-0000000000ff"
	h.jobs.jobs = append(h.jobs.jobs, batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:   otherName,
		Labels: map[string]string{registry.LabelTaskID: "0199c0e4-1234-7abc-8def-0000000000ff"},
	}})

	if err := h.sched.OnClaim(t.Context(), mappingClaim(mappingTaskID)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}

	if !h.jobs.jobDeleted(oldName) {
		t.Errorf("the foreign-named Job %q of the same task must be removed", oldName)
	}
	if !h.jobs.secretDeleted(registry.CredName(oldName)) {
		t.Errorf("the foreign-named Secret %q must be removed with its Job", registry.CredName(oldName))
	}
	if h.jobs.jobDeleted(otherName) {
		t.Errorf("a Job of a different task (%q) must not be touched", otherName)
	}
	if !h.jobs.jobCreated("fm-tsi-9999-lynx-550f91a80824") {
		t.Error("the successor Job must be created under the derived name")
	}
}

// secretCreated reports whether the credential Secret was created.
func (c *jobClient) secretCreated(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, secret := range c.createdSecrets {
		if secret.Name == name {
			return true
		}
	}
	return false
}

// secretDeleted reports whether the Secret was deleted.
func (c *jobClient) secretDeleted(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, deleted := range c.deletedSecrets {
		if deleted == name {
			return true
		}
	}
	return false
}
