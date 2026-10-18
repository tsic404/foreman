// TC-tech-cache-03 covers the isolated-shape half of AC-12: with a template
// whose home/workspaces volumes are not hostPath, the rendered Job carries no
// host path, and the Foreman closed loop still holds — the terminal report
// reaches the server, the entry settles, the Job is deleted and the credential
// is reclaimed. The design case's node-side steps 1-3 run from
// tests/acceptance/tech-cache-03-cluster.sh; application-level success is
// outside this face (ADR-024).
package acceptance

import (
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/scheduler"
)

// tcCache03IsolatedTemplate declares the isolated shape: both shared volumes
// are emptyDir, so a rendered Job never carries a host path.
const tcCache03IsolatedTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
          volumeMounts:
            - { name: home, mountPath: /home/agent }
            - { name: workspaces, mountPath: /state/workspaces }
      volumes:
        - name: home
          emptyDir: {}
        - name: workspaces
          emptyDir: {}
`

func TestCache03IsolatedShapeKeepsForemanClosedLoop(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, writeTemplateFile(t, tcCache03IsolatedTemplate))
	tcPrepareRuntimes(t, h)

	payload := tcClaimPayloadBody{
		ID: "task-1", AgentID: "agent-1", WorkspaceID: "ws-1", RuntimeID: "rid-zz",
		IssueID: "issue-1", IssueIdentifier: "TSI-1",
		Agent: tcAgentObject{Name: "agent-1"},
	}
	tcClaimOnClaim(t, h, payload)
	job := h.createdJob(t, "fm-task-1")

	// The manifest face: no hostPath volume, and the two shared trees are the
	// template's emptyDir.
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.HostPath != nil {
			t.Errorf("volume %q is a hostPath (%q) in the isolated shape", volume.Name, volume.HostPath.Path)
		}
	}
	for _, name := range []string{"home", "workspaces"} {
		volume := volumeByName(t, job.Spec.Template.Spec.Volumes, name)
		if volume.EmptyDir == nil {
			t.Errorf("volume %q = %+v, want emptyDir in the isolated shape", name, volume.VolumeSource)
		}
	}

	// The Foreman closed loop (ADR-024's four items, minus the server-side
	// rows the stub seam stands in for).
	token := issueJobToken(t, h, "task-1")
	registerTaskDaemon(t, h, "task-1", token)
	claimTask(t, h, "task-1", token)
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/start", token, []byte(`{}`)); code != 200 {
		t.Fatalf("S8 = %d (%s), want 200", code, body)
	}
	if code, body := h.call("POST", "/api/daemon/tasks/task-1/complete", token, []byte(`{"result":"ok"}`)); code != 200 {
		t.Fatalf("S13 = %d (%s), want 200", code, body)
	}

	// ②-1 terminal report delivered and the projection settled.
	if got := h.server.forwarded(scheduler.EPComplete); len(got) != 1 {
		t.Errorf("terminal forwards = %d, want 1", len(got))
	}
	terminal := h.events("task.terminal")
	if len(terminal) != 1 {
		t.Fatalf("task.terminal events = %d, want 1", len(terminal))
	}
	if got, _ := terminal[0]["result"].(string); got != "completed" {
		t.Errorf("terminal result = %q, want completed", got)
	}
	settled, ok := h.reg.Settled("task-1")
	if !ok || settled.State != "terminal" || settled.Result != "completed" {
		t.Errorf("settled record = %s/%s (present %v), want terminal/completed", settled.State, settled.Result, ok)
	}

	// ②-2 Job deleted after the verdict.
	if !h.jobs.jobDeleted("fm-task-1") {
		t.Error("the Job must be deleted after the terminal report")
	}

	// ②-3 no zombie pod: the harness's Job client holds no pods for this Job.
	if pods, err := h.jobs.ListPods(t.Context(), "multica-agents"); err != nil || len(pods) != 0 {
		t.Errorf("pods = %d (%v), want none after the verdict", len(pods), err)
	}

	// ②-4 credential reclaimed: Secret gone, event carries the reason.
	if err := h.sched.OnJobTerminated(t.Context(), "fm-task-1",
		scheduler.JobTerminationReasonJobTerminated, time.Now(), 0); err != nil {
		t.Fatalf("OnJobTerminated: %v", err)
	}
	secretGone := false
	for _, name := range h.jobs.deletedSecrets {
		if name == "fm-task-1-cred" {
			secretGone = true
		}
	}
	if !secretGone {
		t.Errorf("deleted Secrets = %v, want fm-task-1-cred", h.jobs.deletedSecrets)
	}
	reclaimed := h.events("cred.reclaimed")
	if len(reclaimed) != 1 {
		t.Fatalf("cred.reclaimed events = %d, want 1", len(reclaimed))
	}
	if got, _ := reclaimed[0]["reason"].(string); got != scheduler.JobTerminationReasonJobTerminated {
		t.Errorf("reclaim reason = %q, want %q", got, scheduler.JobTerminationReasonJobTerminated)
	}
}
