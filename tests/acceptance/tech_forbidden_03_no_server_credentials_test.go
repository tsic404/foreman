// TC-tech-forbidden-03 (F3): the Job face carries no server-side credential.
// The claim payload deliberately carries an mdt_ shape; the rendered Job, its
// annotations/labels/env and the per-task Secret are scanned for that value
// and for the harness's own server credential. Only the per-task Job Token
// may appear, and only in the Secret.
package acceptance

import (
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/auth"
)

func TestForbidden03NoServerCredentialInTheJobFace(t *testing.T) {
	const payload = `{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1",
		"auth_token": "mdt_server_side_credential",
		"agent": {"model": "k3"}
	}`
	_, job, secret := tcCredentialScanTaskRender(t, "task-1", payload)
	raw := tcCredentialObjectJSON(t, job, secret)

	tcCredentialNoServerSecret(t, "the rendered Job and Secret", raw,
		"mdt_server_side_credential", "mdt_test")

	// No env value is a server credential shape.
	for _, container := range job.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			if auth.ContainsServerCredential(env.Value) {
				t.Errorf("container %s env %s carries a server credential", container.Name, env.Name)
			}
		}
	}
	for _, container := range job.Spec.Template.Spec.InitContainers {
		for _, env := range container.Env {
			if auth.ContainsServerCredential(env.Value) {
				t.Errorf("init container %s env %s carries a server credential", container.Name, env.Name)
			}
		}
	}

	// The one credential Foreman writes is the per-task Job Token.
	if len(secret.StringData) != 1 {
		t.Fatalf("credential Secret keys = %v, want one", secret.StringData)
	}
	if !strings.Contains(secret.StringData["config.json"], "fmj_") {
		t.Errorf("credential Secret = %q, want the per-task Job Token", secret.StringData["config.json"])
	}
}
