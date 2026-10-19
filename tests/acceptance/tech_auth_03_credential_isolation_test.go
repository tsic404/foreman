// TC-tech-auth-03 (AC-06, F3 negative): the Job side carries no server-side
// long-lived credential. The scan covers every face Foreman writes — the Job
// object (spec, env, labels, annotations), the per-task Secret and the
// trigger-context annotations — with a claim payload that deliberately carries
// both a server credential shape and the task-level token, so a leak of either
// is observable rather than assumed.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/auth"
)

// tcCredentialObjectJSON renders the objects Foreman wrote into one string for
// the credential scan.
func tcCredentialObjectJSON(t *testing.T, objects ...any) string {
	t.Helper()
	var b strings.Builder
	for _, object := range objects {
		raw, err := json.Marshal(object)
		if err != nil {
			t.Fatalf("marshal object for the credential scan: %v", err)
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	return b.String()
}

// tcCredentialNoServerSecret asserts no mdt_/mul_ credential shape and none of
// the given literals appears in the scanned face.
func tcCredentialNoServerSecret(t *testing.T, label, raw string, literals ...string) {
	t.Helper()
	if auth.ContainsServerCredential(raw) {
		t.Errorf("%s carries an mdt_/mul_ server credential shape", label)
	}
	for _, literal := range literals {
		if literal == "" {
			continue
		}
		if strings.Contains(raw, literal) {
			t.Errorf("%s leaks %q", label, literal)
		}
	}
}

// tcCredentialScanTaskRender claims one task through the production builder and
// returns the rendered Job and Secret.
func tcCredentialScanTaskRender(t *testing.T, taskID, payload string) (*harness, *batchv1.Job, *corev1.Secret) {
	t.Helper()
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	if err := h.sched.OnClaim(t.Context(), json.RawMessage(payload)); err != nil {
		t.Fatalf("OnClaim(%s): %v", taskID, err)
	}
	return h, h.createdJob(t, "fm-"+taskID), h.createdSecret(t, "fm-"+taskID+"-cred")
}

func TestAuth03JobFaceCarriesNoServerCredential(t *testing.T) {
	const payload = `{
		"id": "task-1", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI-1",
		"auth_token": "mdt_server_credential_in_payload",
		"task_token": "mat_task_level_token",
		"agent": {"model": "k3", "prompt": "do not persist"}
	}`
	h, job, secret := tcCredentialScanTaskRender(t, "task-1", payload)

	raw := tcCredentialObjectJSON(t, job, secret)
	tcCredentialNoServerSecret(t, "the rendered Job and Secret", raw,
		"mdt_server_credential_in_payload", "mat_task_level_token", "do not persist")

	// The only credential Foreman writes is the per-task Job Token, and it
	// lives in exactly one place: the Secret's single config.json entry.
	if len(secret.StringData) != 1 || len(secret.Data) != 0 {
		t.Fatalf("credential Secret keys = %v / %v, want exactly config.json", secret.StringData, secret.Data)
	}
	config := secret.StringData["config.json"]
	var parsed struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(config), &parsed); err != nil {
		t.Fatalf("credential Secret config.json: %v", err)
	}
	if !strings.HasPrefix(parsed.Token, "fmj_") {
		t.Errorf("Secret token = %q, want the per-task Job Token (fmj_)", parsed.Token)
	}
	if strings.Count(raw, parsed.Token) != 1 {
		t.Errorf("the Job Token appears %d times in the write face, want only the Secret",
			strings.Count(raw, parsed.Token))
	}

	// The annotations carry the identity keys and the trigger context — never
	// the payload body.
	for key, value := range job.Annotations {
		if strings.Contains(value, "mdt_") || strings.Contains(value, "mat_") {
			t.Errorf("annotation %q carries a credential: %q", key, value)
		}
	}
	if h.createdSecret(t, "fm-task-1-cred").Name != secret.Name {
		t.Errorf("the rendered Secret is not the one the harness recorded")
	}
}
