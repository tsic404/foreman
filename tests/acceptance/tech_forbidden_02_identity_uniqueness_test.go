// TC-tech-forbidden-02 (F2): no two Job pods share a daemon identity. Every
// rendered Job derives its own identity name (fm-<task_id>) into the pod
// label, the Job annotation, the env fieldRef and the credential Secret, and
// the two Jobs' identities and tokens differ. The render writes no node-level
// identity file, and every hostPath volume is a directory, not an identity
// carrier.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/jobbuilder"
)

func TestForbidden02DaemonIdentityIsPerJob(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	for _, taskID := range []string{"task-a", "task-b"} {
		tcClaimOnClaim(t, h, tcBaseClaim(taskID))
	}

	identities := map[string]bool{}
	tokens := map[string]bool{}
	for _, taskID := range []string{"task-a", "task-b"} {
		job := h.createdJob(t, "fm-"+taskID)
		secret := h.createdSecret(t, "fm-"+taskID+"-cred")
		wantIdentity := jobbuilder.DaemonIDFor(taskID)

		if got := job.Spec.Template.Labels["foreman.tsic.top/daemon-id"]; got != wantIdentity {
			t.Errorf("%s: pod daemon-id label = %q, want %q", taskID, got, wantIdentity)
		}
		if got := job.Annotations["foreman.tsic.top/daemon-id"]; got != wantIdentity {
			t.Errorf("%s: Job daemon-id annotation = %q, want %q", taskID, got, wantIdentity)
		}
		if secret.Name != jobbuilder.CredentialSecretName(wantIdentity) {
			t.Errorf("%s: credential Secret = %q, want the identity-derived name", taskID, secret.Name)
		}
		// The token's own claim names the identity, not a shared value.
		var config struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal([]byte(secret.StringData["config.json"]), &config); err != nil {
			t.Fatalf("%s: decode Secret: %v", taskID, err)
		}
		claims, err := h.issuer.Verify(config.Token, auth.RequestScope{TaskID: taskID})
		if err != nil {
			t.Fatalf("%s: verify Job Token: %v", taskID, err)
		}
		if claims.JobName != wantIdentity {
			t.Errorf("%s: token claim job = %q, want %q", taskID, claims.JobName, wantIdentity)
		}
		identities[wantIdentity] = true
		tokens[config.Token] = true
	}
	if len(identities) != 2 {
		t.Errorf("distinct daemon identities = %d, want 2", len(identities))
	}
	if len(tokens) != 2 {
		t.Errorf("distinct Job Tokens = %d, want 2", len(tokens))
	}

	// The env face reads the per-pod label, so a Job cannot inherit another
	// Job's identity; and no rendered object writes a node-level identity file.
	for _, taskID := range []string{"task-a", "task-b"} {
		job := h.createdJob(t, "fm-"+taskID)
		agent := tcJob09Container(t, job.Spec.Template.Spec.Containers, "agent")
		var found bool
		for _, env := range agent.Env {
			if env.Name != "MULTICA_DAEMON_ID" {
				continue
			}
			found = true
			if env.Value != "" {
				t.Errorf("%s: MULTICA_DAEMON_ID is a literal %q; it must read the pod label", taskID, env.Value)
			}
			if env.ValueFrom == nil || env.ValueFrom.FieldRef == nil ||
				env.ValueFrom.FieldRef.FieldPath != "metadata.labels['foreman.tsic.top/daemon-id']" {
				t.Errorf("%s: MULTICA_DAEMON_ID = %+v, want the pod-label fieldRef", taskID, env)
			}
		}
		if !found {
			t.Errorf("%s: MULTICA_DAEMON_ID is absent", taskID)
		}
		rendered, err := json.Marshal(job)
		if err != nil {
			t.Fatalf("marshal Job: %v", err)
		}
		if strings.Contains(string(rendered), "daemon.id") {
			t.Errorf("%s: the render writes a node-level daemon.id file", taskID)
		}
		for _, volume := range job.Spec.Template.Spec.Volumes {
			if volume.HostPath == nil {
				continue
			}
			if volume.HostPath.Type == nil || *volume.HostPath.Type != corev1.HostPathDirectoryOrCreate {
				t.Errorf("%s: hostPath volume %s = %+v, want a directory carrier", taskID, volume.Name, volume.HostPath)
			}
		}
	}
}
