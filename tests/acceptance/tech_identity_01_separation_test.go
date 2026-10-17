// TC-tech-identity-01 implements AC-50's identity-separation face (ADR-034
// decision 8): the daemon-id pod label and Job annotation, the Job Token's j
// claim and the per-task Secret name all derive from the identity name
// fm-<task_id>, never from the display name the template expresses; the Job's
// job-runtime-id annotation carries the entry's runtime id (uuid5 seeded by the
// daemon id); and a template writing fm-<task_id> keeps every value exactly as
// it is today.
package acceptance

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/auth"
)

// identity01ReadableTemplate makes the display name differ from the identity
// name: fm-<issue-segment>-<agent-segment>-<task-key>.
const identity01ReadableTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<issue-segment>-<agent-segment>-<task-key>
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: agent
          image: <job-image>
`

// identity01LegacyTemplate is the compatibility face: the display name is the
// identity name, so the three identity values must be untouched.
const identity01LegacyTemplate = `
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
`

// identity01Claim dispatches a claim whose agent name and issue identifier
// would render a readable display name, so the identity faces cannot be
// confused with it.
func identity01Claim(taskID string) json.RawMessage {
	return json.RawMessage(`{
		"id": "` + taskID + `", "agent_id": "agent-1", "workspace_id": "ws-1",
		"runtime_id": "rid-omp", "issue_id": "issue-1", "issue_identifier": "TSI 123",
		"agent": {"name": "OMP Bot"}
	}`)
}

// identity01Harness wires the real builder over the given template and
// performs the discovery, so a claim creates the Job and its credential.
func identity01Harness(t *testing.T, manifest string) *harness {
	t.Helper()
	h := newHarness(t, false)
	h.useRealBuilderWithTemplate(t, writeTemplateFile(t, manifest))
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())
	return h
}

// identity01SecretToken reads the Job Token the builder wrote into a
// credential Secret's config.json ({"token": "..."}).
func identity01SecretToken(t *testing.T, secret *corev1.Secret) string {
	t.Helper()
	raw := []byte(secret.StringData["config.json"])
	if len(raw) == 0 {
		raw = secret.Data["config.json"]
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode the credential Secret: %v", err)
	}
	if payload.Token == "" {
		t.Fatal("the credential Secret carries no token")
	}
	return payload.Token
}

// identity01Verify decodes the token with the real issuer, so the j claim is
// read off a signature-checked payload rather than a hand-parsed string.
func identity01Verify(t *testing.T, h *harness, token, taskID string) auth.Claims {
	t.Helper()
	claims, err := h.issuer.Verify(token, auth.RequestScope{TaskID: taskID})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	return claims
}

// TestIdentity01SeparateFromDisplayName (AC-50): under a readable-name
// template every identity value is fm-<task_id> — distinct from the display
// name — and MULTICA_DAEMON_ID reads that label, never the job-name label.
func TestIdentity01SeparateFromDisplayName(t *testing.T) {
	const taskID = "11111111-2222-3333-4444-555555555555"
	const displayName = "fm-tsi-123-omp-bot-555555555555"
	const wantDaemon = "fm-" + taskID

	h := identity01Harness(t, identity01ReadableTemplate)
	if err := h.sched.OnClaim(t.Context(), identity01Claim(taskID)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	job := h.createdJob(t, displayName)
	if job.Name != displayName {
		t.Fatalf("Job name = %q, want the display name %q", job.Name, displayName)
	}
	// The fixture must actually separate the two names, or every assertion
	// below is vacuous.
	if job.Name == wantDaemon {
		t.Fatalf("display name == identity name (%q): the readable template did not render", job.Name)
	}

	// Pod-template label: the identity name, and the fieldRef source.
	podDaemon := job.Spec.Template.Labels["foreman.tsic.top/daemon-id"]
	if podDaemon != wantDaemon {
		t.Errorf("pod daemon-id label = %q, want the identity name %q", podDaemon, wantDaemon)
	}
	if podDaemon == job.Name {
		t.Errorf("pod daemon-id label = %q: the identity followed the display name", podDaemon)
	}

	// Job annotation: the same identity name.
	if got := job.Annotations["foreman.tsic.top/daemon-id"]; got != wantDaemon {
		t.Errorf("job daemon-id annotation = %q, want the identity name %q", got, wantDaemon)
	}

	// job-runtime-id: the entry's own runtime id, uuid5-seeded by the daemon id.
	entry, ok := h.reg.Get(taskID)
	if !ok {
		t.Fatalf("task %s is not registered", taskID)
	}
	runtimeID := job.Annotations["foreman.tsic.top/job-runtime-id"]
	if runtimeID != entry.JobRuntimeID {
		t.Errorf("job-runtime-id annotation = %q, want the entry's %q", runtimeID, entry.JobRuntimeID)
	}
	if len(runtimeID) != 36 || runtimeID[14] != '5' || !strings.ContainsRune("89ab", rune(runtimeID[19])) {
		t.Errorf("job-runtime-id = %q, want a UUIDv5 shape", runtimeID)
	}

	// MULTICA_DAEMON_ID: a fieldRef to the daemon-id label, no literal value.
	daemonEnv := envByName(t, job.Spec.Template.Spec.Containers[0].Env, "MULTICA_DAEMON_ID")
	if daemonEnv.Value != "" || daemonEnv.ValueFrom == nil || daemonEnv.ValueFrom.FieldRef == nil ||
		daemonEnv.ValueFrom.FieldRef.FieldPath != "metadata.labels['foreman.tsic.top/daemon-id']" {
		t.Errorf("MULTICA_DAEMON_ID = %+v, want the daemon-id label fieldRef", daemonEnv)
	}
	if daemonEnv.ValueFrom != nil && daemonEnv.ValueFrom.FieldRef != nil &&
		daemonEnv.ValueFrom.FieldRef.FieldPath == "metadata.labels['job-name']" {
		t.Errorf("MULTICA_DAEMON_ID reads the job-name label: the display name would become the identity")
	}

	// Per-task Secret: fm-<task_id>-cred, never the display name.
	secret := h.createdSecret(t, wantDaemon+"-cred")
	if secret.Name != wantDaemon+"-cred" {
		t.Errorf("credential Secret name = %q, want %q", secret.Name, wantDaemon+"-cred")
	}

	// Job Token: the j claim is the daemon id, checked by the real issuer.
	claims := identity01Verify(t, h, identity01SecretToken(t, secret), taskID)
	if claims.JobName != wantDaemon {
		t.Errorf("token j claim = %q, want the identity name %q", claims.JobName, wantDaemon)
	}
	if claims.JobName == job.Name {
		t.Errorf("token j claim = %q: it followed the display name", claims.JobName)
	}
	if claims.TaskID != taskID {
		t.Errorf("token t claim = %q, want %q", claims.TaskID, taskID)
	}
}

// TestIdentity01LegacyTemplateKeepsIdentity (AC-50): a template writing
// metadata.name: fm-<task_id> yields exactly fm-<task_id> for the label, the
// annotation, the Secret name and the token's j claim — and the same values a
// readable-name template derives for the same task, proving the display name
// does not move the identity.
func TestIdentity01LegacyTemplateKeepsIdentity(t *testing.T) {
	const taskID = "task-1"
	const wantDaemon = "fm-" + taskID

	legacy := identity01Harness(t, identity01LegacyTemplate)
	if err := legacy.sched.OnClaim(t.Context(), identity01Claim(taskID)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	job := legacy.createdJob(t, wantDaemon)
	if job.Name != wantDaemon {
		t.Errorf("Job name = %q, want %q", job.Name, wantDaemon)
	}
	if got := job.Spec.Template.Labels["foreman.tsic.top/daemon-id"]; got != wantDaemon {
		t.Errorf("pod daemon-id label = %q, want %q", got, wantDaemon)
	}
	if got := job.Annotations["foreman.tsic.top/daemon-id"]; got != wantDaemon {
		t.Errorf("job daemon-id annotation = %q, want %q", got, wantDaemon)
	}
	secret := legacy.createdSecret(t, wantDaemon+"-cred")
	if secret.Name != wantDaemon+"-cred" {
		t.Errorf("credential Secret name = %q, want %q", secret.Name, wantDaemon+"-cred")
	}
	claims := identity01Verify(t, legacy, identity01SecretToken(t, secret), taskID)
	if claims.JobName != wantDaemon {
		t.Errorf("token j claim = %q, want %q", claims.JobName, wantDaemon)
	}

	// The same task under a readable-name template: the identity values are
	// byte-for-byte identical, so the name value does not change their
	// derivation.
	readable := identity01Harness(t, identity01ReadableTemplate)
	if err := readable.sched.OnClaim(t.Context(), identity01Claim(taskID)); err != nil {
		t.Fatalf("OnClaim: %v", err)
	}
	readableJob := readable.createdJob(t, "fm-tsi-123-omp-bot-task1")
	if readableJob.Name == job.Name {
		t.Fatalf("both templates rendered %q: the comparison would be vacuous", job.Name)
	}
	if got := readableJob.Spec.Template.Labels["foreman.tsic.top/daemon-id"]; got != wantDaemon {
		t.Errorf("readable-template pod daemon-id label = %q, want %q", got, wantDaemon)
	}
	if got := readableJob.Annotations["foreman.tsic.top/daemon-id"]; got != wantDaemon {
		t.Errorf("readable-template job daemon-id annotation = %q, want %q", got, wantDaemon)
	}
	if got := readableJob.Annotations["foreman.tsic.top/job-runtime-id"]; got != job.Annotations["foreman.tsic.top/job-runtime-id"] {
		t.Errorf("job-runtime-id changed with the display name: %q vs %q",
			got, job.Annotations["foreman.tsic.top/job-runtime-id"])
	}
	readableClaims := identity01Verify(t, readable, identity01SecretToken(t, readable.createdSecret(t, wantDaemon+"-cred")), taskID)
	if readableClaims.JobName != claims.JobName {
		t.Errorf("token j claim changed with the display name: %q vs %q", readableClaims.JobName, claims.JobName)
	}
}
