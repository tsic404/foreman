package jobbuilder

import (
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Label and annotation keys (verbatim).
const (
	labelAppName     = "app.kubernetes.io/name"
	labelManagedBy   = "app.kubernetes.io/managed-by"
	labelTaskID      = "foreman.tsic.top/task-id"
	labelAgentID     = "foreman.tsic.top/agent-id"
	labelWorkspaceID = "foreman.tsic.top/workspace-id"
	// labelDaemonID is the pod-template label carrying the identity name
	// fm-<task_id> — the MULTICA_DAEMON_ID fieldRef source and the reason the
	// display name can change without moving any identity (ADR-034 decision 8,
	// §4.3).
	labelDaemonID = "foreman.tsic.top/daemon-id"

	annoIssueID         = "foreman.tsic.top/issue-id"
	annoIssueIdentifier = "foreman.tsic.top/issue-identifier"
	annoAutopilotRunID  = "foreman.tsic.top/autopilot-run-id"
	annoChatSessionID   = "foreman.tsic.top/chat-session-id"
	annoDaemonID        = "foreman.tsic.top/daemon-id"
	annoJobRuntimeID    = "foreman.tsic.top/job-runtime-id"
	annoClaimedAt       = "foreman.tsic.top/claimed-at"
	annoAttempt         = "foreman.tsic.top/attempt"
)

// Names the writers of both render paths share: the Foreman-owned domain, the
// probe render's literals (A1.4 ④) and the credential Secret carrier.
const (
	jobAppName       = "foreman-job"
	managedByForeman = "foreman"
	configFileKey    = "config.json"
	secretCredSuffix = "-cred"
	foremanDomain    = "foreman.tsic.top/"

	// jobProfile is the design's fixed profile name (ADR-013, §6.2): the daemon
	// runs under this named profile and its config.json,
	// daemon.pid and daemon.log resolve to the profile directory. The probe
	// render renames this literal to its own profile (A1.4 ④), so it is also
	// the task-side anchor of the probe's profile directory.
	jobProfile = "foreman-job"
)

// identityLabels is the Job label set (§4.3).
func (b *Builder) identityLabels(e TaskEntry) map[string]string {
	return map[string]string{
		labelAppName:     jobAppName,
		labelManagedBy:   managedByForeman,
		labelTaskID:      e.TaskID,
		labelAgentID:     e.AgentID,
		labelWorkspaceID: e.WorkspaceID,
	}
}

// identityAnnotations is the Job annotation set (§4.3). daemon-id carries the
// identity name fm-<task_id>, never the display name the template expresses
// (ADR-034 decision 8). The four trigger-context keys are conditional blocks
// (ADR-014): a key appears only when its TaskEntry field is non-empty — an
// empty placeholder is indistinguishable from a lost annotation on the
// authoritative Job object. The task payload never appears here: it stays in
// Foreman memory only (F3/ADR-007).
func (b *Builder) identityAnnotations(daemonID string, e TaskEntry) map[string]string {
	annotations := map[string]string{
		annoDaemonID:     daemonID,
		annoJobRuntimeID: e.JobRuntimeID,
		annoClaimedAt:    e.ClaimedAt.UTC().Format(time.RFC3339),
		annoAttempt:      strconv.Itoa(e.Attempt),
	}
	for key, value := range map[string]string{
		annoIssueID:         e.IssueID,
		annoIssueIdentifier: e.IssueIdentifier,
		annoAutopilotRunID:  e.AutopilotRunID,
		annoChatSessionID:   e.ChatSessionID,
	} {
		if value != "" {
			annotations[key] = value
		}
	}
	return annotations
}

// injectedEnv is the A1.1 set Foreman writes into a template's first container
// (§8.2 A1.1): the daemon's protocol surface, and the only env keys a
// deployment template does not get to keep. MULTICA_DAEMON_ID reads the
// pod-template label Foreman writes with the identity name — not the Job
// controller's job-name label, which now carries the template's display name
// (ADR-034 decision 8 / §8.2 A1.1).
func injectedEnv(serverURL string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "MULTICA_SERVER_URL", Value: serverURL},
		{Name: "MULTICA_DAEMON_ID", ValueFrom: envFieldRef("metadata.labels['" + labelDaemonID + "']")},
		{Name: "MULTICA_DAEMON_MAX_CONCURRENT_TASKS", Value: "1"},
	}
}

// envFieldRef is the fieldRef form A1.1 uses for the daemon identity keys.
func envFieldRef(fieldPath string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath}}
}

// requiredAnnotationKeys are the keys every Job carries (§4.3); the four
// trigger-context keys are conditional blocks (ADR-014), present only when
// their TaskEntry field is non-empty.
var (
	requiredAnnotationKeys       = []string{annoDaemonID, annoJobRuntimeID, annoClaimedAt, annoAttempt}
	triggerContextAnnotationKeys = []string{annoIssueID, annoIssueIdentifier, annoAutopilotRunID, annoChatSessionID}
)

// credentialSecret is the per-task Secret. Its name derives from the identity
// name (fm-<task_id>-cred), so it never follows the template's display name
// (ADR-034 decision 8, §3).
func (b *Builder) credentialSecret(daemonID, taskID, token string) *corev1.Secret {
	configJSON := `{"token":` + strconv.Quote(token) + `}`
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CredentialSecretName(daemonID),
			Namespace: b.cfg.JobNamespace,
			Labels: map[string]string{
				labelManagedBy: managedByForeman,
				labelTaskID:    taskID,
			},
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{configFileKey: configJSON},
	}
}
