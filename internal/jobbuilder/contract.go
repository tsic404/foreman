package jobbuilder

import (
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Label and annotation keys (contract §3.2, verbatim).
const (
	labelAppName     = "app.kubernetes.io/name"
	labelManagedBy   = "app.kubernetes.io/managed-by"
	labelTaskID      = "foreman.tsic.top/task-id"
	labelAgentID     = "foreman.tsic.top/agent-id"
	labelWorkspaceID = "foreman.tsic.top/workspace-id"

	annoIssueID         = "foreman.tsic.top/issue-id"
	annoIssueIdentifier = "foreman.tsic.top/issue-identifier"
	annoAutopilotRunID  = "foreman.tsic.top/autopilot-run-id"
	annoChatSessionID   = "foreman.tsic.top/chat-session-id"
	annoDaemonID        = "foreman.tsic.top/daemon-id"
	annoJobRuntimeID    = "foreman.tsic.top/job-runtime-id"
	annoClaimedAt       = "foreman.tsic.top/claimed-at"
	annoAttempt         = "foreman.tsic.top/attempt"
)

// Names the writers of both render paths share: the §3.2 domain, the probe
// render's literals (A1.4 ④) and the credential Secret carrier.
const (
	jobAppName       = "foreman-job"
	managedByForeman = "foreman"
	configFileKey    = "config.json"
	secretCredSuffix = "-cred"
	foremanDomain    = "foreman.tsic.top/"

	// jobProfile is the design constant JOB_PROFILE (ADR-013, 03-contracts
	// §3.1/§5.2): the daemon runs under this named profile and its config.json,
	// daemon.pid and daemon.log resolve to the profile directory. The probe
	// render renames this literal to its own profile (A1.4 ④), so it is also
	// the task-side anchor of the probe's profile directory.
	jobProfile = "foreman-job"
)

// identityLabels is the §3.2 Job label set.
func (b *Builder) identityLabels(e TaskEntry) map[string]string {
	return map[string]string{
		labelAppName:     jobAppName,
		labelManagedBy:   managedByForeman,
		labelTaskID:      e.TaskID,
		labelAgentID:     e.AgentID,
		labelWorkspaceID: e.WorkspaceID,
	}
}

// identityAnnotations is the §3.2 Job annotation set. The four trigger-context
// keys are conditional blocks (ADR-014): a key appears only when its TaskEntry
// field is non-empty — an empty placeholder is indistinguishable from a lost
// annotation on the authoritative Job object. The task payload never appears
// here: it stays in Foreman memory only (F3/ADR-007).
func (b *Builder) identityAnnotations(name string, e TaskEntry) map[string]string {
	annotations := map[string]string{
		annoDaemonID:     name,
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
// (03-contracts §5.4): the daemon's protocol surface, and the only env keys a
// deployment template does not get to keep. On the reference template the same
// three keys already carry these values, so the injection is idempotent there.
func injectedEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "MULTICA_SERVER_URL", Value: foremanServerURL},
		{Name: "MULTICA_DAEMON_ID", ValueFrom: envFieldRef("metadata.labels['job-name']")},
		{Name: "MULTICA_DAEMON_MAX_CONCURRENT_TASKS", Value: "1"},
	}
}

// foremanServerURL is the one server a Job daemon may dial (F5 收窄, §5.2).
const foremanServerURL = "http://foreman.foreman.svc.cluster.local:8080"

// envFieldRef is the fieldRef form §5.2 uses for the daemon identity keys.
func envFieldRef(fieldPath string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath}}
}

// requiredAnnotationKeys are the §3.2 keys every Job carries; the four
// trigger-context keys are conditional blocks (ADR-014), present only when
// their TaskEntry field is non-empty.
var (
	requiredAnnotationKeys       = []string{annoDaemonID, annoJobRuntimeID, annoClaimedAt, annoAttempt}
	triggerContextAnnotationKeys = []string{annoIssueID, annoIssueIdentifier, annoAutopilotRunID, annoChatSessionID}
)

func (b *Builder) credentialSecret(name, taskID, token string) *corev1.Secret {
	configJSON := `{"token":` + strconv.Quote(token) + `}`
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + secretCredSuffix,
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
