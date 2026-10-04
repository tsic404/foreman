package jobbuilder

import (
	"encoding/json"
	"path"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	annoDaemonID        = "foreman.tsic.top/daemon-id"
	annoJobRuntimeID    = "foreman.tsic.top/job-runtime-id"
	annoClaimedAt       = "foreman.tsic.top/claimed-at"
	annoAttempt         = "foreman.tsic.top/attempt"
	annoPayload         = "foreman.tsic.top/payload"
)

// Template constants fixed by the job-template module design (not configurable).
const (
	jobAppName        = "foreman-job"
	managedByForeman  = "foreman"
	agentUID          = 1000
	agentGID          = 1000
	terminationGraceS = 30
	jobServiceAccount = "multica-omp"
	foremanServerURL  = "http://foreman.foreman.svc.cluster.local:8080"
	configFileKey     = "config.json"
	credFileMode      = 0o400
	secretCredSuffix  = "-cred"
)

// initScript copies the Job Token out of the read-only Secret mount and fixes
// hostPath ownership; it is the only code that runs as root (ADR-005).
const initScript = `cp /cred/config.json /home/agent/cred/config.json
chmod 0400 /home/agent/cred/config.json
chown -R 1000:1000 /home/agent /state/workspaces
`

var (
	tmpVolumeSize  = resource.MustParse("2Gi")
	credVolumeSize = resource.MustParse("16Mi")

	initCPURequest    = resource.MustParse("20m")
	initMemoryRequest = resource.MustParse("32Mi")
	initCPULimit      = resource.MustParse("200m")
	initMemoryLimit   = resource.MustParse("128Mi")
)

func (b *Builder) job(name string, e TaskEntry, payload json.RawMessage) *batchv1.Job {
	annotations := map[string]string{
		annoIssueID:         e.IssueID,
		annoIssueIdentifier: e.IssueIdentifier,
		annoDaemonID:        name,
		annoJobRuntimeID:    e.JobRuntimeID,
		annoClaimedAt:       e.ClaimedAt.UTC().Format(time.RFC3339),
		annoAttempt:         strconv.Itoa(e.Attempt),
	}
	if encoded, ok := sanitizePayload(payload); ok {
		annotations[annoPayload] = encoded
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: b.cfg.JobNamespace,
			Labels: map[string]string{
				labelAppName:     jobAppName,
				labelManagedBy:   managedByForeman,
				labelTaskID:      e.TaskID,
				labelAgentID:     e.AgentID,
				labelWorkspaceID: e.WorkspaceID,
			},
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            new(int32(0)),
			Completions:             new(int32(1)),
			Parallelism:             new(int32(1)),
			TTLSecondsAfterFinished: new(b.cfg.JobTTLSeconds),
			Template:                b.podTemplate(name, e),
		},
	}
	if b.cfg.TaskMaxDuration > 0 {
		job.Spec.ActiveDeadlineSeconds = new(int64(b.cfg.TaskMaxDuration / time.Second))
	}
	return job
}

func (b *Builder) podTemplate(name string, e TaskEntry) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				labelAppName: jobAppName,
				labelTaskID:  e.TaskID,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: new(int64(terminationGraceS)),
			ServiceAccountName:            jobServiceAccount,
			AutomountServiceAccountToken:  new(false),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:      new(int64(agentUID)),
				RunAsGroup:     new(int64(agentGID)),
				FSGroup:        new(int64(agentGID)),
				RunAsNonRoot:   new(true),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			ImagePullSecrets: b.imagePullSecrets(),
			NodeSelector:     b.cfg.NodeSelector,
			Tolerations:      b.cfg.Tolerations,
			Affinity:         b.affinity(e),
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew:           3,
				TopologyKey:       "kubernetes.io/hostname",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{labelAppName: jobAppName}},
			}},
			InitContainers: []corev1.Container{b.prepareContainer()},
			Containers:     []corev1.Container{b.agentContainer()},
			Volumes:        b.volumes(name),
		},
	}
}

func (b *Builder) affinity(e TaskEntry) *corev1.Affinity {
	node := b.preferredNode(e)
	if node == "" {
		return nil
	}
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
				Weight: 100,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{node},
					}},
				},
			}},
		},
	}
}

func (b *Builder) prepareContainer() corev1.Container {
	return corev1.Container{
		Name:            "prepare",
		Image:           b.imageRef(),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/bin/sh", "-ec"},
		Args:            []string{initScript},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                new(int64(0)),
			RunAsNonRoot:             new(false),
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: initCPURequest, corev1.ResourceMemory: initMemoryRequest},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: initCPULimit, corev1.ResourceMemory: initMemoryLimit},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "home", MountPath: "/home/agent"},
			{Name: "workspaces", MountPath: "/state/workspaces"},
			{Name: "cred", MountPath: "/home/agent/cred"},
			{Name: "cred-src", MountPath: "/cred", ReadOnly: true},
		},
	}
}

func (b *Builder) agentContainer() corev1.Container {
	return corev1.Container{
		Name:            "agent",
		Image:           b.imageRef(),
		ImagePullPolicy: corev1.PullIfNotPresent,
		WorkingDir:      "/home/agent",
		Command:         []string{"/usr/local/bin/multica"},
		Args:            []string{"daemon", "start", "--foreground"},
		TTY:             true, // daemon writes logs to stderr only when it is a terminal
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                new(int64(agentUID)),
			RunAsNonRoot:             new(true),
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: b.cfg.CPURequest, corev1.ResourceMemory: b.cfg.MemoryRequest},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: b.cfg.CPULimit, corev1.ResourceMemory: b.cfg.MemoryLimit},
		},
		Env: agentEnv(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "home", MountPath: "/home/agent"},
			{Name: "cred", MountPath: "/home/agent/cred"},
			{Name: "workspaces", MountPath: "/state/workspaces"},
			{Name: "tmp", MountPath: "/tmp"},
		},
	}
}

// agentEnv is the contract §5.2 env set: exactly 16 entries, fixed order.
func agentEnv() []corev1.EnvVar {
	fieldRef := func(fieldPath string) *corev1.EnvVarSource {
		return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath}}
	}
	return []corev1.EnvVar{
		{Name: "MULTICA_SERVER_URL", Value: foremanServerURL},
		{Name: "MULTICA_TASK_CONFIG_ROOT", Value: "/home/agent/cred"},
		{Name: "MULTICA_DAEMON_ID", ValueFrom: fieldRef("metadata.labels['job-name']")},
		{Name: "MULTICA_DAEMON_DEVICE_NAME", ValueFrom: fieldRef("spec.nodeName")},
		{Name: "MULTICA_AGENT_RUNTIME_NAME", Value: "foreman-job"},
		{Name: "MULTICA_DAEMON_MAX_CONCURRENT_TASKS", Value: "1"},
		{Name: "MULTICA_WORKSPACES_ROOT", Value: "/state/workspaces"},
		{Name: "TMPDIR", Value: "/tmp"},
		{Name: "TMP", Value: "/tmp"},
		{Name: "TEMP", Value: "/tmp"},
		{Name: "MULTICA_GC_ENABLED", Value: "false"},
		{Name: "MULTICA_DAEMON_AUTO_UPDATE", Value: "false"},
		{Name: "MULTICA_DAEMON_AUTO_RELOAD", Value: "false"},
		{Name: "MULTICA_OMP_PATH", Value: "/usr/local/bin/omp"},
		{Name: "HOME", Value: "/home/agent"},
		{Name: "LOG_LEVEL", Value: "info"},
	}
}

func (b *Builder) volumes(name string) []corev1.Volume {
	var stateVolumes []corev1.Volume
	if b.cacheMode() == CacheModeIsolated {
		stateVolumes = []corev1.Volume{
			{Name: "home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "workspaces", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		}
	} else {
		dirOrCreate := corev1.HostPathDirectoryOrCreate
		stateVolumes = []corev1.Volume{
			{Name: "home", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: path.Join(b.cfg.StateRoot, "home"), Type: &dirOrCreate}}},
			{Name: "workspaces", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: path.Join(b.cfg.StateRoot, "workspaces"), Type: &dirOrCreate}}},
		}
	}
	return append(stateVolumes,
		corev1.Volume{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &tmpVolumeSize}}},
		corev1.Volume{Name: "cred", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &credVolumeSize}}},
		corev1.Volume{Name: "cred-src", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName:  name + secretCredSuffix,
			DefaultMode: new(int32(credFileMode)),
		}}},
	)
}

func (b *Builder) imageRef() string {
	return b.cfg.JobImage + "@" + b.cfg.JobImageDigest
}

func (b *Builder) imagePullSecrets() []corev1.LocalObjectReference {
	refs := make([]corev1.LocalObjectReference, len(b.cfg.ImagePullSecrets))
	for i, name := range b.cfg.ImagePullSecrets {
		refs[i] = corev1.LocalObjectReference{Name: name}
	}
	return refs
}

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
