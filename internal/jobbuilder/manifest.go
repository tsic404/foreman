package jobbuilder

import (
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
	annoAutopilotRunID  = "foreman.tsic.top/autopilot-run-id"
	annoChatSessionID   = "foreman.tsic.top/chat-session-id"
	annoDaemonID        = "foreman.tsic.top/daemon-id"
	annoJobRuntimeID    = "foreman.tsic.top/job-runtime-id"
	annoClaimedAt       = "foreman.tsic.top/claimed-at"
	annoAttempt         = "foreman.tsic.top/attempt"
)

// Container, volume and mount names of the built-in default template
// (03-contracts.md §5.4). The default template, its AC-20 baseline and the
// credential path are all built from these constants, so they cannot drift.
const (
	containerAgent   = "agent"
	containerPrepare = "prepare"

	volumeHome       = "home"
	volumeWorkspaces = "workspaces"
	volumeTmp        = "tmp"
	volumeCred       = "cred"
	volumeCredSrc    = "cred-src"

	mountHome       = "/home/agent"
	mountWorkspaces = "/state/workspaces"
	mountTmp        = "/tmp"
	mountCredSrc    = "/cred"
	// mountCredOut is the prepare-side write end of the cred bucket; the agent
	// reads the bucket through the single-file mount mountCredFile, which lands
	// on the profile directory kept on the node-local hostPath home (ADR-013 r2).
	mountCredOut  = "/credout"
	profileDir    = profilesDir + "/" + jobProfile
	mountCredFile = profileDir + "/" + configFileKey

	foremanDomain = "foreman.tsic.top/"
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

	agentCommand = "/usr/local/bin/multica"
	// dumbInitCommand is the container PID 1 both daemon containers start
	// with: it reaps the orphans the task's tool processes leave behind and
	// forwards signals to the child's process group (ADR-022).
	dumbInitCommand = "/usr/bin/dumb-init"
	// agentWorkingDir is "/" so the upstream task-identity CWD walk never sees
	// an agent-writable subtree ("guard 第三臂"); HOME stays mountHome (ADR-005).
	agentWorkingDir = "/"

	// jobProfile is the design constant JOB_PROFILE (ADR-013, 03-contracts
	// §3.1/§5.2): the daemon runs under this named profile and its config.json,
	// daemon.pid and daemon.log resolve to profileDir. Distinct from jobAppName
	// (the runtime display name) — both read "foreman-job" by contract, not by
	// coupling.
	jobProfile  = "foreman-job"
	profilesDir = "/home/agent/.multica/profiles"
)

// initScript copies the Job Token out of the read-only Secret mount into the
// per-Job cred bucket, heals the node-local profile path and clears a leftover
// task-context marker before the daemon starts (ADR-013 r3); it is the only
// code that runs as root (ADR-005). The paths are built from the constants so
// script and mounts cannot drift apart (AC-20).
const initScript = `for d in ` + mountHome + `/.multica ` + profilesDir + ` ` + profileDir + `; do
  [ -d "$d" ] || { rm -f "$d"; mkdir -p "$d"; }
done
rm -rf ` + mountHome + `/.multica/daemon_task_context.json
cp ` + mountCredSrc + `/` + configFileKey + ` ` + mountCredOut + `/` + configFileKey + `
chmod 0400 ` + mountCredOut + `/` + configFileKey + `
chown -R 1000:1000 ` + mountHome + ` ` + mountCredOut + `
`

var (
	tmpVolumeSize  = resource.MustParse("2Gi")
	credVolumeSize = resource.MustParse("16Mi")

	initCPURequest    = resource.MustParse("20m")
	initMemoryRequest = resource.MustParse("32Mi")
	initCPULimit      = resource.MustParse("200m")
	initMemoryLimit   = resource.MustParse("128Mi")
)

// defaultJob renders the built-in default template (docs/05-modules/job-template.md
// §Job Manifest) with the §5.1 env tuning applied and the per-task fields
// already written. This is the template-absent path; a deployment template
// replaces it wholesale.
func (b *Builder) defaultJob(name string, e TaskEntry) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   b.cfg.JobNamespace,
			Labels:      b.identityLabels(e),
			Annotations: b.identityAnnotations(name, e),
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
			ImagePullSecrets: b.cfg.imagePullSecrets(),
			NodeSelector:     b.cfg.NodeSelector,
			Tolerations:      b.cfg.Tolerations,
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew:           3,
				TopologyKey:       "kubernetes.io/hostname",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{labelAppName: jobAppName}},
			}},
			InitContainers: []corev1.Container{defaultPrepareContainer(b.cfg)},
			Containers:     []corev1.Container{defaultAgentContainer(b.cfg)},
			Volumes:        b.cfg.volumes(name),
		},
	}
}

// preferredTerm is the soft node-reuse affinity item appended after the merge
// (03-contracts §5.4 「affinity 注入」).
func preferredTerm(node string) corev1.PreferredSchedulingTerm {
	return corev1.PreferredSchedulingTerm{
		Weight: 100,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      "kubernetes.io/hostname",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{node},
			}},
		},
	}
}

// defaultPrepareContainer is the built-in default template's credential-copy
// init container (ADR-007): the only code that runs as root. A deployment
// template supplies its own.
func defaultPrepareContainer(cfg Config) corev1.Container {
	return corev1.Container{
		Name:            containerPrepare,
		Image:           cfg.imageRef(),
		ImagePullPolicy: corev1.PullAlways,
		Command:         []string{"/bin/sh", "-ec"},
		Args:            []string{initScript},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                new(int64(0)),
			RunAsNonRoot:             new(false),
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			// drop ALL leaves the uid-0 permitted set empty (permitted comes
			// from bounding at execve), so chown -R needs CHOWN (target uid
			// 1000 ≠ 0) and DAC_OVERRIDE (traverse other-uid hostPath dirs):
			// the 属主/元数据 minimal set of 05-modules/job-template.md.
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
				Add:  []corev1.Capability{"CHOWN", "DAC_OVERRIDE"},
			},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: initCPURequest, corev1.ResourceMemory: initMemoryRequest},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: initCPULimit, corev1.ResourceMemory: initMemoryLimit},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: volumeHome, MountPath: mountHome},
			{Name: volumeWorkspaces, MountPath: mountWorkspaces},
			{Name: volumeCred, MountPath: mountCredOut},
			{Name: volumeCredSrc, MountPath: mountCredSrc, ReadOnly: true},
		},
	}
}

// defaultAgentContainer is the single business container (ADR-001 收窄口径):
// the unmodified upstream daemon, started under the named Job profile with its
// 15-key env set (§5.2). The pull policy is Always so a moved tag reaches
// every node (ADR-012).
func defaultAgentContainer(cfg Config) corev1.Container {
	return corev1.Container{
		Name:            containerAgent,
		Image:           cfg.imageRef(),
		ImagePullPolicy: corev1.PullAlways,
		WorkingDir:      agentWorkingDir,
		Command:         []string{dumbInitCommand, "--", agentCommand},
		Args:            []string{"daemon", "start", "--foreground", "--profile", jobProfile},
		TTY:             true, // daemon writes logs to stderr only when it is a terminal
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                new(int64(agentUID)),
			RunAsNonRoot:             new(true),
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: cfg.CPURequest, corev1.ResourceMemory: cfg.MemoryRequest},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: cfg.CPULimit, corev1.ResourceMemory: cfg.MemoryLimit},
		},
		Env: agentEnv(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: volumeHome, MountPath: mountHome},
			// Single-file mount: only the Job Token enters the profile dir; the
			// directory itself stays on the node-local hostPath home (ADR-013 r2).
			{Name: volumeCred, MountPath: mountCredFile, SubPath: configFileKey, ReadOnly: true},
			{Name: volumeWorkspaces, MountPath: mountWorkspaces},
			{Name: volumeTmp, MountPath: mountTmp},
		},
	}
}

// injectedEnv is the A1.1 set Foreman writes into a template's first container
// (P-006 §A1.1): the daemon's protocol surface, and the only env keys a
// deployment template does not get to keep. On the default path the same three
// keys already carry these values, so the injection is idempotent there.
func injectedEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "MULTICA_SERVER_URL", Value: foremanServerURL},
		{Name: "MULTICA_DAEMON_ID", ValueFrom: envFieldRef("metadata.labels['job-name']")},
		{Name: "MULTICA_DAEMON_MAX_CONCURRENT_TASKS", Value: "1"},
	}
}

// envFieldRef is the fieldRef form §5.2 uses for the daemon identity keys.
func envFieldRef(fieldPath string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath}}
}

// agentEnv is the contract §5.2 env set: exactly 15 entries, fixed order.
// Task-identity env never enters the Job (ADR-013): MULTICA_TASK_CONFIG_ROOT
// in particular makes upstream v0.6.1 refuse `daemon start`.
func agentEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "MULTICA_SERVER_URL", Value: foremanServerURL},
		{Name: "MULTICA_DAEMON_ID", ValueFrom: envFieldRef("metadata.labels['job-name']")},
		{Name: "MULTICA_DAEMON_DEVICE_NAME", ValueFrom: envFieldRef("spec.nodeName")},
		{Name: "MULTICA_AGENT_RUNTIME_NAME", Value: jobAppName},
		{Name: "MULTICA_DAEMON_MAX_CONCURRENT_TASKS", Value: "1"},
		{Name: "MULTICA_WORKSPACES_ROOT", Value: mountWorkspaces},
		{Name: "TMPDIR", Value: mountTmp},
		{Name: "TMP", Value: mountTmp},
		{Name: "TEMP", Value: mountTmp},
		{Name: "MULTICA_GC_ENABLED", Value: "false"},
		{Name: "MULTICA_DAEMON_AUTO_UPDATE", Value: "false"},
		{Name: "MULTICA_DAEMON_AUTO_RELOAD", Value: "false"},
		{Name: "MULTICA_OMP_PATH", Value: "/usr/local/bin/omp"},
		{Name: "HOME", Value: mountHome},
		{Name: "LOG_LEVEL", Value: "info"},
	}
}

// requiredAnnotationKeys are the §3.2 keys every Job carries; the four
// trigger-context keys are conditional blocks (ADR-014), present only when
// their TaskEntry field is non-empty.
var (
	requiredAnnotationKeys       = []string{annoDaemonID, annoJobRuntimeID, annoClaimedAt, annoAttempt}
	triggerContextAnnotationKeys = []string{annoIssueID, annoIssueIdentifier, annoAutopilotRunID, annoChatSessionID}
)

// volumes is the built-in five-volume set: the two cache volumes follow
// FOREMAN_REPO_CACHE_MODE (ADR-005), the rest are fixed. jobName feeds the
// per-task credential Secret of cred-src.
func (c Config) volumes(jobName string) []corev1.Volume {
	return append([]corev1.Volume{
		c.stateVolume(volumeHome),
		c.stateVolume(volumeWorkspaces),
	}, emptyDirVolume(volumeTmp), emptyDirVolume(volumeCred), credSrcVolume(jobName))
}

// stateVolume renders home/workspaces: hostPath under FOREMAN_STATE_ROOT
// (shared) or emptyDir (isolated).
func (c Config) stateVolume(name string) corev1.Volume {
	if c.RepoCacheMode == CacheModeIsolated {
		return emptyDirVolume(name)
	}
	dirOrCreate := corev1.HostPathDirectoryOrCreate
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
		HostPath: &corev1.HostPathVolumeSource{Path: path.Join(c.StateRoot, name), Type: &dirOrCreate},
	}}
}

func emptyDirVolume(name string) corev1.Volume {
	size := credVolumeSize
	if name == volumeTmp {
		size = tmpVolumeSize
	}
	return emptyDirVolumeSized(name, size)
}

// credSrcVolume renders the read-only Secret volume holding the Job Token.
func credSrcVolume(jobName string) corev1.Volume {
	return corev1.Volume{Name: volumeCredSrc, VolumeSource: corev1.VolumeSource{
		Secret: &corev1.SecretVolumeSource{SecretName: jobName + secretCredSuffix, DefaultMode: new(int32(credFileMode))},
	}}
}

// imageRef is the Job image reference exactly as FOREMAN_JOB_IMAGE carries it
// (ADR-012): no digest concatenation, no shape parsing.
func (c Config) imageRef() string {
	return c.JobImage
}

func (c Config) imagePullSecrets() []corev1.LocalObjectReference {
	refs := make([]corev1.LocalObjectReference, len(c.ImagePullSecrets))
	for i, name := range c.ImagePullSecrets {
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
