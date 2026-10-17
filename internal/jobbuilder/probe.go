package jobbuilder

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// The catalog probe's fixed names and values (ADR-019 / ADR-025, 03-contracts.md
// §3.2「目录探针 Job 的标签」). A probe runs the same official daemon as a task
// Job, but under its own profile and its own credential — so a probe and a
// task Job can sit on one node without touching each other's profile directory.
const (
	probeJobPrefix    = "fm-catalog-"
	probeRole         = "catalog-probe"
	labelRole         = "foreman.tsic.top/role"
	containerDaemon   = "daemon"
	probeProfile      = "foreman-catalog"
	probeProfileDir   = profilesDir + "/" + probeProfile
	probeCredMount    = probeProfileDir + "/" + configFileKey
	probeHeartbeatEnv = "MULTICA_DAEMON_HEARTBEAT_INTERVAL"

	// probeJobTTLSeconds is the probe's fallback cleanup: the Job is normally
	// deleted by Foreman as the relay finishes (05-modules/job-template.md
	// §目录探针 Job).
	probeJobTTLSeconds int32 = 300
)

var (
	// The probe's own resource face: it boots, discovers and reports once, so
	// it never inherits the task's tuning (05-modules/job-template.md
	// §与任务 Job 的差异).
	probeCPURequest    = resource.MustParse("100m")
	probeMemoryRequest = resource.MustParse("256Mi")
	probeCPULimit      = resource.MustParse("1")
	probeMemoryLimit   = resource.MustParse("1Gi")

	// probeEmptyDirSize sizes the probe's tmp and workspaces emptyDirs. The
	// workspaces mount exists so MULTICA_WORKSPACES_ROOT points at an existing
	// empty directory — the probe never materializes a task workspace, and it
	// never mounts the node-shared cache.
	probeEmptyDirSize = resource.MustParse("1Gi")
)

// taskCredSecretLiteral is a template's per-task credential Secret name: the
// literal A1.4 ④ substitutes for the probe's own Secret, beside the task
// profile literal (jobProfile) it renames to the probe profile.
var taskCredSecretLiteral = registry.CredName(placeholderJobName)

// probeInitScript is initScript under the probe profile: the probe owns its
// profile directory, so the script's recursive chown and its credential copy
// land in profiles/foreman-catalog.
var probeInitScript = strings.ReplaceAll(initScript, profileDir, probeProfileDir)

// ErrInvalidRequestID marks a request id that cannot name a probe Job: no
// object is created, and the caller reports the relay as failed.
var ErrInvalidRequestID = errors.New("invalid request id")

// BuildCatalogProbe renders the catalog-probe Job (fm-catalog-<requestID>) and
// its credential Secret, returning the digest of the token that Secret carries
// (03-contracts.md §2 CatalogProbe.Start). The shape source follows the CM
// window (ADR-025): absent = the built-in default probe template, present =
// the deployment's template copy plus the probe write face. The budget becomes
// activeDeadlineSeconds in the default window only, and always extends the
// token's lifetime past the Job by the drain bound (ADR-017).
func (b *Builder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	name := probeJobName(requestID)
	if len(name) > 63 || !dns1123Label.MatchString(name) {
		return nil, nil, "", fmt.Errorf("%w: %q does not yield a valid probe Job name %q", ErrInvalidRequestID, requestID, name)
	}
	if b.cfg.Issuer == nil {
		return nil, nil, "", fmt.Errorf("Config.Issuer is required")
	}
	job, err := b.renderProbe(name, requestID, jobRuntimeID, budget)
	if err != nil {
		return nil, nil, "", err
	}
	token, err := b.cfg.Issuer.IssueProbe(name, requestID, workspaceID, budget+b.cfg.JobDrainTimeout)
	if err != nil {
		return nil, nil, "", fmt.Errorf("issue probe Job Token: %w", err)
	}
	return job, b.probeCredentialSecret(name, token), b.cfg.Issuer.Hash(token), nil
}

// probeJobName is the probe naming convention: the Job, its credential Secret
// (plus the -cred suffix) and the daemon's own identity all carry the request
// id, so a probe is recognizable and sweepable by label and by name.
func probeJobName(requestID string) string {
	return probeJobPrefix + requestID
}

// probeLabels is the §3.2 label block every probe Job and probe Secret carry.
// managed-by is the stale-sweep selector, so Foreman writes it even when a
// template omits it; task-id is never written — it is what makes a Job a task
// Job (03-contracts.md §1.3).
func probeLabels() map[string]string {
	return map[string]string{
		labelAppName:   jobAppName,
		labelManagedBy: managedByForeman,
		labelRole:      probeRole,
	}
}

// renderProbe is the probe pipeline (03-contracts.md §5.4「探针渲染的完整写入面」):
// pick the template — the deployment's copy or the built-in default — then
// apply the probe writes to it. The object-level writes run inside
// renderProbeTemplate, the typed ones here.
func (b *Builder) renderProbe(name, requestID, jobRuntimeID string, budget time.Duration) (*batchv1.Job, error) {
	var job *batchv1.Job
	if b.templatePresent {
		rendered, err := renderProbeTemplate(b.template, probeValues{
			jobImage:     b.cfg.imageRef(),
			jobRuntimeID: jobRuntimeID,
			credSecret:   registry.CredName(name),
		})
		if err != nil {
			return nil, err
		}
		job = rendered
	} else {
		job = b.defaultProbeJob(name, budget)
	}
	b.applyProbeWrites(job, name)
	return job, nil
}

// applyProbeWrites performs the probe's typed writes: the A1.1 env keys, the
// namespace overwrite, and A1.4 ①②③ — the probe name, the label block with
// the foreman.tsic.top/ domain cleared first, and the heartbeat key. Annotations
// stay as the template wrote them, minus that domain: a probe maps no task, so
// it carries none of the §3.2 task annotations.
func (b *Builder) applyProbeWrites(job *batchv1.Job, name string) {
	injectContainerEnv(job)

	job.Name = name
	job.Namespace = b.cfg.JobNamespace

	clearForemanDomain(job.Labels)
	clearForemanDomain(job.Annotations)
	clearForemanDomain(job.Spec.Template.Labels)
	clearForemanDomain(job.Spec.Template.Annotations)

	job.Labels = setKeys(job.Labels, probeLabels())
	// Pod side: the probe block's name and role only. managed-by is the Job
	// side's selector — nothing lists pods by it.
	job.Spec.Template.Labels = setKeys(job.Spec.Template.Labels, map[string]string{
		labelAppName: jobAppName,
		labelRole:    probeRole,
	})

	writeProbeHeartbeat(job, b.cfg.probeHeartbeat())
}

// writeProbeHeartbeat writes the probe's heartbeat interval into
// containers[0].env (A1.4 ③), the one writer of that key in both windows. It
// replaces every same-name entry instead of appending beside one: the key is
// Foreman's, and the startup self-check pins the budget against its value.
func writeProbeHeartbeat(job *batchv1.Job, interval time.Duration) {
	containers := job.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		return
	}
	kept := make([]corev1.EnvVar, 0, len(containers[0].Env)+1)
	for _, entry := range containers[0].Env {
		if entry.Name != probeHeartbeatEnv {
			kept = append(kept, entry)
		}
	}
	containers[0].Env = append(kept, corev1.EnvVar{Name: probeHeartbeatEnv, Value: interval.String()})
}

// defaultProbeJob renders the built-in default probe Job
// (docs/05-modules/job-template.md §目录探针 Job, the CM-absent window): the
// task daemon shape without the task's deadline, TTL, resource face and
// node-shared workspace volume. The name, labels and heartbeat key come from
// applyProbeWrites, so the two windows share one writer.
func (b *Builder) defaultProbeJob(name string, budget time.Duration) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: b.cfg.JobNamespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            new(int32(0)),
			Completions:             new(int32(1)),
			Parallelism:             new(int32(1)),
			TTLSecondsAfterFinished: new(probeJobTTLSeconds),
			Template:                b.probePodTemplate(name),
		},
	}
	// The remaining budget is the probe's only deadline — it is the time the
	// probe was bought for. A non-positive budget renders none rather than an
	// already-expired deadline.
	if budget > 0 {
		job.Spec.ActiveDeadlineSeconds = new(int64(budget / time.Second))
	}
	return job
}

// probePodTemplate is the probe's pod: never retried, no cluster API access,
// and neither soft affinity nor topology spread — spreading and node reuse are
// task-placement concerns, and the probe takes the node it lands on.
func (b *Builder) probePodTemplate(name string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			ServiceAccountName:           jobServiceAccount,
			AutomountServiceAccountToken: new(false),
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
			InitContainers:   []corev1.Container{probePrepareContainer(b.cfg)},
			Containers:       []corev1.Container{probeDaemonContainer(b.cfg)},
			Volumes:          b.probeVolumes(name),
		},
	}
}

// probeVolumes is the probe's five-volume set. The node-local home is shared
// with the tasks — the probe discovers the providers configured there — while
// the workspaces mount is a probe-private emptyDir: the probe runs no task, so
// it needs no node-shared cache, only an existing empty directory for
// MULTICA_WORKSPACES_ROOT. FOREMAN_REPO_CACHE_MODE therefore does not reach it.
func (b *Builder) probeVolumes(name string) []corev1.Volume {
	dirOrCreate := corev1.HostPathDirectoryOrCreate
	return []corev1.Volume{
		{
			Name: volumeHome,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: path.Join(b.cfg.StateRoot, volumeHome), Type: &dirOrCreate},
			},
		},
		emptyDirVolumeSized(volumeTmp, probeEmptyDirSize),
		emptyDirVolumeSized(volumeCred, credVolumeSize),
		emptyDirVolumeSized(volumeWorkspaces, probeEmptyDirSize),
		credSrcVolume(name),
	}
}

// emptyDirVolumeSized is an emptyDir of an explicit size: the probe block sizes
// its emptyDirs itself instead of inheriting the task template's.
func emptyDirVolumeSized(name string, size resource.Quantity) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
		EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &size},
	}}
}

// probePrepareContainer is the probe's prepare init container: the task
// container verbatim — same image, capabilities and resources — with the probe
// profile directory in its script. It copies the probe's own credential, and
// touches no task's.
func probePrepareContainer(cfg Config) corev1.Container {
	container := defaultPrepareContainer(cfg)
	container.Args = []string{probeInitScript}
	return container
}

// probeDaemonContainer is the probe's daemon container: the unmodified daemon
// under the probe profile, with the probe's credential mounted single-file
// where that profile reads its config. The heartbeat key is written by
// applyProbeWrites.
func probeDaemonContainer(cfg Config) corev1.Container {
	container := defaultAgentContainer(cfg)
	container.Name = containerDaemon
	container.Args = []string{"daemon", "start", "--foreground", "--profile", probeProfile}
	container.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: probeCPURequest, corev1.ResourceMemory: probeMemoryRequest},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: probeCPULimit, corev1.ResourceMemory: probeMemoryLimit},
	}
	container.VolumeMounts = []corev1.VolumeMount{
		{Name: volumeHome, MountPath: mountHome},
		{Name: volumeCred, MountPath: probeCredMount, SubPath: configFileKey, ReadOnly: true},
		{Name: volumeTmp, MountPath: mountTmp},
		{Name: volumeWorkspaces, MountPath: mountWorkspaces},
	}
	return container
}

// probeCredentialSecret is the probe's credential Secret: the same carrier as
// a task's (one config.json, mounted 0400 through cred-src), labelled with the
// probe role and without a task id, so the task-side orphan sweep leaves it to
// the probe sweep (03-contracts.md §1.3).
func (b *Builder) probeCredentialSecret(name, token string) *corev1.Secret {
	configJSON := `{"token":` + strconv.Quote(token) + `}`
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      registry.CredName(name),
			Namespace: b.cfg.JobNamespace,
			Labels:    probeLabels(),
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{configFileKey: configJSON},
	}
}

// probeValues are the probe render's substitution inputs: the two placeholders
// a probe consumes, plus the credential Secret name A1.4 ④ renames the task's
// to. The generic substitution runs last, after the two literals — a template's
// image reference may itself read "foreman-job", and renaming it after the
// image value landed would corrupt the reference.
type probeValues struct {
	jobImage     string
	jobRuntimeID string
	credSecret   string
}

// literals is A1.4 ④: the task profile and the task credential Secret name are
// renamed over the whole object, substring and every occurrence, because the
// probe and the task Job share one template copy.
func (v probeValues) literals(value string) string {
	value = strings.ReplaceAll(value, jobProfile, probeProfile)
	return strings.ReplaceAll(value, taskCredSecretLiteral, v.credSecret)
}

// placeholders is the probe's generic substitution: exactly the two
// placeholders §5.4 lets a probe consume. Task-identity placeholders stay
// verbatim — they name a task the probe does not have, so there is no value to
// invent (and silence beats failure here: the probe must still register).
func (v probeValues) placeholders(value string) string {
	value = strings.ReplaceAll(value, placeholderJobImage, v.jobImage)
	return strings.ReplaceAll(value, placeholderJobRuntimeID, v.jobRuntimeID)
}

// renderProbeTemplate renders one probe from the startup-parsed template: a
// fresh copy (A1.3), then the object-level steps of the write face in order —
// the A1.4 literals, the generic substitution, then the affinity item deletion
// (the probe has no soft affinity, so the item carrying the placeholder goes
// with the structures the deletion empties).
func renderProbeTemplate(template map[string]any, v probeValues) (*batchv1.Job, error) {
	rendered := deepCopyTemplate(template)
	rewriteStrings(rendered, v.literals)
	rewriteStrings(rendered, v.placeholders)
	dropPreferredAffinityItems(rendered, "")

	raw, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf("render probe Job: %w", err)
	}
	var job batchv1.Job
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, fmt.Errorf("render probe Job: %w", err)
	}
	return &job, nil
}

// rewriteStrings applies rewrite to every string value of the object — map
// values and list elements alike, substring matching with every occurrence
// replaced (§5.4「占位符替换」的匹配与作用域). Keys are not values and are left
// alone, as on the task path.
func rewriteStrings(node any, rewrite func(string) string) any {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			value[key] = rewriteStrings(child, rewrite)
		}
	case []any:
		for i, child := range value {
			value[i] = rewriteStrings(child, rewrite)
		}
	case string:
		return rewrite(value)
	}
	return node
}
