package jobbuilder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/attempt"
)

// The fixed foreman-job's paths. The render is one platform-published
// document: these names are not deployer placeholders (V2-C08).
const (
	fixedWorkDir        = "/run/foreman"
	fixedCredDir        = fixedWorkDir + "/cred"
	fixedCLIDir         = fixedWorkDir + "/clis"
	fixedHomeDir        = fixedWorkDir + "/home"
	fixedSpecFile       = fixedCredDir + "/" + attemptSpecKey
	fixedConfigDir      = fixedWorkDir + "/agent-config"
	fixedStateDir       = fixedWorkDir + "/state"
	fixedStateFile      = fixedStateDir + "/attempt-state.json"
	fixedServiceAccount = "multica-omp"
	fixedRuntimeDir     = fixedWorkDir + "/runtime"
	fixedRuntimeBin     = fixedRuntimeDir + "/foreman-job"
)

// fixedRuntimeScript copies the Foreman-owned Job supervisor (and any
// deployment adapters shipped beside it) into the shared emptyDir. The rsbx
// carrier itself carries neither the supervisor nor a CLI (V2-C08): the
// supervisor is Foreman's own code, not an artifact the carrier must embed.
const fixedRuntimeScript = `set -eu
for name in foreman-job foreman-job-cli rsbx-remote foreman-job-register foreman-job-runner foreman-job-slots; do
  if [ -x "/usr/local/bin/$name" ]; then cp "/usr/local/bin/$name" "/runtime/$name"; fi
done
test -x /runtime/foreman-job
`

// renderFixed builds the fixed foreman-job. It is reached only when the claim
// froze an attempt binding, so every value here is the attempt's own: two CLI
// artifacts, one environment, one configuration projection. There is no
// deployer template, no second backend and no local execution fallback.
func (b *Builder) renderFixed(name, daemonID string, e TaskEntry) (*batchv1.Job, error) {
	var binding attempt.Binding
	decoder := json.NewDecoder(bytes.NewReader(e.FixedAttempt))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&binding); err != nil {
		return nil, fmt.Errorf("decode the fixed attempt binding: %w", err)
	}
	if err := binding.Validate(); err != nil {
		return nil, fmt.Errorf("fixed attempt binding: %w", err)
	}
	if err := bindingMatchesEntry(binding, e, daemonID); err != nil {
		return nil, err
	}
	arch, ok := platformArch(binding.Platform)
	if !ok {
		return nil, fmt.Errorf("the frozen client platform %q is not os/arch", binding.Platform)
	}
	if binding.Config.Revision > 0 {
		switch {
		case binding.Config.SecretName == "":
			return nil, errors.New("the selected Agent configuration names no projection Secret")
		case binding.Config.FileName == "":
			return nil, errors.New("the selected Agent configuration names no file")
		case binding.Config.SecretName != agentconfig.SecretNameForAttempt(binding.AttemptID):
			return nil, errors.New("the selected Agent configuration is not this attempt's projection Secret")
		}
	}

	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   b.cfg.JobNamespace,
			Labels:      b.identityLabels(e),
			Annotations: b.fixedAnnotations(daemonID, binding, e),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptrInt32(0),
			Completions:             ptrInt32(1),
			Parallelism:             ptrInt32(1),
			TTLSecondsAfterFinished: ptrInt32(600),
			ActiveDeadlineSeconds:   ptrInt64(86400),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      b.fixedPodLabels(daemonID, e),
					Annotations: b.fixedAnnotations(daemonID, binding, e),
				},
				Spec: b.fixedPodSpec(binding, arch),
			},
		},
	}
	return job, nil
}

func bindingMatchesEntry(binding attempt.Binding, e TaskEntry, daemonID string) error {
	for field, pair := range map[string][2]string{
		"task id":          {binding.TaskID, e.TaskID},
		"workspace id":     {binding.WorkspaceID, e.WorkspaceID},
		"agent id":         {binding.AgentID, e.AgentID},
		"daemon id":        {binding.DaemonID, daemonID},
		"job runtime id":   {binding.JobRuntimeID, e.JobRuntimeID},
		"environment id":   {binding.EnvironmentID, e.EnvironmentID},
		"environment slot": {binding.SlotID, e.EnvironmentSlotID},
	} {
		if pair[0] != pair[1] {
			return fmt.Errorf("fixed attempt %s %q does not match the claim's %q", field, pair[0], pair[1])
		}
	}
	return nil
}

func (b *Builder) fixedAnnotations(daemonID string, binding attempt.Binding, e TaskEntry) map[string]string {
	annotations := b.identityAnnotations(daemonID, e)
	annotations[annoEnvironmentID] = binding.EnvironmentID
	annotations[annoSlotID] = binding.SlotID
	if binding.EnvironmentGen != "" {
		annotations[annoEnvironmentGen] = binding.EnvironmentGen
	}
	if binding.SessionID != "" {
		annotations[annoSessionID] = binding.SessionID
	}
	return annotations
}

func (b *Builder) fixedPodLabels(daemonID string, e TaskEntry) map[string]string {
	labels := map[string]string{
		labelAppName:     jobAppName,
		labelTaskID:      e.TaskID,
		labelDaemonID:    daemonID,
		labelWorkspaceID: e.WorkspaceID,
	}
	if e.SourceID != "" {
		labels[labelSourceID] = e.SourceID
	}
	return labels
}

func (b *Builder) fixedPodSpec(binding attempt.Binding, arch string) corev1.PodSpec {
	volumes := []corev1.Volume{
		{
			Name: "cred",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  CredentialSecretName(binding.DaemonID),
					DefaultMode: ptrInt32(0o400),
				},
			},
		},
		{Name: "clis", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptrQuantity("1Gi")}}},
		{Name: "home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptrQuantity("512Mi")}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptrQuantity("2Gi")}}},
		{Name: "attempt-state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptrQuantity("64Mi")}}},
		{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptrQuantity("64Mi")}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "cred", MountPath: fixedCredDir, ReadOnly: true},
		{Name: "clis", MountPath: fixedCLIDir},
		{Name: "home", MountPath: fixedHomeDir},
		{Name: "tmp", MountPath: "/tmp"},
		{Name: "attempt-state", MountPath: fixedStateDir},
	}
	env := []corev1.EnvVar{
		{Name: "MULTICA_SERVER_URL", Value: b.cfg.JobServerURL()},
		{Name: "MULTICA_DAEMON_ID", ValueFrom: envFieldRef("metadata.labels['" + labelDaemonID + "']")},
		{Name: "MULTICA_DAEMON_MAX_CONCURRENT_TASKS", Value: "1"},
		{Name: "FOREMAN_JOB_SPEC_FILE", Value: fixedSpecFile},
		{Name: "FOREMAN_JOB_STORE_FILE", Value: fixedStateFile},
		{Name: "FOREMAN_JOB_CLI_DIR", Value: fixedCLIDir},
		{Name: "FOREMAN_JOB_PROXY_URL", Value: binding.ProxyURL},
		{Name: "FOREMAN_JOB_TOKEN_FILE", Value: path.Join(fixedCredDir, configFileKey)},
		{Name: "FOREMAN_JOB_CLI_BUDGET_BYTES", Value: strconv.FormatInt(fixedCLIBudget(binding), 10)},
		{Name: "FOREMAN_JOB_RSBX_HELPER", Value: fixedRuntimeDir + "/rsbx-remote"},
		{Name: "FOREMAN_JOB_REGISTER_HELPER", Value: fixedRuntimeDir + "/foreman-job-register"},
		{Name: "FOREMAN_JOB_RUNNER_HELPER", Value: fixedRuntimeDir + "/foreman-job-runner"},
		{Name: "FOREMAN_JOB_SLOT_HELPER", Value: fixedRuntimeDir + "/foreman-job-slots"},
		{Name: "HOME", Value: fixedHomeDir},
		{Name: "TMPDIR", Value: "/tmp"},
		{Name: "TMP", Value: "/tmp"},
		{Name: "TEMP", Value: "/tmp"},
	}
	if binding.Config.Revision > 0 {
		volumes = append(volumes, corev1.Volume{
			Name: "agent-config",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: binding.Config.SecretName,
					Items: []corev1.KeyToPath{{
						Key:  binding.Config.FileName,
						Path: binding.Config.FileName,
					}},
					DefaultMode: ptrInt32(0o400),
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "agent-config", MountPath: fixedConfigDir, ReadOnly: true})
		env = append(env, corev1.EnvVar{
			Name:  "FOREMAN_JOB_AGENT_CONFIG",
			Value: path.Join(fixedConfigDir, binding.Config.FileName),
		})
	}

	runtimeImage := b.cfg.JobRuntimeImage
	if strings.TrimSpace(runtimeImage) == "" {
		runtimeImage = DefaultJobRuntimeImage
	}

	return corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		TerminationGracePeriodSeconds: ptrInt64(30),
		ServiceAccountName:            fixedServiceAccount,
		AutomountServiceAccountToken:  ptrBool(false),
		SecurityContext: &corev1.PodSecurityContext{
			RunAsUser:      ptrInt64(1000),
			RunAsGroup:     ptrInt64(1000),
			FSGroup:        ptrInt64(1000),
			RunAsNonRoot:   ptrBool(true),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry-tsic"}},
		NodeSelector:     map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": arch},
		Tolerations: []corev1.Toleration{
			{Key: "unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
			{Key: "maybe_unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
		},
		InitContainers: []corev1.Container{{
			Name:            "job-runtime",
			Image:           runtimeImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/bin/sh", "-ec"},
			Args:            []string{fixedRuntimeScript},
			VolumeMounts:    []corev1.VolumeMount{{Name: "runtime", MountPath: fixedRuntimeDir}},
			SecurityContext: &corev1.SecurityContext{
				RunAsUser:                ptrInt64(1000),
				RunAsNonRoot:             ptrBool(true),
				AllowPrivilegeEscalation: ptrBool(false),
				ReadOnlyRootFilesystem:   ptrBool(true),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("20m"),
					corev1.ResourceMemory: resource.MustParse("32Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
		}},
		Containers: []corev1.Container{{
			Name:            "agent",
			Image:           binding.Image + "@" + binding.ImageDigest,
			ImagePullPolicy: corev1.PullIfNotPresent,
			WorkingDir:      "/",
			Command:         []string{fixedRuntimeBin},
			Env:             env,
			VolumeMounts:    mounts,
			SecurityContext: &corev1.SecurityContext{
				RunAsUser:                ptrInt64(1000),
				RunAsNonRoot:             ptrBool(true),
				AllowPrivilegeEscalation: ptrBool(false),
				ReadOnlyRootFilesystem:   ptrBool(false),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
		}},
		Volumes: volumes,
	}
}

// fixedCLIBudget is the frozen attempt budget, with the contract default for a
// hand-built binding that left it unset.
func fixedCLIBudget(binding attempt.Binding) int64 {
	if binding.CLIBudgetBytes > 0 {
		return binding.CLIBudgetBytes
	}
	return attempt.DefaultCLIBudgetBytes
}

// platformArch extracts the arch from an os/arch platform.
func platformArch(platform string) (string, bool) {
	parts := strings.Split(platform, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	return parts[1], true
}

func ptrInt32(value int32) *int32 { return &value }
func ptrInt64(value int64) *int64 { return &value }
func ptrBool(value bool) *bool    { return &value }
func ptrQuantity(value string) *resource.Quantity {
	quantity := resource.MustParse(value)
	return &quantity
}
