package jobbuilder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/rsbxendpoint"
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
	fixedTLSDir         = fixedWorkDir + "/tls"
	fixedTLSServerDir   = fixedTLSDir + "/ref"

	// fixedAttemptStateFloor is the attempt-state volume's budget. It is not a
	// record-only bound: the carrier keeps its sandbox and its verified
	// executable cache under the same root, and that cache snapshots every
	// program the session runs locally — the attempt's two artifacts. A limit
	// below the attempt's own download budget therefore evicts the Pod in the
	// middle of a live session, which is what a 64Mi limit did once sessions
	// started to open.
	fixedAttemptStateFloor = "2Gi"

	// fixedAttemptStateHeadroom is the part of the attempt-state budget that is
	// not the artifact snapshot: the sandbox mount, the session log and the
	// local records. It is an allowance, not a measurement.
	fixedAttemptStateHeadroom int64 = 512 << 20

	// fixedAttemptStateMaxCLIBudget is the largest download budget the
	// attempt-state volume can represent. The volume is the budget plus the
	// headroom, and a sum past the int64 range is not a size: it wraps to a
	// negative quantity that then falls back to the floor, so the largest
	// budgets would shrink the volume instead of raising it. A budget past
	// this point is refused rather than wrapped.
	fixedAttemptStateMaxCLIBudget int64 = math.MaxInt64 - fixedAttemptStateHeadroom
)

// fixedRuntimeScript copies the Foreman-owned Job supervisor (and any
// deployment adapters shipped beside it) into the shared emptyDir. The rsbx
// carrier itself carries neither the supervisor nor a CLI (V2-C08): the
// supervisor is Foreman's own code, not an artifact the carrier must embed.
// Its destination is fixedRuntimeDir, the path both containers mount the same
// "runtime" volume at; the agent entrypoint below is fixedRuntimeBin there.
const fixedRuntimeScript = `set -eu
for name in foreman-job foreman-job-cli rsbx-remote foreman-job-register foreman-job-runner foreman-job-slots; do
  if [ -x "/usr/local/bin/$name" ]; then cp "/usr/local/bin/$name" "` + fixedRuntimeDir + `/$name"; fi
done
test -x "` + fixedRuntimeDir + `/foreman-job"
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
	// The Job must open the frozen endpoint, so an endpoint the carrier
	// cannot serve is refused here instead of producing a Pod that can only
	// fail at the session step.
	spec, err := b.fixedPodSpec(binding, arch)
	if err != nil {
		return nil, err
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
				Spec: spec,
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

func (b *Builder) fixedPodSpec(binding attempt.Binding, arch string) (corev1.PodSpec, error) {
	endpoint, err := rsbxendpoint.Parse(binding.Endpoint)
	if err != nil {
		return corev1.PodSpec{}, fmt.Errorf("the frozen endpoint is not a carrier transport: %w", err)
	}
	attemptState, err := fixedAttemptStateBudget(binding)
	if err != nil {
		return corev1.PodSpec{}, err
	}
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
		{Name: "attempt-state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: attemptState}}},
		{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptrQuantity("64Mi")}}},
		// The carrier client mounts its sandbox with FUSE; without the device
		// the session cannot start at all.
		{Name: "fuse", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/fuse", Type: hostPathType(corev1.HostPathCharDev)}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "cred", MountPath: fixedCredDir, ReadOnly: true},
		{Name: "clis", MountPath: fixedCLIDir},
		{Name: "home", MountPath: fixedHomeDir},
		{Name: "tmp", MountPath: "/tmp"},
		{Name: "attempt-state", MountPath: fixedStateDir},
		{Name: "runtime", MountPath: fixedRuntimeDir},
		{Name: "fuse", MountPath: "/dev/fuse"},
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
		{Name: "FOREMAN_JOB_CLI_HELPER", Value: fixedRuntimeDir + "/foreman-job-cli"},
		{Name: "FOREMAN_JOB_RSBX_HELPER", Value: fixedRuntimeDir + "/rsbx-remote"},
		{Name: "FOREMAN_JOB_REGISTER_HELPER", Value: fixedRuntimeDir + "/foreman-job-register"},
		{Name: "FOREMAN_JOB_RUNNER_HELPER", Value: fixedRuntimeDir + "/foreman-job-runner"},
		{Name: "FOREMAN_JOB_SLOT_HELPER", Value: fixedRuntimeDir + "/foreman-job-slots"},
		// The daemon names its own device: without it a registration carries
		// no node identity, which the upstream records as an unnamed device.
		{Name: "MULTICA_DAEMON_DEVICE_NAME", ValueFrom: envFieldRef("spec.nodeName")},
		{Name: "MULTICA_DAEMON_AUTO_UPDATE", Value: "false"},
		{Name: "MULTICA_DAEMON_AUTO_RELOAD", Value: "false"},
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
	switch endpoint.Transport {
	case rsbxendpoint.TransportUnix:
		// A unix socket is node-local: project exactly that socket, mounted at
		// its own path so the frozen endpoint names the same socket inside the
		// Pod. Projecting the parent directory would carry every neighbour in
		// with it — a read-only mount still permits connect(), so a runtime
		// socket that shares the directory would reach the privileged
		// container (V2-C30).
		socket := endpoint.Socket
		if socket == "" || socket == "/" {
			return corev1.PodSpec{}, fmt.Errorf("the frozen socket %q names no socket file", endpoint.Socket)
		}
		if containerRuntimeSockets[filepath.Base(socket)] {
			return corev1.PodSpec{}, fmt.Errorf("the frozen socket %q is a container runtime socket, which no attempt may project", socket)
		}
		volumes = append(volumes, corev1.Volume{
			Name: "rsbxd-socket",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: socket,
					Type: hostPathType(corev1.HostPathSocket),
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "rsbxd-socket", MountPath: socket, ReadOnly: true,
		})
	case rsbxendpoint.TransportTLS:
		// The authenticated network channel needs this environment's projected
		// material: trust anchor, client certificate and private key. Only the
		// selected environment's material is mounted, read-only, under the
		// reference the adapter already resolves.
		ref := path.Join(fixedTLSDir, binding.TLSRef)
		if !strings.HasPrefix(ref, fixedTLSServerDir+"/") {
			return corev1.PodSpec{}, fmt.Errorf("the frozen TLS reference %q does not name a projected material directory", binding.TLSRef)
		}
		volumes = append(volumes, corev1.Volume{
			Name: "env-tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  EnvironmentTLSSecretName(binding.EnvironmentID),
					DefaultMode: ptrInt32(0o400),
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "env-tls", MountPath: ref, ReadOnly: true,
		})
		env = append(env, corev1.EnvVar{Name: "FOREMAN_JOB_TLS_DIR", Value: fixedTLSDir})
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
			FSGroup:        ptrInt64(1000),
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
				// The carrier client owns the sandbox boundary: it mounts the
				// FUSE view and enables the device the kernel requires. The
				// privilege therefore stays with the sandbox client, while the
				// business commands it starts execute on the remote host.
				RunAsUser:                ptrInt64(0),
				RunAsNonRoot:             ptrBool(false),
				AllowPrivilegeEscalation: ptrBool(true),
				Privileged:               ptrBool(true),
				ReadOnlyRootFilesystem:   ptrBool(false),
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
	}, nil
}

// containerRuntimeSockets are the socket names a container runtime listens
// on. A privileged sandbox client sharing one of them would reach the node's
// runtime through a socket connect, so an endpoint naming one is refused
// before a Pod exists rather than projected.
var containerRuntimeSockets = map[string]bool{
	"containerd.sock": true,
	"crio.sock":       true,
	"docker.sock":     true,
	"podman.sock":     true,
}

// EnvironmentTLSSecretName is the Secret one environment's projected TLS
// material lives in. The deployer creates it beside the registration; the
// reference render mounts only the selected environment's Secret.
func EnvironmentTLSSecretName(environmentID string) string {
	return "foreman-env-tls-" + environmentID
}

func hostPathType(value corev1.HostPathType) *corev1.HostPathType { return &value }

// fixedCLIBudget is the frozen attempt budget, with the contract default for a
// hand-built binding that left it unset.
func fixedCLIBudget(binding attempt.Binding) int64 {
	if binding.CLIBudgetBytes > 0 {
		return binding.CLIBudgetBytes
	}
	return attempt.DefaultCLIBudgetBytes
}

// fixedAttemptStateBudget sizes the attempt-state volume for the live session
// that writes into it. The carrier snapshots every locally allowed program into
// its cache, so the volume must hold a second copy of the attempt's two
// artifacts — bounded by the attempt's own download budget — plus the sandbox,
// the session log and the local records. A deployment that raises the download
// budget raises this volume with it instead of evicting its own Job.
func fixedAttemptStateBudget(binding attempt.Binding) (*resource.Quantity, error) {
	budget := fixedCLIBudget(binding)
	if budget > fixedAttemptStateMaxCLIBudget {
		return nil, fmt.Errorf("the attempt download budget %d leaves no representable attempt-state volume: the largest budget is %d bytes",
			budget, fixedAttemptStateMaxCLIBudget)
	}
	need := resource.NewQuantity(budget+fixedAttemptStateHeadroom, resource.BinarySI)
	floor := resource.MustParse(fixedAttemptStateFloor)
	if need.Cmp(floor) < 0 {
		return &floor, nil
	}
	return need, nil
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
