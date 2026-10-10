package jobbuilder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// InvariantChecker is stage 2 of the two-stage overlay validation
// (03-contracts.md §5.4): it re-checks the merged, normalized Job — the
// startup gate runs it through NewBuilder, Build runs it again as the
// build-time fallback (a failure there after a green startup is an
// implementation-defect signal: the task fails with
// failure_reason=invalid_job_template and no object is created).
type InvariantChecker struct {
	cfg Config
}

var (
	digestPinnedImage = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
	credSecretName    = regexp.MustCompile(`^fm-.*-cred$`)
)

// foremanSecretName is the deployment Secret holding MULTICA_TOKEN and
// FOREMAN_JOB_TOKEN_KEY (deploy/10-secrets.yaml): referencing it from a Job
// is the F3 violation C-2 guards against.
const foremanSecretName = "foreman-secret"

// capabilityEnum is the closed set 清单 C-3 allows appended entries to add:
// the minimum for ownership/file-metadata preparation.
var capabilityEnum = map[string]bool{"CHOWN": true, "DAC_OVERRIDE": true, "FOWNER": true}

var appendedSidecarKeys = map[string]bool{
	"allowPrivilegeEscalation": true,
	"capabilities":             true,
	"runAsNonRoot":             true,
	"privileged":               true,
}

var appendedPrepareInitKeys = map[string]bool{
	"allowPrivilegeEscalation": true,
	"capabilities":             true,
	"runAsUser":                true,
	"runAsGroup":               true,
	"runAsNonRoot":             true,
	"privileged":               true,
}

// Check returns every invariant the merged Job breaks, in deterministic
// order.
func (c InvariantChecker) Check(job *batchv1.Job) []Violation {
	v := newViolations()
	c.checkIdentity(v, job)
	c.checkJobSpec(v, job)
	pod := &job.Spec.Template.Spec
	c.checkPodSpec(v, pod)
	c.checkVolumes(v, job.Name, pod.Volumes)
	c.checkContainers(v, pod)
	return v.list()
}

// checkIdentity re-checks §3.2 identity labels/annotations and the C-5/A
// domain rule on every label and annotation map.
func (c InvariantChecker) checkIdentity(v *violations, job *batchv1.Job) {
	md := &job.ObjectMeta
	if job.Name == "" {
		v.add("metadata.name", "清单 A: per-task 权威写入缺失")
	}
	if job.Namespace != c.cfg.JobNamespace {
		v.add("metadata.namespace", "清单 A/B: namespace 由 FOREMAN_JOB_NAMESPACE 独占")
	}
	if md.Labels[labelAppName] != jobAppName || md.Labels[labelManagedBy] != managedByForeman {
		v.add("metadata.labels", "清单 A: app.kubernetes.io/name|managed-by 身份标签")
	}
	for _, key := range []string{labelTaskID, labelAgentID, labelWorkspaceID} {
		if md.Labels[key] == "" {
			v.add("metadata.labels["+key+"]", "清单 A: per-task 身份标签缺失")
		}
	}
	for _, key := range []string{annoIssueID, annoIssueIdentifier, annoDaemonID, annoJobRuntimeID, annoClaimedAt, annoAttempt} {
		if md.Annotations[key] == "" {
			v.add("metadata.annotations["+key+"]", "清单 A: per-task 身份注解缺失")
		}
	}
	if md.Annotations[annoDaemonID] != job.Name {
		v.add("metadata.annotations["+annoDaemonID+"]", "清单 A: daemon-id 必须等于 Job 名")
	}
	tmplLabels := job.Spec.Template.Labels
	if tmplLabels[labelAppName] != jobAppName {
		v.add("spec.template.metadata.labels["+labelAppName+"]", "清单 A: Pod 模板身份标签")
	}
	if tmplLabels[labelTaskID] == "" || tmplLabels[labelTaskID] != md.Labels[labelTaskID] {
		v.add("spec.template.metadata.labels["+labelTaskID+"]", "清单 A: Pod 模板身份标签必须等于 Job 标签")
	}

	checkDomainKeys(v, "metadata.labels", md.Labels, authoritativeLabelKeys)
	checkDomainKeys(v, "metadata.annotations", md.Annotations, authoritativeAnnotationKeys)
	checkDomainKeys(v, "spec.template.metadata.labels", tmplLabels, authoritativePodLabelKeys)
	// Pod template annotations have no authoritative key: every reserved key
	// there is an overlay addition.
	checkDomainKeys(v, "spec.template.metadata.annotations", job.Spec.Template.Annotations, nil)
}

// authoritativeLabelKeys / authoritativeAnnotationKeys / authoritativePodLabelKeys
// are the §3.2 keys Foreman writes; checkDomainKeys uses them as the allow-list
// so a reserved key outside them can only have come from the overlay.
var (
	authoritativeLabelKeys      = keySet(labelAppName, labelManagedBy, labelTaskID, labelAgentID, labelWorkspaceID)
	authoritativeAnnotationKeys = keySet(annoIssueID, annoIssueIdentifier, annoDaemonID, annoJobRuntimeID, annoClaimedAt, annoAttempt)
	authoritativePodLabelKeys   = keySet(labelAppName, labelTaskID)
	reservedAppKeys             = keySet(labelAppName, labelManagedBy)
)

func keySet(keys ...string) map[string]bool {
	set := make(map[string]bool, len(keys))
	for _, key := range keys {
		set[key] = true
	}
	return set
}

// checkDomainKeys enforces the overlay half of 清单 A + C-5 on the merged
// object: a reserved key (any foreman.tsic.top/ key, or the two
// app.kubernetes.io identity keys) that is not part of the authoritative set
// is an overlay addition and is rejected. allowed may be nil (no authoritative
// key at that level).
func checkDomainKeys(v *violations, path string, keys map[string]string, allowed map[string]bool) {
	for _, key := range sortedStringKeys(keys) {
		if allowed[key] {
			continue
		}
		if strings.HasPrefix(key, foremanDomain) || reservedAppKeys[key] {
			v.add(path+"["+key+"]", "清单 C-5/A: overlay 不得新增 foreman.tsic.top/ 域与 app.kubernetes.io/name|managed-by 键")
		}
	}
}

func (c InvariantChecker) checkJobSpec(v *violations, job *batchv1.Job) {
	spec := &job.Spec
	if spec.BackoffLimit == nil || *spec.BackoffLimit != 0 {
		v.add("spec.backoffLimit", "清单 A: 恒为 0（禁 k8s 重复跑同一任务）")
	}
	if spec.Completions == nil || *spec.Completions != 1 {
		v.add("spec.completions", "清单 A: 恒为 1")
	}
	if spec.Parallelism == nil || *spec.Parallelism != 1 {
		v.add("spec.parallelism", "清单 A: 恒为 1")
	}
	if spec.Suspend != nil {
		v.add("spec.suspend", "清单 A: Job 级字段权威（Foreman 独占）")
	}
	if spec.PodFailurePolicy != nil {
		v.add("spec.podFailurePolicy", "清单 A: Job 级字段权威（Foreman 独占）")
	}
	if spec.Selector != nil {
		v.add("spec.selector", "清单 A: Job 级字段权威（Foreman 独占）")
	}
	if spec.ManualSelector != nil {
		v.add("spec.manualSelector", "清单 A: Job 级字段权威（Foreman 独占）")
	}
	if spec.TTLSecondsAfterFinished == nil || *spec.TTLSecondsAfterFinished != c.cfg.JobTTLSeconds {
		v.add("spec.ttlSecondsAfterFinished", "清单 B: 由 FOREMAN_JOB_TTL_SECONDS 独占")
	}
	wantDeadline := int64(c.cfg.TaskMaxDuration / time.Second)
	switch {
	case c.cfg.TaskMaxDuration <= 0:
		if spec.ActiveDeadlineSeconds != nil {
			v.add("spec.activeDeadlineSeconds", "清单 B: FOREMAN_TASK_MAX_DURATION=0 时不设置该字段")
		}
	case spec.ActiveDeadlineSeconds == nil || *spec.ActiveDeadlineSeconds != wantDeadline:
		v.add("spec.activeDeadlineSeconds", "清单 B: 由 FOREMAN_TASK_MAX_DURATION 独占")
	}
}

func (c InvariantChecker) checkPodSpec(v *violations, pod *corev1.PodSpec) {
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		v.add("spec.template.spec.restartPolicy", "清单 A: 恒为 Never")
	}
	if pod.ServiceAccountName != jobServiceAccount {
		v.add("spec.template.spec.serviceAccountName", "清单 A: 恒为 "+jobServiceAccount)
	}
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		v.add("spec.template.spec.automountServiceAccountToken", "清单 A: 恒为 false")
	}
	if len(pod.EphemeralContainers) > 0 {
		v.add("spec.template.spec.ephemeralContainers", "清单 A: 不得设置（K8s 建 Pod 时拒绝，会被误诊为 job_boot_timeout）")
	}
	if pod.HostNetwork {
		v.add("spec.template.spec.hostNetwork", "清单 C-1: 不得为 true（F4）")
	}
	if pod.HostPID {
		v.add("spec.template.spec.hostPID", "清单 C-1: 不得为 true（F4）")
	}
	if pod.HostIPC {
		v.add("spec.template.spec.hostIPC", "清单 C-1: 不得为 true（F4）")
	}
	if pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds < 30 {
		v.add("spec.template.spec.terminationGracePeriodSeconds", "清单 C-7: 下限 30（daemon 进程树 SIGTERM→SIGKILL 需要）")
	}
	checkPodSecurityContext(v, pod.SecurityContext)
	if !reflect.DeepEqual(pod.ImagePullSecrets, c.cfg.imagePullSecrets()) {
		v.add("spec.template.spec.imagePullSecrets", "清单 B: 由 FOREMAN_JOB_IMAGE_PULL_SECRETS 独占")
	}
	if !reflect.DeepEqual(pod.NodeSelector, c.cfg.NodeSelector) {
		v.add("spec.template.spec.nodeSelector", "清单 B: 由 FOREMAN_JOB_NODE_SELECTOR 独占")
	}
	if !reflect.DeepEqual(pod.Tolerations, c.cfg.Tolerations) {
		v.add("spec.template.spec.tolerations", "清单 B: 由 FOREMAN_JOB_TOLERATIONS 独占")
	}
}

func checkPodSecurityContext(v *violations, sc *corev1.PodSecurityContext) {
	const path = "spec.template.spec.securityContext"
	if sc == nil {
		v.add(path, "清单 A: Pod securityContext 锁定键缺失")
		return
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != agentUID {
		v.add(path+".runAsUser", "清单 A: 恒为 1000")
	}
	if sc.RunAsGroup == nil || *sc.RunAsGroup != agentGID {
		v.add(path+".runAsGroup", "清单 A: 恒为 1000")
	}
	if sc.FSGroup == nil || *sc.FSGroup != agentGID {
		v.add(path+".fsGroup", "清单 A: 恒为 1000")
	}
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
		v.add(path+".runAsNonRoot", "清单 A: 恒为 true")
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		v.add(path+".seccompProfile", "清单 A: 恒为 RuntimeDefault")
	}
}

// checkVolumes verifies the five built-in volumes (清单 A) plus the C-2 lower
// bounds on overlay-added volumes. C-2 is deliberately scoped to those: the
// built-ins are pinned by their definition above, so re-judging their paths
// here would turn a sloppy FOREMAN_STATE_ROOT into a startup refusal that
// blames a volume the operator never wrote.
func (c InvariantChecker) checkVolumes(v *violations, jobName string, volumes []corev1.Volume) {
	byName := map[string]corev1.Volume{}
	for _, vol := range volumes {
		if _, dup := byName[vol.Name]; dup {
			v.add("spec.template.spec.volumes["+vol.Name+"]", "清单 A: 卷名唯一")
		}
		byName[vol.Name] = vol
	}
	for _, name := range []string{volumeHome, volumeWorkspaces, volumeTmp, volumeCred, volumeCredSrc} {
		vol, ok := byName[name]
		if !ok {
			v.add("spec.template.spec.volumes[]", fmt.Sprintf("清单 A: 内置卷 %s 缺失（volumes[].name）", name))
			continue
		}
		c.checkBuiltinVolume(v, jobName, vol)
	}
	stateRoot := path.Clean(c.cfg.StateRoot)
	for _, vol := range volumes {
		if isBuiltinVolume(vol.Name) {
			continue
		}
		base := "spec.template.spec.volumes[" + vol.Name + "]"
		if vol.HostPath != nil {
			hostPath := path.Clean(vol.HostPath.Path)
			if !strings.HasPrefix(hostPath, stateRoot+"/") {
				v.add(base+".hostPath.path",
					"清单 C-2: hostPath 必须位于 ${FOREMAN_STATE_ROOT}/ 之下（volumes[].hostPath.path）")
			}
			switch {
			case strings.HasSuffix(hostPath, "docker.sock"), strings.HasSuffix(hostPath, "containerd.sock"):
				v.add(base+".hostPath.path",
					"清单 C-2/F4: 不得引用 docker.sock/containerd.sock（volumes[].hostPath.path）")
			case strings.HasPrefix(hostPath, "/var/run") || strings.Contains(hostPath, "containerd"):
				// C-2 names the two sockets only; the broader runtime-path
				// heuristic stays a warning until the contract says otherwise
				// (tracked by the design proposal on credential surfaces).
				c.cfg.logger().Warn("job template: hostPath looks like a container-runtime path, outside 清单 C-2",
					"volume", vol.Name, "path", hostPath)
			}
		}
		// Secret references are judged across the whole volume source, not just
		// the plain secret volume: projected/CSI/ISCSI/RBD/... can carry the
		// same banned name (F3).
		for _, ref := range volumeSecretRefs(vol) {
			if forbiddenSecretName(ref.name) {
				v.add(base+ref.path,
					"清单 C-2/F3: 卷不得引用 foreman-secret 或 fm-*-cred（volumes[].secret.secretName、volumes[].projected.sources[].secret.name 等全卷源）")
			}
		}
	}
}

// forbiddenSecretName reports the F3 credentials: foreman-secret carries the
// server token and the Job Token HMAC key (deploy/10-secrets.yaml), fm-*-cred
// carries the Job Token.
func forbiddenSecretName(name string) bool {
	return name != "" && (name == foremanSecretName || credSecretName.MatchString(name))
}

// secretRef is one Secret-name reference inside a volume source or container.
type secretRef struct {
	path string // field path relative to the owner, e.g. projected.sources[0].secret.name
	name string
}

var (
	secretVolumeSourceType = reflect.TypeOf(corev1.SecretVolumeSource{})
	secretProjectionType   = reflect.TypeOf(corev1.SecretProjection{})
	localObjectRefType     = reflect.TypeOf(corev1.LocalObjectReference{})
)

// volumeSecretRefs lists every Secret name a volume source can carry. The walk
// follows field types instead of a hand-written field list, so a Secret cannot
// slip in through projected sources or a CSI/ISCSI/RBD/FlexVolume-style source
// and a new upstream source type is covered without a code change.
func volumeSecretRefs(vol corev1.Volume) []secretRef {
	var refs []secretRef
	collectSecretRefs(reflect.ValueOf(vol.VolumeSource), "", &refs)
	return refs
}

func collectSecretRefs(value reflect.Value, prefix string, refs *[]secretRef) {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return
		}
		switch value.Type().Elem() {
		case secretVolumeSourceType:
			*refs = append(*refs, secretRef{path: prefix + ".secretName", name: value.Elem().FieldByName("SecretName").String()})
			return
		case secretProjectionType, localObjectRefType:
			*refs = append(*refs, secretRef{path: prefix + ".name", name: value.Elem().FieldByName("Name").String()})
			return
		}
		collectSecretRefs(value.Elem(), prefix, refs)
	case reflect.Struct:
		for i := range value.NumField() {
			field := value.Type().Field(i)
			name := jsonFieldName(field)
			child := joinPath(prefix, name)
			if field.Type.Kind() == reflect.String && (name == "secretName" || name == "secretRef") {
				*refs = append(*refs, secretRef{path: child, name: value.Field(i).String()})
				continue
			}
			collectSecretRefs(value.Field(i), child, refs)
		}
	case reflect.Slice:
		for i := range value.Len() {
			collectSecretRefs(value.Index(i), fmt.Sprintf("%s[%d]", prefix, i), refs)
		}
	}
}

func jsonFieldName(field reflect.StructField) string {
	name := strings.Split(field.Tag.Get("json"), ",")[0]
	if name == "" || name == "-" {
		name = field.Name
	}
	return name
}

func (c InvariantChecker) checkBuiltinVolume(v *violations, jobName string, vol corev1.Volume) {
	base := "spec.template.spec.volumes[" + vol.Name + "]"
	switch vol.Name {
	case volumeTmp, volumeCred:
		want := emptyDirVolume(vol.Name)
		if !sameJSON(vol, want) {
			v.add(base, "清单 A: 内置卷定义与来源类型锁定（emptyDir + sizeLimit）")
		}
	case volumeCredSrc:
		want := credSrcVolume(jobName)
		if !sameJSON(vol, want) {
			v.add(base, "清单 A: cred-src 卷锁定（Secret 名为 fm-<task_id>-cred、defaultMode 0400）")
		}
	case volumeHome, volumeWorkspaces:
		if !sameJSON(vol, c.cfg.stateVolume(vol.Name)) && !sameJSON(vol, emptyDirVolume(vol.Name)) {
			v.add(base, "清单 A: 内置卷定义锁定（hostPath ${FOREMAN_STATE_ROOT}/ 或 isolated 模式的 emptyDir）")
		}
	}
}

func (c InvariantChecker) checkContainers(v *violations, pod *corev1.PodSpec) {
	agentImage := c.cfg.imageRef()
	if len(pod.Containers) != 1 {
		v.add("spec.template.spec.containers", "清单 A: containers 恰好 1 项（agent 单业务容器；辅助容器只经 initContainers 追加）")
	}
	wantAgent := defaultAgentContainer(c.cfg)
	for i, ct := range pod.Containers {
		base := fmt.Sprintf("spec.template.spec.containers[%d]", i)
		if ct.Name != containerAgent {
			v.add(base+".name", "清单 A: 主容器只能是 agent（位于首位）")
		}
		if i == 0 {
			checkLockedContainer(v, base, ct, wantAgent, true)
			checkAuthoritativeEnv(v, base+".env", ct.Env)
			checkBuiltinMounts(v, base+".volumeMounts", ct.VolumeMounts, wantAgent.VolumeMounts)
		} else {
			checkAppendedSecurityContext(v, base+".securityContext", ct.SecurityContext, "native sidecar")
		}
		checkCredentialEnvRefs(v, base, ct)
	}

	inits := pod.InitContainers
	if len(inits) == 0 || inits[0].Name != containerPrepare {
		v.add("spec.template.spec.initContainers[0]", "清单 A: [0] 必须是内置 prepare（ADR-007 凭据拷贝注入点）")
	} else {
		want := defaultPrepareContainer(c.cfg)
		want.Image = agentImage
		checkLockedContainer(v, "spec.template.spec.initContainers[0]", inits[0], want, false)
		checkBuiltinMounts(v, "spec.template.spec.initContainers[0].volumeMounts", inits[0].VolumeMounts, want.VolumeMounts)
	}
	for i := range inits {
		checkCredentialEnvRefs(v, fmt.Sprintf("spec.template.spec.initContainers[%d]", i), inits[i])
	}
	for i := 1; i < len(inits); i++ {
		checkAppendedInitContainer(v, i, inits[i])
	}
}

// checkCredentialEnvRefs rejects env/envFrom references to the F3 secrets.
// The volume-side C-2 ban covers volume sources; F3 ("禁止把 server 侧凭据注入
// Job pod") draws the same line for the env surface, where §5.4 had no
// predicate yet — the contract text is extended by the design proposal on
// credential surfaces.
func checkCredentialEnvRefs(v *violations, base string, ct corev1.Container) {
	for _, env := range ct.Env {
		if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
			continue
		}
		if forbiddenSecretName(env.ValueFrom.SecretKeyRef.Name) {
			v.add(fmt.Sprintf("%s.env[%s].valueFrom.secretKeyRef.name", base, env.Name),
				"清单 F3: env 不得引用 foreman-secret 或 fm-*-cred（env[].valueFrom.secretKeyRef）")
		}
	}
	for i, from := range ct.EnvFrom {
		if from.SecretRef == nil {
			continue
		}
		if forbiddenSecretName(from.SecretRef.Name) {
			v.add(fmt.Sprintf("%s.envFrom[%d].secretRef.name", base, i),
				"清单 F3: envFrom 不得引用 foreman-secret 或 fm-*-cred（envFrom[].secretRef）")
		}
	}
}

// checkLockedContainer compares a built-in container against its default
// definition (清单 A), allowing the one C-6 strengthening for the agent. The
// imagePullPolicy is a free field on both containers (§5.4「imagePullPolicy」段)
// and is deliberately not compared.
func checkLockedContainer(v *violations, base string, got, want corev1.Container, allowReadOnlyRoot bool) {
	if got.Name != want.Name {
		v.add(base+".name", "清单 A: 容器名锁定")
	}
	if got.Image != want.Image {
		v.add(base+".image", "清单 B: image 由 FOREMAN_JOB_IMAGE 独占（原样引用，ADR-012）")
	}
	if !reflect.DeepEqual(got.Command, want.Command) {
		v.add(base+".command", "清单 A: 锁定（ADR-004 注入点）")
	}
	if !reflect.DeepEqual(got.Args, want.Args) {
		v.add(base+".args", "清单 A: 锁定（ADR-004 注入点）")
	}
	if got.WorkingDir != want.WorkingDir {
		v.add(base+".workingDir", "清单 A: 锁定")
	}
	if got.TTY != want.TTY {
		v.add(base+".tty", "清单 A: 锁定（日志可见性）")
	}
	if got.RestartPolicy != nil {
		// prepare must stay a plain init container: ADR-007's copy-then-start
		// ordering depends on it, and the agent is never a sidecar.
		v.add(base+".restartPolicy", "清单 A: 内置容器不得携带 restartPolicy（prepare 缺省是 ADR-007 时序依赖）")
	}
	if got.SecurityContext == nil {
		v.add(base+".securityContext", "清单 A: securityContext 键锁定")
	} else {
		gotSC := *got.SecurityContext
		switch {
		case allowReadOnlyRoot && gotSC.ReadOnlyRootFilesystem != nil && *gotSC.ReadOnlyRootFilesystem:
			// C-6: the single legal strengthening of the agent container.
			gotSC.ReadOnlyRootFilesystem = want.SecurityContext.ReadOnlyRootFilesystem
		case gotSC.ReadOnlyRootFilesystem == nil:
			gotSC.ReadOnlyRootFilesystem = want.SecurityContext.ReadOnlyRootFilesystem
		}
		if !reflect.DeepEqual(gotSC, *want.SecurityContext) {
			v.add(base+".securityContext", "清单 A: securityContext 键锁定（唯一例外 C-6 readOnlyRootFilesystem 单向加强为 true）")
		}
	}
	if !sameJSON(got.Resources, want.Resources) {
		v.add(base+".resources", "清单 B: resources 由 §5.1 env 独占")
	}
}

// checkAuthoritativeEnv re-checks the §5.2 env set value by value.
func checkAuthoritativeEnv(v *violations, base string, env []corev1.EnvVar) {
	got := map[string]corev1.EnvVar{}
	duplicates := map[string]bool{}
	for _, entry := range env {
		if _, dup := got[entry.Name]; dup {
			duplicates[entry.Name] = true
		}
		got[entry.Name] = entry
	}
	for _, want := range agentEnv() {
		actual, ok := got[want.Name]
		if !ok {
			v.add(base+"["+want.Name+"]", "清单 A: 15 键权威 env 缺失（§5.2）")
			continue
		}
		if !reflect.DeepEqual(actual, want) {
			v.add(base+"["+want.Name+"]", "清单 A: 15 键权威 env 逐值锁定（F5/§5.2）")
		}
		if duplicates[want.Name] {
			v.add(base+"["+want.Name+"]", "清单 A: env 键重复")
		}
	}
	for _, name := range sortedEnvNames(env) {
		if strings.HasPrefix(name, "MULTICA_") && !authoritativeEnvKeys[name] {
			v.add(base+"["+name+"]", "清单 A: 新增 env 不得使用 MULTICA_ 前缀")
		}
	}
}

// checkBuiltinMounts verifies the locked name+mountPath pairs on
// agent/prepare and applies C-10 to every other mount of the same container.
func checkBuiltinMounts(v *violations, base string, got, want []corev1.VolumeMount) {
	byPair := map[string]corev1.VolumeMount{}
	for _, m := range got {
		byPair[m.Name+"\x00"+m.MountPath] = m
	}
	for _, w := range want {
		actual, ok := byPair[w.Name+"\x00"+w.MountPath]
		if !ok {
			v.add(base+"[]", fmt.Sprintf("清单 A: 内置挂载 name+mountPath 对缺失（name=%s mountPath=%s）", w.Name, w.MountPath))
			continue
		}
		if !reflect.DeepEqual(actual, w) {
			v.add(base+"[]", fmt.Sprintf("清单 A: 内置挂载条目锁定（name=%s mountPath=%s）", w.Name, w.MountPath))
		}
	}
	for i, m := range got {
		if builtinMountEntry(m, want) {
			continue
		}
		for _, w := range want {
			if mountPathCollides(m.MountPath, w.MountPath) {
				v.add(fmt.Sprintf("%s[%d].mountPath", base, i),
					fmt.Sprintf("清单 C-10: 新增挂载不得与内置挂载路径等值或构成真子路径（volumeMounts[].mountPath，内置 %s）", w.MountPath))
			}
		}
	}
}

func builtinMountEntry(mount corev1.VolumeMount, want []corev1.VolumeMount) bool {
	for _, w := range want {
		if mount.Name == w.Name && mount.MountPath == w.MountPath {
			return true
		}
	}
	return false
}

func mountPathCollides(mountPath, builtin string) bool {
	return mountPath == builtin || strings.HasPrefix(mountPath, builtin+"/")
}

// checkAppendedInitContainer applies 清单 A's form rules plus C-3/C-8/C-9 to
// one appended init container.
func checkAppendedInitContainer(v *violations, index int, ct corev1.Container) {
	base := fmt.Sprintf("spec.template.spec.initContainers[%d]", index)
	if ct.Name == "" || len(ct.Name) > 63 || !dns1123Label.MatchString(ct.Name) {
		v.add(base+".name", "清单 A: 追加条目 name 须非空且为 DNS-1123 label")
	}
	if ct.Name == containerPrepare || ct.Name == containerAgent {
		v.add(base+".name", "清单 A: 追加条目不得命名为 prepare/agent")
	}

	kind := ""
	switch {
	case ct.RestartPolicy == nil:
		kind = "prepare-init"
	case *ct.RestartPolicy == corev1.ContainerRestartPolicyAlways:
		kind = "native sidecar"
	default:
		v.add("spec.template.spec.initContainers",
			fmt.Sprintf("清单 A: 追加条目 restartPolicy 只允许缺省（prepare-init）或 Always（native sidecar），got %q（initContainers[].restartPolicy）", *ct.RestartPolicy))
	}
	if !digestPinnedImage.MatchString(ct.Image) {
		v.add(base+".image", "清单 C-8: 追加条目 image 必须以 @sha256:[0-9a-f]{64} 结尾（initContainers[].image）")
	}
	checkAppendedSecurityContext(v, base+".securityContext", ct.SecurityContext, kind)
	for j, mount := range ct.VolumeMounts {
		forbidden := mount.Name == volumeCred || mount.Name == volumeCredSrc
		rule := "清单 C-9: prepare-init 不得挂载 cred/cred-src（volumeMounts[].name）"
		if kind == "native sidecar" {
			forbidden = isBuiltinVolume(mount.Name) && mount.Name != volumeTmp
			rule = "清单 C-9: native sidecar 不得挂载 cred/cred-src/home/workspaces（volumeMounts[].name；tmp 是唯一显式共享例外）"
		}
		if forbidden {
			v.add(fmt.Sprintf("%s.volumeMounts[%d].name", base, j), rule)
		}
	}
}

// checkAppendedSecurityContext enforces 清单 C-3 for the two appended-entry
// branches: the whitelist is closed, the required keys must be present, and
// every direction is one-way.
func checkAppendedSecurityContext(v *violations, base string, sc *corev1.SecurityContext, kind string) {
	if sc == nil {
		v.add(base, "清单 C-3: 追加条目必须显式声明 securityContext 且 allowPrivilegeEscalation/capabilities 必填")
		return
	}
	if sc.AllowPrivilegeEscalation == nil {
		v.add(base+".allowPrivilegeEscalation", "清单 C-3: allowPrivilegeEscalation 必填（缺省不会设置 no_new_privs）")
	} else if *sc.AllowPrivilegeEscalation {
		v.add(base+".allowPrivilegeEscalation", "清单 C-3: allowPrivilegeEscalation 必须 =false（F4）")
	}
	if sc.Capabilities == nil {
		v.add(base+".capabilities", "清单 C-3: capabilities 必填（缺省会落入运行时默认 capability 集）")
	} else {
		for _, added := range sc.Capabilities.Add {
			if !capabilityEnum[string(added)] {
				v.add(base+".capabilities.add",
					fmt.Sprintf("清单 C-3: capability %s 超出枚举集 {CHOWN, DAC_OVERRIDE, FOWNER}（securityContext.capabilities.add）", added))
			}
		}
		if !containsCapability(sc.Capabilities.Drop, "ALL") {
			v.add(base+".capabilities.drop", "清单 C-3: capabilities.drop 必须 ⊇ [ALL]（securityContext.capabilities.drop）")
		}
	}
	if sc.Privileged != nil && *sc.Privileged {
		v.add(base+".privileged", "清单 C-3/F4: privileged 不得为 true")
	}

	allowed := appendedSidecarKeys
	switch kind {
	case "prepare-init":
		allowed = appendedPrepareInitKeys
		if sc.RunAsUser != nil && *sc.RunAsUser == 0 && (sc.RunAsNonRoot == nil || *sc.RunAsNonRoot) {
			v.add(base+".runAsNonRoot", "清单 C-3: 显式 runAsUser: 0 ⇒ 必须同时显式声明 runAsNonRoot: false（单向判据）")
		}
	case "native sidecar":
		if sc.RunAsNonRoot != nil && !*sc.RunAsNonRoot {
			v.add(base+".runAsNonRoot", "清单 C-3: native sidecar runAsNonRoot 只能 =true")
		}
	}
	for _, key := range presentSecurityContextKeys(sc) {
		if !allowed[key] {
			v.add(base+"."+key, "清单 C-3: securityContext 白名单外的键出现即拒（"+kind+" 分支）")
		}
	}
}

// presentSecurityContextKeys lists the securityContext keys actually set, by
// reflection so a new upstream field cannot slip past the whitelist.
func presentSecurityContextKeys(sc *corev1.SecurityContext) []string {
	value := reflect.ValueOf(sc).Elem()
	typ := value.Type()
	var keys []string
	for i := range typ.NumField() {
		if value.Field(i).Kind() != reflect.Pointer || value.Field(i).IsNil() {
			continue
		}
		name := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			name = typ.Field(i).Name
		}
		keys = append(keys, name)
	}
	sort.Strings(keys)
	return keys
}

func containsCapability(caps []corev1.Capability, want string) bool {
	for _, cap := range caps {
		if string(cap) == want {
			return true
		}
	}
	return false
}

func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedEnvNames(env []corev1.EnvVar) []string {
	names := make([]string, 0, len(env))
	for _, entry := range env {
		names = append(names, entry.Name)
	}
	sort.Strings(names)
	return names
}

func isBuiltinVolume(name string) bool {
	switch name {
	case volumeHome, volumeWorkspaces, volumeTmp, volumeCred, volumeCredSrc:
		return true
	}
	return false
}

// sameJSON compares two API fragments by their canonical JSON form. Struct
// comparison is unreliable here: resource.Quantity carries a lazily-filled
// string cache, so two equal quantities can differ field-by-field depending
// on whether they have been marshaled yet.
func sameJSON(left, right any) bool {
	leftJSON, err := json.Marshal(left)
	if err != nil {
		return false
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		return false
	}
	return bytes.Equal(leftJSON, rightJSON)
}
