package jobbuilder

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The catalog probe's fixed names and values (ADR-019 / ADR-025, §8.4). A
// probe runs the same official daemon as a task Job, but under its own profile
// and its own credential — so a probe and a task Job can sit on one node
// without touching each other's profile directory.
const (
	probeJobPrefix    = "fm-catalog-"
	probeRole         = "catalog-probe"
	labelRole         = "foreman.tsic.top/role"
	probeProfile      = "foreman-catalog"
	probeHeartbeatEnv = "MULTICA_DAEMON_HEARTBEAT_INTERVAL"
)

// taskCredSecretLiteral is a template's per-task credential Secret name: the
// literal A1.4 ④ substitutes for the probe's own Secret, beside the task
// profile literal (jobProfile) it renames to the probe profile. The task
// Secret's own name derives from the identity name, never from the display
// name (ADR-034 decision 8).
var taskCredSecretLiteral = CredentialSecretName(DaemonIDFor(placeholderTaskID))

// ErrInvalidRequestID marks a request id that cannot name a probe Job: no
// object is created, and the caller reports the relay as failed.
var ErrInvalidRequestID = errors.New("invalid request id")

// BuildCatalogProbe renders the catalog-probe Job (fm-catalog-<requestID>) and
// its credential Secret, returning the digest of the token that Secret carries.
// Its shape is the deployment template's copy plus the probe write face
// (ADR-025): A1.1's env keys, the namespace, A1.4's four items and the four
// placeholders a probe consumes. The budget becomes activeDeadlineSeconds —
// the probe's own deadline, written by Foreman and not by the template
// (§8.4) — and the token always outlives the Job by the drain bound
// (ADR-017).
func (b *Builder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	name := ProbeJobName(requestID)
	if len(name) > 63 || !dns1123Label.MatchString(name) {
		return nil, nil, "", fmt.Errorf("%w: %q does not yield a valid probe Job name %q", ErrInvalidRequestID, requestID, name)
	}
	if b.cfg.Issuer == nil {
		return nil, nil, "", fmt.Errorf("Config.Issuer is required")
	}
	job, err := b.renderProbe(name, jobRuntimeID, budget)
	if err != nil {
		return nil, nil, "", err
	}
	token, err := b.cfg.Issuer.IssueProbe(name, requestID, workspaceID, budget+b.cfg.JobDrainTimeout)
	if err != nil {
		return nil, nil, "", fmt.Errorf("issue probe Job Token: %w", err)
	}
	return job, b.probeCredentialSecret(name, token), b.cfg.Issuer.Hash(token), nil
}

// ProbeJobName is the probe naming convention: the Job, its credential Secret
// (plus the -cred suffix) and the daemon's own identity all carry the request
// id, so a probe is recognizable and sweepable by label and by name. Exported
// as the one source: the scheduler creates the probe with the same name the
// builder renders into it.
func ProbeJobName(requestID string) string {
	return probeJobPrefix + requestID
}

// probeLabels is the probe label block (§8.4) every probe Job and probe Secret
// carry: the probe's daemon identity, which is also what its MULTICA_DAEMON_ID
// fieldRef reads (ADR-019 / ADR-034). managed-by is the stale-sweep selector,
// so Foreman writes it even when a template omits it; task-id is never written
// — it is what makes a Job a task Job (§3).
func probeLabels(name string) map[string]string {
	return map[string]string{
		labelAppName:   jobAppName,
		labelManagedBy: managedByForeman,
		labelRole:      probeRole,
		labelDaemonID:  name,
	}
}

// renderProbe is the probe pipeline (§8.4): the template copy through the
// probe write face, then the budget deadline.
func (b *Builder) renderProbe(name, jobRuntimeID string, budget time.Duration) (*batchv1.Job, error) {
	job, err := renderProbeTemplate(b.template, probeValues{
		jobImage:     b.cfg.JobImage,
		jobRuntimeID: jobRuntimeID,
		jobName:      name,
		daemonID:     name,
		credSecret:   CredentialSecretName(name),
	})
	if err != nil {
		return nil, err
	}
	b.applyProbeWrites(job, name)
	writeProbeDeadline(job, budget)
	return job, nil
}

// writeProbeDeadline writes the probe's own budget into activeDeadlineSeconds:
// it is the time the probe was bought for, and the one field Foreman overwrites
// on a probe beyond the A1 faces (§8.4). The field counts whole seconds, so the
// budget is rounded up — the probe must never be killed before the time it was
// bought for, and the truncating form rendered a sub-second budget as zero,
// which is an already-expired deadline (immediate DeadlineExceeded). A
// non-positive budget renders none and leaves the template's value alone.
func writeProbeDeadline(job *batchv1.Job, budget time.Duration) {
	if budget <= 0 {
		return
	}
	seconds := int64(budget / time.Second)
	if budget%time.Second != 0 {
		seconds++
	}
	job.Spec.ActiveDeadlineSeconds = &seconds
}

// applyProbeWrites performs the probe's typed writes: the A1.1 env keys, the
// namespace overwrite, and A1.4 ①②③ — the probe name, the label block with
// the foreman.tsic.top/ domain cleared first, and the heartbeat key (④'s
// literal renames run on the template copy in renderProbeTemplate, ahead of the
// generic substitution). Annotations stay as the template wrote them, minus
// that domain: a probe maps no task, so it carries none of the task
// annotations (§4.3).
func (b *Builder) applyProbeWrites(job *batchv1.Job, name string) {
	injectContainerEnv(job, b.cfg.serverURL())

	job.Name = name
	job.Namespace = b.cfg.JobNamespace

	clearForemanDomain(job.Labels)
	clearForemanDomain(job.Annotations)
	clearForemanDomain(job.Spec.Template.Labels)
	clearForemanDomain(job.Spec.Template.Annotations)
	clearOwnedKeys(job.Labels)
	clearOwnedKeys(job.Annotations)
	clearOwnedKeys(job.Spec.Template.Labels)
	clearOwnedKeys(job.Spec.Template.Annotations)

	job.Labels = setKeys(job.Labels, probeLabels(name))
	// Pod side: the probe block's name, role and daemon id — the last is the
	// fieldRef source, and the same writing rule as a task Job's. managed-by is
	// the Job side's selector — nothing lists pods by it.
	job.Spec.Template.Labels = setKeys(job.Spec.Template.Labels, map[string]string{
		labelAppName:  jobAppName,
		labelRole:     probeRole,
		labelDaemonID: name,
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

// probeCredentialSecret is the probe's credential Secret: the same carrier as
// a task's (one config.json, mounted 0400 through cred-src), labelled with the
// probe role and without a task id, so the task-side orphan sweep leaves it to
// the probe sweep (§6.5).
func (b *Builder) probeCredentialSecret(name, token string) *corev1.Secret {
	configJSON := `{"token":` + strconv.Quote(token) + `}`
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CredentialSecretName(name),
			Namespace: b.cfg.JobNamespace,
			Labels:    probeLabels(name),
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{configFileKey: configJSON},
	}
}

// probeValues are the probe render's substitution inputs: the four
// placeholders §8.4 lets a probe consume, plus the credential Secret name
// A1.4 ④ writes on the volume references.
type probeValues struct {
	jobImage     string
	jobRuntimeID string
	// jobName is the probe's own final Job name (A1.4 ①): the template's
	// metadata.name is overwritten with it, and every other reading of
	// <job-name> takes the same value.
	jobName string
	// daemonID is the probe's identity name — a probe's identity is its Job
	// name, which is what its daemon-id label and Job Token carry (§8.4 ②),
	// so the two placeholders agree on this path.
	daemonID   string
	credSecret string
}

// replacement is the probe's consumption face: exactly the four placeholders
// §8.4 lets a probe consume — image, job runtime identity, final Job name and
// daemon identity. Every other registered placeholder is returned unchanged and
// so stays verbatim — it names a task the probe does not have, so there is no
// value to invent (and silence beats failure here: the probe must still
// register).
func (v probeValues) replacement(placeholder string) string {
	switch placeholder {
	case placeholderJobImage:
		return v.jobImage
	case placeholderJobRuntimeID:
		return v.jobRuntimeID
	case placeholderJobName:
		return v.jobName
	case placeholderDaemonID:
		return v.daemonID
	default:
		return placeholder
	}
}

// renderProbeTemplate renders one probe from the startup-parsed template: a
// fresh copy (A1.3), then the object-level steps of the write face in order —
// the A1.4 ④ literal renames, the generic substitution, then the node-reuse
// recognition site with no node (the probe has no soft affinity, so the item
// carrying the recognition element goes with the structures the deletion
// empties). The renames run first, as §8.4 pins: a value the substitution lands
// afterwards — an image reference, say — can never be drawn into a written
// face.
func renderProbeTemplate(template map[string]any, v probeValues) (*batchv1.Job, error) {
	rendered := deepCopyTemplate(template)
	writeProbeLiterals(rendered, v.credSecret)
	substituteValue(rendered, v)

	job, err := decodeJobTemplate(rendered)
	if err != nil {
		return nil, fmt.Errorf("render probe Job: %w", err)
	}
	resolvePreferredNode(job, "")
	return job, nil
}

// writeProbeLiterals performs A1.4 ④ on the template copy: the task profile
// name and the task credential Secret name, each renamed along the face the
// probe write face declares. The profile name is written where the profile is
// expressed — every container's command, arguments and mount path, the daemon's
// own start line and the profile directory beside it (§6.2: profile name and
// mount point come from one constant). The credential Secret name is written
// where the token reaches that directory — every Secret reference a pod volume
// carries. Images, pull credentials, env values, labels and volume names are
// not among those positions, so the write cannot reach them: the template's own
// text there survives (A2) by construction, not by an exclusion list.
func writeProbeLiterals(rendered map[string]any, credSecret string) {
	podSpec := nestedObject(nestedObject(nestedObject(rendered, "spec"), "template"), "spec")
	for _, side := range []string{"containers", "initContainers"} {
		for _, container := range objectList(podSpec, side) {
			renameLiteral(container, jobProfile, probeProfile, "command", "args")
			for _, mount := range objectList(container, "volumeMounts") {
				renameLiteral(mount, jobProfile, probeProfile, "mountPath")
			}
		}
	}
	for _, volume := range objectList(podSpec, "volumes") {
		renameLiteral(nestedObject(volume, "secret"), taskCredSecretLiteral, credSecret, "secretName")
		for _, source := range objectList(nestedObject(volume, "projected"), "sources") {
			renameLiteral(nestedObject(source, "secret"), taskCredSecretLiteral, credSecret, "name")
		}
	}
}

// nestedObject returns one named child object of a raw template node, or nil
// when the template carries none — every position this render writes is
// optional.
func nestedObject(node map[string]any, field string) map[string]any {
	child, _ := node[field].(map[string]any)
	return child
}

// objectList returns the object elements of one named list of a raw template
// node. A missing list, and entries of another shape, contribute none.
func objectList(node map[string]any, field string) []map[string]any {
	children, _ := node[field].([]any)
	objects := make([]map[string]any, 0, len(children))
	for _, child := range children {
		if object, ok := child.(map[string]any); ok {
			objects = append(objects, object)
		}
	}
	return objects
}

// renameLiteral replaces one literal inside the named fields of one raw node:
// a string field's value, or every string element of a list field. A nil node
// is an absent position and is skipped.
func renameLiteral(node map[string]any, from, to string, fields ...string) {
	if node == nil {
		return
	}
	for _, field := range fields {
		switch value := node[field].(type) {
		case string:
			node[field] = strings.ReplaceAll(value, from, to)
		case []any:
			for i, element := range value {
				if text, ok := element.(string); ok {
					value[i] = strings.ReplaceAll(text, from, to)
				}
			}
		}
	}
}
