package jobbuilder

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The catalog probe's fixed names and values (ADR-019 / ADR-025, 03-contracts.md
// §3.2「目录探针 Job 的标签」). A probe runs the same official daemon as a task
// Job, but under its own profile and its own credential — so a probe and a
// task Job can sit on one node without touching each other's profile directory.
const (
	probeJobPrefix    = "fm-catalog-"
	probeRole         = "catalog-probe"
	labelRole         = "foreman.tsic.top/role"
	probeProfile      = "foreman-catalog"
	probeHeartbeatEnv = "MULTICA_DAEMON_HEARTBEAT_INTERVAL"
)

// taskCredSecretLiteral is a template's per-task credential Secret name: the
// literal A1.4 ④ substitutes for the probe's own Secret, beside the task
// profile literal (jobProfile) it renames to the probe profile.
var taskCredSecretLiteral = jobName(placeholderTaskID) + secretCredSuffix

// ErrInvalidRequestID marks a request id that cannot name a probe Job: no
// object is created, and the caller reports the relay as failed.
var ErrInvalidRequestID = errors.New("invalid request id")

// BuildCatalogProbe renders the catalog-probe Job (fm-catalog-<requestID>) and
// its credential Secret, returning the digest of the token that Secret carries
// (03-contracts.md §2 CatalogProbe.Start). Its shape is the deployment
// template's copy plus the probe write face (ADR-025): A1.1's env keys, the
// namespace, A1.4's four items and the two placeholders a probe consumes. The
// budget becomes activeDeadlineSeconds — the probe's own deadline, written by
// Foreman and not by the template (§5.1) — and the token always outlives the
// Job by the drain bound (ADR-017).
func (b *Builder) BuildCatalogProbe(requestID, jobRuntimeID, workspaceID string, budget time.Duration) (*batchv1.Job, *corev1.Secret, string, error) {
	name := probeJobName(requestID)
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
// the template copy through the probe write face, then the budget deadline.
func (b *Builder) renderProbe(name, jobRuntimeID string, budget time.Duration) (*batchv1.Job, error) {
	job, err := renderProbeTemplate(b.template, probeValues{
		jobImage:     b.cfg.JobImage,
		jobRuntimeID: jobRuntimeID,
		credSecret:   name + secretCredSuffix,
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
// on a probe beyond the A1 faces (§5.1 探针的行为面). A non-positive budget
// renders none rather than an already-expired deadline.
func writeProbeDeadline(job *batchv1.Job, budget time.Duration) {
	if budget <= 0 {
		return
	}
	job.Spec.ActiveDeadlineSeconds = new(int64(budget / time.Second))
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

// probeCredentialSecret is the probe's credential Secret: the same carrier as
// a task's (one config.json, mounted 0400 through cred-src), labelled with the
// probe role and without a task id, so the task-side orphan sweep leaves it to
// the probe sweep (03-contracts.md §1.3).
func (b *Builder) probeCredentialSecret(name, token string) *corev1.Secret {
	configJSON := `{"token":` + strconv.Quote(token) + `}`
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + secretCredSuffix,
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
// the A1.4 literals, the generic substitution, then the node-reuse recognition
// site with no node (the probe has no soft affinity, so the item carrying the
// recognition element goes with the structures the deletion empties).
func renderProbeTemplate(template map[string]any, v probeValues) (*batchv1.Job, error) {
	rendered := deepCopyTemplate(template)
	rewriteStrings(rendered, v.literals)
	rewriteStrings(rendered, v.placeholders)
	resolvePreferredNode(rendered, "")

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
