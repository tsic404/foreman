// Package jobbuilder renders a TaskEntry into the Job and credential Secret
// the scheduler creates, including the deployment overlay pipeline
// (ConfigMap job-overlay.yaml + two-stage invariant validation). Design:
// docs/05-modules/job-template.md, contracts §3.2/§5.2/§5.4.
package jobbuilder

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// TaskEntry is the subset of the registry entry (contract §3.1) that Build needs.
type TaskEntry struct {
	TaskID          string
	AgentID         string
	IssueID         string
	IssueIdentifier string
	WorkspaceID     string
	JobRuntimeID    string
	ClaimedAt       time.Time
	Attempt         int
}

// TokenIssuer signs per-Job Tokens; satisfied by auth.Issuer.
type TokenIssuer interface {
	Issue(jobName, taskID, workspaceID string, ttl time.Duration) (string, error)
}

// NodeIndex reports the node an issue's previous Job ran on; it backs the
// soft node-reuse affinity (FOREMAN_PREFER_NODE_REUSE).
type NodeIndex interface {
	LastNodeForIssue(issueID string) string
	// NodeSaturated reports whether the node currently sits at or over the
	// per-node soft cap (FOREMAN_MAX_JOBS_PER_NODE, ADR-006 节流). A
	// saturated node drops out of the candidate set of the next Job, so the
	// cap is a placement preference — never admission control.
	NodeSaturated(nodeName string) bool
}

var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ErrInvalidTaskID marks a task whose ID cannot become a legal Job name:
// Build fails before any object exists and the scheduler settles the task with
// failure_reason=invalid_task_id (05-modules/job-template.md §错误处理).
var ErrInvalidTaskID = errors.New("invalid task id")

// Builder renders Job/Secret objects; it never talks to the K8s API
// (creation is the scheduler's job).
type Builder struct {
	cfg            Config
	overlay        map[string]any
	overlayPresent bool
}

// NewBuilder runs the startup gate (job-template.md §关键流程 step 0): it
// loads and validates the optional overlay, so an illegal template makes main
// exit non-zero before the listener starts (CrashLoopBackOff under the
// Deployment) instead of surfacing at the first claim.
func NewBuilder(cfg Config) (*Builder, error) {
	overlay, present, err := LoadAndValidateOverlay(cfg)
	if err != nil {
		return nil, err
	}
	return &Builder{cfg: cfg, overlay: overlay, overlayPresent: present}, nil
}

// NewBuilderWithOverlay injects an overlay that bypassed the startup gate, so
// tests can drive the build-time InvariantChecker fallback (TC-tech-job-03
// 「构建期兜底」). Production paths go through NewBuilder.
func NewBuilderWithOverlay(cfg Config, overlay map[string]any) *Builder {
	return &Builder{cfg: cfg, overlay: overlay, overlayPresent: true}
}

// Build renders the Job and its credential Secret for e. An invalid task ID
// fails before any object is produced; a template that breaks the invariants
// fails with *ValidationError (the scheduler maps it to
// failure_reason=invalid_job_template and creates nothing).
func (b *Builder) Build(e TaskEntry) (*batchv1.Job, *corev1.Secret, error) {
	name := jobName(e.TaskID)
	if len(name) > 63 || !dns1123Label.MatchString(name) {
		return nil, nil, fmt.Errorf("%w: %q does not yield a valid Job name %q", ErrInvalidTaskID, e.TaskID, name)
	}
	if b.cfg.Issuer == nil {
		return nil, nil, fmt.Errorf("Config.Issuer is required")
	}
	job, err := b.render(name, e)
	if err != nil {
		return nil, nil, err
	}
	token, err := b.cfg.Issuer.Issue(name, e.TaskID, e.WorkspaceID, b.cfg.JobTokenTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("issue Job Token: %w", err)
	}
	return job, b.credentialSecret(name, e.TaskID, token), nil
}

// render is the build pipeline (job-template.md §关键流程 steps 4–8): default
// template → strategic merge patch → initContainers normalization →
// authoritative per-task writes → node-reuse affinity → InvariantChecker.
func (b *Builder) render(name string, e TaskEntry) (*batchv1.Job, error) {
	job := b.defaultJob(name, e)
	if b.overlayPresent {
		merged, err := mergeOverlay(job, b.overlay)
		if err != nil {
			// e.g. an appended initContainers entry without a name: fail
			// closed instead of dropping the entry (ErrNoMergeKey).
			return nil, &ValidationError{Source: "job overlay", Violations: []Violation{{
				Path: "job-overlay.yaml",
				Rule: "清单 A/§5.4: strategic merge patch failed: " + err.Error(),
			}}}
		}
		job = merged
	}
	normalizeInitContainers(job, b.overlay)
	b.applyAuthoritative(job, name, e)
	b.applyPreferredNode(job, e)
	if violations := b.checker().Check(job); len(violations) > 0 {
		return nil, &ValidationError{Source: "job template", Violations: violations}
	}
	return job, nil
}

// applyAuthoritative re-writes the per-task fields after the merge (§5.4 step
// 3): an overlay value for any of them was already rejected, so the write is
// the second half of the 权威写入 guarantee rather than a silent override.
func (b *Builder) applyAuthoritative(job *batchv1.Job, name string, e TaskEntry) {
	job.Name = name
	job.Namespace = b.cfg.JobNamespace
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	for key, value := range b.identityLabels(e) {
		job.Labels[key] = value
	}
	for key, value := range b.identityAnnotations(name, e) {
		job.Annotations[key] = value
	}
	template := &job.Spec.Template
	if template.Labels == nil {
		template.Labels = map[string]string{}
	}
	template.Labels[labelAppName] = jobAppName
	template.Labels[labelTaskID] = e.TaskID

	for i := range template.Spec.Volumes {
		if template.Spec.Volumes[i].Name == volumeCredSrc {
			template.Spec.Volumes[i].VolumeSource = credSrcVolume(name).VolumeSource
		}
	}
	for i := range template.Spec.Containers {
		if template.Spec.Containers[i].Name == containerAgent {
			template.Spec.Containers[i].Image = b.cfg.imageRef()
			template.Spec.Containers[i].Env = authoritativeEnv(template.Spec.Containers[i].Env)
		}
	}
	// prepare and agent share one image so the credential-copy script runs
	// against the same tool set (清单 B).
	for i := range template.Spec.InitContainers {
		if template.Spec.InitContainers[i].Name == containerPrepare {
			template.Spec.InitContainers[i].Image = b.cfg.imageRef()
		}
	}
}

// applyPreferredNode appends the node-reuse affinity item (free path, §5.4
// 「affinity 注入」): appended after the merge so an overlay affinity block is
// extended, never replaced.
func (b *Builder) applyPreferredNode(job *batchv1.Job, e TaskEntry) {
	node := b.preferredNode(e)
	if node == "" {
		return
	}
	pod := &job.Spec.Template.Spec
	if pod.Affinity == nil {
		pod.Affinity = &corev1.Affinity{}
	}
	if pod.Affinity.NodeAffinity == nil {
		pod.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	pod.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		pod.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, preferredTerm(node))
}

// authoritativeEnv keeps the 16 §5.2 keys at their contracted values while
// preserving overlay-added entries (§5.4 清单 A env 保留).
func authoritativeEnv(existing []corev1.EnvVar) []corev1.EnvVar {
	want := agentEnv()
	byName := make(map[string]corev1.EnvVar, len(want))
	for _, entry := range want {
		byName[entry.Name] = entry
	}
	present := make(map[string]bool, len(want))
	for i, entry := range existing {
		if contracted, ok := byName[entry.Name]; ok {
			existing[i] = contracted
			present[entry.Name] = true
		}
	}
	for _, contracted := range want {
		if !present[contracted.Name] {
			existing = append(existing, contracted)
		}
	}
	return existing
}

func (b *Builder) checker() InvariantChecker {
	return InvariantChecker{cfg: b.cfg}
}

// preferredNode returns the soft-affinity node for e's issue, "" when reuse
// is disabled, no history exists, or the node sits at its concurrency cap:
// a saturated node must not attract another Job (task-mapping §同节点并发 —
// the cap lowers the node's priority, it never blocks the Job).
func (b *Builder) preferredNode(e TaskEntry) string {
	if !b.cfg.PreferNodeReuse || b.cfg.Nodes == nil {
		return ""
	}
	node := b.cfg.Nodes.LastNodeForIssue(e.IssueID)
	if node == "" || b.cfg.Nodes.NodeSaturated(node) {
		return ""
	}
	return node
}

// jobName is the single source of the Job/Secret/daemon naming convention.
func jobName(taskID string) string {
	return "fm-" + taskID
}

// The startup gate renders the default template once to reach stage 2; the
// per-task placeholders are irrelevant to the invariant check and stay out of
// any real object.
const dryRunJobName = "fm-overlay-probe"

func dryRunEntry() TaskEntry {
	return TaskEntry{
		TaskID:          "overlay-probe",
		AgentID:         "overlay-probe",
		IssueID:         "overlay-probe",
		IssueIdentifier: "overlay-probe",
		WorkspaceID:     "overlay-probe",
		JobRuntimeID:    "overlay-probe",
		ClaimedAt:       time.Unix(0, 0).UTC(),
		Attempt:         1,
	}
}
