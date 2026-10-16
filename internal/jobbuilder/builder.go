// Package jobbuilder renders a TaskEntry into the Job and credential Secret
// the scheduler creates, and the catalog-probe Job with its own credential
// (ADR-019). A deployment may supply a complete Job template
// (ConfigMap job-overlay.yaml); Foreman uses it verbatim apart from its own
// writes — the A1.1 env keys, the per-task metadata and the §3.2 key set.
// Design: docs/05-modules/job-template.md, contracts §3.2/§5.2,
// proposals/P-006-template-passthrough-env-injection.md.
package jobbuilder

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// TaskEntry is the subset of the registry entry (contract §3.1) that Build needs.
type TaskEntry struct {
	TaskID          string
	AgentID         string
	IssueID         string
	IssueIdentifier string
	AutopilotRunID  string
	ChatSessionID   string
	WorkspaceID     string
	JobRuntimeID    string
	ClaimedAt       time.Time
	Attempt         int
	// PriorSessionID is the claim payload's session-resume path; its session
	// key is the only soft-affinity key (ADR-015).
	PriorSessionID string
}

// TokenIssuer signs per-Job Tokens; satisfied by auth.Issuer. Hash is the
// digest of the issued token — the value OnClaim records as token_hash
// (ADR-017), so the token itself never travels past the Secret.
type TokenIssuer interface {
	Issue(jobName, taskID, workspaceID string, ttl time.Duration) (string, error)
	// IssueProbe signs a catalog probe's Job Token (ADR-019): the same
	// mechanism with a role-marked payload and no task id. requestID is part
	// of the call's parity with the probe's Start, not of the payload.
	IssueProbe(jobName, requestID, workspaceID string, ttl time.Duration) (string, error)
	Hash(token string) string
}

// NodeIndex exposes the registry's session index and per-node load markers;
// it backs the soft node-reuse affinity (FOREMAN_PREFER_NODE_REUSE).
type NodeIndex interface {
	// LastNodeForSession reports the node a session key was learned on
	// ("" when unknown); the key is a normalized session key (§3.1).
	LastNodeForSession(key string) string
	// Saturated reports whether the node currently sits at or over the
	// per-node soft cap (FOREMAN_MAX_JOBS_PER_NODE, ADR-006 节流). A
	// saturated node drops out of the candidate set of the next Job, so the
	// cap is a placement preference — never admission control.
	Saturated(nodeName string) bool
}

var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ErrInvalidTaskID marks a task whose ID cannot become a legal Job name:
// Build fails before any object exists and the scheduler settles the task with
// failure_reason=invalid_task_id (05-modules/job-template.md §错误处理).
var ErrInvalidTaskID = errors.New("invalid task id")

// Builder renders Job/Secret objects; it never talks to the K8s API
// (creation is the scheduler's job).
type Builder struct {
	cfg      Config
	template map[string]any
	// templatePresent is true when the deployment supplied a template; the
	// parsed map is then read-only and every Build renders a copy of it.
	templatePresent bool
}

// NewBuilder runs the startup gate (job-template.md §关键流程 step 0): it
// loads and parses the optional Job template, so an unreadable or unparseable
// one makes main exit non-zero before the listener starts (CrashLoopBackOff
// under the Deployment) instead of surfacing at the first claim.
func NewBuilder(cfg Config) (*Builder, error) {
	template, present, err := LoadJobTemplate(cfg)
	if err != nil {
		return nil, err
	}
	return &Builder{cfg: cfg, template: template, templatePresent: present}, nil
}

// Build renders the Job, its credential Secret and the digest of the token
// that Secret carries (contract §2: the token_hash OnClaim records is the
// only handle the reclaim path has on the credential). An invalid task ID
// fails before any object is produced.
func (b *Builder) Build(e TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	name := jobName(e.TaskID)
	if len(name) > 63 || !dns1123Label.MatchString(name) {
		return nil, nil, "", fmt.Errorf("%w: %q does not yield a valid Job name %q", ErrInvalidTaskID, e.TaskID, name)
	}
	if b.cfg.Issuer == nil {
		return nil, nil, "", fmt.Errorf("Config.Issuer is required")
	}
	job, err := b.render(name, e)
	if err != nil {
		return nil, nil, "", err
	}
	token, err := b.cfg.Issuer.Issue(name, e.TaskID, e.WorkspaceID, b.cfg.JobTokenTTL)
	if err != nil {
		return nil, nil, "", fmt.Errorf("issue Job Token: %w", err)
	}
	return job, b.credentialSecret(name, e.TaskID, token), b.cfg.Issuer.Hash(token), nil
}

// render is the build pipeline: pick the template — the deployment's copy or
// the built-in default — then apply Foreman's writes to it.
func (b *Builder) render(name string, e TaskEntry) (*batchv1.Job, error) {
	var job *batchv1.Job
	if b.templatePresent {
		rendered, err := renderTemplate(b.template, b.placeholderValues(e))
		if err != nil {
			return nil, err
		}
		job = rendered
	} else {
		job = b.defaultJob(name, e)
		// The built-in default template carries no placeholders: its
		// node-reuse item is appended directly (AC-20 keeps this path
		// byte-identical to the baseline).
		b.applyPreferredNode(job, e)
	}
	b.applyWrites(job, name, e)
	return job, nil
}

// placeholderValues resolves the template placeholders for one task; the
// preferred node comes from the same soft-affinity decision the default
// template uses.
func (b *Builder) placeholderValues(e TaskEntry) placeholderValues {
	v := placeholderValues{
		taskID:          e.TaskID,
		agentID:         e.AgentID,
		workspaceID:     e.WorkspaceID,
		issueID:         e.IssueID,
		issueIdentifier: e.IssueIdentifier,
		autopilotRunID:  e.AutopilotRunID,
		chatSessionID:   e.ChatSessionID,
		jobRuntimeID:    e.JobRuntimeID,
		claimedAt:       e.ClaimedAt.UTC().Format(time.RFC3339),
		jobImage:        b.cfg.imageRef(),
	}
	if node, ok := b.preferredNode(e); ok {
		v.preferredNode = node
	}
	return v
}

// applyWrites performs Foreman's own writes on the rendered Job — the only
// three kinds a template passthrough allows (P-006 §A1.1/A1.2): the env keys on
// containers[0], the per-task metadata, and the §3.2 key set by side.
func (b *Builder) applyWrites(job *batchv1.Job, name string, e TaskEntry) {
	injectContainerEnv(job)

	job.Name = name
	job.Namespace = b.cfg.JobNamespace

	// foreman.tsic.top/ is Foreman's own domain: clear it before writing, so a
	// static template key (a hard-coded issue-id above all) cannot survive
	// into a task it does not describe.
	clearForemanDomain(job.Labels)
	clearForemanDomain(job.Annotations)
	clearForemanDomain(job.Spec.Template.Labels)
	clearForemanDomain(job.Spec.Template.Annotations)

	job.Labels = setKeys(job.Labels, b.identityLabels(e))
	job.Annotations = setKeys(job.Annotations, b.identityAnnotations(name, e))
	// Pod side: the two §3.2 labels only — no annotations, no managed-by
	// (task-mapping lists Jobs, never pods).
	job.Spec.Template.Labels = setKeys(job.Spec.Template.Labels, map[string]string{
		labelAppName: jobAppName,
		labelTaskID:  e.TaskID,
	})
}

// injectContainerEnv writes the A1.1 keys into containers[0] — the template's
// first entry by writing order, never one matched by name (P-006 §A1.1). Every
// same-name entry is replaced whole and only one survives: a template may
// declare the key twice, and the daemon reads the last one (os.Getenv), so
// leaving the later entry would let the template's value win over Foreman's.
// A template without containers is left alone — the apiserver rejects the Job
// and the scheduler reports the existing job_create_failed.
func injectContainerEnv(job *batchv1.Job) {
	containers := job.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		return
	}
	injected := injectedEnv()
	wanted := make(map[string]corev1.EnvVar, len(injected))
	for _, entry := range injected {
		wanted[entry.Name] = entry
	}

	env := containers[0].Env
	kept := make([]corev1.EnvVar, 0, len(env)+len(injected))
	written := make(map[string]bool, len(injected))
	for _, entry := range env {
		want, isInjected := wanted[entry.Name]
		if !isInjected {
			kept = append(kept, entry)
			continue
		}
		if written[entry.Name] {
			continue
		}
		kept = append(kept, want)
		written[entry.Name] = true
	}
	for _, want := range injected {
		if !written[want.Name] {
			kept = append(kept, want)
		}
	}
	containers[0].Env = kept
}

// clearForemanDomain drops every key of Foreman's own domain, the half of the
// §3.2 write that makes the conditional trigger-context keys deletable: an
// empty TaskEntry field must remove a template's static key, not leave it
// pointing at the wrong task.
func clearForemanDomain(keys map[string]string) {
	for key := range keys {
		if strings.HasPrefix(key, foremanDomain) {
			delete(keys, key)
		}
	}
}

// setKeys merges keys into dst, allocating it when the template declared none.
func setKeys(dst map[string]string, keys map[string]string) map[string]string {
	if dst == nil {
		dst = make(map[string]string, len(keys))
	}
	for key, value := range keys {
		dst[key] = value
	}
	return dst
}

// applyPreferredNode appends the node-reuse affinity item to the built-in
// default template (03-contracts §5.4 「affinity 注入」). A deployment template
// expresses the same decision with the <preferred node> placeholder instead —
// Foreman never adds an affinity item to it (P-006 §A2).
func (b *Builder) applyPreferredNode(job *batchv1.Job, e TaskEntry) {
	node, ok := b.preferredNode(e)
	if !ok {
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

// preferredNode resolves the single soft-affinity node for e's session: the
// session key must be non-empty and known to the index, and its node must not
// sit at the per-node cap (task-mapping §同节点并发 — the cap lowers the
// node's priority, it never blocks the Job). Reuse off, no resume session, an
// unknown key and a saturated node all answer (_, false), which drops the
// item. A task without a prior session has no key at all: `issue_id` is
// deliberately not one (ADR-015).
func (b *Builder) preferredNode(e TaskEntry) (string, bool) {
	if !b.cfg.PreferNodeReuse || b.cfg.Nodes == nil {
		return "", false
	}
	key := registry.SessionKey(e.PriorSessionID)
	if key == "" {
		return "", false
	}
	node := b.cfg.Nodes.LastNodeForSession(key)
	if node == "" || b.cfg.Nodes.Saturated(node) {
		return "", false
	}
	return node, true
}

// jobName is the single source of the Job/Secret/daemon naming convention.
func jobName(taskID string) string {
	return "fm-" + taskID
}
