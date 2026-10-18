// Package jobbuilder renders a TaskEntry into the Job and credential Secret
// the scheduler creates, and the catalog-probe Job with its own credential
// (ADR-019). The Job's shape comes from one place: the deployment's complete
// Job template (ConfigMap foreman-job-template, key job-overlay.yaml), which
// Foreman uses verbatim apart from its own writes — the A1.1 env keys and the
// A1.2 per-task writes. The template is a required
// deployment artifact: a missing, unreadable or unparseable one refuses
// startup (ADR-030), and there is no built-in fallback.
// Design: 02-contracts.md §3, §6.2, §8.1–§8.7, §10.
package jobbuilder

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

// TaskEntry is the subset of the registry entry (§4.1) that Build needs.
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
	// AgentName is the payload's raw agent.name — the agent axis's identity
	// input and the <agent-name> placeholder's value (ADR-035 keyspace =
	// names). Empty means the agent's identity is unavailable, never a
	// failure.
	AgentName string
	// ProjectName is the task's project identity (§4.1): the payload's
	// structured project key name, or the read-only reverse lookup's answer.
	ProjectName string
	// ProjectSource records where ProjectName came from (payload / api / none);
	// it is projected into policy.applied only.
	ProjectSource string
	// NameSource is set by Build from the template's name format: "template"
	// when the template expresses a format, "fallback" when it does not.
	NameSource string
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
	// ("" when unknown); the key is a normalized session key (§4.5).
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
// failure_reason=invalid_task_id (§4.4).
var ErrInvalidTaskID = errors.New("invalid task id")

// Builder renders Job/Secret objects; it never talks to the K8s API
// (creation is the scheduler's job).
type Builder struct {
	cfg Config
	// template is the startup-parsed template; it is read-only and every
	// Build renders a copy of it (A1.3).
	template map[string]any
	// policy is the startup-parsed policy table: read-only, shared by every
	// Build, never reloaded (ADR-035 decision 7). A Builder constructed
	// without it renders with all three axes at None.
	policy Policy
}

// NewBuilder runs the startup gate (§8.1): it loads
// and parses the required Job template, so a missing, unreadable or
// unparseable one — or one whose metadata.name is not a legal name format —
// makes main exit non-zero before the listener starts (CrashLoopBackOff under
// the Deployment) instead of surfacing at the first claim. Step 0b loads the
// optional policy file at the same point, with its own failure classes.
func NewBuilder(cfg Config) (*Builder, error) {
	template, err := LoadJobTemplate(cfg)
	if err != nil {
		return nil, err
	}
	policy, err := LoadPolicy(cfg)
	if err != nil {
		return nil, err
	}
	return &Builder{cfg: cfg, template: template, policy: policy}, nil
}

// Build renders the Job, its credential Secret and the digest of the token
// that Secret carries (the digest OnClaim records is the only handle the
// reclaim path has on the credential). The display name comes
// from the template's format; the identity name fm-<task_id> is what the Job
// Token, the Secret and the daemon-id faces derive from (ADR-034 decision 8).
// An invalid task id, or a name that leaves the name face, fails before any
// object is produced.
func (b *Builder) Build(e TaskEntry) (*batchv1.Job, *corev1.Secret, string, error) {
	daemonID := daemonIDFor(e.TaskID)
	if len(daemonID) > maxJobNameLength || !dns1123Label.MatchString(daemonID) {
		return nil, nil, "", fmt.Errorf("%w: %q does not yield a valid daemon id %q", ErrInvalidTaskID, e.TaskID, daemonID)
	}
	if b.cfg.Issuer == nil {
		return nil, nil, "", fmt.Errorf("Config.Issuer is required")
	}
	e.NameSource = b.nameSource()
	name, err := b.ResolveName(e)
	if err != nil {
		return nil, nil, "", err
	}
	job, ev, err := b.render(name, daemonID, e)
	if err != nil {
		return nil, nil, "", err
	}
	token, err := b.cfg.Issuer.Issue(daemonID, e.TaskID, e.WorkspaceID, b.cfg.JobTokenTTL)
	if err != nil {
		return nil, nil, "", fmt.Errorf("issue Job Token: %w", err)
	}
	b.logPolicyApplied(e, name, ev)
	return job, b.credentialSecret(daemonID, e.TaskID, token), b.cfg.Issuer.Hash(token), nil
}

// nameSource reports whether the template expresses a name format (ADR-034):
// the sole input of policy.applied's name_source.
func (b *Builder) nameSource() string {
	if templateNameFormat(b.template) == "" {
		return "fallback"
	}
	return "template"
}

// render is the build pipeline (02-contracts.md §8.2, one order for
// every task): take a template copy, compose the name, substitute the
// registered placeholders, inject the policy's three axes, settle the
// <preferred node> item against the policy outcome, then perform Foreman's own
// ownership writes. There is one shape source — the startup-parsed template.
func (b *Builder) render(name, daemonID string, e TaskEntry) (*batchv1.Job, Evaluation, error) {
	rendered := deepCopyTemplate(b.template)
	// Step 2: the name composition writes metadata.name before the
	// substitution, because <job-name> must already hold the final name.
	setTemplateName(rendered, name)
	// Step 3: the generic substitution — policy values are not in this copy
	// yet, so a strategy's own <...> literal can never be re-substituted.
	substituteValue(rendered, b.placeholderValues(name, daemonID, e))

	job, err := decodeJobTemplate(rendered)
	if err != nil {
		return nil, Evaluation{}, err
	}
	// Step 4: the three-axis policy injection.
	ev := b.policy.Evaluate(e)
	applyPolicy(job, ev)
	// Step 5: the <preferred node> conditional block, which depends on the
	// nodeSelector axis outcome (ADR-036).
	node, suppressed := b.preferredNode(e, len(ev.nodeSelector) > 0)
	if ev.suppressed != SuppressionPolicy {
		ev.suppressed = suppressed
	}
	resolvePreferredNode(job, node)
	// Step 6: forced ownership — domain clear-then-write, the A1.2 key sets by
	// side, the A1.1 env keys and the namespace.
	b.applyWrites(job, name, daemonID, e)
	return job, ev, nil
}

// setTemplateName writes the composed display name into the template copy
// (A1.2's first item). The template's metadata object is created when it
// declares none — the format may only be absent, never the carrier.
func setTemplateName(template map[string]any, name string) {
	metadata, ok := template["metadata"].(map[string]any)
	if !ok {
		metadata = map[string]any{}
		template["metadata"] = metadata
	}
	metadata["name"] = name
}

// decodeJobTemplate turns the rendered map into the typed Job the writes act
// on. Numbers were kept as json.Number by the parser, so an int64 field above
// 2^53 survives the round trip.
func decodeJobTemplate(rendered map[string]any) (*batchv1.Job, error) {
	raw, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf("render job template: %w", err)
	}
	var job batchv1.Job
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, fmt.Errorf("render job template: %w", err)
	}
	return &job, nil
}

// placeholderValues resolves the template placeholders for one task; the
// preferred node comes from the soft-affinity decision §8.6 pins to the
// session key.
func (b *Builder) placeholderValues(name, daemonID string, e TaskEntry) placeholderValues {
	return placeholderValues{
		taskID:          e.TaskID,
		agentID:         e.AgentID,
		workspaceID:     e.WorkspaceID,
		issueID:         e.IssueID,
		issueIdentifier: e.IssueIdentifier,
		autopilotRunID:  e.AutopilotRunID,
		chatSessionID:   e.ChatSessionID,
		jobRuntimeID:    e.JobRuntimeID,
		claimedAt:       e.ClaimedAt.UTC().Format(time.RFC3339),
		jobImage:        b.cfg.JobImage,
		jobName:         name,
		daemonID:        daemonID,
		agentName:       e.AgentName,
	}
}

// applyWrites performs Foreman's own writes on the rendered Job — the only
// kinds a template passthrough allows (§8.2 A1.1/A1.2): the env keys on
// containers[0], the per-task metadata, and the per-side key set.
func (b *Builder) applyWrites(job *batchv1.Job, name, daemonID string, e TaskEntry) {
	injectContainerEnv(job)

	job.Name = name
	job.Namespace = b.cfg.JobNamespace

	// foreman.tsic.top/ is Foreman's own domain: clear it before writing, so a
	// static template key (a hard-coded issue-id above all) cannot survive
	// into a task it does not describe. The two app.kubernetes.io keys are
	// Foreman-owned too, and each side's key set decides whether they return:
	// the Job side keeps both, the pod side keeps only the name (ADR-034
	// decision 7, §8.2 A1.2).
	clearForemanDomain(job.Labels)
	clearForemanDomain(job.Annotations)
	clearForemanDomain(job.Spec.Template.Labels)
	clearForemanDomain(job.Spec.Template.Annotations)
	clearOwnedKeys(job.Labels)
	clearOwnedKeys(job.Annotations)
	clearOwnedKeys(job.Spec.Template.Labels)
	clearOwnedKeys(job.Spec.Template.Annotations)

	job.Labels = setKeys(job.Labels, b.identityLabels(e))
	job.Annotations = setKeys(job.Annotations, b.identityAnnotations(daemonID, e))
	// Pod side: the three A1.2 labels (app name, task id, daemon id) — no
	// annotations, no managed-by (§4.3 lists Jobs, never pods). The
	// daemon-id label is what MULTICA_DAEMON_ID's fieldRef reads, so it is
	// Foreman's own write and never the template's (ADR-034 decision 8).
	job.Spec.Template.Labels = setKeys(job.Spec.Template.Labels, map[string]string{
		labelAppName:  jobAppName,
		labelTaskID:   e.TaskID,
		labelDaemonID: daemonID,
	})
}

// injectContainerEnv writes the A1.1 keys into containers[0] — the template's
// first entry by writing order, never one matched by name (§8.2 A1.1). Every
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
// A1.2 write that makes the conditional trigger-context keys deletable: an
// empty TaskEntry field must remove a template's static key, not leave it
// pointing at the wrong task.
func clearForemanDomain(keys map[string]string) {
	for key := range keys {
		if strings.HasPrefix(key, foremanDomain) {
			delete(keys, key)
		}
	}
}

// clearOwnedKeys drops the two app.kubernetes.io keys Foreman owns
// (ADR-034 decision 7: their value is Foreman's fixed value wherever the key
// set keeps them, so a template's same-name value is discarded). Only the
// per-side key set decides whether they come back: the Job side carries both,
// the pod side carries app.kubernetes.io/name alone, and the probe's Job side
// carries both — never managed-by on a pod.
func clearOwnedKeys(keys map[string]string) {
	delete(keys, labelAppName)
	delete(keys, labelManagedBy)
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

// preferredNode resolves the single soft-affinity node for e's session and
// reports the cause when the item must go. A nodeSelector axis injection wins
// outright — the hard constraint is not knowable against the session's node, so
// the conservative suppression applies (ADR-036) and the cause is policy. The
// session key must be non-empty and known to the index, and its node must not
// sit at the per-node cap (§8.6, AC-26 — the cap lowers the node's
// priority, it never blocks the Job); that failure is the saturation
// cause. Reuse off, no resume session and an unknown key report none. A task
// without a prior session has no key at all: `issue_id` is deliberately not one
// (ADR-015).
func (b *Builder) preferredNode(e TaskEntry, policyInjected bool) (string, string) {
	if policyInjected {
		return "", SuppressionPolicy
	}
	if !b.cfg.PreferNodeReuse || b.cfg.Nodes == nil {
		return "", SuppressionNone
	}
	key := registry.SessionKey(e.PriorSessionID)
	if key == "" {
		return "", SuppressionNone
	}
	node := b.cfg.Nodes.LastNodeForSession(key)
	if node == "" {
		return "", SuppressionNone
	}
	if b.cfg.Nodes.Saturated(node) {
		return "", SuppressionSaturation
	}
	return node, SuppressionNone
}
