// Package jobbuilder renders a TaskEntry plus task payload into the Job and
// credential Secret the scheduler creates. Design: docs/05-modules/job-template.md,
// contracts §3.2/§5.2.
package jobbuilder

import (
	"encoding/json"
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

// Builder renders Job/Secret objects; it never talks to the K8s API
// (creation is the scheduler's job).
type Builder struct {
	cfg Config
}

// NewBuilder returns a Builder for cfg (see LoadConfig); cfg.Issuer must be set.
func NewBuilder(cfg Config) *Builder {
	return &Builder{cfg: cfg}
}

// Build renders the Job and its credential Secret for e. An invalid task ID
// fails before any object is produced; the payload annotation is best-effort
// (dropped when it cannot be redacted safely, never a Build error).
func (b *Builder) Build(e TaskEntry, payload json.RawMessage) (*batchv1.Job, *corev1.Secret, error) {
	name := jobName(e.TaskID)
	if len(name) > 63 || !dns1123Label.MatchString(name) {
		return nil, nil, fmt.Errorf("task_id %q does not yield a valid Job name %q", e.TaskID, name)
	}
	if b.cfg.Issuer == nil {
		return nil, nil, fmt.Errorf("Config.Issuer is required")
	}
	token, err := b.cfg.Issuer.Issue(name, e.TaskID, e.WorkspaceID, b.cfg.JobTokenTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("issue Job Token: %w", err)
	}
	return b.job(name, e, payload), b.credentialSecret(name, e.TaskID, token), nil
}

// jobName is the single source of the Job/Secret/daemon naming convention.
func jobName(taskID string) string {
	return "fm-" + taskID
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

func (b *Builder) cacheMode() string {
	return b.cfg.RepoCacheMode
}
