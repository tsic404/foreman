package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// ProjectIdentity is the task's project identity input (§4.1
// 项目身份, ADR-048 decision 1). Its two sources are both platform-side
// capabilities the design leaves to the platform — the claim payload's
// top-level project identifier and the read-only reverse lookup by issue id —
// so a nil seam means the platform offers neither: the task reads
// project_source=none and the policy table's project tiers degrade to
// no_identity, while the task runs on.
type ProjectIdentity interface {
	// FromPayload reports the platform project identifier the claim payload
	// carries at its top-level project_id; "" means the payload has none,
	// which is legal and never a payload error. It must not read the
	// project_* prompt-context fields — title and description are not the
	// identity.
	FromPayload(task json.RawMessage) string
	// ByIssue performs the read-only identity query for one issue id, with
	// the Job's workspace credential (§4.1 多 workspace 口径). One attempt, no
	// retry: an error (unreachable, timeout, no such query) is the caller's
	// signal to record project.lookup_failed and continue with no identity.
	ByIssue(ctx context.Context, workspaceID, issueID string) (string, error)
}

// projectCacheTTL is the short TTL of the issue-id lookup cache (ADR-048
// decision 4: 按 issue 的短 TTL 缓存属实现面; the cache is not a contract
// face).
const projectCacheTTL = 60 * time.Second

// projectCache memoizes the reverse lookup per issue id, negative answers
// included, so a burst of tasks from one issue does not query the platform
// once per task.
type projectCache struct {
	mu      sync.Mutex
	entries map[string]projectCacheEntry
}

type projectCacheEntry struct {
	projectID string
	cachedAt  time.Time
}

// get returns the cached identifier and whether the entry is still fresh. A
// cached empty identifier is a fresh negative answer, not a miss. The key
// carries the workspace: the same issue id resolves independently per
// workspace (§4.1).
func (c *projectCache) get(workspaceID, issueID string, now time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[projectCacheKey(workspaceID, issueID)]
	if !ok || now.Sub(entry.cachedAt) > projectCacheTTL {
		return "", false
	}
	return entry.projectID, true
}

func (c *projectCache) put(workspaceID, issueID, projectID string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]projectCacheEntry{}
	}
	c.entries[projectCacheKey(workspaceID, issueID)] = projectCacheEntry{projectID: projectID, cachedAt: now}
}

// projectCacheKey partitions the reverse-lookup cache by workspace.
func projectCacheKey(workspaceID, issueID string) string {
	return workspaceID + "\x00" + issueID
}

// projectIdentity resolves the project identity of one claim: the payload's
// identifier first, then — only when the issue id exists — the read-only
// lookup, and none when neither yields an identifier. Every failure is a warn;
// the task continues with the project tiers degraded (ADR-048 decision 4,
// AC-54).
func (s *Scheduler) projectIdentity(ctx context.Context, task json.RawMessage, head claimHead) (string, string) {
	workspaceID := head.WorkspaceID
	if s.cfg.Project == nil {
		return "", jobbuilder.ProjectSourceNone
	}
	if projectID := strings.TrimSpace(s.cfg.Project.FromPayload(task)); projectID != "" {
		return projectID, jobbuilder.ProjectSourcePayload
	}
	if head.IssueID == "" {
		return "", jobbuilder.ProjectSourceNone
	}
	now := s.now()
	if projectID, ok := s.project.get(workspaceID, head.IssueID, now); ok {
		if projectID == "" {
			return "", jobbuilder.ProjectSourceNone
		}
		return projectID, jobbuilder.ProjectSourceAPI
	}
	lookupCtx, cancel := context.WithTimeout(ctx, s.projectLookupTimeout())
	defer cancel()
	projectID, err := s.cfg.Project.ByIssue(lookupCtx, workspaceID, head.IssueID)
	if err != nil {
		s.log.WarnContext(ctx, "project.lookup_failed",
			"task_id", head.ID, "workspace_id", workspaceID, "issue_id", head.IssueID, "err", err.Error())
		s.project.put(workspaceID, head.IssueID, "", now)
		return "", jobbuilder.ProjectSourceNone
	}
	projectID = strings.TrimSpace(projectID)
	s.project.put(workspaceID, head.IssueID, projectID, now)
	if projectID == "" {
		return "", jobbuilder.ProjectSourceNone
	}
	return projectID, jobbuilder.ProjectSourceAPI
}

// projectLookupTimeout bounds one reverse lookup so the claim → Job budget
// holds (ADR-048 decision 4): a lookup that outlives it converges on the
// failure face, never on a slow Job create.
func (s *Scheduler) projectLookupTimeout() time.Duration {
	if s.cfg.ProjectLookupTimeout > 0 {
		return s.cfg.ProjectLookupTimeout
	}
	return DefaultProjectLookupTimeout
}
