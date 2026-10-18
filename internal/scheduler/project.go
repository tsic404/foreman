package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/tsic404/foreman/internal/jobbuilder"
)

// ProjectIdentity is the task's project identity input (contract §3.1
// 项目身份, ADR-035 decision 3). Its two sources are both platform-side
// capabilities the design leaves to the platform — the claim payload's
// structured project key and the read-only reverse lookup by issue id — so a
// nil seam means the platform offers neither: the task reads
// project_source=none and the policy table's project tiers degrade to
// no_identity, while the task runs on.
type ProjectIdentity interface {
	// FromPayload reports the project name the claim payload's structured
	// project key carries; "" means the payload has none, which is legal and
	// never a payload error. It must not read the project_* prompt-context
	// fields — those are not the identity key.
	FromPayload(task json.RawMessage) string
	// ByIssue performs the read-only identity query for one issue id. One
	// attempt, no retry: an error (unreachable, timeout, no such query) is the
	// caller's signal to record project.lookup_failed and continue with no
	// identity.
	ByIssue(ctx context.Context, issueID string) (string, error)
}

// projectCacheTTL is the short TTL of the issue-id lookup cache (ADR-035
// decision 3: 按 issue_id 做短 TTL 缓存; the cache is not a contract face).
const projectCacheTTL = 60 * time.Second

// projectCache memoizes the reverse lookup per issue id, negative answers
// included, so a burst of tasks from one issue does not query the platform
// once per task.
type projectCache struct {
	mu      sync.Mutex
	entries map[string]projectCacheEntry
}

type projectCacheEntry struct {
	name     string
	cachedAt time.Time
}

// get returns the cached name and whether the entry is still fresh. A cached
// empty name is a fresh negative answer, not a miss.
func (c *projectCache) get(issueID string, now time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[issueID]
	if !ok || now.Sub(entry.cachedAt) > projectCacheTTL {
		return "", false
	}
	return entry.name, true
}

func (c *projectCache) put(issueID, name string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]projectCacheEntry{}
	}
	c.entries[issueID] = projectCacheEntry{name: name, cachedAt: now}
}

// projectIdentity resolves the project identity of one claim: the payload's
// structured key first, then — only when the issue id exists — the read-only
// lookup, and none when neither yields a name. Every failure is a warn; the
// task continues with the project tiers degrades (ADR-035 decision 3, AC-54).
func (s *Scheduler) projectIdentity(ctx context.Context, task json.RawMessage, head claimHead) (string, string) {
	if s.cfg.Project == nil {
		return "", jobbuilder.ProjectSourceNone
	}
	if name := strings.TrimSpace(s.cfg.Project.FromPayload(task)); name != "" {
		return name, jobbuilder.ProjectSourcePayload
	}
	if head.IssueID == "" {
		return "", jobbuilder.ProjectSourceNone
	}
	now := s.now()
	if name, ok := s.project.get(head.IssueID, now); ok {
		if name == "" {
			return "", jobbuilder.ProjectSourceNone
		}
		return name, jobbuilder.ProjectSourceAPI
	}
	lookupCtx, cancel := context.WithTimeout(ctx, s.projectLookupTimeout())
	defer cancel()
	name, err := s.cfg.Project.ByIssue(lookupCtx, head.IssueID)
	if err != nil {
		s.log.WarnContext(ctx, "project.lookup_failed",
			"task_id", head.ID, "issue_id", head.IssueID, "err", err.Error())
		s.project.put(head.IssueID, "", now)
		return "", jobbuilder.ProjectSourceNone
	}
	name = strings.TrimSpace(name)
	s.project.put(head.IssueID, name, now)
	if name == "" {
		return "", jobbuilder.ProjectSourceNone
	}
	return name, jobbuilder.ProjectSourceAPI
}

// projectLookupTimeout bounds one reverse lookup so the claim → Job budget
// holds (ADR-035 decision 3): a lookup that outlives it converges on the
// failure face, never on a slow Job create.
func (s *Scheduler) projectLookupTimeout() time.Duration {
	if s.cfg.ProjectLookupTimeout > 0 {
		return s.cfg.ProjectLookupTimeout
	}
	return DefaultProjectLookupTimeout
}
