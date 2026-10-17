package registry

import "strings"

// The task Job name (contract §3.1 「任务 Job 名」): "fm-" + issue segment +
// "-" + agent segment + "-" + task key. The readable segments answer the
// operator's question in `kubectl get jobs` (which issue, which agent); the
// key keeps the name one-to-one with the task. Bounded so the whole name is a
// legal DNS-1123 label under the 63-character cap the pod's job-name label
// imposes.
const (
	jobNamePrefix = "fm-"
	// Per-segment budgets. The key is never truncated, so the longest name is
	// 3 + 20 + 1 + 24 + 1 + 12 = 61 ≤ 63.
	issueSegmentMax = 20
	agentSegmentMax = 24
	taskKeyLen      = 12
	// Fallbacks for a missing — or wholly unnormalizable — segment. Neither is
	// an error: refusing would invent a new failure surface.
	issueFallback = "task"
	agentFallback = "agent"
	// reservedIssueSegment is the catalog probe's name domain (fm-catalog-,
	// contract §1.3): a task Job name must never start with it, so an issue
	// identifier normalizing to exactly this literal falls back.
	reservedIssueSegment = "catalog"
	credSuffix           = "-cred"
)

// JobName derives the task Job name from the claim payload — one-way
// derivation, no lookup, no allocator, no "extend the number on collision":
// the same task always yields the same name, which is what the pre-start
// redispatch path relies on ("same name = same task", task-mapping invariant
// 2). An empty answer means the task id yields no key at all; the scheduler
// settles that task as invalid_task_id (failure-handling 场景 #0).
func JobName(taskID, issueIdentifier, agentName string) string {
	key := taskKey(taskID)
	if key == "" {
		return ""
	}
	issue := segment(issueIdentifier, issueSegmentMax)
	if issue == "" || issue == reservedIssueSegment {
		issue = issueFallback
	}
	agent := segment(agentName, agentSegmentMax)
	if agent == "" {
		agent = agentFallback
	}
	return jobNamePrefix + issue + "-" + agent + "-" + key
}

// CredName is a task Job's credential Secret (contract §1.3): the Job name
// plus the -cred suffix. The probe face uses the same rule.
func CredName(jobName string) string { return jobName + credSuffix }

// taskKey is the task id's tail: lowercase, dashes dropped, last 12
// characters — the front of a UUID is its timestamp and only the tail is
// random. A character outside [a-z0-9] makes the key underivable (""): the
// name must be a DNS-1123 label, and silently rewriting an id the contract
// says is a UUID would hide a payload defect.
func taskKey(taskID string) string {
	var b strings.Builder
	b.Grow(len(taskID))
	for _, r := range strings.ToLower(taskID) {
		if r == '-' {
			continue
		}
		if !isLowerAlnum(r) {
			return ""
		}
		b.WriteRune(r)
	}
	key := b.String()
	if len(key) > taskKeyLen {
		key = key[len(key)-taskKeyLen:]
	}
	return key
}

// segment normalizes one readable name segment: lowercase, every run of
// characters outside [a-z0-9] collapses to a single "-", leading and trailing
// dashes dropped, truncated to max with trailing dashes dropped again. "" is
// the caller's cue to take the fallback literal; an over-long segment is
// truncated, never rejected.
func segment(value string, max int) string {
	var b strings.Builder
	b.Grow(len(value))
	dashed := false
	for _, r := range strings.ToLower(value) {
		if isLowerAlnum(r) {
			b.WriteRune(r)
			dashed = false
			continue
		}
		if b.Len() > 0 && !dashed {
			b.WriteByte('-')
			dashed = true
		}
	}
	normalized := strings.Trim(b.String(), "-")
	if len(normalized) > max {
		normalized = strings.TrimRight(normalized[:max], "-")
	}
	return normalized
}

// isLowerAlnum reports whether r is one of the characters a DNS-1123 label
// segment may carry.
func isLowerAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}
