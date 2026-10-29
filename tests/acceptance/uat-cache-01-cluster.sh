#!/bin/sh
# TC-uat-cache-01 — G5: a repeated task on the same node does not re-clone the
# repository. The design's authoritative face for "reuse, not a full clone" is
# the node cache object's creation instant versus the second pod's start
# instant (AC-12); the technical carrier owns that judgement and this case adds
# the user-visible half — the first task completed and the repeated task ran on
# the same node without a fresh clone.
#
#   TASK1=<id> TASK2=<id> ./uat-cache-01-cluster.sh
#
# No Job is read: TASK1 has already settled and its Job is gone with the
# terminal cleanup, so only its projection is judged here. TASK2's pod must
# still be present — run this while the repeated task is still running. The
# projections are judged after the technical carrier.
#
# Trigger TASK2 once TASK1's Job is gone (`job_of <task1>` empty): reuse is a
# property of the previous node's cache, and it survives that wait. Two Jobs of
# the same agent overlapping on one node is a different window — it is where a
# continuation task has been seen to die before its daemon registers — and a
# task with no registration kills the evidence this case is made of.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

TASK1=${TASK1:-}
TASK2=${TASK2:-}

need kubectl
need curl
need jq

[ -n "$TASK1" ] || blocked "TASK1 is required (the first task of the same-agent pair)"
[ -n "$TASK2" ] || blocked "TASK2 is required (the continuation task)"

one=$(entry "$TASK1")
two=$(entry "$TASK2")
observe "task1=$one"
observe "task2=$two"
node1=$(printf '%s' "$one" | jq -r '.node_name // empty')
node2=$(printf '%s' "$two" | jq -r '.node_name // empty')
[ -n "$node1" ] && [ "$node1" = "$node2" ] ||
	blocked "the two tasks did not share a node ($node1 vs $node2) — the reuse precondition does not hold"
observe "shared node=$node1"

# capture_job_end <task-id>: the Job-end evidence for a task that died before
# its daemon registered. The projection keeps only `job_failed`; the Job goes
# with the drain and the pod with it, so the reason survives only in the
# objects (while they last) and in the event stream (which outlives them).
# The discriminator is the container state, not the event text: with
# backoffLimit 0 an init container's exit and the daemon's own exit leave the
# same `BackoffLimitExceeded`, and only the terminated container says which.
# A failure to read is reported as a read failure, never as "no event".
capture_job_end() { # <task-id>
	_tid=$1
	_job=$(entry "$_tid" | jq -r '.job_name // empty') || _job=""
	if [ -z "$_job" ]; then
		observe "job-end evidence for $_tid: the projection names no Job"
		return 0
	fi
	observe "job-end evidence for $_tid (job $_job)"
	if _raw=$(kubectl -n "$JOB_NS" get job "$_job" -o json 2>&1); then
		printf '%s' "$_raw" |
			jq -r '.status.conditions[]? | "job condition: \(.type)=\(.status) reason=\(.reason // "") message=\(.message // "")"' |
			while IFS= read -r _line; do observe "$_line"; done
	else
		observe "job read failed: $(printf '%s' "$_raw" | tr '\n' ' ')"
	fi
	if _raw=$(kubectl -n "$JOB_NS" get pods -l "foreman.tsic.top/task-id=$_tid" -o json 2>&1); then
		printf '%s' "$_raw" |
			jq -r '.items[] | .metadata.name as $p
				| ([.status.initContainerStatuses[]? | {kind: "init", cs: .}]
				   + [.status.containerStatuses[]? | {kind: "main", cs: .}])[]
				| select(.cs.state.terminated != null)
				| "pod \($p) \(.kind) container \(.cs.name) terminated reason=\(.cs.state.terminated.reason // "") exit=\(.cs.state.terminated.exitCode)"' |
			while IFS= read -r _line; do observe "$_line"; done
	else
		observe "pod read failed: $(printf '%s' "$_raw" | tr '\n' ' ')"
	fi
	if _raw=$(kubectl -n "$JOB_NS" get events --sort-by=.lastTimestamp -o json 2>&1); then
		printf '%s' "$_raw" |
			jq -r --arg job "$_job" '.items[]
				| select(.involvedObject.name == $job or (.involvedObject.name | startswith($job + "-")))
				| "event \(.lastTimestamp) \(.involvedObject.kind)/\(.involvedObject.name) \(.reason): \(.message)"' |
			tail -20 |
			while IFS= read -r _line; do observe "$_line"; done
	else
		observe "event read failed: $(printf '%s' "$_raw" | tr '\n' ' ')"
	fi
}

# The technical carrier exits before the user half when TASK2 has no pod with a
# start time — the shape a pre-registration death leaves, and the same death
# this capture exists for. Take the evidence first, while the projection still
# names the Job, and again if the carrier returns non-zero.
two=$(entry "$TASK2")
if ! printf '%s' "$two" | jq -e '(.state == "terminal" and .result == "completed") or .state == "running"' >/dev/null; then
	capture_job_end "$TASK2"
fi

# The authoritative reuse face first: the cache object existed before the
# second pod started. Its blocked/failed verdict is this case's verdict too.
rc=0
out=$(TASK1="$TASK1" TASK2="$TASK2" NODE="$node1" FOREMAN_URL="$FOREMAN_URL" \
	FOREMAN_JOB_NAMESPACE="$JOB_NS" FOREMAN_STATE_ROOT="${FOREMAN_STATE_ROOT:-/var/lib/foreman}" \
	"$UAT_DIR/tech-cache-01-cluster.sh") || rc=$?
printf '%s\n' "$out"
[ "$rc" -eq 0 ] || {
	capture_job_end "$TASK2"
	exit "$rc"
}

# The user-facing half, read after the technical carrier: the first task
# completed, and the repeated task is not a failure (it may still be running).
one=$(entry "$TASK1")
two=$(entry "$TASK2")
printf '%s' "$one" | jq -e '.state == "terminal" and .result == "completed"' >/dev/null ||
	fail "TASK1 did not complete: $one"
printf '%s' "$two" | jq -e '(.state == "terminal" and .result == "completed") or .state == "running"' >/dev/null || {
	capture_job_end "$TASK2"
	fail "TASK2 is neither running nor completed: $two"
}
observe "task1 result=$(printf '%s' "$one" | jq -r .result) task2 state=$(printf '%s' "$two" | jq -r .state)"

echo "PASS: TC-uat-cache-01 (repeated task on $node1 reused the cache; TASK1 completed, TASK2 did not fail, no full clone)"
