#!/bin/sh
# TC-uat-recovery-01 — G6: a restart or a fault injection loses no task and
# executes none twice. The carrier takes the tasks that were in flight across
# the fault window, judges that each reached a terminal state exactly once and
# that each task's start fact appears at most once, and checks the server-side
# run is single. The fault itself (rollout restart or Job pod deletion) is the
# operator's step; FAULT names which one was injected so the carrier can
# require its evidence.
#
#   TASKS="id1 id2 ..." [SINCE=6h] [FAULT=restart|job] ./uat-recovery-01-cluster.sh
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

TASKS=${TASKS:-}
SINCE=${SINCE:-6h}
FAULT=${FAULT:-restart}
WAIT=${WAIT:-600}

need kubectl
need curl
need jq

[ -n "$TASKS" ] || blocked "TASKS is required (the task ids that were in flight across the fault window)"
case $FAULT in restart | job) : ;; *) blocked "FAULT=$FAULT is not restart or job" ;; esac

# The current and the previous Foreman container's logs: a task that settled
# before the restart left its terminal line in the previous container.
logs=$(foreman_logs "$SINCE")
previous=$(foreman_previous_logs "$SINCE")
all_logs=$(printf '%s\n%s\n' "$logs" "$previous")
[ -n "$(printf '%s' "$all_logs" | tr -d '[:space:]')" ] ||
	blocked "the Foreman log is empty for the last $SINCE — cannot judge the window"

# The injected fault's evidence.
case $FAULT in
restart)
	restarts=$(kubectl -n "$FOREMAN_NS" get pods -o json |
		jq '[.items[].status.containerStatuses[]?.restartCount] | max // 0') ||
		blocked "cannot read the foreman pods' restart counts"
	case $restarts in '' | *[!0-9]*) blocked "the restart count is not a number: '$restarts'" ;; esac
	started_lines=$(printf '%s\n' "$logs" | grep -c '"msg":"foreman.started"' || true)
	observe "restart evidence: container restarts=$restarts foreman.started lines=$started_lines"
	[ "$restarts" -gt 0 ] || [ "$started_lines" -gt 0 ] ||
		blocked "no restart evidence in the window — restart the Deployment first or set FAULT=job"
	;;
job)
	compensated=$(printf '%s\n' "$all_logs" | grep -cE '"msg":"task.failed_compensated"|"msg":"job.stale_deleted"' || true)
	observe "job fault evidence: failed_compensated/stale_deleted lines=$compensated"
	[ "$compensated" -gt 0 ] ||
		blocked "no Job-failure compensation line in the window — inject the fault first"
	;;
esac

for id in $TASKS; do
	body=$(wait_terminal "$id" "$WAIT") ||
		fail "task $id did not reach terminal within ${WAIT}s (lost across the fault)"
	observe "task $id: $body"
	terminals=$(count_log "$all_logs" task.terminal "$id")
	starts=$(count_log "$all_logs" task.started "$id")
	creates=$(count_log "$all_logs" job.created "$id")
	observe "task $id: job.created=$creates task.started=$starts task.terminal=$terminals (0 = the old pod's log is gone)"
	# A rollout deletes the old pod, so its terminal line can be unavailable;
	# the projection's wait_terminal already proves settlement. The log face
	# only judges duplication.
	[ "$terminals" -le 1 ] ||
		fail "task $id has $terminals terminal lines — a duplicate settlement"
	[ "$starts" -le 1 ] ||
		fail "task $id has $starts start lines — it executed more than once"

	agent=$(agent_id) || blocked "cannot resolve the test agent (run ./reset.sh and ./bind-agent.sh first)"
	[ -n "$agent" ] || blocked "the test agent $TEST_AGENT_NAME does not exist"
	runs=$(mcli agent tasks "$agent" --output json |
		jq --arg t "$id" '[.. | objects | select(((.id? // "") == $t) or ((.task_id? // "") == $t))] | length') ||
		blocked "cannot list the test agent's runs"
	case $runs in '' | *[!0-9]*) blocked "the server run list for $id is not a count: '$runs'" ;; esac
	[ "$runs" -eq 1 ] ||
		fail "task $id has $runs server-side runs, want exactly 1 (lost or duplicated across the fault)"
	observe "task $id: server runs=$runs"
done

echo "PASS: TC-uat-recovery-01 (fault=$FAULT; every task reached terminal, no task executed twice)"
