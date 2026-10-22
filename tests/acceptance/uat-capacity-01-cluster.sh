#!/bin/sh
# TC-uat-capacity-01 — G1: cluster elasticity decides concurrency, not the
# machine count. The reference Deployment's cap (100) is far above what the
# test agent may run at once (max_concurrent_tasks = 6), so the carrier first
# lowers the cap to a baseline and then raises it above that baseline. With
# the baseline in force the same window cannot exceed it, so reaching the
# raised cap proves the concurrency followed the cap. The machine count is
# held constant throughout.
#
#   ./uat-capacity-01-cluster.sh lower LIMIT   # record the deployment's value, set the baseline
#   ./uat-capacity-01-cluster.sh raise LIMIT   # raise above the baseline and record the window
#   ./uat-capacity-01-cluster.sh check LIMIT   # trigger ≥LIMIT tasks, then judge
#   ./uat-capacity-01-cluster.sh restore       # put the deployment's original value back
#
# The raise/check LIMIT must exceed the lowered baseline and must not exceed
# the test agent's max_concurrent_tasks (6 in the test environment; override
# with AGENT_MAX) or the server will never dispatch that many tasks.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

STATE=${STATE:-$HOME/.foreman-uat-capacity-01}
WINDOW=${WINDOW:-300}
AGENT_MAX=${AGENT_MAX:-6}
ACTION=${1:-}
LIMIT=${2:-}

need kubectl
need jq

state_get() {
	[ -r "$STATE" ] || blocked "no open window at $STATE — run '$0 lower LIMIT' first"
	sed -n "s/^$1=//p" "$STATE" | head -1
}

# Every blocked exit from lower/raise tells the operator how to put the
# deployment back: the traps below restore it on failure, but a path that
# could not run its trap (or whose restore failed) still has the state file.
blocked_window() { blocked "$* — if the deployment is still at the low cap, run '$0 restore'"; }

# cleanup_lower / cleanup_raise: an aborted window must not leave the
# deployment at the lowered baseline. lower restores only when it touched the
# deployment; raise always aborts the open window. A failed restore keeps the
# state file so '$0 restore' can retry.
cleanup_lower() {
	if [ "$lower_done" -eq 1 ]; then return 0; fi
	lower_done=1
	if [ "$deploy_touched" -eq 1 ]; then
		if kubectl -n "$FOREMAN_NS" set env deploy/foreman "FOREMAN_MAX_INFLIGHT_JOBS=$orig" >/dev/null 2>&1; then
			rm -f "$STATE"
		else
			printf 'warning: could not restore FOREMAN_MAX_INFLIGHT_JOBS=%s; state kept at %s — run %s restore\n' \
				"$orig" "$STATE" "$0" >&2
		fi
	else
		rm -f "$STATE"
	fi
	return 0
}

cleanup_raise() {
	if [ "$raise_done" -eq 1 ]; then return 0; fi
	raise_done=1
	if [ -n "$orig" ] &&
		kubectl -n "$FOREMAN_NS" set env deploy/foreman "FOREMAN_MAX_INFLIGHT_JOBS=$orig" >/dev/null 2>&1; then
		rm -f "$STATE"
	else
		printf 'warning: could not restore FOREMAN_MAX_INFLIGHT_JOBS=%s; state kept at %s — run %s restore\n' \
			"$orig" "$STATE" "$0" >&2
	fi
	return 0
}

case $ACTION in
lower)
	case $LIMIT in '' | *[!0-9]*) blocked_window "usage: $0 lower LIMIT (a whole number)" ;; esac
	[ "$LIMIT" -gt 0 ] || blocked_window "LIMIT must be positive"
	[ ! -r "$STATE" ] ||
		blocked_window "a window is already open at $STATE — run '$0 restore' first"
	orig=$(deploy_env FOREMAN_MAX_INFLIGHT_JOBS)
	[ -n "$orig" ] || blocked_window "the foreman Deployment carries no FOREMAN_MAX_INFLIGHT_JOBS"
	case $orig in '' | *[!0-9]*) blocked_window "FOREMAN_MAX_INFLIGHT_JOBS='$orig' is not a whole number" ;; esac
	[ "$LIMIT" -lt "$orig" ] ||
		blocked_window "LIMIT=$LIMIT is not below the deployment's current cap $orig — the rise would not be observable"
	nodes=$(node_count) || blocked_window "cannot count the cluster's schedulable Linux nodes"
	lower_done=0
	deploy_touched=0
	trap cleanup_lower EXIT
	trap 'cleanup_lower; exit 130' INT
	trap 'cleanup_lower; exit 143' TERM
	{
		printf 'original=%s\n' "$orig"
		printf 'baseline=%s\n' "$LIMIT"
		printf 'nodes=%s\n' "$nodes"
		printf 'lowered_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	} >"$STATE"
	deploy_touched=1
	kubectl -n "$FOREMAN_NS" set env deploy/foreman "FOREMAN_MAX_INFLIGHT_JOBS=$LIMIT" >/dev/null ||
		blocked_window "cannot set FOREMAN_MAX_INFLIGHT_JOBS=$LIMIT on deploy/foreman"
	kubectl -n "$FOREMAN_NS" rollout status deploy/foreman --timeout=180s >/dev/null ||
		blocked_window "the foreman rollout did not complete"
	lower_done=1
	observe "baseline set: deployment_original=$orig baseline=$LIMIT nodes=$nodes"
	echo "run '$0 raise RAISED' next (RAISED > $LIMIT and ≤ $AGENT_MAX), trigger at least RAISED tasks, then '$0 check RAISED'"
	;;

raise)
	orig=$(state_get original)
	baseline=$(state_get baseline)
	nodes=$(state_get nodes)
	raise_done=0
	trap cleanup_raise EXIT
	trap 'cleanup_raise; exit 130' INT
	trap 'cleanup_raise; exit 143' TERM
	case $LIMIT in '' | *[!0-9]*) blocked_window "usage: $0 raise LIMIT (a whole number)" ;; esac
	[ "$LIMIT" -gt 0 ] || blocked_window "LIMIT must be positive"
	case $baseline in '' | *[!0-9]*) blocked_window "no baseline recorded at $STATE — run '$0 lower LIMIT' first" ;; esac
	case $AGENT_MAX in '' | *[!0-9]*) blocked_window "AGENT_MAX='$AGENT_MAX' is not a whole number" ;; esac
	[ "$LIMIT" -gt "$baseline" ] ||
		blocked_window "LIMIT=$LIMIT does not exceed the baseline $baseline — the rise would not be observable"
	[ "$LIMIT" -le "$AGENT_MAX" ] ||
		blocked_window "LIMIT=$LIMIT exceeds the test agent's max_concurrent_tasks $AGENT_MAX — the server will never dispatch that many tasks"
	current=$(deploy_env FOREMAN_MAX_INFLIGHT_JOBS)
	[ "$current" = "$baseline" ] ||
		blocked_window "the deployment cap is $current, not the recorded baseline $baseline — run '$0 restore' and start over"
	{
		printf 'original=%s\n' "$orig"
		printf 'baseline=%s\n' "$baseline"
		printf 'nodes=%s\n' "$nodes"
		printf 'limit=%s\n' "$LIMIT"
		printf 'raised_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	} >"$STATE"
	kubectl -n "$FOREMAN_NS" set env deploy/foreman "FOREMAN_MAX_INFLIGHT_JOBS=$LIMIT" >/dev/null ||
		blocked_window "cannot set FOREMAN_MAX_INFLIGHT_JOBS=$LIMIT on deploy/foreman"
	kubectl -n "$FOREMAN_NS" rollout status deploy/foreman --timeout=180s >/dev/null ||
		blocked_window "the foreman rollout did not complete"
	raise_done=1
	observe "cap raised: original=$orig baseline=$baseline limit=$LIMIT nodes=$nodes"
	echo "trigger at least $LIMIT concurrent tasks now (one agent, up to its max_concurrent_tasks), then run: $0 check $LIMIT"
	;;

check)
	case $LIMIT in '' | *[!0-9]*) blocked "usage: $0 check LIMIT" ;; esac
	orig=$(state_get original)
	baseline=$(state_get baseline)
	recorded_limit=$(state_get limit)
	recorded_nodes=$(state_get nodes)
	[ "$recorded_limit" = "$LIMIT" ] || blocked "the raised window is for LIMIT=$recorded_limit, not $LIMIT"
	current=$(deploy_env FOREMAN_MAX_INFLIGHT_JOBS)
	[ "$current" = "$LIMIT" ] ||
		blocked "the deployment cap is $current, not the recorded limit $LIMIT — run '$0 restore' and start over"
	nodes=$(node_count) || blocked "cannot count the cluster's schedulable Linux nodes"
	[ "$nodes" = "$recorded_nodes" ] ||
		fail "the machine count changed from $recorded_nodes to $nodes — G1 keeps it constant"
	# Put the deployment's original cap back whatever the judgement says, and
	# close the window: a state file left behind makes the next lower refuse
	# with "a window is already open". A failed restore keeps it so
	# '$0 restore' can retry.
	check_done=0
	cleanup_check() {
		if [ "$check_done" -eq 1 ]; then return 0; fi
		check_done=1
		if kubectl -n "$FOREMAN_NS" set env deploy/foreman "FOREMAN_MAX_INFLIGHT_JOBS=$orig" >/dev/null 2>&1; then
			rm -f "$STATE"
		else
			printf 'warning: could not restore FOREMAN_MAX_INFLIGHT_JOBS=%s; state kept at %s — run %s restore\n' \
				"$orig" "$STATE" "$0" >&2
		fi
		return 0
	}
	trap cleanup_check EXIT
	trap 'cleanup_check; exit 130' INT
	trap 'cleanup_check; exit 143' TERM

	max=0
	deadline=$(( $(date +%s) + WINDOW ))
	while [ "$(date +%s)" -lt "$deadline" ]; do
		pods=$(kubectl -n "$JOB_NS" get pods -l foreman.tsic.top/task-id -o json 2>/dev/null || echo '{"items":[]}')
		running=$(printf '%s' "$pods" | jq '[.items[] | select(.status.phase == "Running")] | length')
		jobs=$(kubectl -n "$JOB_NS" get jobs -l foreman.tsic.top/task-id -o json 2>/dev/null || echo '{"items":[]}')
		active=$(printf '%s' "$jobs" | jq '[.items[] | select((.status.active // 0) > 0)] | length')
		n=$running
		[ "$active" -gt "$n" ] && n=$active
		if [ "$n" -gt "$max" ]; then
			max=$n
			observe "concurrency=$max (running_pods=$running active_jobs=$active)"
		fi
		[ "$max" -ge "$LIMIT" ] && break
		sleep 2
	done
	observe "window: limit=$LIMIT baseline=$baseline max_concurrent=$max nodes=$nodes"
	[ "$max" -ge "$LIMIT" ] ||
		fail "the maximum concurrency was $max, below the raised cap $LIMIT (the baseline cap $baseline bounded it before the raise) — concurrency did not follow the cap"
	echo "PASS: TC-uat-capacity-01 (concurrency $max reached the raised cap $LIMIT above the baseline $baseline; machine count $nodes unchanged)"
	;;

restore)
	orig=$(state_get original)
	kubectl -n "$FOREMAN_NS" set env deploy/foreman "FOREMAN_MAX_INFLIGHT_JOBS=$orig" >/dev/null ||
		blocked "cannot restore FOREMAN_MAX_INFLIGHT_JOBS=$orig"
	kubectl -n "$FOREMAN_NS" rollout status deploy/foreman --timeout=180s >/dev/null ||
		blocked "the foreman rollout did not complete"
	rm -f "$STATE"
	echo "restored: FOREMAN_MAX_INFLIGHT_JOBS=$orig"
	;;

*)
	blocked "usage: $0 lower LIMIT | raise LIMIT | check LIMIT | restore"
	;;
esac
