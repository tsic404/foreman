#!/bin/sh
# TC-uat-lifecycle-01 — G3: a task has a start, a process and one of the four
# terminal classes (completed / failed / cancelled / platform timeout). The
# carrier takes one task per class, judges the Foreman projection, the
# server-side run and the post-terminal object cleanup (Job and credential
# Secret gone), which is the lifecycle's product face.
#
#   COMPLETED=<id> FAILED=<id> CANCELLED=<id> TIMEOUT=<id> \
#     ./uat-lifecycle-01-cluster.sh
#
# TIMEOUT is the platform-deadline task (result failed, attribution
# job_deadline_exceeded); the other three are the ordinary classes.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

COMPLETED=${COMPLETED:-}
FAILED=${FAILED:-}
CANCELLED=${CANCELLED:-}
TIMEOUT=${TIMEOUT:-}
CLEANUP_WAIT=${CLEANUP_WAIT:-240}

need kubectl
need curl
need jq

[ -n "$COMPLETED" ] || blocked "COMPLETED is required"
[ -n "$FAILED" ] || blocked "FAILED is required"
[ -n "$CANCELLED" ] || blocked "CANCELLED is required"
[ -n "$TIMEOUT" ] || blocked "TIMEOUT is required"

check_one() { # <task-id> <class> <result> [reason]
	_id=$1
	_class=$2
	_want_result=$3
	_want_reason=${4:-}
	body=$(wait_terminal "$_id" 600) ||
		fail "$_class task $_id did not reach terminal within 600s"
	observe "$_class projection: $body"
	result=$(printf '%s' "$body" | jq -r '.result // empty')
	[ "$result" = "$_want_result" ] ||
		fail "$_class task $_id result=$result, want $_want_result"
	if [ -n "$_want_reason" ]; then
		reason=$(printf '%s' "$body" | jq -r '.failure_reason // empty')
		[ "$reason" = "$_want_reason" ] ||
			fail "$_class task $_id failure_reason=$reason, want $_want_reason"
	fi
	registered=$(printf '%s' "$body" | jq -r '.daemon_registered_at // empty')
	started=$(printf '%s' "$body" | jq -r '.started_at // empty')
	case $_class in
	cancelled) : ;; # a cancel before start is a legal lifecycle branch
	*)
		[ -n "$registered" ] || fail "$_class task $_id has no daemon_registered_at (no start)"
		[ -n "$started" ] || fail "$_class task $_id has no started_at (no process)"
		;;
	esac

	# The lifecycle product: the Job and its credential Secret are gone once
	# the drain window has run. The token outlives the verdict by design, so
	# the wait covers FOREMAN_JOB_DRAIN_TIMEOUT (120s default).
	deadline=$(( $(date +%s) + CLEANUP_WAIT ))
	job=""
	secret=""
	while [ "$(date +%s)" -lt "$deadline" ]; do
		job=$(job_of "$_id")
		secret=$(kubectl -n "$JOB_NS" get secret "$(secret_of "$_id")" --ignore-not-found -o name 2>/dev/null || true)
		[ -z "$job" ] && [ -z "$secret" ] && break
		sleep 2
	done
	[ -z "$job" ] || fail "$_class task $_id still has Job $job after terminal"
	[ -z "$secret" ] || fail "$_class task $_id still has Secret $secret after terminal"
	observe "$_class cleanup: Job and Secret absent"

	run=$(server_run "$_id") ||
		blocked "cannot list the test agent's runs (run ./reset.sh and ./bind-agent.sh first)"
	[ -n "$run" ] || blocked "the test server has no run for task $_id"
	status=$(run_status "$run")
	case $_class in
	completed) case $status in *completed* | *succeeded* | *success*) : ;; *) fail "server run for $_id is '$status', want completed: $run" ;; esac ;;
	failed) case $status in *fail*) : ;; *) fail "server run for $_id is '$status', want failed: $run" ;; esac ;;
	cancelled) case $status in *cancel*) : ;; *) fail "server run for $_id is '$status', want cancelled: $run" ;; esac ;;
	timeout) case $status in *fail*) : ;; *) fail "server run for $_id is '$status', want failed (platform timeout): $run" ;; esac ;;
	esac
	observe "$_class server run: $run"
}

check_one "$COMPLETED" completed completed
check_one "$FAILED" failed failed
check_one "$CANCELLED" cancelled cancelled
check_one "$TIMEOUT" timeout failed job_deadline_exceeded

echo "PASS: TC-uat-lifecycle-01 (completed/failed/cancelled/timeout each reached terminal, server state matched, Job and Secret reclaimed)"
