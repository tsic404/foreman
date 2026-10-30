#!/bin/sh
# The scenario builder behind the two user-perspective terminal-class cases:
# it constructs one task per terminal class so neither case depends on an
# operator improvising a model or a task by hand.
#
#   ./uat-scenarios.sh prepare      # all four classes, then record the window
#   ./uat-scenarios.sh completed|failed|cancelled|timeout
#   ./uat-scenarios.sh show         # the window prepare recorded
#
# `prepare` takes the terminal counters before the first task, builds the four
# tasks, and writes their ids plus those baselines to $SCENARIOS_STATE
# (default ~/.foreman-uat-scenarios). uat-lifecycle-01-cluster.sh and
# uat-observability-01-cluster.sh read that window, so both judge the same
# tasks and the failed one is checked on the server side, in the counters and
# in the log.
#
# Each class is built by the mechanism its class needs: the working agent for
# completed, the test environment's model-less failure agent for failed, a
# server-side cancel of an in-flight run for cancelled, and a compressed Job
# deadline for the platform timeout. Each function states why that
# construction is deterministic.
#
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

ACTION=${1:-}
STATE=${SCENARIOS_STATE}
WAIT=${SCENARIO_WAIT:-600}
START_WAIT=${SCENARIO_START_WAIT:-300}
TIMEOUT_TRIES=${SCENARIO_TIMEOUT_TRIES:-3}
CANCEL_TRIES=${SCENARIO_CANCEL_TRIES:-3}
ABANDON_WAIT=${SCENARIO_ABANDON_WAIT:-180}

need curl
need jq
need kubectl

# The task id the last scenario built. The scenario functions report through
# this variable so their observations stay on stdout without corrupting it.
SCENARIO_TASK=

# new_issue <title> <description> -> the issue id. The issues a round creates
# stay active after it (the fixture completes none of them), so the CLI's
# duplicate guard would reject every round after the first in one workspace.
# The create is deliberate — one issue per class per round — so the guard is
# waived rather than dodged with a per-round title: the flag says what the
# round means, while a nonce would only make each name unique and leave the
# list full of near-identical titles no one can tell apart.
new_issue() {
	mcli issue create --allow-duplicate --title "$1" --description "$2" --output json |
		jq -er '.id'
}

# start_issue <issue-id> <agent-name>: assigning starts the agent run.
start_issue() {
	mcli issue assign "$1" --to "$2" --output json >/dev/null ||
		fail "could not assign issue $1 to agent $2"
}

# issue_runs <issue-id> [active]: the CLI's run list, either scope.
issue_runs() {
	if [ "${2:-}" = active ]; then
		mcli issue runs "$1" --active --output json 2>/dev/null
	else
		mcli issue runs "$1" --output json 2>/dev/null
	fi
}

# first_run_id: the newest run id in the CLI's answer. The active scope empties
# as soon as the run is terminal, so the caller falls back to the full history.
first_run_id() {
	jq -r '(if type == "array" then .
		elif (.runs? | type) == "array" then .runs
		else [.] end)
		| map(select((.id? // "") != ""))
		| .[0].id // empty'
}

# issue_run <issue-id> [timeout-s] -> the run id the server created.
issue_run() {
	_deadline=$(( $(date +%s) + ${2:-120} ))
	while :; do
		for _scope in active all; do
			_id=$(issue_runs "$1" "$_scope" | first_run_id) || _id=
			if [ -n "$_id" ]; then
				printf '%s' "$_id"
				return 0
			fi
		done
		[ "$(date +%s)" -lt "$_deadline" ] || return 1
		sleep 1
	done
}

# wait_started <task-id> [timeout-s]: 0 once the agent has started, 1 when the
# task settled first or the wait expired. The poll is short because the next
# step must land while the agent is still working.
wait_started() {
	_deadline=$(( $(date +%s) + ${2:-300} ))
	while :; do
		_body=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" 2>/dev/null || true)
		if [ -n "$_body" ]; then
			printf '%s' "$_body" | jq -e '.started_at // empty | length > 0' >/dev/null 2>&1 && return 0
			printf '%s' "$_body" | jq -e '.state == "terminal"' >/dev/null 2>&1 && return 1
		fi
		[ "$(date +%s)" -lt "$_deadline" ] || return 1
		sleep 0.5
	done
}

# wait_job <task-id> [timeout-s] -> the live Job name, or empty.
wait_job() {
	_deadline=$(( $(date +%s) + ${2:-60} ))
	while :; do
		_job=$(job_of "$1")
		if [ -n "$_job" ]; then
			printf '%s' "$_job"
			return 0
		fi
		[ "$(date +%s)" -lt "$_deadline" ] || return 1
		sleep 0.5
	done
}

terminal_result() { printf '%s' "$1" | jq -r '.result // empty'; }
terminal_reason() { printf '%s' "$1" | jq -r '.failure_reason // empty'; }

# settle_abandoned <task-id> <issue-id> <label>: close a run this builder gives
# up on, so it cannot complete later as an extra sample or hold an in-flight
# slot when the observability case reads the gauge. A run the projection never
# saw never reached Foreman; the server-side cancel is all there is to do.
settle_abandoned() {
	_body=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" 2>/dev/null || true)
	if [ -z "$_body" ]; then
		mcli issue cancel-task "$1" --issue "$2" --output json >/dev/null 2>&1 ||
			observe "$3: abandoned run $1 is unknown to Foreman and was not cancellable"
		return 0
	fi
	if printf '%s' "$_body" | jq -e '.state == "terminal"' >/dev/null 2>&1; then
		observe "$3: abandoned run $1 already settled ($(terminal_result "$_body"))"
		return 0
	fi
	mcli issue cancel-task "$1" --issue "$2" --output json >/dev/null 2>&1 ||
		observe "$3: could not cancel abandoned run $1"
	wait_terminal "$1" "$ABANDON_WAIT" >/dev/null 2>&1 ||
		observe "$3: abandoned run $1 did not settle within ${ABANDON_WAIT}s"
}

# write_window: replace the window atomically from stdin. The partial
# placeholder prepare writes first is what keeps a failed prepare invisible to
# the carriers: they require STATUS=complete, so a half-built window can never
# answer for this round, and the previous round's file is gone before the first
# task exists.
write_window() {
	_tmp=$(mktemp) || fail "cannot create a temporary file"
	cat >"$_tmp"
	mv "$_tmp" "$STATE"
}

# scenario_completed: the success path. The model source is the round's own, so
# a failure here is a broken environment, not a scenario.
scenario_completed() {
	_issue=$(new_issue "foreman uat: completed task" \
		"Reply with the single word OK. Do not modify anything.") ||
		fail "issue creation failed"
	start_issue "$_issue" "$TEST_AGENT_NAME"
	_id=$(issue_run "$_issue") ||
		fail "the server created no run for issue $_issue"
	observe "completed: issue=$_issue task=$_id"
	_body=$(wait_terminal "$_id" "$WAIT") ||
		fail "the completed task $_id did not reach terminal within ${WAIT}s"
	_result=$(terminal_result "$_body")
	[ "$_result" = completed ] ||
		fail "the completed scenario converged to $_body — the round's model source is not usable"
	observe "completed: $_body"
	SCENARIO_TASK=$_id
}

# scenario_failed: the failure agent cannot run a task at all, so the failure
# is the fixture's guarantee rather than a race with a mutated model.
scenario_failed() {
	[ -n "$(agent_named "$FAULT_AGENT_NAME")" ] ||
		blocked "agent $FAULT_AGENT_NAME does not exist — run testinfra/test-multica/reset.sh and bind-agent.sh first"
	_issue=$(new_issue "foreman uat: failed task" \
		"Reply with the single word OK. Do not modify anything.") ||
		fail "issue creation failed"
	start_issue "$_issue" "$FAULT_AGENT_NAME"
	_id=$(issue_run "$_issue") ||
		fail "the server created no run for issue $_issue"
	observe "failed: issue=$_issue task=$_id (agent $FAULT_AGENT_NAME)"
	_body=$(wait_terminal "$_id" "$WAIT") ||
		fail "the failed task $_id did not reach terminal within ${WAIT}s"
	_result=$(terminal_result "$_body")
	_reason=$(terminal_reason "$_body")
	[ "$_result" = failed ] ||
		fail "the failure agent's task converged to $_body, want result=failed"
	# §4.1: a daemon self-reported failure carries no attribution; only the
	# failures Foreman decides for the daemon (boot timeout, Job failure, …)
	# do. Those would be the fixture passing off an environment fault as the
	# failure agent's own failure, so the sample must be the empty form.
	[ -z "$_reason" ] ||
		fail "the failure sample $_id carries failure_reason='$_reason' — a Foreman-decided failure is not the failure agent's own report"
	observe "failed: $_body"
	SCENARIO_TASK=$_id
}

# scenario_cancelled: cancel a run the daemon is polling (§4.6). The class is
# the daemon-confirmed path, so the run must have started before the cancel:
# a pre-start cancel never travels through the status poll and would let the
# class pass without that confirmation. A run that ends on its own before the
# cancellation takes effect never met it either — that is a lost race, so the
# sample is discarded and a fresh one built, exactly like the timeout class.
scenario_cancelled() {
	_attempt=0
	while [ "$_attempt" -lt "$CANCEL_TRIES" ]; do
		_attempt=$((_attempt + 1))
		_issue=$(new_issue "foreman uat: cancelled task" \
			"Read README.md and reply with a five-sentence summary. Do not modify anything.") ||
			fail "issue creation failed"
		start_issue "$_issue" "$TEST_AGENT_NAME"
		_id=$(issue_run "$_issue") ||
			fail "the server created no run for issue $_issue"
		if ! wait_started "$_id" "$START_WAIT"; then
			settle_abandoned "$_id" "$_issue" "cancelled attempt $_attempt"
			observe "cancelled attempt $_attempt: task $_id never started — rebuilding"
			continue
		fi
		observe "cancelled attempt $_attempt: task $_id started; cancelling the in-flight run"
		if ! mcli issue cancel-task "$_id" --issue "$_issue" --output json >/dev/null; then
			_body=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$_id" 2>/dev/null || true)
			if printf '%s' "$_body" | jq -e '.state == "terminal"' >/dev/null 2>&1; then
				observe "cancelled attempt $_attempt: task $_id settled before the cancel landed — rebuilding"
				continue
			fi
			fail "cancelling run $_id failed"
		fi
		_body=$(wait_terminal "$_id" "$WAIT") ||
			fail "the cancelled task $_id did not reach terminal within ${WAIT}s"
		_result=$(terminal_result "$_body")
		_reason=$(terminal_reason "$_body")
		case $_result in
		cancelled)
			observe "cancelled: $_body"
			SCENARIO_TASK=$_id
			return 0
			;;
		completed)
			# The run finished before the cancellation took effect: a legal
			# race, not a cancelled sample. It is terminal, so nothing is left
			# in flight.
			observe "cancelled attempt $_attempt: task $_id completed before the cancel took effect — rebuilding"
			continue
			;;
		failed)
			# An empty attribution is the daemon's own failure report — the run
			# died on its own before the cancel landed, another lost race. A
			# non-empty one is Foreman deciding a cancelled run failed, which is
			# a product defect, never a race to retry away.
			[ -z "$_reason" ] ||
				fail "the cancelled task $_id is attributed '$_reason' — Foreman decided a cancelled run failed"
			observe "cancelled attempt $_attempt: task $_id failed on its own before the cancel took effect — rebuilding"
			continue
			;;
		*)
			fail "the cancelled task $_id converged to $_body, want result=cancelled"
			;;
		esac
	done
	fail "could not construct a cancelled terminal in $CANCEL_TRIES attempts"
}

# scenario_timeout: compress the running Job's deadline. The platform fact is
# the Job controller's Failed/DeadlineExceeded condition, which is exactly what
# the attribution reads; a task that completes before the patch lands never saw
# the deadline, so it is discarded and a fresh one built.
scenario_timeout() {
	_attempt=0
	while [ "$_attempt" -lt "$TIMEOUT_TRIES" ]; do
		_attempt=$((_attempt + 1))
		_issue=$(new_issue "foreman uat: platform timeout task" \
			"Read README.md and reply with a five-sentence summary. Do not modify anything.") ||
			fail "issue creation failed"
		start_issue "$_issue" "$TEST_AGENT_NAME"
		_id=$(issue_run "$_issue") ||
			fail "the server created no run for issue $_issue"
		if ! wait_started "$_id" "$START_WAIT"; then
			settle_abandoned "$_id" "$_issue" "timeout attempt $_attempt"
			observe "timeout attempt $_attempt: task $_id never started — rebuilding"
			continue
		fi
		if ! _job=$(wait_job "$_id"); then
			settle_abandoned "$_id" "$_issue" "timeout attempt $_attempt"
			observe "timeout attempt $_attempt: no Job visible for task $_id — rebuilding"
			continue
		fi
		kubectl -n "$JOB_NS" patch job "$_job" --type=merge \
			-p '{"spec":{"activeDeadlineSeconds":1}}' >/dev/null ||
			fail "could not compress the deadline of Job $_job"
		observe "timeout attempt $_attempt: Job $_job deadline compressed while task $_id ran"
		_body=$(wait_terminal "$_id" "$WAIT") ||
			fail "the timeout task $_id did not reach terminal within ${WAIT}s"
		_result=$(terminal_result "$_body")
		_reason=$(terminal_reason "$_body")
		case $_result in
		completed)
			# Lost the race: the agent finished before the deadline took
			# effect, so the task never met the platform's termination. Not a
			# verdict either way.
			observe "timeout attempt $_attempt: task $_id completed before the deadline acted — rebuilding"
			continue
			;;
		failed)
			[ "$_reason" = job_deadline_exceeded ] ||
				fail "the deadline-killed task $_id is attributed '$_reason', want job_deadline_exceeded"
			observe "timeout: $_body"
			SCENARIO_TASK=$_id
			return 0
			;;
		*)
			fail "the timeout task $_id converged to $_body, want result=failed"
			;;
		esac
	done
	fail "could not construct a platform-timeout terminal in $TIMEOUT_TRIES attempts"
}

# prepare: one window for both terminal-class carriers. The counters are read
# before the first task so the observability case can judge the delta instead
# of an absolute value an earlier round could have supplied.
prepare() {
	# The placeholder comes first: a prepare that dies part-way leaves a window
	# the carriers refuse (they require STATUS=complete), never the previous
	# round's ids and baselines.
	write_window <<EOF
STATUS=partial
PREPARED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF
	_completed_before=$(metric_value foreman_tasks_total 'result="completed"')
	_failed_before=$(metric_value foreman_tasks_total 'result="failed"')
	_cancelled_before=$(metric_value foreman_tasks_total 'result="cancelled"')
	observe "baseline: completed=$_completed_before failed=$_failed_before cancelled=$_cancelled_before"

	scenario_completed
	_completed=$SCENARIO_TASK
	scenario_failed
	_failed=$SCENARIO_TASK
	scenario_cancelled
	_cancelled=$SCENARIO_TASK
	scenario_timeout
	_timeout=$SCENARIO_TASK

	write_window <<EOF
STATUS=complete
COMPLETED=$_completed
FAILED=$_failed
CANCELLED=$_cancelled
TIMEOUT=$_timeout
COMPLETED_BEFORE=$_completed_before
FAILED_BEFORE=$_failed_before
CANCELLED_BEFORE=$_cancelled_before
PREPARED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

	echo "PREPARE: window written to $STATE"
	echo "         completed=$_completed failed=$_failed cancelled=$_cancelled timeout=$_timeout"
	echo "next: ./uat-lifecycle-01-cluster.sh && ./uat-observability-01-cluster.sh"
}

case $ACTION in
completed)
	scenario_completed
	printf '%s\n' "$SCENARIO_TASK"
	;;
failed)
	scenario_failed
	printf '%s\n' "$SCENARIO_TASK"
	;;
cancelled)
	scenario_cancelled
	printf '%s\n' "$SCENARIO_TASK"
	;;
timeout)
	scenario_timeout
	printf '%s\n' "$SCENARIO_TASK"
	;;
prepare)
	prepare
	;;
show)
	[ -r "$STATE" ] || blocked "no window at $STATE — run '$0 prepare' first"
	cat "$STATE"
	;;
*)
	blocked "usage: $0 prepare | completed | failed | cancelled | timeout | show"
	;;
esac
