#!/bin/sh
# Shared helpers for the user-perspective TC-uat cluster carriers. Sourced by
# the uat-*-cluster.sh scripts after they set `set -eu`; it is not executable
# on its own.
#
# The environment gate is the same as the technical carriers: missing kubectl,
# an unreachable Foreman ops endpoint or a missing test-multica profile is
# `QA_BLOCKED` (exit 2), never a pass.

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_NS=${FOREMAN_NAMESPACE:-foreman}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
TEST_MULTICA_NS=${TEST_MULTICA_NS:-multica-test}
TEST_MULTICA_SERVER_URL=${TEST_MULTICA_SERVER_URL:-http://127.0.0.1:30080}
TEST_MULTICA_VERSION_ANCHOR=${TEST_MULTICA_VERSION_ANCHOR:-v0.6.1}
TEST_AGENT_NAME=${TEST_AGENT_NAME:-foreman-test-agent}
# The failure agent the test environment creates (testinfra/test-multica
# README §The failure agent): same runtime, no model source, so a task
# dispatched to it always ends failed.
FAULT_AGENT_NAME=${TEST_MULTICA_FAULT_AGENT:-foreman-test-agent-failing}
# The window uat-scenarios.sh builds and the two terminal-class carriers read:
# four task ids and the terminal counters taken before the first of them.
SCENARIOS_STATE=${SCENARIOS_STATE:-$HOME/.foreman-uat-scenarios}
UAT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TEST_MULTICA_DIR=${TEST_MULTICA_DIR:-$(CDPATH= cd -- "$UAT_DIR/../../testinfra/test-multica" && pwd)}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

entry() {
	curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward the foreman Service first)"
}

metrics() {
	curl -fsS -m 30 "$FOREMAN_URL/metrics" ||
		blocked "cannot read $FOREMAN_URL/metrics (port-forward the foreman Service first)"
}

deploy_env() {
	kubectl -n "$FOREMAN_NS" get deploy foreman -o json 2>/dev/null |
		jq -r --arg k "$1" '[.spec.template.spec.containers[]?.env[]? | select(.name == $k) | .value][0] // empty'
}

# The scheduler pods, by the labels deploy/30-foreman.yaml gives them; the
# shared-state StatefulSet carries name=foreman too, so both keys are needed.
FOREMAN_POD_SELECTOR='app.kubernetes.io/name=foreman,app.kubernetes.io/component=scheduler'

# Naming the pods keeps the log assertions on the scheduler even while the
# Deployment object is being recreated, and concatenating every replica's lines
# covers a task handled by any of them.
foreman_pods() {
	kubectl -n "$FOREMAN_NS" get pods -l "$FOREMAN_POD_SELECTOR" -o name 2>/dev/null || true
}

foreman_logs() { # [since]
	for _pod in $(foreman_pods); do
		kubectl -n "$FOREMAN_NS" logs "$_pod" --since="${1:-6h}" 2>/dev/null || true
	done
}

foreman_previous_logs() { # [since]
	for _pod in $(foreman_pods); do
		kubectl -n "$FOREMAN_NS" logs "$_pod" --previous --since="${1:-6h}" 2>/dev/null || true
	done
}

# foreman_restarts: the highest container restart count across the scheduler
# pods. A namespace-wide read would count a shared-state pod's restart as the
# scheduler's fault evidence.
foreman_restarts() {
	kubectl -n "$FOREMAN_NS" get pods -l "$FOREMAN_POD_SELECTOR" -o json 2>/dev/null |
		jq '[.items[].status.containerStatuses[]?.restartCount] | max // 0'
}

wait_terminal() { # <task-id> [timeout-s] -> prints the terminal projection
	_deadline=$(( $(date +%s) + ${2:-300} ))
	while :; do
		_body=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" 2>/dev/null || true)
		if [ -n "$_body" ] && printf '%s' "$_body" | jq -e '.state == "terminal"' >/dev/null 2>&1; then
			printf '%s' "$_body"
			return 0
		fi
		[ "$(date +%s)" -lt "$_deadline" ] || return 1
		sleep 2
	done
}

epoch() {
	printf '%s' "$1" | jq -Rr '
		capture("^(?<b>[^.]+)(?<f>\\.[0-9]+)?(Z|\\+00:00)$")
		| ((.b + "Z") | fromdateiso8601)
		  + ((.f // "") | if . == "" then 0 else (("0" + .) | tonumber) end)'
}

mcli() {
	[ -x "$TEST_MULTICA_DIR/multica-test" ] ||
		blocked "no test-multica CLI at $TEST_MULTICA_DIR/multica-test — run ./reset.sh and ./bind-agent.sh first"
	"$TEST_MULTICA_DIR/multica-test" "$@"
}

agent_id() {
	mcli agent list --output json |
		jq -r --arg n "$TEST_AGENT_NAME" '[.[] | select(.name == $n and (.archived_at == null))][0].id // empty'
}

# agent_named <name>: the live agent row's id, or empty.
agent_named() {
	mcli agent list --output json |
		jq -r --arg n "$1" '[.[] | select(.name == $n and (.archived_at == null))][0].id // empty'
}

# scenario_state <KEY>: one value of the window uat-scenarios.sh recorded, or
# empty when no window was built (an operator-triggered run says so itself).
scenario_state() {
	[ -r "$SCENARIOS_STATE" ] || return 1
	sed -n "s/^$1=//p" "$SCENARIOS_STATE" | head -1
}

# scenario_window_ready: 0 only for a window a successful prepare wrote. A
# missing file and the partial placeholder a failed prepare leaves behind both
# read as "no window": the previous round's ids and baselines must never answer
# for this one, so a carrier that would fall back to them blocks instead.
scenario_window_ready() {
	[ -r "$SCENARIOS_STATE" ] || return 1
	[ "$(scenario_state STATUS)" = complete ] || return 1
	[ -n "$(scenario_state PREPARED_AT)" ] || return 1
	return 0
}

# scenario_value <explicit-value> <KEY>: the caller's value wins over the
# recorded window, so an operator can still name the tasks by hand.
scenario_value() {
	if [ -n "$1" ]; then
		printf '%s' "$1"
		return 0
	fi
	scenario_state "$2" || true
}

# server_run <task-id>: the server-side run object for the task, or empty. A
# task can belong to either test agent (the failure class runs on the failure
# agent), so both are searched; a read failure of one agent's list is a
# blocked face, never "no run".
server_run() {
	for _name in "$TEST_AGENT_NAME" "$FAULT_AGENT_NAME"; do
		_agent=$(agent_named "$_name") || return 1
		[ -n "$_agent" ] || continue
		_run=$(mcli agent tasks "$_agent" --output json 2>/dev/null |
			jq -c --arg t "$1" \
				'[.. | objects | select(((.id? // "") == $t) or ((.task_id? // "") == $t))][0] // empty') || return 1
		if [ -n "$_run" ]; then
			printf '%s' "$_run"
			return 0
		fi
	done
	return 0
}

# run_status <run-json>: the server-side terminal class, lower-cased. The class
# is a status field (`status`, else `state`); `result` is the run's output and
# is read only when it is a bare string, never as object text — a run whose
# output merely says "completed" must not pass for a completed one.
run_status() {
	printf '%s' "$1" | jq -r '
		[.status, .state, .result]
		| map(select(type == "string") | select(length > 0))
		| .[0] // ""' | tr '[:upper:]' '[:lower:]'
}

# metric_value <name> [label-substring]: the sum of one metric's samples whose
# exposition line contains the label substring.
metric_value() {
	_body=$(metrics)
	printf '%s\n' "$_body" | awk -v name="$1" -v sel="${2:-}" '
		$0 ~ "^" name "(\\{| )" {
			if (sel != "" && index($0, sel) == 0) next
			v = $NF
			if (v ~ /^[0-9.eE+-]+$/) sum += v
		}
		END { printf "%.0f\n", sum + 0 }'
}

# metric_family_present <name>
metric_family_present() {
	_body=$(metrics)
	printf '%s\n' "$_body" | grep -qE "^$1([{ ]|$)" ||
		fail "metric family $1 is absent from $FOREMAN_URL/metrics"
}

# job_of <task-id>: the live Job name, or empty.
job_of() {
	kubectl -n "$JOB_NS" get jobs -l "foreman.tsic.top/task-id=$1" \
		-o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

# secret_of <task-id>: the per-task credential Secret name (identity-derived).
secret_of() {
	printf 'fm-%s-cred' "$1"
}

# count_log <logs> <msg> <task-id>: occurrences of one event for one task.
count_log() {
	printf '%s\n' "$1" | jq -Rr --arg m "$2" --arg t "$3" \
		'fromjson? | select(.msg == $m and .task_id == $t) | 1' | wc -l | tr -d ' '
}

# node_count: the cluster's schedulable Linux nodes (the "machine count" G1
# and G6 keep constant).
node_count() {
	kubectl get nodes -l kubernetes.io/os=linux -o json |
		jq '[.items[] | select(.spec.unschedulable != true)] | length'
}
