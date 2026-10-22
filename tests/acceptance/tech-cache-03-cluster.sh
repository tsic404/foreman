#!/bin/sh
# TC-tech-cache-03 — the cache-degradation steps of AC-12 as four ordered,
# independently repeatable actions; the trigger between them stays with the
# operator (each step needs a task triggered under the state the previous
# action installed). The isolated-shape half is tech_cache_03_isolated_test.go.
#
#   ./tech-cache-03-cluster.sh break                  # workspaces unwritable
#   ./tech-cache-03-cluster.sh verify-unwritable TID  # must fail; restores
#   ./tech-cache-03-cluster.sh rebuild-prepare        # delete bare repos
#   ./tech-cache-03-cluster.sh rebuild-verify TID     # must complete, cache back
# Exit: 0 = PASS for the action · 1 = FAIL · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
STATE_ROOT=${FOREMAN_STATE_ROOT:-/var/lib/foreman}
# GNU coreutils inside the node-debug container (stat/find/chmod/rm).
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
NODE=${NODE:-}
ACTION=${1:-}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

if [ -z "$NODE" ]; then
	NODE=$(kubectl -n "$JOB_NS" get pods -l foreman.tsic.top/task-id \
		-o jsonpath='{.items[*].spec.nodeName}' 2>/dev/null | tr ' ' '\n' | sort -u | head -1) ||
		blocked "cannot discover a worker node"
fi
[ -n "$NODE" ] || blocked "no task pod names a worker node — trigger one task first or set NODE"
observe "node=$NODE action=$ACTION"

# node_sh runs a shell on the node with the state root resolved. The debug
# container needs an explicit image and stdin (--attach defaults on with -i);
# without both the call never reaches the node and its stdout stays empty.
node_sh() {
	kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
		[ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
		R='/host$STATE_ROOT'
		[ -d \"\$R\" ] || R='$STATE_ROOT'
		$1"
}

# The original mode is recorded on the node so verify-unwritable can restore what
# break changed; /host is the node's own /tmp, which survives the debug pod.
MODE_MARKER=/host/tmp/.foreman-tc-cache-03-workspaces-mode

task_entry() {
	curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward the foreman Service first)"
}

wait_terminal() {
	body=$(task_entry "$1")
	printf '%s' "$body" | jq -e '.state == "terminal"' >/dev/null ||
		blocked "task $1 is not terminal yet — wait for it, then re-run this action"
}

case $ACTION in
break)
	out=$(node_sh "[ -d \"\$R/workspaces\" ] || { echo 'QA_BLOCKED: no workspaces tree' >&2; exit 2; }
		if [ ! -e '$MODE_MARKER' ]; then stat -c '%a' \"\$R/workspaces\" > '$MODE_MARKER' || exit 2; fi
		original=\$(cat '$MODE_MARKER')
		chmod 0555 \"\$R/workspaces\" && echo \"workspaces=0555 original=\$original\"") ||
		blocked "cannot make the workspaces tree unwritable on $NODE"
	[ -n "$out" ] || blocked "the node-side call returned no output (image $NODE_DEBUG_IMAGE not attached?)"
	observe "break: $out"
	case $out in
	*workspaces=0555*) : ;;
	*) blocked "the workspaces tree was not made unwritable: $out" ;;
	esac
	observe "trigger one task now (issue mention or chat), then run: $0 verify-unwritable <task id>"
	;;
verify-unwritable)
	tid=${2:-}
	# Installed before any call that can exit: break left the tree at 0555, so
	# every early exit (not terminal, endpoint unreachable) must restore it.
	restore() {
		_r=$(node_sh "if [ -e '$MODE_MARKER' ]; then m=\$(cat '$MODE_MARKER')
			chmod \"\$m\" \"\$R/workspaces\" && rm -f '$MODE_MARKER' && echo \"workspaces restored=\$m\"
		else
			chmod 0755 \"\$R/workspaces\" && echo 'workspaces restored=0755 (no recorded mode — break did not run here)'
		fi" 2>&1) || true
		printf '# observation: restore: %s\n' "${_r:-<no output>}" >&2
	}
	trap restore EXIT INT TERM
	[ -n "$tid" ] || blocked "a task id is required"
	wait_terminal "$tid"
	body=$(task_entry "$tid")
	observe "task=$body"
	printf '%s' "$body" | jq -e '.result == "failed"' >/dev/null ||
		fail "task $tid did not fail explicitly with the cache tree unwritable (result=$(printf '%s' "$body" | jq -r .result))"
	reason=$(printf '%s' "$body" | jq -r '.failure_reason // empty')
	observe "failure_reason=$reason (a job_* or checkout failure is expected)"
	restore
	trap - EXIT INT TERM
	echo "PASS: TC-tech-cache-03 step 2 (task $tid failed explicitly; workspaces restored)"
	;;
rebuild-prepare)
	out=$(node_sh "repos=\$(find \"\$R/workspaces/.repos\" -mindepth 2 -maxdepth 2 -name '*.git' -type d 2>/dev/null)
		[ -n \"\$repos\" ] || { echo 'QA_BLOCKED: no bare repo to delete' >&2; exit 2; }
		for repo in \$repos; do rm -rf \"\$repo\" && echo \"deleted=\$repo\"; done") ||
		blocked "cannot delete the bare caches on $NODE"
	[ -n "$out" ] || blocked "the node-side call returned no output (image $NODE_DEBUG_IMAGE not attached?)"
	observe "rebuild-prepare: $out"
	case $out in
	*deleted=*) : ;;
	*) blocked "no bare repo was deleted: $out" ;;
	esac
	observe "trigger one task now; then run: $0 rebuild-verify <task id>"
	;;
rebuild-verify)
	tid=${2:-}
	[ -n "$tid" ] || blocked "a task id is required"
	wait_terminal "$tid"
	body=$(task_entry "$tid")
	observe "task=$body"
	printf '%s' "$body" | jq -e '.result == "completed"' >/dev/null ||
		fail "task $tid failed after the cache deletion (result=$(printf '%s' "$body" | jq -r .result)); losing .repos only degrades"
	# The container-side marker is the only evidence the node was reached: a
	# missing marker is an environment block (image/attach/debug pod failing),
	# while a marker of 0 is the judged failure. rc alone cannot tell them
	# apart — a debug call that never ran also exits non-zero.
	rc=0
	out=$(node_sh "n=\$(find \"\$R/workspaces/.repos\" -mindepth 2 -maxdepth 2 -name '*.git' -type d 2>/dev/null | wc -l)
		echo \"bare_repos=\$n\"") || rc=$?
	observe "rebuild-verify: ${out:-<empty>} (rc=$rc)"
	n=$(printf '%s\n' "$out" | sed -n 's/^bare_repos=\([0-9]*\)$/\1/p' | head -1)
	[ -n "$n" ] ||
		blocked "no bare_repos marker in the node-side output (rc=$rc, image $NODE_DEBUG_IMAGE) — the debug call did not reach the node"
	[ "$n" -gt 0 ] || fail "the bare cache was not rebuilt after the deletion (bare_repos=0)"
	echo "PASS: TC-tech-cache-03 step 3 (task $tid completed and the cache was rebuilt)"
	;;
*)
	blocked "usage: $0 break | verify-unwritable <task> | rebuild-prepare | rebuild-verify <task>"
	;;
esac
