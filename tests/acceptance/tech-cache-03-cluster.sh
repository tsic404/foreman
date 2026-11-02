#!/bin/sh
# TC-tech-cache-03 — the cache-degradation steps of AC-12 as five ordered,
# independently repeatable actions; the trigger between them stays with the
# operator (each step needs a task triggered under the state the previous
# action installed). The isolated-shape half is tech_cache_03_isolated_test.go.
#
#   ./tech-cache-03-cluster.sh break [TASK]           # workspaces unwritable
#   ./tech-cache-03-cluster.sh verify-unwritable TID  # must fail; restores
#   ./tech-cache-03-cluster.sh rebuild-prepare [TASK] # delete bare repos
#   ./tech-cache-03-cluster.sh rebuild-verify TID     # must complete, cache back
#   ./tech-cache-03-cluster.sh restore NODE           # undo a break by name
#
# Every action works on the node its task ran on (the projection's node_name,
# which survives settlement), never on a node discovered by chance. The
# same-agent continuation is only soft-affine there (a preferred node affinity
# of weight 100), so the landing is checked, never assumed: a judged task on
# another node blocks and names the node a break left unwritable, which
# `restore NODE` cleans. verify-unwritable also requires the break instant to
# precede the task's started_at — a tree broken later may never have been met.
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

# epoch <RFC3339>: the projection's started_at as seconds; non-zero when the
# value is not one (an unparsable instant is incomparable, never a verdict).
epoch() {
	printf '%s' "$1" | jq -Rr 'sub("\\.[0-9]+"; "") | sub("\\+00:00$"; "Z") | fromdateiso8601'
}

record_broken() {
	grep -Fxq "$1" "$BROKEN_LOG" 2>/dev/null || printf '%s\n' "$1" >>"$BROKEN_LOG"
}

forget_broken() {
	[ -f "$BROKEN_LOG" ] || return 0
	grep -Fxv "$1" "$BROKEN_LOG" >"$BROKEN_LOG.tmp" 2>/dev/null || true
	mv "$BROKEN_LOG.tmp" "$BROKEN_LOG"
}

still_broken() {
	[ -f "$BROKEN_LOG" ] || return 0
	sed '/^$/d' "$BROKEN_LOG"
}

# residue names the nodes a blocked run leaves at 0555 and the command that
# cleans them, so the operator is not left with a poisoned node to find later.
residue() {
	_left=$(still_broken)
	[ -n "$_left" ] || return 0
	printf '; %s still carries the break — run: %s restore %s' \
		"$(printf '%s' "$_left" | tr '\n' ' ')" "$0" "$(printf '%s' "$_left" | head -1)"
}

# The judged node is the task's own placement: node_name survives settlement
# (the Job and its pods are deleted with Background propagation), and it is the
# node whose tree the task met. NODE may pin an action, but a node the task did
# not run on judges nothing — an untouched tree completes tasks for reasons
# this TC is not about.
place_node() {
	_body=$(task_entry "$1")
	printf '%s' "$_body" | jq -r '.node_name // empty'
}

# A cluster whose task pods name several nodes has no single worker to break;
# picking one of them is what left the judged task on an unbroken node.
discover_node() {
	_raw=$(kubectl -n "$JOB_NS" get pods -l foreman.tsic.top/task-id \
		-o jsonpath='{.items[*].spec.nodeName}' 2>/dev/null) ||
		blocked "cannot list task pods in $JOB_NS to discover a worker node"
	_nodes=$(printf '%s' "$_raw" | tr ' ' '\n' | sed '/^$/d' | sort -u)
	[ -n "$_nodes" ] || blocked "no task pod names a worker node — trigger one task first or set NODE"
	_n=$(printf '%s\n' "$_nodes" | wc -l)
	[ "$_n" -eq 1 ] ||
		blocked "task pods name $_n nodes ($(printf '%s' "$_nodes" | tr '\n' ' ')) — name the task this action follows, or set NODE, so the state lands on the task's node"
	printf '%s\n' "$_nodes"
}

# pin_node <task-id?>: the node a state action installs on. A named task pins
# it to that task's placement (its continuation is preferred there, not pinned
# — the preferred-node affinity carries weight 100); with no task id NODE names
# the node, or the only discovered one.
pin_node() {
	if [ -z "${1:-}" ]; then
		[ -z "$NODE" ] || { printf '%s\n' "$NODE"; return 0; }
		discover_node
		return 0
	fi
	_tn=$(place_node "$1")
	[ -n "$_tn" ] ||
		blocked "task $1 has no node_name — it never landed, so the node its cache tree sits on is unknown"
	[ -z "$NODE" ] || [ "$NODE" = "$_tn" ] ||
		blocked "NODE=$NODE but task $1 ran on '$_tn' — a state action targets the task's node"
	printf '%s\n' "$_tn"
}

# pin_place <task-id>: the node a judged action judges. A task that never
# landed met no cache tree; an explicit NODE naming another node is a
# construction error, not a judgement.
pin_place() {
	_tn=$(place_node "$1")
	[ -n "$_tn" ] ||
		blocked "task $1 has no node_name — its placement was never observed, so it met no cache tree"
	[ -z "$NODE" ] || [ "$NODE" = "$_tn" ] ||
		blocked "NODE=$NODE but task $1 ran on '$_tn' — the judged node is the task's placement"
	printf '%s\n' "$_tn"
}

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
# The break instant, read in the node's clock domain: a judging run compares it
# with the task projection's started_at, so a tree broken after the task started
# is never read as met by it.
BREAK_AT_MARKER=/host/tmp/.foreman-tc-cache-03-break-at
# Nodes a break left unwritable, one per line. The markers live on the node, so
# this operator-side list is the only record a blocked run can name the residue
# by; `restore NODE` drops the line once the node is back.
BROKEN_LOG=${FOREMAN_TC_CACHE_03_BROKEN_LOG:-${TMPDIR:-/tmp}/foreman-tc-cache-03-broken-nodes}

task_entry() {
	curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward the foreman Service first)"
}

# restore_node puts $NODE's workspaces tree back and drops both markers. It
# echoes the node-side answer and returns that call's status, so a caller can
# tell a restored node from an unreachable one; the observation goes to stderr.
restore_node() {
	_rc=0
	_out=$(node_sh "if [ -e '$MODE_MARKER' ]; then m=\$(cat '$MODE_MARKER')
		chmod \"\$m\" \"\$R/workspaces\" && rm -f '$MODE_MARKER' '$BREAK_AT_MARKER' && echo \"workspaces restored=\$m\"
	else
		chmod 0755 \"\$R/workspaces\" && rm -f '$BREAK_AT_MARKER' && echo 'workspaces restored=0755 (no recorded mode)'
	fi" 2>&1) || _rc=$?
	printf '# observation: restore: %s\n' "${_out:-<no output>}" >&2
	printf '%s' "$_out"
	return "$_rc"
}

wait_terminal() {
	body=$(task_entry "$1")
	printf '%s' "$body" | jq -e '.state == "terminal"' >/dev/null ||
		blocked "task $1 is not terminal yet — wait for it, then re-run this action"
}

case $ACTION in
break)
	tid=${2:-}
	NODE=$(pin_node "$tid")
	observe "node=$NODE action=$ACTION${tid:+ target=$tid}"
	out=$(node_sh "[ -d \"\$R/workspaces\" ] || { echo 'QA_BLOCKED: no workspaces tree' >&2; exit 2; }
		if [ ! -e '$MODE_MARKER' ]; then stat -c '%a' \"\$R/workspaces\" > '$MODE_MARKER' || exit 2; fi
		if [ ! -e '$BREAK_AT_MARKER' ]; then date +%s > '$BREAK_AT_MARKER' || exit 2; fi
		original=\$(cat '$MODE_MARKER')
		broken_at=\$(cat '$BREAK_AT_MARKER')
		chmod 0555 \"\$R/workspaces\" && echo \"workspaces=0555 original=\$original broken_at=\$broken_at\"") ||
		blocked "cannot make the workspaces tree unwritable on $NODE"
	[ -n "$out" ] || blocked "the node-side call returned no output (image $NODE_DEBUG_IMAGE not attached?)"
	observe "break: $out"
	case $out in
	*workspaces=0555*) : ;;
	*) blocked "the workspaces tree was not made unwritable: $out" ;;
	esac
	record_broken "$NODE"
	observe "trigger the same agent's continuation now (soft affinity prefers $NODE; the next action blocks if it landed elsewhere), then run: $0 verify-unwritable <task id>"
	;;
verify-unwritable)
	tid=${2:-}
	[ -n "$tid" ] || blocked "a task id is required"
	NODE=$(pin_place "$tid")
	body=$(task_entry "$tid")
	started=$(printf '%s' "$body" | jq -r '.started_at // empty')
	BROKEN=0
	# Every exit after the tree is confirmed broken restores it and drops the
	# journal line; a tree this run did not break is never touched. The trap is
	# installed before the terminal wait so a task that is still running does
	# not leave the node at 0555.
	restore() {
		[ "$BROKEN" = 1 ] || return 0
		restore_node >/dev/null || return 0
		forget_broken "$NODE"
	}
	trap restore EXIT INT TERM
	# The break must be installed on the node this task ran on, and before it
	# started: a task that met a writable tree — or one the tree changed under
	# mid-flight — completes for reasons this TC does not judge. Both facts are
	# read here, before the result.
	rc=0
	probe=$(node_sh "m=none; [ -e '$MODE_MARKER' ] && m=\$(cat '$MODE_MARKER')
		b=none; [ -e '$BREAK_AT_MARKER' ] && b=\$(cat '$BREAK_AT_MARKER')
		[ -d \"\$R/workspaces\" ] || { echo \"mode=absent marker=\$m broken_at=\$b\"; exit 0; }
		echo \"mode=\$(stat -c '%a' \"\$R/workspaces\") marker=\$m broken_at=\$b\"") || rc=$?
	observe "workspaces: ${probe:-<empty>} (rc=$rc)"
	mode=$(printf '%s\n' "$probe" | sed -n 's/^mode=\([^ ]*\) marker=.*$/\1/p' | head -1)
	[ -n "$mode" ] ||
		blocked "no mode marker in the node-side output (rc=$rc, image $NODE_DEBUG_IMAGE) — the debug call did not reach $NODE$(residue)"
	[ "$mode" = 555 ] ||
		blocked "workspaces on $NODE is not the tree break installs (mode=$mode) — the task never met the broken cache on its own node$(residue)"
	BROKEN=1
	broken_at=$(printf '%s\n' "$probe" | sed -n 's/^.*broken_at=\([^ ]*\)$/\1/p' | head -1)
	case $broken_at in
	'' | *[!0-9]*)
		blocked "the break instant on $NODE is unreadable (broken_at=${broken_at:-<missing>}) — a tree broken after the task started is not a scenario this run can judge" ;;
	esac
	[ -n "$started" ] ||
		blocked "task $tid has no started_at — the break on $NODE cannot be placed before its start"
	start_epoch=$(epoch "$started") ||
		blocked "cannot parse task $tid's started_at '$started' — the two instants are incomparable"
	[ "$broken_at" -lt "$start_epoch" ] ||
		blocked "the break on $NODE was installed at $broken_at, not before task $tid started ($start_epoch) — the task may have passed the cache write before the tree changed"
	wait_terminal "$tid"
	body=$(task_entry "$tid")
	observe "task=$body"
	printf '%s' "$body" | jq -e '.result == "failed"' >/dev/null ||
		fail "task $tid did not fail explicitly with the cache tree unwritable (result=$(printf '%s' "$body" | jq -r .result))"
	reason=$(printf '%s' "$body" | jq -r '.failure_reason // empty')
	observe "failure_reason=$reason (a job_* or checkout failure is expected)"
	trap - EXIT INT TERM
	restore_node >/dev/null ||
		blocked "task $tid failed as required, but $NODE could not be restored — retry with: $0 restore $NODE"
	forget_broken "$NODE"
	echo "PASS: TC-tech-cache-03 step 2 (task $tid failed explicitly; workspaces restored)"
	;;
rebuild-prepare)
	tid=${2:-}
	NODE=$(pin_node "$tid")
	observe "node=$NODE action=$ACTION${tid:+ target=$tid}"
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
	observe "trigger the same agent's continuation now (soft affinity prefers $NODE; the next action blocks if it landed elsewhere), then run: $0 rebuild-verify <task id>"
	;;
rebuild-verify)
	tid=${2:-}
	[ -n "$tid" ] || blocked "a task id is required"
	NODE=$(pin_place "$tid")
	observe "node=$NODE action=$ACTION"
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
restore)
	node_name=${2:-}
	[ -n "$node_name" ] || blocked "usage: $0 restore <node>"
	NODE=$node_name
	rc=0
	out=$(restore_node) || rc=$?
	[ "$rc" -eq 0 ] ||
		blocked "cannot restore $NODE (rc=$rc, image $NODE_DEBUG_IMAGE): ${out:-<no output>} — the journal keeps it"
	forget_broken "$NODE"
	observe "restore: $out"
	echo "PASS: TC-tech-cache-03 cleanup (workspaces on $NODE restored)"
	;;
*)
	blocked "usage: $0 break [task] | verify-unwritable <task> | rebuild-prepare [task] | rebuild-verify <task> | restore <node>"
	;;
esac
