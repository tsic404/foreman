#!/bin/sh
# TC-tech-auth-05 — AC-21's self-healing face: prepare survives the two
# abnormal file shapes a task payload can leave on the node's shared home
# volume — the profile directory's position occupied by a regular file, and the
# marker path occupied by a directory — and the Job still boots. The carrier
# installs both shapes on the schedulable nodes, the operator triggers a task,
# and `verify` judges the landed pod, its prepare init container and the token
# mount, then restores the node trees.
#
#   ./tech-auth-05-cluster.sh prepare [NODE=<node>]
#       install both shapes (existing profile and marker are moved aside on the
#       node, not deleted) and print the trigger instruction.
#   ./tech-auth-05-cluster.sh verify TASK_ID
#       judge the task's pod, restore every prepared node, clear the state.
#   ./tech-auth-05-cluster.sh restore
#       restore without judging (aborted window).
#
# The reference-template window is a precondition: the task Job must carry the
# prepare init container with the three-path heal and the marker `rm -rf`.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
STATE_ROOT=${FOREMAN_STATE_ROOT:-/var/lib/foreman}
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
WAIT=${WAIT:-240}
STATE_DIR=${STATE_DIR:-$HOME/.foreman-tc-auth-05}
ACTION=${1:-}
RUN=tc-auth-05-$(date +%s)-$$

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

node_sh() { # <node> <script>
	kubectl debug "node/$1" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "$2" 2>&1
}

node_blocked_reason() {
	printf '%s\n' "$1" | sed -n 's/^QA_BLOCKED: //p' | head -1
}

state_nodes() {
	[ -r "$STATE_DIR/nodes" ] || blocked "no prepared window at $STATE_DIR — run '$0 prepare' first"
	cat "$STATE_DIR/nodes"
}

# restore_node <node> <meta>: undo the anomaly and put the moved-aside profile
# and marker back. Idempotent; returns non-zero with a diagnostic instead of
# exiting, so a partial restore keeps the remaining nodes' state.
restore_node() {
	_node=$1
	_meta=$2
	rest=${_meta#*|}
	pb=${rest%%|*}
	rest=${rest#*|}
	mb=${rest#*|}
	_script=$(sed -e "s|@STATE_ROOT@|$STATE_ROOT|g" -e "s|@PB@|$pb|g" -e "s|@MB@|$mb|g" <<'NODE'
set -eu
[ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
R='/host@STATE_ROOT@'
[ -d "$R" ] || R='@STATE_ROOT@'
base="$R/home/.multica"
prof="$base/profiles/foreman-job"
marker="$base/daemon_task_context.json"
rm -rf "$prof"
if [ -n "@PB@" ]; then
	mv "$R/home/@PB@" "$prof"
else
	mkdir -p "$prof"
	chown 1000:1000 "$prof"
fi
rm -rf "$marker"
if [ -n "@MB@" ]; then
	mv "$R/home/@MB@" "$marker"
fi
echo "restored"
NODE
	)
	if ! out=$(node_sh "$_node" "$_script"); then
		printf 'restore error on %s: %s\n' "$_node" "$out" >&2
		return 1
	fi
	case $out in
	*QA_BLOCKED*) printf 'restore blocked on %s: %s\n' "$_node" "$(node_blocked_reason "$out")" >&2; return 1 ;;
	*restored*) return 0 ;;
	*) printf 'restore returned no marker on %s: %s\n' "$_node" "$out" >&2; return 1 ;;
	esac
}

restore_all() {
	_fail=""
	for _n in $(state_nodes); do
		[ -r "$STATE_DIR/$_n.meta" ] || { _fail="$_fail $_n(no-meta)"; continue; }
		_meta=$(cat "$STATE_DIR/$_n.meta")
		restore_node "$_n" "$_meta" || _fail="$_fail $_n"
	done
	[ -z "$_fail" ] || { printf 'restore failed on:%s\n' "$_fail" >&2; return 1; }
	rm -rf "$STATE_DIR"
	return 0
}

case $ACTION in
prepare)
	case ${2:-} in
	NODE=*) NODE=${2#NODE=} ;;
	"") NODE=${NODE:-} ;;
	*) NODE=$2 ;;
	esac
	if [ -z "$NODE" ]; then
		nodes=$(kubectl get nodes -l kubernetes.io/os=linux -o json 2>/dev/null |
			jq -r '.items[] | select(.spec.unschedulable != true) | .metadata.name') ||
			blocked "cannot list the cluster's Linux nodes"
	else
		nodes=$NODE
	fi
	[ -n "$nodes" ] || blocked "no schedulable Linux node — the anomaly has no target"
	[ ! -r "$STATE_DIR/nodes" ] ||
		blocked "a prepared window is active at $STATE_DIR — run '$0 restore' first"
	rm -rf "$STATE_DIR"
	mkdir -p "$STATE_DIR"
	: >"$STATE_DIR/nodes"
	for n in $nodes; do
		_script=$(sed -e "s|@STATE_ROOT@|$STATE_ROOT|g" -e "s|@RUN@|$RUN|g" <<'NODE'
set -eu
[ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
R='/host@STATE_ROOT@'
[ -d "$R" ] || R='@STATE_ROOT@'
[ -d "$R/home" ] || { echo "QA_BLOCKED: no home tree under $R" >&2; exit 2; }
base="$R/home/.multica"
[ -d "$base" ] || { echo 'QA_BLOCKED: .multica is not a directory on this node' >&2; exit 2; }
[ -d "$base/profiles" ] || { echo 'QA_BLOCKED: .multica/profiles is not a directory on this node' >&2; exit 2; }
prof="$base/profiles/foreman-job"
pe=0
pb=""
if [ -e "$prof" ] || [ -L "$prof" ]; then
	pb=".tc-auth-05-@RUN@-profile"
	rm -rf "$R/home/$pb"
	mv "$prof" "$R/home/$pb"
	pe=1
fi
printf 'anomaly\n' >"$prof"
marker="$base/daemon_task_context.json"
me=0
mb=""
if [ -e "$marker" ] || [ -L "$marker" ]; then
	mb=".tc-auth-05-@RUN@-marker"
	rm -rf "$R/home/$mb"
	mv "$marker" "$R/home/$mb"
	me=1
fi
mkdir -p "$marker/nested"
printf 'anomaly\n' >"$marker/nested/marker"
echo "state=$pe|$pb|$me|$mb"
NODE
		)
		out=$(node_sh "$n" "$_script") ||
			{ restore_all >/dev/null 2>&1 || true; blocked "cannot install the anomaly on $n: $out"; }
		case $out in
		*QA_BLOCKED*) restore_all >/dev/null 2>&1 || true; blocked "$(node_blocked_reason "$out")" ;;
		esac
		meta=$(printf '%s\n' "$out" | sed -n 's/^state=//p' | head -1)
		[ -n "$meta" ] ||
			{ restore_all >/dev/null 2>&1 || true; blocked "the anomaly install on $n returned no state marker: $out"; }
		printf '%s\n' "$n" >>"$STATE_DIR/nodes"
		printf '%s\n' "$meta" >"$STATE_DIR/$n.meta"
		observe "prepared node=$n state=$meta"
	done
	observe "window: both shapes installed on $(wc -l <"$STATE_DIR/nodes" | tr -d ' ') node(s)"
	echo "trigger a task now, then run: $0 verify <task id>"
	echo "abort: $0 restore"
	;;

verify)
	TASK=${2:-}
	[ -n "$TASK" ] || blocked "usage: $0 verify TASK_ID"
	state_nodes >/dev/null
	entry=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$TASK") ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$TASK (port-forward the foreman Service first)"
	job=$(printf '%s' "$entry" | jq -r '.job_name // empty')
	[ -n "$job" ] || blocked "task $TASK has no job_name in its projection"
	job_json=$(kubectl -n "$JOB_NS" get job "$job" -o json 2>/dev/null) ||
		blocked "Job $job is gone — run verify while the task's pod is still alive"
	prepare_args=$(printf '%s' "$job_json" | jq -r \
		'[.spec.template.spec.initContainers[]? | select(.name == "prepare") | .args[0]][0] // empty')
	[ -n "$prepare_args" ] ||
		blocked "Job $job carries no prepare init container — not the reference-template window"
	printf '%s' "$prepare_args" | grep -Fq '[ -d "$d" ] || { rm -f "$d"; mkdir -p "$d"; }' ||
		blocked "prepare does not carry the three-path heal — not the reference-template window"
	printf '%s' "$prepare_args" | grep -Fq 'rm -rf /home/agent/.multica/daemon_task_context.json' ||
		blocked "prepare does not carry the marker rm -rf — not the reference-template window"

	# Wait for prepare to finish and the agent container to start. A pod that
	# stays in CreateContainerConfigError is the failure this case exists to
	# catch, so the wait is bounded and the raw status is printed.
	deadline=$(( $(date +%s) + WAIT ))
	pod=""
	node=""
	why=""
	while [ "$(date +%s)" -lt "$deadline" ]; do
		pods=$(kubectl -n "$JOB_NS" get pods -l "job-name=$job" -o json 2>/dev/null) || true
		pod=$(printf '%s' "$pods" | jq -r '[.items[]] | sort_by(.metadata.creationTimestamp) | last | .metadata.name // empty')
		node=$(printf '%s' "$pods" | jq -r '[.items[]] | sort_by(.metadata.creationTimestamp) | last | .spec.nodeName // empty')
		prepare_ok=$(printf '%s' "$pods" | jq -r '
			[.items[] | .status.initContainerStatuses[]? | select(.name == "prepare") | .state.terminated.exitCode] | first // empty')
		agent_started=$(printf '%s' "$pods" | jq -r '
			[.items[] | .status.containerStatuses[]? | select(.name == "agent")
			 | select(.state.running != null or .state.terminated != null)] | length > 0')
		if [ "$prepare_ok" = "0" ] && [ "$agent_started" = "true" ]; then
			break
		fi
		why=$(printf '%s' "$pods" | jq -c '[.items[] | {pod: .metadata.name, phase: .status.phase,
			init: [.status.initContainerStatuses[]? | {name, state}], containers: [.status.containerStatuses[]? | {name, state}]}]')
		sleep 2
	done
	[ -n "$pod" ] || blocked "no pod exists for Job $job"
	[ "$prepare_ok" = "0" ] ||
		fail "prepare did not complete successfully on pod $pod: $why"
	[ "$agent_started" = "true" ] ||
		fail "the agent container never started on pod $pod: $why"
	observe "pod=$pod node=$node prepare_exit=0 agent_started=true"

	case " $(state_nodes | tr '\n' ' ') " in
	*" $node "*) : ;;
	*) blocked "the pod landed on $node, which was not prepared — the case's precondition does not hold" ;;
	esac

	# The token mount and the healed tree, read from inside the agent
	# container: the subPath mount is created after prepare, so a healed
	# profile position is what lets it exist at all.
	rc=0
	runtime=$(kubectl -n "$JOB_NS" exec "pod/$pod" -c agent -- sh -c '
		stat -c "%a %u:%g %n" /home/agent/.multica/profiles/foreman-job/config.json
		[ -d /home/agent/.multica/profiles/foreman-job ] && echo profile=dir || echo profile=notdir
		[ -e /home/agent/.multica/daemon_task_context.json ] && echo marker=present || echo marker=absent
	') || rc=$?
	[ "$rc" -eq 0 ] || blocked "cannot exec into pod $pod (the agent container may still be starting): exit $rc"
	observe "runtime: $(printf '%s' "$runtime" | tr '\n' ' ')"
	cred=$(printf '%s\n' "$runtime" | sed -n '1p')
	case $cred in
	400\ 1000:1000\ *) : ;;
	*) fail "the token mount is '$cred', want 400 1000:1000" ;;
	esac
	printf '%s\n' "$runtime" | grep -qx 'profile=dir' ||
		fail "the profile position is still not a directory: $runtime"
	printf '%s\n' "$runtime" | grep -qx 'marker=absent' ||
		fail "the marker path still exists after prepare: $runtime"

	restore_all || blocked "the tree restore failed — the state file is kept for '$0 restore'"
	echo "PASS: TC-tech-auth-05 (task $TASK on $node booted with both abnormal shapes; prepare healed the tree and the token mount is 400 1000:1000)"
	;;

restore)
	restore_all || blocked "the tree restore failed — the state file is kept for '$0 restore'"
	echo "restored: $STATE_DIR removed"
	;;

*)
	blocked "usage: $0 prepare [NODE=<node>] | verify TASK_ID | restore"
	;;
esac
