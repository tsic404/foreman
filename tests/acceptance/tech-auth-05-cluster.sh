#!/bin/sh
# TC-tech-auth-05 — AC-21's self-healing face: prepare survives the two
# abnormal file shapes a task payload can leave on the node's shared home
# volume — the profile directory's position occupied by a regular file, and the
# marker path occupied by a directory — and the Job still boots. The carrier
# installs both shapes on the schedulable nodes, the operator triggers a task,
# and `verify` judges the landed pod, its prepare init container and the token
# mount, then restores the node trees.
#
#   WORKSPACE_ID=<workspace_id> ./tech-auth-05-cluster.sh prepare [NODE=<node>]
#       install both shapes (existing profile and marker are moved aside on the
#       node, not deleted) and print the trigger instruction. The home
#       partition is resolved from the reference template's home volume —
#       `hostPath: .../home/<workspace_id>` — so only the workspace under test
#       is touched and the other partitions on a shared node stay intact.
#   ./tech-auth-05-cluster.sh verify TASK_ID
#       judge the task's pod, restore every prepared node, clear the state.
#   ./tech-auth-05-cluster.sh restore
#       restore without judging (aborted window).
#
# The reference-template window is a precondition: the task Job must carry the
# prepare init container with the three-path heal and the marker `rm -rf`, and
# its home volume must mount the prepared partition.
#
# NODE_HOST_ROOT names where `kubectl debug node/...` mounts the node root; the
# default /host is what the profile's sysadmin image provides. Only a debug
# image with a different mount point needs it.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TEMPLATE=${FOREMAN_JOB_TEMPLATE:-$SCRIPT_DIR/../../deploy/25-job-template.yaml}
NODE_HOST_ROOT=${NODE_HOST_ROOT:-/host}
JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
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

# template_home_pattern prints the home volume's hostPath as written in the
# reference template. Both YAML shapes are accepted: the inline map
# (`hostPath: { path: … }`) and the block map (`hostPath:` then `path: …`).
# The template is the authority for the partition layout, so the carrier
# follows its prefix instead of assuming one.
template_home_pattern() {
	awk '
		function emit(v,   p) {
			p = v
			sub(/^.*path:[[:space:]]*/, "", p)
			sub(/[[:space:]]*[,}].*$/, "", p)
			gsub(/^[[:space:]]+|[[:space:]]+$/, "", p)
			if (p ~ /^\//) print p
			else print "!unreadable"
		}
		$1 == "-" && $2 == "name:" && $3 == "home" { in_home = 1; next }
		in_home && /hostPath:/ {
			hp = $0
			sub(/^.*hostPath:[[:space:]]*/, "", hp)
			gsub(/^[[:space:]]+|[[:space:]]+$/, "", hp)
			if (hp ~ /path:/) { emit(hp); exit }
			if (hp != "" && hp !~ /^\{[[:space:]]*\}?[[:space:]]*$/) { emit(hp); exit }
			while ((getline line) > 0) {
				if (line ~ /^[[:space:]]*-[[:space:]]/) break
				if (line ~ /path:/) { emit(line); exit }
			}
			print "!unreadable"
			exit
		}
		in_home && /^[[:space:]]*-[[:space:]]/ { exit }
	' "$TEMPLATE"
}

state_home() {
	[ -r "$STATE_DIR/home" ] || blocked "no prepared window at $STATE_DIR — run '$0 prepare' first"
	cat "$STATE_DIR/home"
}

state_host_root() {
	[ -r "$STATE_DIR/host_root" ] || blocked "no prepared window at $STATE_DIR — run '$0 prepare' first"
	cat "$STATE_DIR/host_root"
}

state_workspace() {
	[ -r "$STATE_DIR/workspace" ] || blocked "no prepared window at $STATE_DIR — run '$0 prepare' first"
	cat "$STATE_DIR/workspace"
}

# restore_node <node> <meta> <host_root> <home>: undo the anomaly and put the
# moved-aside profile and marker back. <host_root> is where the debug image
# mounts the node root; <home> is the node-side home partition written by
# prepare. Idempotent; returns non-zero with a diagnostic instead of exiting,
# so a partial restore keeps the remaining nodes' state.
restore_node() {
	_node=$1
	_meta=$2
	_host_root=$3
	_home=$4
	rest=${_meta#*|}
	pb=${rest%%|*}
	rest=${rest#*|}
	mb=${rest#*|}
	_script=$(sed -e "s|@HOST_ROOT@|$_host_root|g" -e "s|@HOME@|$_home|g" -e "s|@PB@|$pb|g" -e "s|@MB@|$mb|g" <<'NODE'
set -eu
[ -d '@HOST_ROOT@' ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at @HOST_ROOT@' >&2; exit 2; }
H='@HOST_ROOT@@HOME@'
[ -d "$H" ] || H='@HOME@'
[ -d "$H" ] || { echo 'QA_BLOCKED: the prepared home partition @HOME@ does not exist on this node' >&2; exit 2; }
base="$H/.multica"
prof="$base/profiles/foreman-job"
marker="$base/daemon_task_context.json"
rm -rf "$prof"
if [ -n "@PB@" ]; then
	mv "$H/@PB@" "$prof"
else
	mkdir -p "$prof"
	chown 1000:1000 "$prof"
fi
rm -rf "$marker"
if [ -n "@MB@" ]; then
	mv "$H/@MB@" "$marker"
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
	_host_root=$(state_host_root) || return 1
	_home=$(state_home) || return 1
	_fail=""
	for _n in $(state_nodes); do
		[ -r "$STATE_DIR/$_n.meta" ] || { _fail="$_fail $_n(no-meta)"; continue; }
		_meta=$(cat "$STATE_DIR/$_n.meta")
		restore_node "$_n" "$_meta" "$_host_root" "$_home" || _fail="$_fail $_n"
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
	WORKSPACE_ID=${WORKSPACE_ID:-}
	[ -n "$WORKSPACE_ID" ] ||
		blocked "WORKSPACE_ID is required: pass the workspace whose home partition this task uses (WORKSPACE_ID=<workspace_id> $0 prepare) — the reference template partitions the home volume as home/<workspace_id>"
	case $WORKSPACE_ID in
	*[!A-Za-z0-9._-]*|.|..) blocked "WORKSPACE_ID '$WORKSPACE_ID' is not a single path segment" ;;
	esac
	[ -r "$TEMPLATE" ] ||
		blocked "cannot read the reference job template at $TEMPLATE — set FOREMAN_JOB_TEMPLATE to deploy/25-job-template.yaml"
	home_pattern=$(template_home_pattern)
	case $home_pattern in
	'') blocked "the reference job template $TEMPLATE carries no home volume hostPath — not the reference-template window" ;;
	'!unreadable') blocked "cannot read the home volume's hostPath from the reference job template $TEMPLATE — want an absolute path in inline (hostPath: { path: ... }) or block (hostPath: then path: ...) form" ;;
	/*) : ;;
	*) blocked "the reference template's home hostPath '$home_pattern' is not an absolute path — cannot expand it on the node" ;;
	esac
	case $home_pattern in
	*'<workspace_id>'*) : ;;
	*) blocked "the reference template's home hostPath '$home_pattern' is not partitioned by <workspace_id> — this carrier selects one workspace partition" ;;
	esac
	home_root=${home_pattern%%<workspace_id>*}${WORKSPACE_ID}${home_pattern#*<workspace_id>}
	case $NODE_HOST_ROOT$home_root in
	*'&'*|*'|'*|*'\'*) blocked "the rendered node paths carry a sed metacharacter — refusing to build the node script" ;;
	esac
	rm -rf "$STATE_DIR"
	mkdir -p "$STATE_DIR"
	: >"$STATE_DIR/nodes"
	printf '%s\n' "$WORKSPACE_ID" >"$STATE_DIR/workspace"
	printf '%s\n' "$home_root" >"$STATE_DIR/home"
	printf '%s\n' "$NODE_HOST_ROOT" >"$STATE_DIR/host_root"
	for n in $nodes; do
		_script=$(sed -e "s|@HOST_ROOT@|$NODE_HOST_ROOT|g" -e "s|@HOME@|$home_root|g" -e "s|@WS@|$WORKSPACE_ID|g" -e "s|@RUN@|$RUN|g" <<'NODE'
set -eu
[ -d '@HOST_ROOT@' ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at @HOST_ROOT@' >&2; exit 2; }
H='@HOST_ROOT@@HOME@'
[ -d "$H" ] || H='@HOME@'
[ -d "$H" ] || { echo 'QA_BLOCKED: the home partition @HOME@ for workspace @WS@ is not a directory on this node — run a task of this workspace here first (the template creates it with DirectoryOrCreate)' >&2; exit 2; }
base="$H/.multica"
[ -d "$base" ] || { echo 'QA_BLOCKED: @HOME@/.multica is not a directory on this node' >&2; exit 2; }
[ -d "$base/profiles" ] || { echo 'QA_BLOCKED: @HOME@/.multica/profiles is not a directory on this node' >&2; exit 2; }
prof="$base/profiles/foreman-job"
pe=0
pb=""
if [ -e "$prof" ] || [ -L "$prof" ]; then
	pb=".tc-auth-05-@RUN@-profile"
	rm -rf "$H/$pb"
	mv "$prof" "$H/$pb"
	pe=1
fi
printf 'anomaly\n' >"$prof"
marker="$base/daemon_task_context.json"
me=0
mb=""
if [ -e "$marker" ] || [ -L "$marker" ]; then
	mb=".tc-auth-05-@RUN@-marker"
	rm -rf "$H/$mb"
	mv "$marker" "$H/$mb"
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
	echo "trigger a task of workspace $WORKSPACE_ID now, then run: $0 verify <task id>"
	echo "abort: $0 restore"
	;;

verify)
	TASK=${2:-}
	[ -n "$TASK" ] || blocked "usage: $0 verify TASK_ID"
	state_nodes >/dev/null
	home_root=$(state_home)
	workspace=$(state_workspace)
	entry=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$TASK?workspace=$workspace") ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$TASK?workspace=$workspace (port-forward the foreman Service first)"
	task_workspace=$(printf '%s' "$entry" | jq -r '.workspace_id // empty')
	[ "$task_workspace" = "$workspace" ] ||
		blocked "task $TASK belongs to workspace '$task_workspace', but this window prepared '$workspace'"
	job=$(printf '%s' "$entry" | jq -r '.job_name // empty')
	[ -n "$job" ] || blocked "task $TASK has no job_name in its projection"
	job_json=$(kubectl -n "$JOB_NS" get job "$job" -o json 2>/dev/null) ||
		blocked "Job $job is gone — run verify while the task's pod is still alive"
	job_home=$(printf '%s' "$job_json" | jq -r \
		'[.spec.template.spec.volumes[]? | select(.name == "home") | .hostPath.path][0] // empty')
	[ "$job_home" = "$home_root" ] ||
		blocked "Job $job mounts home at '$job_home', but this window prepared '$home_root'"
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
	state_nodes >/dev/null
	state_host_root >/dev/null
	state_home >/dev/null
	restore_all || blocked "the tree restore failed — the state file is kept for '$0 restore'"
	echo "restored: $STATE_DIR removed"
	;;

*)
	blocked "usage: WORKSPACE_ID=<workspace_id> $0 prepare [NODE=<node>] | $0 verify TASK_ID | $0 restore"
	;;
esac
