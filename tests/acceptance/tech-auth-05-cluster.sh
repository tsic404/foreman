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
# prepare init container with the three-path heal, the marker `rm -rf` and the
# 0400 credential copy, its home volume must mount the prepared partition, and
# the agent container must mount the credential as the single read-only file at
# the profile path — AC-21 judges that face on the rendered Job, so `verify`
# reads it there.
#
# `verify` reads the healed tree from the node itself, so the verdict does not
# depend on the agent container's lifetime. The mounted credential file is read
# inside that container when an exec still lands there; when the task finishes
# its few seconds first, the rendered Job's mount shape is the evidence, and
# the run still reaches a verdict instead of blocking.
#
# NODE_HOST_ROOT names where the node debugger mounts the node root; the
# default /host is what the profile's sysadmin image provides. Only a debug
# image with a different mount point needs it. Each node command runs in its
# own debugging pod; DEBUG_NS defaults to the current kubeconfig context's
# namespace (falling back to `default`). The carrier waits NODE_WAIT seconds
# (default 180) for the command to finish, and a pod that outlives that window
# is reported and removed instead of being read as a half answer — the node
# may already be rewritten at that point, so every node is registered for
# restore before its command starts.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TEMPLATE=${FOREMAN_JOB_TEMPLATE:-$SCRIPT_DIR/../../deploy/25-job-template.yaml}
NODE_HOST_ROOT=${NODE_HOST_ROOT:-/host}
JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
DEBUG_NS=${DEBUG_NS:-}
NODE_WAIT=${NODE_WAIT:-180}
WAIT=${WAIT:-240}
STATE_DIR=${STATE_DIR:-$HOME/.foreman-tc-auth-05}
ACTION=${1:-}
RUN=tc-auth-05-$(date +%s)-$$

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

# The node debugger pod inherits the kubeconfig context's namespace when the
# caller names none: a flat `default` would move the three operations into a
# namespace the operator may have no pod-create right in, and every node
# command would read back as a permission blocker.
if [ -z "$DEBUG_NS" ]; then
	DEBUG_NS=$(kubectl config view --minify -o jsonpath='{..namespace}' 2>/dev/null) || DEBUG_NS=""
	DEBUG_NS=${DEBUG_NS:-default}
fi

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

# node_sh <node> <script>: run the script in a node debugger (the node root is
# at NODE_HOST_ROOT) and print what it wrote. The debugger pod is created
# detached and its log is read once it finishes: kubectl's attach stream loses
# the tail of a command that exits in milliseconds — it falls back to a log
# stream the container has already outrun — so the state marker line used to
# vanish and the install read back as "no state marker". The pod's log is the
# durable record, and it also keeps a failed node script's output readable.
# A pod still running at NODE_WAIT is not read as an answer: its command may
# keep rewriting the node after the caller has moved on, so the pod is deleted
# (stopping the command) and the timeout is reported as the failure it is. A
# failed log read keeps the pod for inspection.
node_sh() {
	_create=$(kubectl -n "$DEBUG_NS" debug "node/$1" --profile=sysadmin --attach=false \
		--image="$NODE_DEBUG_IMAGE" -- sh -c "$2" 2>&1) || true
	_pod=$(printf '%s\n' "$_create" | sed -n 's/^Creating debugging pod \([^ ]*\) with container .*/\1/p' | head -1)
	[ -n "$_pod" ] || { printf '%s\n' "${_create:-the node debugger pod was not created}"; return 1; }
	_phase=""
	_deadline=$(( $(date +%s) + NODE_WAIT ))
	while :; do
		_phase=$(kubectl -n "$DEBUG_NS" get pod "$_pod" -o jsonpath='{.status.phase}' 2>/dev/null) || _phase=""
		case $_phase in Succeeded|Failed) break ;; esac
		[ "$(date +%s)" -lt "$_deadline" ] || break
		sleep 1
	done
	_out=""
	_rc=0
	_out=$(kubectl -n "$DEBUG_NS" logs "pod/$_pod" 2>&1) || _rc=$?
	case $_phase in
	Succeeded|Failed) : ;;
	*)
		kubectl -n "$DEBUG_NS" delete pod "$_pod" --ignore-not-found >/dev/null 2>&1 || true
		printf 'the node debugger pod %s did not finish within %ss (phase %s); its command may have rewritten the node; log so far:\n%s\n' \
			"$_pod" "$NODE_WAIT" "${_phase:-unknown}" "$_out"
		return 1
		;;
	esac
	_exit=$(kubectl -n "$DEBUG_NS" get pod "$_pod" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null) || _exit=""
	if [ "$_rc" -ne 0 ]; then
		printf 'cannot read the node debugger pod %s log (phase %s, exit %s); the pod is kept for inspection: %s\n' \
			"$_pod" "${_phase:-unknown}" "${_exit:-unknown}" "$_out"
		return 1
	fi
	[ -n "$_out" ] ||
		{ printf 'the node debugger pod %s left no output (phase %s, exit %s); the pod is kept for inspection\n' \
			"$_pod" "${_phase:-unknown}" "${_exit:-unknown}"; return 1; }
	kubectl -n "$DEBUG_NS" delete pod "$_pod" --ignore-not-found >/dev/null 2>&1 || true
	printf '%s\n' "$_out"
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
# prepare. The install writes a per-run journal on the node before touching
# either position, and records the positions whose original was absent: a
# command that is cut off mid-way (log lost, NODE_WAIT expiry) must still be
# undone without deleting an original that was never moved. Idempotent;
# returns non-zero with a diagnostic instead of exiting, so a partial restore
# keeps the remaining nodes' state.
restore_node() {
	_node=$1
	_meta=$2
	_host_root=$3
	_home=$4
	rest=${_meta#*|}
	pb=${rest%%|*}
	rest=${rest#*|}
	rest=${rest#*|}
	mb=${rest%%|*}
	journal=""
	case $rest in
	*'|'*) journal=${rest#*|} ;;
	esac
	journal=${journal:-.tc-auth-05-$RUN.journal}
	_script=$(sed -e "s|@HOST_ROOT@|$_host_root|g" -e "s|@HOME@|$_home|g" -e "s|@PB@|$pb|g" -e "s|@MB@|$mb|g" -e "s|@JOURNAL@|$journal|g" <<'NODE'
set -eu
[ -d '@HOST_ROOT@' ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at @HOST_ROOT@' >&2; exit 2; }
H='@HOST_ROOT@@HOME@'
[ -d "$H" ] || H='@HOME@'
[ -d "$H" ] || { echo 'QA_BLOCKED: the prepared home partition @HOME@ does not exist on this node' >&2; exit 2; }
journal="$H/@JOURNAL@"
# An empty backup name carries no on-node name to probe: `[ ! -e "$H/" ]`
# would test the home directory itself and never hold, so a window whose two
# originals were absent would fall through to the success line as if it had
# restored something. A missing journal and no backup means nothing to undo.
if [ ! -e "$journal" ] &&
	{ [ -z "@PB@" ] || [ ! -e "$H/@PB@" ]; } &&
	{ [ -z "@MB@" ] || [ ! -e "$H/@MB@" ]; }; then
	echo "restored: the install never touched this node"
	exit 0
fi
prof_state=$(sed -n 's/^profile=//p' "$journal" 2>/dev/null | head -1)
marker_state=$(sed -n 's/^marker=//p' "$journal" 2>/dev/null | head -1)
base="$H/.multica"
prof="$base/profiles/foreman-job"
marker="$base/daemon_task_context.json"
if [ -n "@PB@" ] && [ -e "$H/@PB@" ]; then
	rm -rf "$prof"
	mv "$H/@PB@" "$prof"
elif [ "$prof_state" = "absent" ] || [ "$prof_state" = "moved" ]; then
	# "moved" without the backup means it was lost after the move; the
	# directory shape is still what every task expects, so restore it empty
	# instead of leaving the anomaly file in place.
	rm -rf "$prof"
	mkdir -p "$prof"
	chown 1000:1000 "$prof"
fi
if [ -n "@MB@" ] && [ -e "$H/@MB@" ]; then
	rm -rf "$marker"
	mv "$H/@MB@" "$marker"
elif [ "$marker_state" = "absent" ] || [ "$marker_state" = "moved" ]; then
	rm -rf "$marker"
fi
rm -f "$journal"
echo "restored"
NODE
	)
	# Keep this assignment out of the caller's `out`: a caller that prints its
	# own command's output around a restore would otherwise print this one.
	if ! _rout=$(node_sh "$_node" "$_script"); then
		printf 'restore error on %s: %s\n' "$_node" "$_rout" >&2
		return 1
	fi
	case $_rout in
	*QA_BLOCKED*) printf 'restore blocked on %s: %s\n' "$_node" "$(node_blocked_reason "$_rout")" >&2; return 1 ;;
	*restored*) return 0 ;;
	*) printf 'restore returned no marker on %s: %s\n' "$_node" "$_rout" >&2; return 1 ;;
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
		pb=".tc-auth-05-$RUN-profile"
		mb=".tc-auth-05-$RUN-marker"
		journal=".tc-auth-05-$RUN.journal"
		# Register the node and its deterministic backup/journal names before
		# its command runs: a command can rewrite the node and still fail to
		# report (lost log, NODE_WAIT expiry), and restore must cover it even
		# when it runs as a separate `$0 restore` process with its own RUN.
		printf '%s\n' "$n" >>"$STATE_DIR/nodes"
		printf '%s\n' "1|$pb|1|$mb|$journal" >"$STATE_DIR/$n.meta"
		_script=$(sed -e "s|@HOST_ROOT@|$NODE_HOST_ROOT|g" -e "s|@HOME@|$home_root|g" -e "s|@WS@|$WORKSPACE_ID|g" -e "s|@PB@|$pb|g" -e "s|@MB@|$mb|g" -e "s|@JOURNAL@|$journal|g" <<'NODE'
set -eu
[ -d '@HOST_ROOT@' ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at @HOST_ROOT@' >&2; exit 2; }
H='@HOST_ROOT@@HOME@'
[ -d "$H" ] || H='@HOME@'
[ -d "$H" ] || { echo 'QA_BLOCKED: the home partition @HOME@ for workspace @WS@ is not a directory on this node — run a task of this workspace here first (the template creates it with DirectoryOrCreate)' >&2; exit 2; }
base="$H/.multica"
[ -d "$base" ] || { echo 'QA_BLOCKED: @HOME@/.multica is not a directory on this node' >&2; exit 2; }
[ -d "$base/profiles" ] || { echo 'QA_BLOCKED: @HOME@/.multica/profiles is not a directory on this node' >&2; exit 2; }
journal="$H/@JOURNAL@"
: >"$journal"
prof="$base/profiles/foreman-job"
pe=0
pb=""
if [ -e "$prof" ] || [ -L "$prof" ]; then
	pb='@PB@'
	rm -rf "$H/$pb"
	mv "$prof" "$H/$pb"
	pe=1
	printf 'profile=moved\n' >>"$journal"
else
	printf 'profile=absent\n' >>"$journal"
fi
printf 'anomaly\n' >"$prof"
marker="$base/daemon_task_context.json"
me=0
mb=""
if [ -e "$marker" ] || [ -L "$marker" ]; then
	mb='@MB@'
	rm -rf "$H/$mb"
	mv "$marker" "$H/$mb"
	me=1
	printf 'marker=moved\n' >>"$journal"
else
	printf 'marker=absent\n' >>"$journal"
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
		printf '%s\n' "$meta|$journal" >"$STATE_DIR/$n.meta"
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
	host_root=$(state_host_root)
	workspace=$(state_workspace)
	entry=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$TASK?workspace=$workspace") ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$TASK?workspace=$workspace (port-forward the foreman Service first)"
	task_workspace=$(printf '%s' "$entry" | jq -r '.workspace_id // empty')
	[ "$task_workspace" = "$workspace" ] ||
		blocked "task $TASK belongs to workspace '$task_workspace', but this window prepared '$workspace'"
	job=$(printf '%s' "$entry" | jq -r '.job_name // empty')
	[ -n "$job" ] || blocked "task $TASK has no job_name in its projection"
	job_json=$(kubectl -n "$JOB_NS" get job "$job" -o json 2>/dev/null) ||
		blocked "Job $job is gone — run verify before the task's terminal settles (the Job is deleted with it)"
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

	# The credential face, judged on the rendered Job (the design's judgment
	# object): one read-only file at the profile path, out of the emptyDir that
	# prepare fills with mode 0400 and owner 1000. This is also the evidence
	# that stands when the agent container ends before an exec lands inside it.
	printf '%s' "$prepare_args" | grep -Fq 'chmod 0400 /credout/config.json' ||
		blocked "prepare does not set the credential copy to 0400 — not the reference-template window"
	printf '%s' "$prepare_args" | grep -Fq 'chown -R 1000:1000' ||
		blocked "prepare does not own the credential copy as 1000:1000 — not the reference-template window"
	cred_mount=$(printf '%s' "$job_json" | jq -r '
		[.spec.template.spec.containers[]? | select(.name == "agent") | .volumeMounts[]? | select(.name == "cred")]
		| first // empty | "\(.subPath // "")|\(.readOnly // false)|\(.mountPath // "")"')
	[ "$cred_mount" = "config.json|true|/home/agent/.multica/profiles/foreman-job/config.json" ] ||
		blocked "Job $job's agent container does not mount the credential as the single read-only file at the profile path (got '${cred_mount:-absent}')"
	cred_volume=$(printf '%s' "$job_json" | jq -r '
		[.spec.template.spec.volumes[]? | select(.name == "cred") | if has("emptyDir") then "emptyDir" else "other" end]
		| first // empty')
	[ "$cred_volume" = "emptyDir" ] ||
		blocked "Job $job's credential volume is not the emptyDir prepare writes (got '${cred_volume:-absent}')"

	# The live window. A reference-template task can start and finish its agent
	# container within seconds, so the exec happens as soon as the container is
	# seen running — not after the loop has settled the other observations —
	# and is retried while the container lives. A container that ends before an
	# exec lands is a task that outran the carrier, not a broken window: the
	# credential face is then the rendered Job's above, and the healed tree is
	# read from the node below. A pod that stays in CreateContainerConfigError
	# is the failure this case exists to catch, so the wait is bounded and the
	# raw status is printed.
	deadline=$(( $(date +%s) + WAIT ))
	pod=""
	node=""
	seen_pod=""
	live=0
	runtime=""
	exec_err=""
	prepare_ok=""
	agent_state=""
	why=""
	while [ "$(date +%s)" -lt "$deadline" ]; do
		pods=$(kubectl -n "$JOB_NS" get pods -l "job-name=$job" -o json 2>/dev/null) || true
		pod=$(printf '%s' "$pods" | jq -r '[.items[]] | sort_by(.metadata.creationTimestamp) | last | .metadata.name // empty')
		node=$(printf '%s' "$pods" | jq -r '[.items[]] | sort_by(.metadata.creationTimestamp) | last | .spec.nodeName // empty')
		[ -z "$pod" ] || seen_pod=$pod
		prepare_ok=$(printf '%s' "$pods" | jq -r '
			[.items[] | .status.initContainerStatuses[]? | select(.name == "prepare") | .state.terminated.exitCode] | first // empty')
		agent_state=$(printf '%s' "$pods" | jq -r '
			[.items[] | .status.containerStatuses[]? | select(.name == "agent") | .state
			 | if .running != null then "running" elif .terminated != null then "terminated" else "waiting" end]
			| first // empty')
		why=$(printf '%s' "$pods" | jq -c '[.items[] | {pod: .metadata.name, phase: .status.phase,
			init: [.status.initContainerStatuses[]? | {name, state}], containers: [.status.containerStatuses[]? | {name, state}]}]')
		if [ -n "$prepare_ok" ] && [ "$prepare_ok" != "0" ]; then
			fail "prepare did not complete successfully on pod $pod: $why"
		fi
		if [ "$live" -eq 0 ] && [ -n "$pod" ] && [ "$prepare_ok" = "0" ] && [ "$agent_state" = "running" ]; then
			rc=0
			runtime=$(kubectl -n "$JOB_NS" exec "pod/$pod" -c agent -- sh -c '
				stat -c "%a %u:%g %n" /home/agent/.multica/profiles/foreman-job/config.json
				[ -d /home/agent/.multica/profiles/foreman-job ] && echo profile=dir || echo profile=notdir
				[ -e /home/agent/.multica/daemon_task_context.json ] && echo marker=present || echo marker=absent
			' 2>&1) || rc=$?
			if [ "$rc" -eq 0 ]; then
				live=1
			else
				exec_err=$(printf '%s' "$runtime" | tr '\n' ' ')
				runtime=""
			fi
		fi
		if [ "$live" -eq 1 ] && [ "$prepare_ok" = "0" ]; then
			break
		fi
		if [ "$live" -eq 0 ] && [ "$agent_state" = "terminated" ]; then
			break
		fi
		sleep 1
	done
	if [ -z "$pod" ]; then
		if [ -n "$seen_pod" ]; then
			blocked "pod $seen_pod disappeared before the window settled — the Job is deleted with the task's terminal"
		fi
		blocked "no pod exists for Job $job"
	fi
	[ "$prepare_ok" = "0" ] ||
		fail "prepare did not complete successfully on pod $pod: $why"
	case $agent_state in
	running|terminated) : ;;
	*) fail "the agent container never started on pod $pod: $why" ;;
	esac
	observe "pod=$pod node=$node prepare_exit=0 agent=$agent_state"

	case " $(state_nodes | tr '\n' ' ') " in
	*" $node "*) : ;;
	*) blocked "the pod landed on $node, which was not prepared — the case's precondition does not hold" ;;
	esac

	# The mounted credential file is only visible inside the agent container.
	# If it is still running and the exec cannot land, that is a carrier
	# permission problem rather than a task that finished first — say so
	# instead of judging the window on the rendered Job alone.
	if [ "$live" -eq 0 ] && [ "$agent_state" = "running" ]; then
		blocked "cannot exec into pod $pod while its agent container is running: ${exec_err:-no error output}"
	fi
	if [ "$live" -eq 1 ]; then
		observe "runtime: $(printf '%s' "$runtime" | tr '\n' ' ')"
		cred=$(printf '%s\n' "$runtime" |
			grep -E '^[0-7]{3,4} [0-9]+:[0-9]+ /home/agent/.multica/profiles/foreman-job/config.json$' | head -1)
		[ -n "$cred" ] ||
			fail "the credential file is not readable at the profile path inside pod $pod: $runtime"
		case $cred in
		400\ 1000:1000\ *) : ;;
		*) fail "the token mount is '$cred', want 400 1000:1000" ;;
		esac
		printf '%s\n' "$runtime" | grep -qx 'profile=dir' ||
			fail "the profile position is still not a directory: $runtime"
		printf '%s\n' "$runtime" | grep -qx 'marker=absent' ||
			fail "the marker path still exists after prepare: $runtime"
	else
		observe "the agent container ended before an exec landed (last exec error: ${exec_err:-none}) — the credential face is judged on the rendered Job, the healed tree on the node"
	fi

	# The healed tree, read from the node's shared home partition: this is what
	# prepare rewrote, and the read does not depend on the container's
	# lifetime. The credential file itself is a per-pod subPath mount, so it
	# has no node-side position to read.
	_script=$(sed -e "s|@HOST_ROOT@|$host_root|g" -e "s|@HOME@|$home_root|g" <<'NODE'
set -eu
[ -d '@HOST_ROOT@' ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at @HOST_ROOT@' >&2; exit 2; }
H='@HOST_ROOT@@HOME@'
[ -d "$H" ] || H='@HOME@'
[ -d "$H" ] || { echo 'QA_BLOCKED: the prepared home partition @HOME@ does not exist on this node' >&2; exit 2; }
base="$H/.multica"
[ -d "$base/profiles/foreman-job" ] && echo profile=dir || echo profile=notdir
[ -e "$base/daemon_task_context.json" ] && echo marker=present || echo marker=absent
NODE
	)
	rc=0
	tree=$(node_sh "$node" "$_script") || rc=$?
	[ "$rc" -eq 0 ] || blocked "cannot read the prepared tree on $node: $tree"
	case $tree in
	*QA_BLOCKED*) blocked "$(node_blocked_reason "$tree")" ;;
	esac
	observe "node tree: $(printf '%s' "$tree" | tr '\n' ' ')"
	printf '%s\n' "$tree" | grep -qx 'profile=dir' ||
		fail "the profile position on $node is still not a directory: $tree"
	printf '%s\n' "$tree" | grep -qx 'marker=absent' ||
		fail "the marker path still exists on $node after prepare: $tree"

	restore_all || blocked "the tree restore failed — the state file is kept for '$0 restore'"
	if [ "$live" -eq 1 ]; then
		echo "PASS: TC-tech-auth-05 (task $TASK on $node booted with both abnormal shapes; prepare healed the tree and the token mount is 400 1000:1000)"
	else
		echo "PASS: TC-tech-auth-05 (task $TASK on $node booted with both abnormal shapes; prepare healed the tree and the rendered Job carries the read-only 0400 credential mount)"
	fi
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
