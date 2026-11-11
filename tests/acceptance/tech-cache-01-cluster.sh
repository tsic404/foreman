#!/bin/sh
# TC-tech-cache-01 — the node-side half of AC-12: the bare cache object existed
# before the second task's pod started (both read in the node's clock domain).
#
# The creation instant used to be the filesystem's birth time (`stat %W`), which
# the test nodes' filesystem does not provide: every run blocked on an
# undecidable criterion. The fixture now establishes the instant itself, with a
# mark written *inside* the cache object after the first task settled and before
# the second one starts:
#
#   TASK1=<id> ./tech-cache-01-cluster.sh mark   # after TASK1 settled, before TASK2
#   TASK2=<id> ./tech-cache-01-cluster.sh        # while TASK2 is still running
#
# The mark's timestamp comes from the node's own clock (the debug container runs
# on the node), the same clock the apiserver records the pod's startTime from.
# It is an upper bound on the object's creation — the mark exists inside the
# object, so the object existed when it was written — and a second task that
# rebuilds the cache removes it. Without a mark the criterion is undecidable and
# the run blocks; the clone banner is still not a fallback (it reports the target
# directory, which a full clone and a cache clone share).
#
# The first task settled before this runs, and a settled task's Job (with its
# pods and Secret) is deleted, so its only surviving placement fact is the
# task projection's node_name. The second task is still running: its pod is
# reached by the task-id label the render writes on the pod template, never
# through the Job's job-name label. The affinity item and the reverse assertion
# are tech_cache_01_reuse_affinity_test.go.
#
#   TASK1=<id> TASK2=<id> [NODE=n] [REPO_GIT=p] ./tech-cache-01-cluster.sh
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8081}
STATE_ROOT=${FOREMAN_STATE_ROOT:-/var/lib/foreman}
# GNU coreutils inside the node-debug container: stat/date/du/find.
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
TASK1=${TASK1:-}
TASK2=${TASK2:-}
NODE=${NODE:-}
REPO_GIT=${REPO_GIT:-}
ACTION=${1:-check}
MARK_NAME=.foreman-cache-mark

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }

# The ops surface is a separate, credential-gated listener (contract S-25):
# these scripts read it, so every ops request carries FOREMAN_OPS_TOKEN.
FOREMAN_OPS_TOKEN=${FOREMAN_OPS_TOKEN:-}
ops_curl() {
	[ -n "$FOREMAN_OPS_TOKEN" ] || blocked "FOREMAN_OPS_TOKEN is required for the ops surface (kubectl -n foreman get secret foreman-secret -o jsonpath='{.data.FOREMAN_OPS_TOKEN}' | base64 -d)"
	curl -fsS -m 30 -H "Authorization: Bearer $FOREMAN_OPS_TOKEN" "$@"
}
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

[ -n "$TASK1" ] || blocked "TASK1 is required (the first task of the same-agent pair)"

case $ACTION in
mark | check) ;;
*) blocked "usage: $0 [mark|check] (TASK1/TASK2 as documented in the header)" ;;
esac

# node_of <task-id>: the node the projection recorded for the task.
node_of() {
	ops_curl "$FOREMAN_URL/foreman/tasks/$1" | jq -r '.node_name // empty' ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward svc/foreman-ops 8081:8081 first)"
}

# mark_window: write the carrier's mark into every bare repository on the
# first task's node. The time is the node's clock, read inside the debug
# container that also writes the file, so the two facts share one domain. The
# check reads the mark from the repository it selected; a second task that
# rebuilds that repository removes the file, which is exactly what makes the
# mark evidence of the object surviving rather than of a fresh clone.
if [ "$ACTION" = mark ]; then
	[ -n "$NODE" ] || NODE=$(node_of "$TASK1")
	[ -n "$NODE" ] || blocked "TASK1 has no node_name — the node its cache sits on was never observed"
	observe "task1=$TASK1 node=$NODE action=mark"
	rc=0
	out=$(kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
	  [ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
	  R='/host$STATE_ROOT'
	  [ -d \"\$R\" ] || R='$STATE_ROOT'
	  repos=\$(find \"\$R/workspaces/.repos\" -mindepth 2 -maxdepth 2 -name '*.git' -type d 2>/dev/null)
	  [ -n \"\$repos\" ] || { echo 'QA_BLOCKED: no bare repo under '\$R'/workspaces/.repos — run the first task first' >&2; exit 2; }
	  for repo in \$repos; do
	    now=\$(date +%s)
	    printf '%s %s\n' \"\$now\" '$TASK1' >\"\$repo/$MARK_NAME\"
	    echo \"marked repo=\$repo at=\$now\"
	  done
	" 2>&1) || rc=$?
	if [ "$rc" -ne 0 ]; then
		why=$(printf '%s\n' "$out" | sed -n 's/^QA_BLOCKED: //p' | head -1)
		[ -z "$why" ] || blocked "$why"
		blocked "cannot mark the node's cache (kubectl debug node/$NODE failed, rc=$rc): ${out:-<empty>}"
	fi
	printf '%s\n' "$out"
	echo "PASS: the cache mark is on $NODE; trigger TASK2 now, then run $0 check"
	exit 0
fi

[ -n "$TASK2" ] || blocked "TASK2 is required (the continuation task)"

# The second pod's start is the comparison instant, and it is only readable
# while the pod exists: settlement deletes the Job with Background
# propagation, whose GC then removes the pods.
pods2=$(kubectl -n "$JOB_NS" get pods -l "foreman.tsic.top/task-id=$TASK2" -o json 2>/dev/null) ||
	blocked "cannot list TASK2's pods in $JOB_NS"
pod2_filter='[.items[] | select(.status.startTime != null)] | sort_by(.status.startTime) | first // empty'
pod2=$(printf '%s' "$pods2" | jq -r "$pod2_filter | .metadata.name // empty") ||
	blocked "cannot parse TASK2's pod list"
pod2_start=$(printf '%s' "$pods2" | jq -r "$pod2_filter | .status.startTime // empty") ||
	blocked "cannot parse TASK2's pod list"
pod2_node=$(printf '%s' "$pods2" | jq -r "$pod2_filter | .spec.nodeName // empty") ||
	blocked "cannot parse TASK2's pod list"
[ -n "$pod2_start" ] ||
	blocked "TASK2 has no pod carrying a startTime (already deleted, or never scheduled) — run this while the second task is still running"

if [ -z "$NODE" ]; then
	NODE=$(node_of "$TASK1")
fi
[ -n "$NODE" ] || blocked "TASK1 has no node_name — the node its cache sits on was never observed"
[ "$pod2_node" = "$NODE" ] ||
	blocked "TASK2's pod ran on '$pod2_node', not on TASK1's node '$NODE' — the same-node precondition does not hold"
observe "task1=$TASK1 node=$NODE task2=$TASK2 pod2=$pod2 pod2_start=$pod2_start"

# The node-side cache face: the carrier's mark (written before TASK2 started)
# plus the repository's mtime and size as observations. The debug container
# needs an explicit image and stdin (--attach defaults on with -i); without
# both the call never reaches the node and its stdout stays empty.
rc=0
node_report=$(kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
  [ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
  R='/host$STATE_ROOT'
  [ -d \"\$R\" ] || R='$STATE_ROOT'
  repos=\$(find \"\$R/workspaces/.repos\" -mindepth 2 -maxdepth 2 -name '*.git' -type d 2>/dev/null)
  [ -n \"\$repos\" ] || { echo 'QA_BLOCKED: no bare repo under '\$R'/workspaces/.repos' >&2; exit 2; }
  for repo in \$repos; do
    mark=
    [ -r \"\$repo/$MARK_NAME\" ] && mark=\$(cat \"\$repo/$MARK_NAME\" | tr -d '\n')
    modified=\$(stat -c '%Y' \"\$repo\" 2>/dev/null || echo 0)
    echo \"repo=\$repo mtime=\$modified size=\$(du -sh \"\$repo\" 2>/dev/null | cut -f1) mark=\$mark\"
  done
" 2>&1) || rc=$?
if [ "$rc" -ne 0 ]; then
	# The container-side diagnostic is the real cause (no bare cache, /host not
	# mounted); a bare kubectl failure is reported with its own output.
	why=$(printf '%s\n' "$node_report" | sed -n 's/^QA_BLOCKED: //p' | head -1)
	if [ -n "$why" ]; then
		blocked "$why"
	fi
	blocked "cannot inspect the node's cache (kubectl debug node/$NODE failed, rc=$rc): ${node_report:-<empty>}"
fi
[ -n "$node_report" ] || blocked "the node-side inspection returned no output (image $NODE_DEBUG_IMAGE not attached?)"
printf '%s\n' "$node_report" | while IFS= read -r line; do observe "$line"; done
case $node_report in
*QA_BLOCKED*) blocked "the node-side inspection reported a block: $node_report" ;;
esac

if [ -n "$REPO_GIT" ]; then
	row=$(printf '%s\n' "$node_report" | grep -F "repo=" | grep -F "/$REPO_GIT " || true)
	[ -n "$row" ] || row=$(printf '%s\n' "$node_report" | grep -F "/$REPO_GIT" || true)
else
	row=$(printf '%s\n' "$node_report" | awk '
		match($0, /mtime=[0-9]+/) {
			m = substr($0, RSTART + 6, RLENGTH - 6) + 0
			if (m > best) { best = m; line = $0 }
		}
		END { print line }')
fi
[ -n "$row" ] || blocked "cannot locate the bare repo (REPO_GIT=$REPO_GIT) in the node report"

mark=$(printf '%s\n' "$row" | sed -n 's/.* mark=\([0-9][0-9]* [^ ]*\)$/\1/p')
[ -n "$mark" ] ||
	blocked "the cache object carries no carrier mark — run 'TASK1=$TASK1 $0 mark' after the first task settled and before triggering the second; the creation instant is otherwise undecidable (the clone banner is not a fallback)"
mark_at=$(printf '%s' "$mark" | cut -d' ' -f1)
case $mark_at in '' | *[!0-9]*) blocked "the carrier mark is not an epoch second: '$mark'" ;; esac
mark_task=$(printf '%s' "$mark" | cut -d' ' -f2)
[ "$mark_task" = "$TASK1" ] ||
	blocked "the carrier mark inside the cache names '$mark_task', not TASK1=$TASK1 — the mark predates this window"

start_epoch=$(printf '%s' "$pod2_start" | jq -Rr 'sub("\\.[0-9]+"; "") | fromdateiso8601') ||
	blocked "cannot parse the pod's startTime '$pod2_start'"
observe "cache_mark=$mark_at pod2_start_epoch=$start_epoch cache_row=$row"
[ "$mark_at" -lt "$start_epoch" ] ||
	blocked "the cache mark was written at $mark_at, not before the second pod started ($start_epoch) — the reuse window was not constructed (the mark must be written before TASK2 is triggered), so the criterion is undecidable here"

echo "PASS: TC-tech-cache-01 (task $TASK2 reuses the cache object established before its pod)"
