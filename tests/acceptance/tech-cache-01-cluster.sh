#!/bin/sh
# TC-tech-cache-01 — the node-side half of AC-12: the bare cache object existed
# before the second task's pod started (both read in the node's clock domain).
# The first task settled before this runs, and a settled task's Job (with its
# pods and Secret) is deleted, so its only surviving placement fact is the
# task projection's node_name. The second task is still running: its pod is
# reached by the task-id label the render writes on the pod template, never
# through the Job's job-name label. The clone banner is not a fallback: when
# the creation instant is unreadable the criterion is undecidable and the run
# is blocked. The affinity item and the reverse assertion are
# tech_cache_01_reuse_affinity_test.go.
#
#   TASK1=<id> TASK2=<id> [NODE=n] [REPO_GIT=p] ./tech-cache-01-cluster.sh
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
STATE_ROOT=${FOREMAN_STATE_ROOT:-/var/lib/foreman}
# GNU coreutils inside the node-debug container: stat %W (birth time), du, find.
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
TASK1=${TASK1:-}
TASK2=${TASK2:-}
NODE=${NODE:-}
REPO_GIT=${REPO_GIT:-}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

[ -n "$TASK1" ] || blocked "TASK1 is required (the first task of the same-agent pair)"
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
	NODE=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$TASK1" | jq -r '.node_name // empty') ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$TASK1 (port-forward the foreman Service first)"
fi
[ -n "$NODE" ] || blocked "TASK1 has no node_name — the node its cache sits on was never observed"
[ "$pod2_node" = "$NODE" ] ||
	blocked "TASK2's pod ran on '$pod2_node', not on TASK1's node '$NODE' — the same-node precondition does not hold"
observe "task1=$TASK1 node=$NODE task2=$TASK2 pod2=$pod2 pod2_start=$pod2_start"

# The node-side cache face: the bare repo's creation instant (statx birth time)
# and, when REPO_GIT names none, the most recently written repository. The
# debug container needs an explicit image and stdin (--attach defaults on with
# -i); without both the call never reaches the node and its stdout stays empty.
rc=0
node_report=$(kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
  [ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
  R='/host$STATE_ROOT'
  [ -d \"\$R\" ] || R='$STATE_ROOT'
  repos=\$(find \"\$R/workspaces/.repos\" -mindepth 2 -maxdepth 2 -name '*.git' -type d 2>/dev/null)
  [ -n \"\$repos\" ] || { echo 'QA_BLOCKED: no bare repo under '\$R'/workspaces/.repos' >&2; exit 2; }
  for repo in \$repos; do
    birth=\$(stat -c '%W' \"\$repo\" 2>/dev/null || echo 0)
    modified=\$(stat -c '%Y' \"\$repo\" 2>/dev/null || echo 0)
    echo \"repo=\$repo birth=\$birth mtime=\$modified size=\$(du -sh \"\$repo\" 2>/dev/null | cut -f1)\"
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

birth=$(printf '%s\n' "$row" | sed -n 's/.* birth=\([0-9-]*\).*/\1/p')
[ -n "$birth" ] || blocked "the cache object's creation instant is unreadable"
[ "$birth" -gt 0 ] ||
	blocked "the cache object's birth time is unavailable on $NODE — AC-12 makes the criterion undecidable here (the clone banner is not a fallback)"

start_epoch=$(printf '%s' "$pod2_start" | jq -Rr 'sub("\\.[0-9]+"; "") | fromdateiso8601') ||
	blocked "cannot parse the pod's startTime '$pod2_start'"
observe "cache_birth=$birth pod2_start_epoch=$start_epoch repo=$row"
[ "$birth" -lt "$start_epoch" ] ||
	fail "the cache object was created at $birth, not before the second pod started ($start_epoch) — not a reuse"

echo "PASS: TC-tech-cache-01 (task $TASK2 reuses the cache object established before its pod)"
