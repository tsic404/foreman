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
# object, so the object existed when it was written. The harness keeps the
# witness beside it (TASK1, its epoch, the workspace and the one repository it
# marked), because the mark alone cannot tell "the object survived" from "the
# object was rebuilt": a rebuild removes the mark, and AC-12 makes exactly that
# the failure — a cache that appears during this task is a failure, not an
# undecidable case.
#
# So the judgement is: witness present and the object the witness names still
# carries *this* task's mark before the pod started ⇒ PASS; witness present and
# that object carries no mark of this task ⇒ FAIL (rebuilt inside the window);
# no witness ⇒ blocked (the window was never constructed). A mark timestamp at
# or after the pod's start stays blocked: an upper bound that lands after the
# start cannot place the creation instant, which is not the same as a rebuild.
# REPO_GIT never selects the judged object — the witness does; the operator's
# name only has to agree with it, so a stale mark on a sibling cannot answer
# for the object the task actually used.
#
# The mark is written only into the first task's workspace (`workspace_id` of
# its projection), never into every repository the node happens to hold, and
# within that workspace it names exactly one object: several repositories must
# be told apart by REPO_GIT, and a name matching none or several blocks rather
# than marking an arbitrary set.
#
# The first task settled before this runs, and a settled task's Job (with its
# pods and Secret) is deleted, so its only surviving placement fact is the
# task projection's node_name. The second task is still running: its pod is
# reached by the task-id label the render writes on the pod template, never
# through the Job's job-name label. The affinity item and the reverse assertion
# are tech_cache_01_reuse_affinity_test.go.
#
#   TASK1=<id> TASK2=<id> [NODE=n] [REPO_GIT=p] ./tech-cache-01-cluster.sh
# REPO_GIT names the bare repository directory TASK1 used (its trailing name,
# e.g. `github.com+owner+repo.git`) and is required when the workspace holds
# more than one; on `check` it must agree with the witness.
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
# The harness keeps its own copy of the mark (TASK1, epoch, workspace). Without
# it a missing in-object mark is undecidable; with it the same observation is
# the rebuild AC-12 makes a failure.
WITNESS=${FOREMAN_CACHE_WITNESS:-$HOME/.foreman-cache-01-mark}

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

# task_workspace <task-id>: the workspace the projection recorded for the task.
# AC-12 partitions the cache per workspace, so the mark may only touch this one.
task_workspace() {
	ops_curl "$FOREMAN_URL/foreman/tasks/$1" | jq -r '.workspace_id // empty' ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward svc/foreman-ops 8081:8081 first)"
}

# node_of <task-id>: the node the projection recorded for the task.
node_of() {
	ops_curl "$FOREMAN_URL/foreman/tasks/$1" | jq -r '.node_name // empty' ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward svc/foreman-ops 8081:8081 first)"
}

# mark_window: write the carrier's mark into the first task's workspace cache —
# never into every repository the node holds. The time is the node's clock, read
# inside the debug container that also writes the file, so both facts share one
# domain. The harness keeps a witness of the same write (TASK1, instant,
# workspace, repository) at $WITNESS: the object's mark alone cannot separate
# "survived" from "rebuilt", and AC-12 makes a rebuild inside the window a
# failure.
if [ "$ACTION" = mark ]; then
	[ -n "$NODE" ] || NODE=$(node_of "$TASK1")
	[ -n "$NODE" ] || blocked "TASK1 has no node_name — the node its cache sits on was never observed"
	workspace=$(task_workspace "$TASK1")
	[ -n "$workspace" ] ||
		blocked "TASK1's projection carries no workspace_id — the cache namespace to mark is unknown"
	observe "task1=$TASK1 node=$NODE workspace=$workspace action=mark"
	# The mark names exactly one object. REPO_GIT reaches the node-side script
	# as an argument, never spliced into its text: whatever it holds stays data,
	# so it needs no character set of its own. A name selecting none or several
	# repositories blocks — a mark on two objects, or on the wrong one, would
	# let the survivor answer for a rebuilt object.
	rc=0
	out=$(kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
	  want=\${1-}
	  [ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
	  R='/host$STATE_ROOT'
	  [ -d \"\$R\" ] || R='$STATE_ROOT'
	  W=\"\$R/workspaces/.repos/$workspace\"
	  [ -d \"\$W\" ] || { echo 'QA_BLOCKED: no cache directory for workspace $workspace — run the first task first' >&2; exit 2; }
	  repos=\$(find \"\$W\" -mindepth 1 -maxdepth 1 -name '*.git' -type d 2>/dev/null)
	  [ -n \"\$repos\" ] || { echo 'QA_BLOCKED: no bare repo under the first task workspace — run the first task first' >&2; exit 2; }
	  chosen=
	  count=0
	  for repo in \$repos; do
	    if [ -n \"\$want\" ]; then
	      case \${repo##*/} in
	      *\"\$want\") ;;
	      *) continue ;;
	      esac
	    fi
	    chosen=\$repo
	    count=\$((count + 1))
	  done
	  if [ -z \"\$want\" ] && [ \"\$count\" -gt 1 ]; then
	    echo \"QA_BLOCKED: the first task workspace holds \$count cache repositories — pass REPO_GIT=<name> to choose the one TASK1 used\" >&2
	    exit 2
	  fi
	  if [ \"\$count\" -eq 0 ]; then
	    echo \"QA_BLOCKED: no cache repository matching REPO_GIT=\$want under the first task workspace\" >&2
	    exit 2
	  fi
	  if [ \"\$count\" -gt 1 ]; then
	    echo \"QA_BLOCKED: REPO_GIT=\$want matches \$count cache repositories in the first task workspace — the mark names exactly one, so pass the bare repo's full directory name\" >&2
	    exit 2
	  fi
	  now=\$(date +%s)
	  printf '%s %s\n' \"\$now\" '$TASK1' >\"\$chosen/$MARK_NAME\"
	  echo \"marked repo=\$chosen at=\$now\"
	" _ "$REPO_GIT" 2>&1) || rc=$?
	if [ "$rc" -ne 0 ]; then
		why=$(printf '%s\n' "$out" | sed -n 's/^QA_BLOCKED: //p' | head -1)
		[ -z "$why" ] || blocked "$why"
		blocked "cannot mark the node's cache (kubectl debug node/$NODE failed, rc=$rc): ${out:-<empty>}"
	fi
	printf '%s\n' "$out"
	line=$(printf '%s\n' "$out" | grep -m1 '^marked repo=' || true)
	[ -n "$line" ] ||
		blocked "the mark ran but named no repository — the node's cache tree could not be read"
	marked_at=${line##* at=}
	marked_repo=${line#marked repo=}
	marked_repo=${marked_repo% at=*}
	[ -n "$marked_repo" ] || blocked "the mark line names no repository: $line"
	mkdir -p "$(dirname "$WITNESS")" ||
		blocked "cannot create the witness directory for $WITNESS"
	printf '%s %s %s %s\n' "$TASK1" "$marked_at" "$workspace" "$marked_repo" >"$WITNESS" ||
		blocked "cannot write the harness witness $WITNESS"
	echo "PASS: the cache mark is on $NODE (witness $WITNESS); trigger TASK2 now, then run $0 check"
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

# The harness witness: its presence is what separates a rebuilt cache (the
# failure AC-12 names) from a window that was never constructed (a block).
[ -r "$WITNESS" ] ||
	blocked "no harness witness at $WITNESS — run 'TASK1=$TASK1 $0 mark' after the first task settled and before triggering the second; without it a missing in-object mark cannot be told from a rebuild"
witness_task=
witness_at=
witness_ws=
witness_repo=
read -r witness_task witness_at witness_ws witness_repo <"$WITNESS" || true
[ -n "$witness_task" ] && [ -n "$witness_at" ] && [ -n "$witness_ws" ] && [ -n "$witness_repo" ] ||
	blocked "the harness witness $WITNESS is malformed (want 'TASK1 epoch workspace repo')"
[ "$witness_task" = "$TASK1" ] ||
	blocked "the harness witness names '$witness_task', not TASK1=$TASK1 — it belongs to another round"
observe "witness task=$witness_task at=$witness_at workspace=$witness_ws repo=$witness_repo"

rows=$(printf '%s\n' "$node_report" | grep -F "/.repos/$witness_ws/" || true)
[ -n "$rows" ] ||
	blocked "the node report carries no cache object under workspace $witness_ws — run the first task first"
# The judged object is the one the witness names, and only that one. Selecting
# by REPO_GIT instead would read a stale mark (mark B, then mark A, leaves B's
# mark behind: judging B on request passes while A, the object the task used,
# was rebuilt) and would judge an object that was never marked when the two
# disagree. REPO_GIT is a consistency check on the witness, not a selector.
row=$(printf '%s\n' "$rows" | grep -F "repo=$witness_repo " || true)
[ -n "$row" ] ||
	blocked "the cache object the harness marked ($witness_repo) is no longer on $NODE — the first task's cache was removed, so neither reuse nor a rebuild is decidable here"
if [ -n "$REPO_GIT" ]; then
	case ${witness_repo##*/} in
	*"$REPO_GIT") ;;
	*)
		fail "REPO_GIT=$REPO_GIT does not name the object the harness marked (${witness_repo##*/}) — the judgement object is the witness's, so the two must agree before any verdict is read from it"
		;;
	esac
fi

start_epoch=$(printf '%s' "$pod2_start" | jq -Rr 'sub("\\.[0-9]+"; "") | fromdateiso8601') ||
	blocked "cannot parse the pod's startTime '$pod2_start'"

mark=$(printf '%s\n' "$row" | sed -n 's/.* mark=\([0-9][0-9]* [^ ]*\)$/\1/p')
observe "cache_mark=${mark:-<none>} pod2_start_epoch=$start_epoch cache_row=$row"
[ -n "$mark" ] ||
	fail "the cache object carries no mark although the harness wrote one at $witness_at — the object was rebuilt during this task; AC-12 makes a cache that appears inside the window a failure (the clone banner is not a fallback)"
mark_at=$(printf '%s' "$mark" | cut -d' ' -f1)
case $mark_at in '' | *[!0-9]*) blocked "the carrier mark is not an epoch second: '$mark'" ;; esac
mark_task=$(printf '%s' "$mark" | cut -d' ' -f2)
[ "$mark_task" = "$TASK1" ] ||
	fail "the cache object carries the mark of '$mark_task', not TASK1=$TASK1 — the object was rebuilt during this task (AC-12: appearing inside the window is a failure)"
[ "$mark_at" -lt "$start_epoch" ] ||
	blocked "the cache mark was written at $mark_at, not before the second pod started ($start_epoch) — the mark is an upper bound on the creation instant, and an upper bound after the start places nothing (this is not a rebuild)"

echo "PASS: TC-tech-cache-01 (task $TASK2 reuses the cache object established before its pod)"
