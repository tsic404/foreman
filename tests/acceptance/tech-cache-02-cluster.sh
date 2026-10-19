#!/bin/sh
# TC-tech-cache-02 — concurrent checkouts on one node do not corrupt the shared
# bare cache (AC-12 / ADR-006). Both tasks must have run concurrently on the
# same node (verified from the projection); a window that does not hold is a
# block. The judgement is `git fsck` (dangling objects allowed) plus both tasks
# completing; busy/lock counts are recorded, not gated.
#
#   TASK1=<id> TASK2=<id> [NODE=n] ./tech-cache-02-cluster.sh
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_NS=${FOREMAN_NAMESPACE:-foreman}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
STATE_ROOT=${FOREMAN_STATE_ROOT:-/var/lib/foreman}
# The node-debug container must carry git (fsck) plus find: the default image is
# Alpine + git. Override when the cluster's pull policy does not allow it.
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-alpine/git:latest}
LOG_SINCE=${LOG_SINCE:-2h}
TASK1=${TASK1:-}
TASK2=${TASK2:-}
NODE=${NODE:-}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

[ -n "$TASK1" ] || blocked "TASK1 is required"
[ -n "$TASK2" ] || blocked "TASK2 is required"

entry() {
	curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward the foreman Service first)"
}
one=$(entry "$TASK1")
two=$(entry "$TASK2")
observe "task1=$one"
observe "task2=$two"

for pair in "TASK1:$one" "TASK2:$two"; do
	name=${pair%%:*}
	body=${pair#*:}
	printf '%s' "$body" | jq -e '.state == "terminal"' >/dev/null ||
		blocked "$name is not terminal — the concurrency window has not closed"
	printf '%s' "$body" | jq -e '.result == "completed"' >/dev/null ||
		fail "$name did not complete (result=$(printf '%s' "$body" | jq -r .result))"
done

node1=$(printf '%s' "$one" | jq -r '.node_name // empty')
node2=$(printf '%s' "$two" | jq -r '.node_name // empty')
[ -n "$node1" ] && [ "$node1" = "$node2" ] ||
	blocked "the two tasks did not share a node ($node1 vs $node2) — the case's precondition does not hold"
[ -n "$NODE" ] || NODE=$node1
[ "$NODE" = "$node1" ] || blocked "NODE=$NODE, but both tasks ran on $node1"
observe "node=$NODE"

# Concurrency: the later start is before the earlier terminal instant. Projected
# timestamps (registry.ProjectedTime) and slog's .ts are RFC3339Nano, so the
# comparison runs on whole seconds derived in jq — never on the raw strings.
start1=$(printf '%s' "$one" | jq -r '.started_at // empty')
start2=$(printf '%s' "$two" | jq -r '.started_at // empty')
[ -n "$start1" ] && [ -n "$start2" ] || blocked "a task has no started_at — the overlap cannot be established"
logs=$(kubectl -n "$FOREMAN_NS" logs deploy/foreman --since="$LOG_SINCE" 2>/dev/null) ||
	blocked "cannot read the Foreman log"
terminal_of() {
	printf '%s\n' "$logs" | jq -Rr --arg t "$1" 'fromjson? | select(.msg=="task.terminal" and .task_id==$t) | .ts' | head -1
}
end1=$(terminal_of "$TASK1")
end2=$(terminal_of "$TASK2")
[ -n "$end1" ] && [ -n "$end2" ] || blocked "a task.terminal line is missing from the $LOG_SINCE window"
epoch() {
	printf '%s' "$1" | jq -Rr 'sub("\\.[0-9]+"; "") | sub("\\+00:00$"; "Z") | fromdateiso8601' ||
		return 1
}
s1=$(epoch "$start1") || blocked "cannot parse started_at '$start1'"
s2=$(epoch "$start2") || blocked "cannot parse started_at '$start2'"
e1=$(epoch "$end1") || blocked "cannot parse task.terminal ts '$end1'"
e2=$(epoch "$end2") || blocked "cannot parse task.terminal ts '$end2'"
later_start=$s1
if [ "$s2" -gt "$later_start" ]; then later_start=$s2; fi
earlier_end=$e1
if [ "$e2" -lt "$earlier_end" ]; then earlier_end=$e2; fi
observe "starts[$start1,$start2] terminals[$end1,$end2]"
if [ "$later_start" -ge "$earlier_end" ]; then
	blocked "the two tasks did not overlap — the concurrency window was never constructed"
fi

# The bare caches: git's own locks are what the design trusts, so a corrupted
# object database is the failure this case exists to catch. The debug container
# needs an explicit image and stdin (-i attaches); an empty answer blocks rather
# than counting as a clean fsck.
rc=0
fsck=$(kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
  [ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
  command -v git >/dev/null 2>&1 || { echo 'QA_BLOCKED: git is not available in the debug image' >&2; exit 2; }
  R='/host$STATE_ROOT'
  [ -d \"\$R\" ] || R='$STATE_ROOT'
  repos=\$(find \"\$R/workspaces/.repos\" -mindepth 2 -maxdepth 2 -name '*.git' -type d 2>/dev/null)
  [ -n \"\$repos\" ] || { echo 'QA_BLOCKED: no bare repo on the node' >&2; exit 2; }
  rc=0
  for repo in \$repos; do
    echo \"repo=\$repo\"
    out=\$(git -C \"\$repo\" fsck --no-progress 2>&1) || rc=1
    printf '%s\n' \"\$out\" | sed 's/^/fsck: /'
    printf '%s\n' \"\$out\" | grep -qiE 'error|fatal|corrupt|bad object' && rc=1
  done
  echo \"fsck_rc=\$rc\"
  exit \$rc
") || rc=$?
[ -n "$fsck" ] || blocked "the node-side fsck returned no output (image $NODE_DEBUG_IMAGE not attached?)"
printf '%s\n' "$fsck" | while IFS= read -r line; do observe "$line"; done
case $fsck in
*QA_BLOCKED*) blocked "the bare-cache fsck face could not be constructed" ;;
esac
printf '%s\n' "$fsck" | grep -q '^repo=' || blocked "no repo= line in the fsck output — the node side was not reached"
printf '%s\n' "$fsck" | grep -q '^fsck_rc=' || blocked "no fsck_rc= line in the output — the fsck result is unknown, never a pass"
[ "$rc" -eq 0 ] || fail "git fsck reported a corrupt shared cache (exit $rc)"

# Retry evidence (recorded, not gated): busy/lock lines in each task's window.
for task in "$TASK1" "$TASK2"; do
	n=$(printf '%s\n' "$logs" | jq -Rr --arg t "$task" \
		'fromjson? | select((.task_id // "")==$t) | tostring' | grep -ciE 'busy|lock|retry' || true)
	observe "task=$task busy_or_lock_lines=$n (design target ≤1 per task)"
done

echo "PASS: TC-tech-cache-02 (tasks $TASK1 and $TASK2 shared $NODE without cache corruption)"
