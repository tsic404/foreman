#!/bin/sh
# TC-uat-cache-01 — G5: a repeated task on the same node does not re-clone the
# repository. The design's authoritative face for "reuse, not a full clone" is
# the node cache object's creation instant versus the second pod's start
# instant (AC-12); the technical carrier owns that judgement and this case adds
# the user-visible half — the first task completed and the repeated task ran on
# the same node without a fresh clone.
#
#   TASK1=<id> TASK2=<id> ./uat-cache-01-cluster.sh
#
# Both tasks' Jobs must still exist and TASK2's pod must still be present: run
# this before the drain window deletes them. The projections are judged after
# the technical carrier, so TASK1 may settle during the run and TASK2 may still
# be running — neither is required to be terminal beforehand.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

TASK1=${TASK1:-}
TASK2=${TASK2:-}

need kubectl
need curl
need jq

[ -n "$TASK1" ] || blocked "TASK1 is required (the first task of the same-agent pair)"
[ -n "$TASK2" ] || blocked "TASK2 is required (the continuation task)"

one=$(entry "$TASK1")
two=$(entry "$TASK2")
observe "task1=$one"
observe "task2=$two"
node1=$(printf '%s' "$one" | jq -r '.node_name // empty')
node2=$(printf '%s' "$two" | jq -r '.node_name // empty')
[ -n "$node1" ] && [ "$node1" = "$node2" ] ||
	blocked "the two tasks did not share a node ($node1 vs $node2) — the reuse precondition does not hold"
observe "shared node=$node1"

# The authoritative reuse face first: the cache object existed before the
# second pod started, and both Jobs were still live to be read. Its
# blocked/failed verdict is this case's verdict too.
rc=0
out=$(TASK1="$TASK1" TASK2="$TASK2" NODE="$node1" FOREMAN_URL="$FOREMAN_URL" \
	FOREMAN_JOB_NAMESPACE="$JOB_NS" FOREMAN_STATE_ROOT="${FOREMAN_STATE_ROOT:-/var/lib/foreman}" \
	"$UAT_DIR/tech-cache-01-cluster.sh") || rc=$?
printf '%s\n' "$out"
[ "$rc" -eq 0 ] || exit "$rc"

# The user-facing half, read after the technical carrier: the first task
# completed, and the repeated task is not a failure (it may still be running).
one=$(entry "$TASK1")
two=$(entry "$TASK2")
printf '%s' "$one" | jq -e '.state == "terminal" and .result == "completed"' >/dev/null ||
	fail "TASK1 did not complete: $one"
printf '%s' "$two" | jq -e '(.state == "terminal" and .result == "completed") or .state == "running"' >/dev/null ||
	fail "TASK2 is neither running nor completed: $two"
observe "task1 result=$(printf '%s' "$one" | jq -r .result) task2 state=$(printf '%s' "$two" | jq -r .state)"

echo "PASS: TC-uat-cache-01 (repeated task on $node1 reused the cache; TASK1 completed, TASK2 did not fail, no full clone)"
