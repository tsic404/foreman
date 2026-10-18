#!/bin/sh
# TC-tech-cache-05 — the placement half of AC-25: the continuation's Job carries
# exactly one affinity item naming the node its previous chat run learned, and
# its pod landed there. TASK1 must be terminal (learning fired); TASK2 must
# still be running (its Job and pod are the judged objects). The item's shape
# is also tech_cache_05_session_affinity_test.go.
#
#   TASK1=<id> TASK2=<id> ./tech-cache-05-cluster.sh
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
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

[ -n "$TASK1" ] || blocked "TASK1 is required (the chat session's first task)"
[ -n "$TASK2" ] || blocked "TASK2 is required (the continuation task, still running)"

entry() {
	curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward the foreman Service first)"
}
first=$(entry "$TASK1")
second=$(entry "$TASK2")
observe "task1=$first"
observe "task2=$second"

printf '%s' "$first" | jq -e '.state == "terminal"' >/dev/null ||
	blocked "TASK1 has not reached its terminal state — the learning face has not fired"
printf '%s' "$first" | jq -e '.issue_id == "" and (.chat_session_id | length > 0)' >/dev/null ||
	blocked "TASK1 is not a chat-entry task (no chat_session_id or an issue context)"
chat=$(printf '%s' "$first" | jq -r .chat_session_id)
printf '%s' "$second" | jq -e --arg c "$chat" '.chat_session_id == $c and .issue_id == ""' >/dev/null ||
	blocked "TASK2 is not the same chat session's continuation"
printf '%s' "$second" | jq -e '.state != "terminal"' >/dev/null ||
	blocked "TASK2 already settled: its Job (the affinity item) is gone — run this while it is live"

node1=$(printf '%s' "$first" | jq -r '.node_name // empty')
[ -n "$node1" ] || blocked "TASK1 has no node_name — the learned node is unknown"
[ -n "$NODE" ] || NODE=$node1
[ "$NODE" = "$node1" ] || blocked "NODE=$NODE, but TASK1 ran on $node1"
observe "chat=$chat node=$node1"

job2=$(kubectl -n "$JOB_NS" get jobs -l "foreman.tsic.top/task-id=$TASK2" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) ||
	blocked "cannot read TASK2's Job in $JOB_NS"
[ -n "$job2" ] || blocked "no Job carries task-id=$TASK2"
job_json=$(kubectl -n "$JOB_NS" get job "$job2" -o json) ||
	blocked "cannot read Job $job2"

# The item: exactly one, weight 100, hostname In [the learned node].
printf '%s' "$job_json" | jq -e --arg n "$node1" '
	.spec.template.spec.affinity.nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution as $p
	| ($p | length == 1)
	  and ($p[0].weight == 100)
	  and ($p[0].preference.matchExpressions | length == 1)
	  and ($p[0].preference.matchExpressions[0]
	       | .key == "kubernetes.io/hostname" and .operator == "In" and .values == [$n])' >/dev/null ||
	fail "the continuation Job does not carry exactly one item naming $node1: $(printf '%s' "$job_json" | jq -c '.spec.template.spec.affinity')"

# The landing: the pod's spec.nodeName equals the learned node. A pod that is
# not scheduled yet cannot answer it.
pod_node=$(kubectl -n "$JOB_NS" get pods -l "job-name=$job2" -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null) ||
	blocked "cannot list TASK2's pods"
[ -n "$pod_node" ] || blocked "TASK2's pod has no nodeName yet (not scheduled) — the landing face is not readable"
observe "job=$job2 pod_node=$pod_node learned_node=$node1"
[ "$pod_node" = "$node1" ] ||
	fail "the pod landed on $pod_node, not on the learned node $node1"

echo "PASS: TC-tech-cache-05 (continuation landed on $node1 with the single affinity item)"
