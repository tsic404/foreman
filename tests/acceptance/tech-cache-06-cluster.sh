#!/bin/sh
# TC-tech-cache-06 — AC-26's single-key soft-affinity shapes on live Jobs: the
# issue key does not participate, a saturated node suppresses the item, the
# switch being off suppresses it, and an expired session-index entry suppresses
# it. Each shape is judged on the task Job while it is live (a settled task's
# Job is already deleted); the trigger and the environment change stay the
# operator's steps, as in tech-cache-03-cluster.sh.
#
#   ./tech-cache-06-cluster.sh issue-key PRIOR TASK_OTHER TASK_CONT
#       PRIOR settled on a node; TASK_OTHER is another agent's task on the same
#       issue (no block); TASK_CONT is PRIOR's agent continuation (one item).
#   ./tech-cache-06-cluster.sh saturated PRIOR TASK_CONT
#       a node is at FOREMAN_MAX_JOBS_PER_NODE; TASK_CONT must carry no item.
#   ./tech-cache-06-cluster.sh switch-off PRIOR TASK_CONT SINCE
#       FOREMAN_PREFER_NODE_REUSE=false; TASK_CONT must carry no item.
#   ./tech-cache-06-cluster.sh index-expired PRIOR TASK_CONT
#       PRIOR's learning is older than FOREMAN_AGENT_STATE_TTL and a prune
#       round dropped it; no item.
#
# PRIOR_SESSION is required by the three continuation shapes: the session key
# PRIOR learned (its terminal report body's session_id; the projection does not
# expose it). switch-off also needs SINCE, the rollout completion instant.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_NS=${FOREMAN_NAMESPACE:-foreman}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
LOG_SINCE=${LOG_SINCE:-6h}
ACTION=${1:-}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

entry() {
	curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward the foreman Service first)"
}

task_list() {
	curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks (port-forward the foreman Service first)"
}

live_job() {
	_name=$(kubectl -n "$JOB_NS" get jobs -l "foreman.tsic.top/task-id=$1" \
		-o jsonpath='{.items[0].metadata.name}' 2>/dev/null) ||
		blocked "cannot read task $1's Job in $JOB_NS"
	[ -n "$_name" ] ||
		blocked "task $1 has no live Job — run this before the task settles (its Job is the judged object)"
	kubectl -n "$JOB_NS" get job "$_name" -o json ||
		blocked "cannot read Job $_name"
}

no_affinity() { # <job-json> <label>
	printf '%s' "$1" | jq -e '(.spec.template.spec.affinity // null) == null' >/dev/null ||
		fail "$2 carries an affinity block, want none: $(printf '%s' "$1" | jq -c '.spec.template.spec.affinity')"
}

one_item() { # <job-json> <node> <label>
	printf '%s' "$1" | jq -e --arg n "$2" '
		.spec.template.spec.affinity.nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution as $p
		| ($p | length == 1)
		  and ($p[0].weight == 100)
		  and ($p[0].preference.matchExpressions | length == 1)
		  and ($p[0].preference.matchExpressions[0]
		       | .key == "kubernetes.io/hostname" and .operator == "In" and .values == [$n])' >/dev/null ||
		fail "$3 does not carry exactly one item naming $2: $(printf '%s' "$1" | jq -c '.spec.template.spec.affinity')"
}

deploy_env() {
	kubectl -n "$FOREMAN_NS" get deploy foreman -o json 2>/dev/null |
		jq -r --arg k "$1" '[.spec.template.spec.containers[]?.env[]? | select(.name == $k) | .value][0] // empty'
}

settled_on_node() { # <task-id> <label> -> prints the learned node
	_body=$(entry "$1")
	printf '%s' "$_body" | jq -e '.state == "terminal"' >/dev/null ||
		blocked "$2 ($1) is not terminal — the learning face has not fired"
	_node=$(printf '%s' "$_body" | jq -r '.node_name // empty')
	[ -n "$_node" ] || blocked "$2 ($1) has no node_name — the learned node is unknown"
	printf '%s' "$_node"
}

epoch() {
	printf '%s' "$1" | jq -Rr '
		capture("^(?<b>[^.]+)(?<f>\\.[0-9]+)?(Z|\\+00:00)$")
		| ((.b + "Z") | fromdateiso8601)
		  + ((.f // "") | if . == "" then 0 else (("0" + .) | tonumber) end)'
}

same_issue() { # <a> <b>
	_ai=$(entry "$1" | jq -r '.issue_id // empty')
	_bi=$(entry "$2" | jq -r '.issue_id // empty')
	[ -n "$_ai" ] || blocked "task $1 carries no issue context"
	[ "$_ai" = "$_bi" ] || blocked "tasks $1 and $2 are not on the same issue ($_ai vs $_bi)"
}

# payload_prior_session <task-id>: the delivered claim payload's
# prior_session_id. The preview endpoint is the only readable face of the
# payload (it is memory-only by design); a 404 means the entry is not live and
# delivered, which is a block rather than a judgement.
payload_prior_session() {
	_p=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$1/payload-preview") ||
		blocked "cannot read $1's delivered payload — run while the entry is live and delivered"
	printf '%s' "$_p" | jq -r '.prior_session_id // empty'
}

# session_key is the design's normalization (§4.5): empty stays empty, a bare
# id stays as is, a path keeps its base name without extension. The payload
# side and the learning side must use the same normalization.
session_key() {
	case $1 in
	"") : ;;
	*/*)
		_b=${1##*/}
		_b=${_b%.*}
		printf '%s' "$_b"
		;;
	*) printf '%s' "$1" ;;
	esac
}

# verify_continuation <prior> <cont>: TASK_CONT's delivered prior_session_id
# normalizes to the key PRIOR learned. PRIOR_SESSION is the operator-supplied
# session key (the terminal report body's session_id); the projection does not
# expose it, so it is an input rather than a guess.
verify_continuation() {
	[ -n "${PRIOR_SESSION:-}" ] ||
		blocked "set PRIOR_SESSION to the session key PRIOR learned (its terminal report's session_id)"
	_cont_key=$(payload_prior_session "$2")
	[ -n "$_cont_key" ] ||
		fail "TASK_CONT carries no prior_session_id — it is not a continuation"
	[ "$(session_key "$_cont_key")" = "$(session_key "$PRIOR_SESSION")" ] ||
		fail "TASK_CONT resumes '$_cont_key', PRIOR learned '$PRIOR_SESSION' — not the same session key"
	observe "continuation key: prior=$PRIOR_SESSION cont=$_cont_key normalized=$(session_key "$PRIOR_SESSION")"
}

job_created_at() { # <job-json>
	printf '%s' "$1" | jq -r '.metadata.creationTimestamp // empty'
}

duration_seconds() { # <go-duration> — whole h/m/s values only
	case $1 in
	*h)
		case ${1%h} in '' | *[!0-9]*) return 1 ;; esac
		echo $((${1%h} * 3600))
		;;
	*m)
		case ${1%m} in '' | *[!0-9]*) return 1 ;; esac
		echo $((${1%m} * 60))
		;;
	*s)
		case ${1%s} in '' | *[!0-9]*) return 1 ;; esac
		echo ${1%s}
		;;
	*) return 1 ;;
	esac
}

case $ACTION in
issue-key)
	PRIOR=${2:-}
	TASK_OTHER=${3:-}
	TASK_CONT=${4:-}
	[ -n "$PRIOR" ] && [ -n "$TASK_OTHER" ] && [ -n "$TASK_CONT" ] ||
		blocked "usage: $0 issue-key PRIOR TASK_OTHER TASK_CONT"
	learned=$(settled_on_node "$PRIOR" PRIOR)
	observe "prior=$PRIOR learned_node=$learned"

	# The issue key is shared by all three tasks; the other task comes from a
	# different agent. If the issue key participated, TASK_OTHER would carry a
	# block even with no prior session of its own.
	same_issue "$PRIOR" "$TASK_OTHER"
	same_issue "$PRIOR" "$TASK_CONT"
	other_agent=$(entry "$TASK_OTHER" | jq -r '.agent_id // empty')
	cont_agent=$(entry "$TASK_CONT" | jq -r '.agent_id // empty')
	[ -n "$other_agent" ] && [ -n "$cont_agent" ] &&
		[ "$other_agent" != "$cont_agent" ] ||
		blocked "TASK_OTHER and TASK_CONT do not name two different agents ($other_agent vs $cont_agent)"
	other_prior=$(payload_prior_session "$TASK_OTHER")
	[ -z "$other_prior" ] ||
		fail "TASK_OTHER resumes '$other_prior' — the negative control needs its own first run (no prior session)"
	verify_continuation "$PRIOR" "$TASK_CONT"
	observe "issue=$(entry "$PRIOR" | jq -r .issue_id) agents: other=$other_agent cont=$cont_agent"

	other_job=$(live_job "$TASK_OTHER")
	no_affinity "$other_job" "TASK_OTHER's Job"
	observe "TASK_OTHER job: $(printf '%s' "$other_job" | jq -c '{job: .metadata.name, affinity: (.spec.template.spec.affinity // null)}')"

	cont_job=$(live_job "$TASK_CONT")
	one_item "$cont_job" "$learned" "TASK_CONT's Job"
	observe "TASK_CONT job: $(printf '%s' "$cont_job" | jq -c '{job: .metadata.name, affinity: .spec.template.spec.affinity}')"
	echo "PASS: TC-tech-cache-06 issue-key (no block for another agent on the same issue; one item for the continuation)"
	;;

saturated)
	PRIOR=${2:-}
	TASK_CONT=${3:-}
	[ -n "$PRIOR" ] && [ -n "$TASK_CONT" ] ||
		blocked "usage: $0 saturated PRIOR TASK_CONT"
	reuse=$(deploy_env FOREMAN_PREFER_NODE_REUSE)
	[ "$reuse" = "true" ] ||
		blocked "FOREMAN_PREFER_NODE_REUSE='$reuse', want true — the saturation shape only suppresses while reuse is on"
	cap=$(deploy_env FOREMAN_MAX_JOBS_PER_NODE)
	[ -n "$cap" ] || blocked "the foreman Deployment carries no FOREMAN_MAX_JOBS_PER_NODE"
	case $cap in '' | *[!0-9]*) blocked "FOREMAN_MAX_JOBS_PER_NODE='$cap' is not a whole number" ;; esac
	[ "$cap" -gt 0 ] || blocked "FOREMAN_MAX_JOBS_PER_NODE=0 means no per-node cap — the saturation shape is unconstructible"
	learned=$(settled_on_node "$PRIOR" PRIOR)

	# §8.6's saturation is about the session key's node, not any node: count
	# the live entries on the node PRIOR learned, and require that node at the
	# soft cap. An implementation that suppresses on any saturated node would
	# otherwise pass this shape, and a correct one would fail it on a cluster
	# where another node is the saturated one.
	tasks=$(task_list)
	count=$(printf '%s' "$tasks" | jq -r --arg n "$learned" \
		'[.[] | select(.state != "terminal") | select((.node_name // "") == $n)] | length')
	observe "cap=$cap learned_node=$learned live_on_learned=$count"
	[ "$count" -ge "$cap" ] ||
		blocked "the learned node $learned holds $count live task Jobs, below cap $cap — construct saturation on that node first"
	verify_continuation "$PRIOR" "$TASK_CONT"

	job=$(live_job "$TASK_CONT")
	no_affinity "$job" "TASK_CONT's Job"
	observe "TASK_CONT job: $(printf '%s' "$job" | jq -c '{job: .metadata.name, affinity: (.spec.template.spec.affinity // null)}')"
	echo "PASS: TC-tech-cache-06 saturated (learned node $learned at cap $cap; the continuation carries no item)"
	;;

switch-off)
	PRIOR=${2:-}
	TASK_CONT=${3:-}
	SINCE=${4:-${SWITCH_SINCE:-}}
	[ -n "$PRIOR" ] && [ -n "$TASK_CONT" ] ||
		blocked "usage: $0 switch-off PRIOR TASK_CONT SINCE"
	[ -n "$SINCE" ] ||
		blocked "SINCE is required: the instant the switch became effective (the rollout completion time)"
	switch=$(deploy_env FOREMAN_PREFER_NODE_REUSE)
	[ "$switch" = "false" ] ||
		blocked "FOREMAN_PREFER_NODE_REUSE='$switch', want false — the switch-off window is not installed"
	learned=$(settled_on_node "$PRIOR" PRIOR)
	verify_continuation "$PRIOR" "$TASK_CONT"
	job=$(live_job "$TASK_CONT")
	created=$(job_created_at "$job")
	created_s=$(epoch "$created") || blocked "cannot parse the Job creation instant '$created'"
	since_s=$(epoch "$SINCE") || blocked "cannot parse SINCE '$SINCE' (want RFC3339 UTC)"
	observe "switch=$switch prior=$PRIOR learned_node=$learned job_created=$created since=$SINCE"
	[ "$created_s" -gt "$since_s" ] ||
		blocked "the Job was created at $created, not after the switch became effective at $SINCE — it may legally carry the item"
	no_affinity "$job" "TASK_CONT's Job"
	observe "TASK_CONT job: $(printf '%s' "$job" | jq -c '{job: .metadata.name, affinity: (.spec.template.spec.affinity // null)}')"
	echo "PASS: TC-tech-cache-06 switch-off (reuse off before the Job was created; the continuation carries no item)"
	;;

index-expired)
	PRIOR=${2:-}
	TASK_CONT=${3:-}
	[ -n "$PRIOR" ] && [ -n "$TASK_CONT" ] || blocked "usage: $0 index-expired PRIOR TASK_CONT"
	reuse=$(deploy_env FOREMAN_PREFER_NODE_REUSE)
	[ "$reuse" = "true" ] ||
		blocked "FOREMAN_PREFER_NODE_REUSE='$reuse', want true — the expired-index shape only suppresses while reuse is on"
	ttl=$(deploy_env FOREMAN_AGENT_STATE_TTL)
	[ -n "$ttl" ] || blocked "the foreman Deployment carries no FOREMAN_AGENT_STATE_TTL"
	[ "$ttl" != "0" ] && [ "$ttl" != "0s" ] ||
		blocked "FOREMAN_AGENT_STATE_TTL=$ttl (zero = the index never expires) — the shape is unconstructible"
	learned=$(settled_on_node "$PRIOR" PRIOR)
	verify_continuation "$PRIOR" "$TASK_CONT"

	logs=$(kubectl -n "$FOREMAN_NS" logs deploy/foreman --since="$LOG_SINCE" 2>/dev/null) ||
		blocked "cannot read the Foreman log"
	if [ -n "${PRIOR_TERMINAL_AT:-}" ]; then
		terminal_at=$PRIOR_TERMINAL_AT
	else
		terminal_at=$(printf '%s\n' "$logs" | jq -Rr --arg t "$PRIOR" \
			'fromjson? | select(.msg=="task.terminal" and .task_id==$t) | .ts' | head -1)
		[ -n "$terminal_at" ] ||
			blocked "no task.terminal line for $PRIOR inside $LOG_SINCE — set PRIOR_TERMINAL_AT to its terminal instant"
	fi
	observe "prior=$PRIOR learned_node=$learned terminal_at=$terminal_at ttl=$ttl"

	ttl_s=$(duration_seconds "$ttl") ||
		blocked "FOREMAN_AGENT_STATE_TTL='$ttl' is not a whole-hour/minute/second duration"
	MAX_TTL_S=${MAX_TTL_S:-3600}
	[ "$ttl_s" -le "$MAX_TTL_S" ] ||
		blocked "FOREMAN_AGENT_STATE_TTL=$ttl is too long to construct the expired-index window in one run (limit ${MAX_TTL_S}s); shorten it and restart the instance"
	reconcile=$(deploy_env FOREMAN_RECONCILE_INTERVAL)
	[ -n "$reconcile" ] || reconcile=30s
	reconcile_s=$(duration_seconds "$reconcile") ||
		blocked "FOREMAN_RECONCILE_INTERVAL='$reconcile' is not a whole-hour/minute/second duration"
	term_s=$(epoch "$terminal_at") || blocked "cannot parse the terminal instant '$terminal_at'"
	now_s=$(date +%s)
	age_s=$((now_s - term_s))
	# The prune runs once per reconcile round, so the earliest round that can
	# have evicted the entry is terminal_at + ttl + one interval. learned_at
	# is refreshed by every repeated terminal report (ADR-015), so the first
	# task.terminal is only a lower bound on the eviction instant: the prune
	# sweep itself is the evidence, and it logs session_index.pruned only when
	# it dropped an entry.
	eligible_s=$((term_s + ttl_s + reconcile_s))
	observe "index age=${age_s}s ttl=${ttl_s}s reconcile=${reconcile_s}s eligible_at=$eligible_s"

	job=$(live_job "$TASK_CONT")
	created=$(job_created_at "$job")
	created_s=$(epoch "$created") || blocked "cannot parse the Job creation instant '$created'"
	observe "continuation job_created=$created"
	pruned_at=""
	for ts in $(printf '%s\n' "$logs" | jq -Rr \
		'fromjson? | select(.msg=="session_index.pruned" and ((.count // 0) > 0)) | .ts'); do
		ts_s=$(epoch "$ts") || blocked "cannot parse the session_index.pruned instant '$ts'"
		if [ "$ts_s" -gt "$eligible_s" ] && [ "$ts_s" -lt "$created_s" ]; then
			pruned_at=$ts
			break
		fi
	done
	[ -n "$pruned_at" ] ||
		blocked "no session_index.pruned round between the eligible instant (epoch $eligible_s) and the Job creation ($created) — wait for the next reconcile round after the index expires, then create a new continuation task"
	observe "session_index.pruned_at=$pruned_at (after eligible_at, before job_created)"
	no_affinity "$job" "TASK_CONT's Job"
	observe "TASK_CONT job: $(printf '%s' "$job" | jq -c '{job: .metadata.name, affinity: (.spec.template.spec.affinity // null)}')"
	echo "PASS: TC-tech-cache-06 index-expired (entry evicted before the Job was created; the continuation carries no item)"
	;;

*)
	blocked "usage: $0 issue-key PRIOR TASK_OTHER TASK_CONT | saturated PRIOR TASK_CONT | switch-off PRIOR TASK_CONT SINCE | index-expired PRIOR TASK_CONT"
	;;
esac
