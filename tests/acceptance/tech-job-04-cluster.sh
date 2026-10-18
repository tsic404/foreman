#!/bin/sh
# TC-tech-job-04 — the real-entry half of AC-23 / AC-24: observe a task produced
# by an actual autopilot or chat trigger, assert the claim → Job create latency
# from the Foreman log's own `ts` fields (≤5s), and read the live Job's
# trigger-context annotation block. The trigger itself stays the operator's
# precondition (only the autopilot entry has a CLI; the chat entry does not).
#
#   TASK=<task id> [ENTRY=autopilot|chat] [ANCHOR=<run id|chat session id>] \
#     ./tech-job-04-cluster.sh
# ENTRY/ANCHOR omit the entry-equality assertion when unset; the anchor key is
# still required to exist. LOG_SINCE (default 1h) must cover the trigger.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_NS=${FOREMAN_NAMESPACE:-foreman}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
LOG_SINCE=${LOG_SINCE:-1h}
TASK=${TASK:-${1:-}}
ENTRY=${ENTRY:-}
ANCHOR=${ANCHOR:-}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

[ -n "$TASK" ] || blocked "TASK is required (the task id a real autopilot/chat trigger produced)"
JOB_NAME=$(kubectl -n "$JOB_NS" get jobs -l "foreman.tsic.top/task-id=$TASK" \
	-o jsonpath='{.items[0].metadata.name}' 2>/dev/null) ||
	blocked "cannot read the task's Job in $JOB_NS"
[ -n "$JOB_NAME" ] || blocked "no live Job carries task-id=$TASK — run this before the task settles"
observe "task=$TASK job=$JOB_NAME"

# The real claim → Job latency: both instants come from the Foreman log's ts
# fields (one clock). The fractional part is kept so the 5s bound is exact.
logs=$(kubectl -n "$FOREMAN_NS" logs deploy/foreman --since="$LOG_SINCE" 2>/dev/null) ||
	blocked "cannot read the Foreman log"
ts_of() {
	printf '%s\n' "$logs" | jq -Rr --arg t "$TASK" --arg m "$1" \
		'fromjson? | select(.msg==$m and .task_id==$t) | .ts' | head -1
}
claim_ts=$(ts_of task.claimed)
create_ts=$(ts_of job.created)
[ -n "$claim_ts" ] || blocked "no task.claimed line for $TASK inside $LOG_SINCE"
[ -n "$create_ts" ] || blocked "no job.created line for $TASK inside $LOG_SINCE"
delta=$(jq -Rn --arg c "$claim_ts" --arg g "$create_ts" '
	def epoch:
		capture("^(?<b>[^.]+)(?<f>\\.[0-9]+)?(Z|\\+00:00)$")
		| ((.b + "Z") | fromdateiso8601)
		  + ((.f // "") | if . == "" then 0 else (("0" + .) | tonumber) end);
	($g | epoch) - ($c | epoch)') ||
	blocked "cannot parse the event timestamps ($claim_ts / $create_ts)"
observe "task.claimed ts=$claim_ts job.created ts=$create_ts delta_seconds=$delta"
jq -en --argjson d "$delta" '($d >= 0) and ($d <= 5)' >/dev/null ||
	fail "claim → Job create took ${delta}s, want ≤5s (AC-23)"

# The trigger-context block on the live Job (AC-24).
job_json=$(kubectl -n "$JOB_NS" get job "$JOB_NAME" -o json) ||
	blocked "cannot read Job $JOB_NAME"
observe "annotations: $(printf '%s' "$job_json" | jq -c '.metadata.annotations')"
entry=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/tasks/$TASK") ||
	blocked "cannot read $FOREMAN_URL/foreman/tasks/$TASK (port-forward the foreman Service first)"
observe "projection: $entry"
printf '%s' "$entry" | jq -e '.issue_id == "" and .issue_identifier == ""' >/dev/null ||
	fail "the task carries an issue context: $(printf '%s' "$entry" | jq -c '{issue_id,issue_identifier}')"

autopilot=$(printf '%s' "$job_json" | jq -r '.metadata.annotations["foreman.tsic.top/autopilot-run-id"] // empty')
chat=$(printf '%s' "$job_json" | jq -r '.metadata.annotations["foreman.tsic.top/chat-session-id"] // empty')
[ -n "$autopilot" ] || [ -n "$chat" ] || fail "neither entry anchor annotation exists on the Job"
[ -z "$autopilot" ] || [ -z "$chat" ] || fail "both entry anchors exist, want exactly one"
if [ -z "$ENTRY" ]; then
	if [ -n "$chat" ]; then ENTRY=chat; else ENTRY=autopilot; fi
fi
case $ENTRY in
autopilot)
	[ -n "$autopilot" ] || fail "ENTRY=autopilot, but no autopilot-run-id annotation"
	[ -z "$chat" ] || fail "ENTRY=autopilot, but chat-session-id is present"
	anchor_key=foreman.tsic.top/autopilot-run-id
	anchor_value=$autopilot
	;;
chat)
	[ -n "$chat" ] || fail "ENTRY=chat, but no chat-session-id annotation"
	[ -z "$autopilot" ] || fail "ENTRY=chat, but autopilot-run-id is present"
	anchor_key=foreman.tsic.top/chat-session-id
	anchor_value=$chat
	;;
*) fail "ENTRY=$ENTRY is not autopilot or chat" ;;
esac
if [ -n "$ANCHOR" ] && [ "$ANCHOR" != "$anchor_value" ]; then
	fail "$anchor_key = $anchor_value, want the trigger's $ANCHOR"
fi
for key in foreman.tsic.top/issue-id foreman.tsic.top/issue-identifier; do
	if printf '%s' "$job_json" | jq -e --arg k "$key" '.metadata.annotations | has($k)' >/dev/null; then
		fail "$key is present on a no-issue-context task"
	fi
done
for key in foreman.tsic.top/daemon-id foreman.tsic.top/job-runtime-id \
	foreman.tsic.top/claimed-at foreman.tsic.top/attempt; do
	value=$(printf '%s' "$job_json" | jq -r --arg k "$key" '.metadata.annotations[$k] // empty')
	[ -n "$value" ] || fail "$key is empty (identity keys are unconditional)"
done
[ "$(printf '%s' "$job_json" | jq -r '.metadata.annotations["foreman.tsic.top/attempt"]')" = "1" ] ||
	fail "attempt is not 1 (the case is a first run)"
if printf '%s' "$job_json" | jq -e '[.metadata.annotations | to_entries[]
	| select(.key | startswith("foreman.tsic.top/")) | select(.value == "")] | length > 0' >/dev/null; then
	fail "an empty placeholder sits under foreman.tsic.top/ on a no-issue-context task"
fi

echo "PASS: TC-tech-job-04 ($ENTRY entry, claim → Job ${delta}s, no issue annotations)"
