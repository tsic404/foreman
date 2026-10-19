#!/bin/sh
# TC-uat-observability-01 — G4: the recent-task question is answerable from
# the metrics and the logs: the success/failure totals (and the rate computed
# from them), Job create and boot latency, the in-flight count, token usage,
# and one task's claim → Job → start → terminal chain in the structured log.
#
#   COMPLETED_TASK=<id> FAILED_TASK=<id> [TASK_ID=<id>] [INFLIGHT_ZERO=1] \
#     ./uat-observability-01-cluster.sh
#
# COMPLETED_TASK/FAILED_TASK are one successful and one failed run of the
# window; TASK_ID defaults to COMPLETED_TASK and selects the log chain.
# INFLIGHT_ZERO=1 (default) asserts the gauge is back to 0 — set it to 0 when
# other tasks are in flight in the same window.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

COMPLETED_TASK=${COMPLETED_TASK:-}
FAILED_TASK=${FAILED_TASK:-}
TASK=${TASK_ID:-${COMPLETED_TASK:-}}
INFLIGHT_ZERO=${INFLIGHT_ZERO:-1}
LOG_SINCE=${LOG_SINCE:-6h}

need kubectl
need curl
need jq

[ -n "$COMPLETED_TASK" ] || blocked "COMPLETED_TASK is required (one successful task of the window)"
[ -n "$FAILED_TASK" ] || blocked "FAILED_TASK is required (one failed task of the same window)"

# ---- The two projections the counters must answer for -----------------------
completed=$(entry "$COMPLETED_TASK")
failed=$(entry "$FAILED_TASK")
printf '%s' "$completed" | jq -e '.state == "terminal" and .result == "completed"' >/dev/null ||
	fail "COMPLETED_TASK=$COMPLETED_TASK is not a terminal completed task: $completed"
printf '%s' "$failed" | jq -e '.state == "terminal" and .result == "failed"' >/dev/null ||
	fail "FAILED_TASK=$FAILED_TASK is not a terminal failed task: $failed"
observe "completed=$completed"
observe "failed=$failed"

# ---- The metric families the question needs --------------------------------
for family in \
	foreman_tasks_total \
	foreman_task_claims_total \
	foreman_job_create_seconds_bucket \
	foreman_job_boot_seconds_bucket \
	foreman_inflight_jobs \
	foreman_token_usage_total \
	foreman_forward_total \
	foreman_auth_failures_total \
	foreman_pending_reports \
	foreman_duplicate_dispatch_total; do
	metric_family_present "$family"
done
observe "metric families present (tasks/claims/create/boot/inflight/tokens/forward/auth/pending/duplicate)"

# ---- Success rate: both sides must be present and non-zero ------------------
completed_total=$(metric_value foreman_tasks_total 'result="completed"')
failed_total=$(metric_value foreman_tasks_total 'result="failed"')
total=$((completed_total + failed_total))
observe "tasks_total: completed=$completed_total failed=$failed_total total=$total"
[ "$completed_total" -ge 1 ] || fail "foreman_tasks_total{result=\"completed\"} is $completed_total after $COMPLETED_TASK"
[ "$failed_total" -ge 1 ] || fail "foreman_tasks_total{result=\"failed\"} is $failed_total after $FAILED_TASK"
[ "$total" -gt 0 ] || fail "the task totals are empty — the success rate is unanswerable"
rate=$(awk -v c="$completed_total" -v t="$total" 'BEGIN { printf "%.3f", c / t }')
observe "success_rate=$rate ($completed_total/$total)"

# ---- Latency, in-flight and usage values -----------------------------------
create_count=$(metric_value foreman_job_create_seconds_count)
boot_count=$(metric_value foreman_job_boot_seconds_count)
observe "job_create_seconds_count=$create_count job_boot_seconds_count=$boot_count"
[ "$create_count" -ge 1 ] || fail "no Job create latency sample after a started task"
[ "$boot_count" -ge 1 ] || fail "no Job boot latency sample after a started task"

_metrics=$(metrics)
inflight=$(printf '%s\n' "$_metrics" | awk '$1 == "foreman_inflight_jobs" { print $2; exit }')
[ -n "$inflight" ] || fail "foreman_inflight_jobs has no readable value"
observe "inflight=$inflight"
if [ "$INFLIGHT_ZERO" = "1" ]; then
	[ "$inflight" = "0" ] || fail "the in-flight gauge is $inflight, want 0 after the window settled"
fi

tokens=$(metric_value foreman_token_usage_total)
observe "token_usage_total=$tokens"
[ "$tokens" -gt 0 ] || fail "foreman_token_usage_total has no non-zero sample — usage is unanswerable"

# ---- The task chain in the structured log ----------------------------------
logs=$(foreman_logs "$LOG_SINCE")
for pair in "completed:$COMPLETED_TASK" "failed:$FAILED_TASK"; do
	class=${pair%%:*}
	id=${pair#*:}
	body=$completed
	[ "$class" = "failed" ] && body=$failed
	for msg in task.claimed job.created task.terminal; do
		n=$(count_log "$logs" "$msg" "$id")
		[ "$n" -ge 1 ] || fail "no $msg line for the $class task $id in the last $LOG_SINCE"
	done
	if [ -n "$(printf '%s' "$body" | jq -r '.started_at // empty')" ]; then
		n=$(count_log "$logs" task.started "$id")
		[ "$n" -ge 1 ] || fail "no task.started line for the $class task $id in the last $LOG_SINCE"
	fi
	observe "$class task $id: claim→job→start→terminal present"
done

echo "PASS: TC-uat-observability-01 (success rate $rate from $total tasks, create/boot latency, in-flight $inflight, token usage $tokens; both task chains readable from logs)"
