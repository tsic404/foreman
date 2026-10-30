#!/bin/sh
# TC-uat-observability-01 — G4: the recent-task question is answerable from
# the metrics and the logs: the success/failure totals (and the rate computed
# from them), Job create and boot latency, the in-flight count, token usage,
# and one task's claim → Job → start → terminal chain in the structured log.
#
#   ./uat-scenarios.sh prepare         # builds the window this case judges
#   ./uat-observability-01-cluster.sh
#
# COMPLETED_TASK/FAILED_TASK default to that window ($SCENARIOS_STATE) and can
# be named explicitly. When the window carries a cancelled and a platform
# timeout task they are judged too: the cancelled counter must move and both
# chains must be readable, which is what makes the failure side answerable
# instead of implied. The window's baselines turn the totals into deltas, so a
# task an earlier round produced cannot answer for this one.
# INFLIGHT_ZERO=1 (default) asserts the gauge is back to 0 — set it to 0 when
# other tasks are in flight in the same window.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

INFLIGHT_ZERO=${INFLIGHT_ZERO:-1}
LOG_SINCE=${LOG_SINCE:-6h}

need kubectl
need curl
need jq

# A window only answers when a successful prepare wrote it: the partial
# placeholder a failed prepare leaves behind is refused, so last round's ids
# and baselines cannot make this round green.
if [ -z "${COMPLETED_TASK:-}" ] || [ -z "${FAILED_TASK:-}" ]; then
	scenario_window_ready ||
		blocked "no complete window at $SCENARIOS_STATE — run ./uat-scenarios.sh prepare (or pass COMPLETED_TASK/FAILED_TASK)"
fi
COMPLETED_TASK=$(scenario_value "${COMPLETED_TASK:-}" COMPLETED)
FAILED_TASK=$(scenario_value "${FAILED_TASK:-}" FAILED)
CANCELLED_TASK=$(scenario_value "${CANCELLED_TASK:-}" CANCELLED)
TIMEOUT_TASK=$(scenario_value "${TIMEOUT_TASK:-}" TIMEOUT)

[ -n "$COMPLETED_TASK" ] || blocked "COMPLETED_TASK is required (one successful task of the window) — run ./uat-scenarios.sh prepare"
[ -n "$FAILED_TASK" ] || blocked "FAILED_TASK is required (one failed task of the same window) — run ./uat-scenarios.sh prepare"

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
# The baselines come from the window the scenario builder recorded before its
# first task; without one (a hand-built window) the absolute value has to
# answer instead. Either way the verdict is about this window's tasks.
completed_before=$(scenario_state COMPLETED_BEFORE) || completed_before=
failed_before=$(scenario_state FAILED_BEFORE) || failed_before=
cancelled_before=$(scenario_state CANCELLED_BEFORE) || cancelled_before=
completed_total=$(metric_value foreman_tasks_total 'result="completed"')
failed_total=$(metric_value foreman_tasks_total 'result="failed"')
cancelled_total=$(metric_value foreman_tasks_total 'result="cancelled"')
total=$((completed_total + failed_total))

# moved_in_window <label> <before> <after>
moved_in_window() {
	case $2 in
	'' | *[!0-9]*) [ "$3" -ge 1 ] ;;
	*) [ "$(( $3 - $2 ))" -ge 1 ] ;;
	esac
}

observe "tasks_total: completed=$completed_before→$completed_total failed=$failed_before→$failed_total cancelled=$cancelled_before→$cancelled_total"
[ "$completed_total" -ge 1 ] || fail "foreman_tasks_total{result=\"completed\"} is $completed_total after $COMPLETED_TASK"
[ "$failed_total" -ge 1 ] || fail "foreman_tasks_total{result=\"failed\"} is $failed_total after $FAILED_TASK"
[ "$total" -gt 0 ] || fail "the task totals are empty — the success rate is unanswerable"
moved_in_window completed "$completed_before" "$completed_total" ||
	fail "foreman_tasks_total{result=\"completed\"} did not move across this window ($completed_before→$completed_total) after $COMPLETED_TASK"
moved_in_window failed "$failed_before" "$failed_total" ||
	fail "foreman_tasks_total{result=\"failed\"} did not move across this window ($failed_before→$failed_total) after $FAILED_TASK"
if [ -n "$CANCELLED_TASK" ]; then
	moved_in_window cancelled "$cancelled_before" "$cancelled_total" ||
		fail "foreman_tasks_total{result=\"cancelled\"} did not move across this window ($cancelled_before→$cancelled_total) after $CANCELLED_TASK"
fi
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

# ---- The task chains in the structured log ---------------------------------
# One chain per terminal class the window carries. The terminal line is the
# settled one (task.terminal) or the compensation that settled a Job the
# daemon could not report on (task.failed_compensated) — the platform-deadline
# class takes the second.
logs=$(foreman_logs "$LOG_SINCE")
for pair in "completed:$COMPLETED_TASK" "failed:$FAILED_TASK" \
	"cancelled:$CANCELLED_TASK" "timeout:$TIMEOUT_TASK"; do
	class=${pair%%:*}
	id=${pair#*:}
	[ -n "$id" ] || continue
	body=$(entry "$id")
	for msg in task.claimed job.created; do
		n=$(count_log "$logs" "$msg" "$id")
		[ "$n" -ge 1 ] || fail "no $msg line for the $class task $id in the last $LOG_SINCE"
	done
	terminals=$(( $(count_log "$logs" task.terminal "$id") + $(count_log "$logs" task.failed_compensated "$id") ))
	[ "$terminals" -ge 1 ] ||
		fail "no terminal line (task.terminal/task.failed_compensated) for the $class task $id in the last $LOG_SINCE"
	if [ -n "$(printf '%s' "$body" | jq -r '.started_at // empty')" ]; then
		n=$(count_log "$logs" task.started "$id")
		[ "$n" -ge 1 ] || fail "no task.started line for the $class task $id in the last $LOG_SINCE"
	fi
	observe "$class task $id: claim→job→start→terminal present"
done

echo "PASS: TC-uat-observability-01 (success rate $rate from $total tasks, create/boot latency, in-flight $inflight, token usage $tokens; completed/failed and every terminal class in the window readable from metrics and logs)"
