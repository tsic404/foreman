#!/bin/sh
# TC-tech-job-09 — PID 1 is the init tool, orphan reaping and the signal face
# (AC-35, reference-template window). The task must still be running: step 5
# signals PID 1 and ends it. The static half (the template's command prefix and
# args, carried verbatim into the task Job) is
# tests/acceptance/tech_job_09_pid1_dumb_init_test.go.
#
#   TASK_ID=<task id> ./tech-job-09-cluster.sh   (or pass the id as $1)
#   FOREMAN_TERMINATION_HANG_GUARD=<seconds>     (default 120, range 1..3600;
#                                                 a hang guard, not a latency
#                                                 criterion)
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
TASK_ID=${TASK_ID:-${1:-}}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

# Wall clock in deciseconds for the step-5 poll and its observation. jq is
# already required, and its `now` is sub-second where a busybox date ignores
# %N (it prints the seconds alone), so both ends of the measurement share one
# clock instead of mixing a fractional sleep with a whole-second reading.
now_ds() { jq -n 'now * 10 | floor'; }
# Deciseconds as seconds with one fractional digit: 3 -> 0.3, 162 -> 16.2.
format_seconds() { echo "$(( $1 / 10 )).$(( $1 % 10 ))"; }

# The hang guard is whole seconds in [1, 3600], validated before the cluster is
# touched: a typo must block, not read as a failed termination (writing
# requirement 4 — the boundary states its values and its bad input). Leading
# zeros are stripped so the guard reads canonically in the observation and
# failure messages (POSIX test compares decimal anyway); the ceiling compares
# digit counts first, since an out-of-range value would error the poll's integer
# comparison and `|| break` would turn it into a FAIL.
guard_max=3600
guard_input=${FOREMAN_TERMINATION_HANG_GUARD:-120}
case $guard_input in
*[!0-9]*) blocked "FOREMAN_TERMINATION_HANG_GUARD must be whole seconds, got '$guard_input'" ;;
esac
guard=$(printf '%s\n' "$guard_input" | sed 's/^0*//')
[ -n "$guard" ] || blocked "FOREMAN_TERMINATION_HANG_GUARD must be at least 1 second, got '$guard_input'"
if [ "${#guard}" -gt "${#guard_max}" ] || [ "$guard" -gt "$guard_max" ]; then
	blocked "FOREMAN_TERMINATION_HANG_GUARD must be at most ${guard_max} seconds, got '$guard_input'"
fi
guard_ds=$(( guard * 10 ))

if [ -z "$TASK_ID" ]; then
	TASK_ID=$(kubectl -n "$JOB_NS" get jobs -l foreman.tsic.top/task-id -o json 2>/dev/null |
		jq -r '[.items[] | select((.status.active // 0) > 0)]
			| sort_by(.metadata.creationTimestamp) | first
			| .metadata.labels["foreman.tsic.top/task-id"] // empty') ||
		blocked "cannot list task Jobs in $JOB_NS"
fi
[ -n "$TASK_ID" ] || blocked "no running task Job in $JOB_NS — trigger a long task first (the design case needs it running)"

JOB_NAME=$(kubectl -n "$JOB_NS" get jobs -l "foreman.tsic.top/task-id=$TASK_ID" \
	-o jsonpath='{.items[0].metadata.name}' 2>/dev/null) ||
	blocked "cannot read the task $TASK_ID's Job in $JOB_NS"
[ -n "$JOB_NAME" ] || blocked "no Job carries foreman.tsic.top/task-id=$TASK_ID"
observe "task=$TASK_ID job=$JOB_NAME namespace=$JOB_NS"

# ---- Step 1: the command prefix and PID 1's identity ------------------------
job_json=$(kubectl -n "$JOB_NS" get job "$JOB_NAME" -o json) ||
	blocked "cannot read Job $JOB_NAME"
printf '%s' "$job_json" | jq -e \
	'[.spec.template.spec.containers[] | select(.name=="agent") | .command[0:2]] == [["/usr/bin/dumb-init", "--"]]' \
	>/dev/null || fail "agent command prefix is not [\"/usr/bin/dumb-init\", \"--\"]"
printf '%s' "$job_json" | jq -e \
	'[.spec.template.spec.containers[] | select(.name=="agent") | .args[0:1]] == [["daemon"]]' \
	>/dev/null || fail "agent args do not start with the daemon subcommand"

pid1=$(kubectl -n "$JOB_NS" exec "job/$JOB_NAME" -c agent -- sh -c \
	'cat /proc/1/comm; tr "\0" " " < /proc/1/cmdline; echo') ||
	blocked "cannot exec into $JOB_NAME (the pod may still be starting)"
observe "proc/1: $pid1"
comm=$(printf '%s\n' "$pid1" | sed -n '1p' | tr -d '[:space:]')
[ "$comm" = "dumb-init" ] || fail "/proc/1/comm = '$comm', want dumb-init"
case $(printf '%s\n' "$pid1" | sed -n '2p') in
/usr/bin/dumb-init\ --\ /usr/local/bin/multica*) : ;;
*) fail "/proc/1/cmdline does not start with the prefix and the daemon binary" ;;
esac

# ---- Step 2: the parent-child chain ----------------------------------------
rc=0
children=$(kubectl -n "$JOB_NS" exec "job/$JOB_NAME" -c agent -- sh -c '
  dp=""
  for p in $(cat /proc/1/task/*/children); do
    tr "\0" " " < /proc/$p/cmdline | grep -q "^/usr/local/bin/multica " && dp=$p
  done
  [ -n "$dp" ] || { echo "QA_BLOCKED: no daemon pid under PID 1" >&2; exit 2; }
  echo "daemon_pid=$dp"
  for p in $(cat /proc/1/task/*/children); do echo "pid1_child=$(tr "\0" " " < /proc/$p/cmdline)"; done
  ap=""; i=0
  while [ -z "$ap" ] && [ "$i" -lt 30 ]; do
    for p in $(cat /proc/$dp/task/*/children); do
      tr "\0" " " < /proc/$p/cmdline 2>/dev/null | grep -qE "^(/usr/local/bin/)?omp( |$)" && ap=$p && break
    done
    [ -n "$ap" ] || { i=$((i+1)); sleep 2; }
  done
  for p in $(cat /proc/$dp/task/*/children); do echo "daemon_child=$(tr "\0" " " < /proc/$p/cmdline 2>/dev/null)"; done
  [ -n "$ap" ] || { echo "QA_BLOCKED: no agent process in the daemon child set within the window" >&2; exit 2; }
  echo "agent_pid=$ap"
') || rc=$?
observe "process tree: $children"
[ "$rc" -eq 0 ] || blocked "process-tree collection failed (exit $rc)"
printf '%s\n' "$children" | grep -q '^daemon_pid=' || fail "no daemon_pid in the collected tree"
printf '%s\n' "$children" | grep -q '^agent_pid=' || fail "no agent_pid in the collected tree"

# ---- Steps 3/4: orphan reaping, two batches --------------------------------
rc=0
orphans=$(kubectl -n "$JOB_NS" exec "job/$JOB_NAME" -c agent -- sh -c '
  z() { grep -lE "^State:[[:space:]]*Z" /proc/[0-9]*/status 2>/dev/null | wc -l; }
  live() { c=0; for p in /proc/[0-9]*; do [ "$(tr "\0" " " < $p/cmdline 2>/dev/null)" = "sleep 2 " ] && c=$((c+1)); done; echo $c; }
  z0=$(z)
  for i in $(seq 1 30); do ( sleep 2 & ); done
  m1=$(live); [ "$m1" -gt 0 ] || { echo "QA_BLOCKED: first batch injected but no live process" >&2; exit 2; }
  sleep 6; z1=$(z)
  for i in $(seq 1 60); do ( sleep 2 & ); done
  m2=$(live); [ "$m2" -gt 0 ] || { echo "QA_BLOCKED: second batch injected but no live process" >&2; exit 2; }
  sleep 6; z2=$(z)
  echo "z0=$z0 m1=$m1 z1=$z1 m2=$m2 z2=$z2"
') || rc=$?
observe "orphans: $orphans"
[ "$rc" -eq 0 ] || blocked "orphan collection failed (exit $rc)"
values=$(printf '%s\n' "$orphans" | sed -n 's/^z0=\([0-9]*\) m1=\([0-9]*\) z1=\([0-9]*\) m2=\([0-9]*\) z2=\([0-9]*\)$/\1 \2 \3 \4 \5/p')
[ -n "$values" ] || fail "orphan counters are not readable"
set -- $values
z0=$1 z1=$3 z2=$5
[ "$z1" -le "$z0" ] || fail "z1=$z1 > z0=$z0: the first batch was not reaped"
[ "$z2" -le "$z1" ] || fail "z2=$z2 > z1=$z1: the orphan count grows with the injected batch"

# ---- Step 5: SIGTERM to PID 1 (destructive, last) --------------------------
# AC-35 asks only that the container terminate: it sets no deadline, and this
# path enforces none. $guard is a hang guard separating "does not terminate"
# from "terminates slowly"; the measured latency is observed, never asserted.
# The pod is pinned before the signal, so the verdict covers the container that
# received it. An observed terminal state passes, the container observed still
# running at the guard fails, and every unobserved answer blocks: a signal exec
# that fails, pod object gone (contracts §4.2: removal is not termination), a
# read that never succeeds or a status that does not parse.
running_pods=$(kubectl -n "$JOB_NS" get pods -l "foreman.tsic.top/task-id=$TASK_ID" \
	--field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}') ||
	blocked "cannot list the task's running pods"
case $running_pods in
*' '*) blocked "more than one running pod carries task $TASK_ID: $running_pods" ;;
'') blocked "no running pod carries task $TASK_ID" ;;
esac
pod=$running_pods
observe "signalled pod: $pod"
kubectl -n "$JOB_NS" exec "$pod" -c agent -- kill -TERM 1 ||
	blocked "cannot signal PID 1 (kubectl exec failed): the termination is unobserved, not a verdict"
started=$(now_ds)
termination=
read_error=
# The task pod is deleted about a second after the container exits, so the poll
# samples well inside that window: at the previous 2s cadence it read before the
# termination and again after the deletion, and reported a termination it never
# saw as unobserved.
poll_interval=0.1
while :; do
	# Status from stdout only: a client/server version-skew warning rides on
	# stderr and would corrupt the payload. --ignore-not-found keeps "the object
	# is gone" (exit 0, no output) apart from "the read failed" (non-zero).
	if pod_json=$(kubectl -n "$JOB_NS" get pod "$pod" -o json --ignore-not-found); then
		[ -n "$pod_json" ] ||
			blocked "pod $pod is gone with no agent terminal state observed (object removal is not termination)"
		termination=$(printf '%s' "$pod_json" | jq -c \
			'[.status.containerStatuses[]? | select(.name=="agent") | .state.terminated | select(. != null)]') ||
			blocked "cannot read pod $pod's container status: $pod_json"
		read_error=
		[ "$termination" = "[]" ] || break
	else
		# A failed read is an unobserved answer, not a verdict: retry until the
		# guard expires, then block rather than deny AC-35.
		read_error="kubectl get pod $pod failed (exit $?); see stderr above"
	fi
	elapsed_ds=$(( $(now_ds) - started ))
	[ "$elapsed_ds" -lt "$guard_ds" ] || break
	sleep "$poll_interval"
done
elapsed_ds=$(( $(now_ds) - started ))
if [ -n "$read_error" ]; then
	blocked "cannot read pod $pod's status within ${guard}s of SIGTERM to PID 1 (last termination state read: ${termination:-none}); $read_error"
fi
observe "agent termination: +$(format_seconds "$elapsed_ds")s (hang guard ${guard}s): $termination"
if [ "$termination" = "[]" ]; then
	fail "the agent container did not terminate within ${guard}s of SIGTERM to PID 1"
fi

echo "PASS: TC-tech-job-09 (task $TASK_ID)"
