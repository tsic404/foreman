#!/bin/sh
# TC-tech-cache-02 — concurrent checkouts on one node do not corrupt the shared
# bare cache (AC-12 / ADR-006): two tasks ran concurrently on one node, both
# succeeded, and the shared bare cache passes `git fsck` (dangling objects
# allowed). Busy/lock counts are recorded, not gated.
#
# The window is constructed by the fixture, not hoped for: `pin-node.sh` writes
# the job policy's nodeSelector axis, so two independent tasks land on one node.
# The design's shared daemon profile then refuses the second Job's daemon
# (`daemon profile "foreman-job" is already running or starting`), its container
# exits 1 and the task settles failed (ADR-059 裁定 A: the profile becomes the
# Job's credential volume; the change is the product's, not this carrier's).
# That is this case's own failure criterion — 任务全部成功 — observed, never an
# unconstructible block: the window held and one task did not succeed.
#
# So the carrier waits for both tasks to settle, capturing each Job's pod while
# it exists (settlement deletes the Job, and the pod log with it), and judges:
#
#   * a task that does not settle within WAIT_SECONDS ⇒ blocked (window open);
#   * the two tasks on different nodes               ⇒ blocked (precondition);
#   * their in-flight windows do not overlap          ⇒ blocked (same reason);
#   * either task not `completed`                     ⇒ FAIL, with the failed
#     task's projection, its Job pod's container exit code, its pod log (the
#     daemon's refusal line included) and its server-side run record printed as
#     observations;
#   * both completed                                  ⇒ `git fsck` decides.
#
# A failed task never started — its daemon was refused — so its in-flight
# window begins at the Job's creation instant; a started task's begins at
# `started_at`. The end of both is the `task.terminal` instant in Foreman's log.
#
#   TASK1=<id> TASK2=<id> [NODE=n] ./tech-cache-02-cluster.sh
# Run it right after triggering the second task: it polls while the window is
# open, which is the only moment the failing pod's log and exit code are
# observable. A later run still judges from the projections, and reports the
# pod observations as unavailable instead of blocking.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_NS=${FOREMAN_NAMESPACE:-foreman}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8081}
STATE_ROOT=${FOREMAN_STATE_ROOT:-/var/lib/foreman}
# The node-debug container must carry git (fsck) plus find: the default image is
# Alpine + git. Override when the cluster's pull policy does not allow it.
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-alpine/git:2.54.0}
LOG_SINCE=${LOG_SINCE:-2h}
TASK1=${TASK1:-}
TASK2=${TASK2:-}
NODE=${NODE:-}
# How long the carrier waits for the window to close, and how often it looks
# while waiting: every look captures the Job pods still standing.
WAIT_SECONDS=${WAIT_SECONDS:-900}
POLL_SECONDS=${POLL_SECONDS:-5}
# Where the pod observations are kept while the window is open. Every poll
# writes one file per task and capture: the refused pod can vanish between two
# looks (settlement deletes the Job), so no capture overwrites another — the
# verdict reads the richest set afterwards.
EVIDENCE_DIR=${FOREMAN_CACHE_EVIDENCE:-$HOME/.foreman-cache-02-evidence}
# The fixture's CLI wrapper: the server-side run record is the one failed-task
# fact that outlives the Job. Optional — without it the carrier still judges
# and says the record is unavailable.
MULTICA_TEST=${FOREMAN_MULTICA_TEST:-}
if [ -z "$MULTICA_TEST" ]; then
	_fixture=$(CDPATH= cd -- "$(dirname -- "$0")/../../testinfra/test-multica" 2>/dev/null && pwd) || _fixture=
	if [ -n "$_fixture" ]; then MULTICA_TEST="$_fixture/multica-test"; fi
fi

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

[ -n "$TASK1" ] || blocked "TASK1 is required"
[ -n "$TASK2" ] || blocked "TASK2 is required"

entry() {
	ops_curl "$FOREMAN_URL/foreman/tasks/$1" ||
		blocked "cannot read $FOREMAN_URL/foreman/tasks/$1 (port-forward svc/foreman-ops 8081:8081 first)"
}

# read_entry <task-id>: the projection, or nothing while the task is not yet
# visible (a task the round just triggered is not projected until its claim is
# consumed — a 404 there is "not yet", not a broken environment).
read_entry() {
	ops_curl "$FOREMAN_URL/foreman/tasks/$1" 2>/dev/null || true
}

# field <projection> <jq filter>: one value out of a task projection.
field() { printf '%s' "$1" | jq -r "($2) // empty"; }

# capture_task <task-id>: while the task's Job pod exists, keep its container
# exit codes and log tails. Settlement deletes the Job, and with it the only
# copy of the refused daemon's line, so every poll leaves its own file: a
# container that has just been created has no log yet, and the next look may
# find the pod already gone. Best effort on purpose.
capture_task() {
	[ -n "$EVIDENCE_DIR" ] || return 0
	mkdir -p "$EVIDENCE_DIR" 2>/dev/null || return 0
	_pods=$(kubectl -n "$JOB_NS" get pods -l "foreman.tsic.top/task-id=$1" -o json 2>/dev/null) || return 0
	[ -n "$_pods" ] || return 0
	_pod=$(printf '%s' "$_pods" | jq -r \
		'[.items[] | select(.status.startTime != null)] | sort_by(.status.startTime) | last | .metadata.name // empty' \
		2>/dev/null) || return 0
	[ -n "$_pod" ] || return 0
	{
		printf 'pod=%s\n' "$_pod"
		printf '%s' "$_pods" | jq -r --arg p "$_pod" '
			.items[] | select(.metadata.name == $p)
			| (.status.containerStatuses // [])[]
			| "container=\(.name) exit=\(.state.terminated.exitCode // "") reason=\(.state.terminated.reason // "")"' 2>/dev/null || true
		for _c in $(printf '%s' "$_pods" | jq -r --arg p "$_pod" \
			'.items[] | select(.metadata.name == $p) | .spec.containers[].name' 2>/dev/null); do
			printf 'log %s:\n' "$_c"
			kubectl -n "$JOB_NS" logs "$_pod" -c "$_c" --tail=200 2>/dev/null || true
		done
	} >"$EVIDENCE_DIR/$1.pod.$(date +%s%N)" 2>/dev/null || return 0
}

# print_evidence <task-id> <projection>: the failed task's observable record —
# Foreman's projection, the Job pod's exit code and log, and the server's run.
print_evidence() {
	_label=$1
	_body=$2
	observe "task=$_label projection=$(printf '%s' "$_body" | jq -c \
		'{state,result,failure_reason,started_at,job_created_at,node_name,agent_id,last_status_seen}')"
	_caps=$(ls -1 "$EVIDENCE_DIR/$_label.pod."* 2>/dev/null || true)
	if [ -n "$_caps" ]; then
		# The exit code exists only once the container terminated: the newest
		# capture that carries one is the answer. The log lines are unioned
		# across captures — an early look can catch the daemon's refusal while
		# the pod is still up, and the last look before the Job vanished holds
		# none.
		_pod=$(sed -n 's/^pod=//p' $(printf '%s\n' "$_caps" | tail -1) | head -1)
		_exit=$(for _f in $_caps; do sed -n 's/^container=/container=/p' "$_f"; done | grep 'exit=[0-9]' | tail -1)
		observe "task=$_label pod: pod=${_pod:-unknown} ${_exit:-<no terminated container status>}"
		_union="$EVIDENCE_DIR/$_label.loglines"
		: >"$_union"
		for _f in $_caps; do
			awk '/^log / { inside = 1; next } /^container=/ { inside = 0 } inside' "$_f" >>"$_union" 2>/dev/null || true
		done
		awk '!seen[$0]++' "$_union" >"$_union.uniq" 2>/dev/null || : >"$_union.uniq"
		while IFS= read -r _line; do
			[ -z "$_line" ] || observe "task=$_label pod log: $_line"
		done <"$_union.uniq"
		rm -f "$_union" "$_union.uniq"
	else
		observe "task=$_label pod: no observation — the Job was already reclaimed when the carrier looked (settlement deletes it); run this carrier while the window is open to capture the log and the exit code"
	fi
	_agent=$(field "$_body" '.agent_id')
	if [ -n "$_agent" ] && [ -n "$MULTICA_TEST" ] && [ -x "$MULTICA_TEST" ]; then
		_run=$("$MULTICA_TEST" agent tasks "$_agent" --output json 2>/dev/null |
			jq -c --arg t "$_label" \
				'[.. | objects | select(((.id? // "") == $t) or ((.task_id? // "") == $t))][0] // empty' \
				2>/dev/null) || _run=
		if [ -n "$_run" ]; then
			observe "task=$_label server_run: $_run"
		else
			observe "task=$_label server_run: unavailable (agent $_agent lists no run for the task)"
		fi
	else
		observe "task=$_label server_run: unavailable (no ${MULTICA_TEST:-multica-test wrapper})"
	fi
}

# Wait for the window to close, capturing the pods while they are still there.
deadline=$(( $(date +%s) + WAIT_SECONDS ))
while :; do
	one=$(read_entry "$TASK1")
	two=$(read_entry "$TASK2")
	capture_task "$TASK1"
	capture_task "$TASK2"
	state1=$(field "$one" '.state')
	state2=$(field "$two" '.state')
	if [ "$state1" = terminal ] && [ "$state2" = terminal ]; then break; fi
	if [ "$(date +%s)" -ge "$deadline" ]; then
		blocked "the tasks did not both settle within ${WAIT_SECONDS}s (TASK1=${state1:-not visible}, TASK2=${state2:-not visible}) — the concurrency window has not closed"
	fi
	sleep "$POLL_SECONDS"
done
observe "task1=$one"
observe "task2=$two"

# The case's precondition: both tasks ran on one node (the pin decides it, the
# projection proves it). Checked before the verdict so a FAIL always means the
# window held.
node1=$(field "$one" '.node_name')
node2=$(field "$two" '.node_name')
[ -n "$node1" ] && [ "$node1" = "$node2" ] ||
	blocked "the two tasks did not share a node ($node1 vs $node2) — the case's precondition does not hold"
[ -n "$NODE" ] || NODE=$node1
[ "$NODE" = "$node1" ] || blocked "NODE=$NODE, but both tasks ran on $node1"
observe "node=$NODE"

# In-flight window: a task that ran begins at its start; a task whose daemon was
# refused before it could start begins at its Job's creation — the earliest
# instant it was in flight. The end is the task's terminal instant in Foreman's
# log: `task.terminal` for a verdict written through the terminal face, and
# `task.failed_compensated` for a failure Foreman judged itself (the refused
# daemon's Job lands there). Both carry RFC3339Nano .ts, so the comparison runs
# on whole seconds derived in jq — never on the raw strings.
begin_of() {
	_b=$(field "$1" '.started_at')
	[ -n "$_b" ] || _b=$(field "$1" '.job_created_at')
	case $_b in 0001-01-01*) _b= ;; esac
	printf '%s' "$_b"
}
logs=$(kubectl -n "$FOREMAN_NS" logs deploy/foreman --since="$LOG_SINCE" 2>/dev/null) ||
	blocked "cannot read the Foreman log"
terminal_of() {
	printf '%s\n' "$logs" | jq -Rr --arg t "$1" '
		fromjson?
		| select(.task_id == $t)
		| select(.msg == "task.terminal" or .msg == "task.failed_compensated")
		| .ts' | head -1
}
epoch() {
	printf '%s' "$1" | jq -Rr 'sub("\\.[0-9]+"; "") | sub("\\+00:00$"; "Z") | fromdateiso8601' ||
		return 1
}
begin1=$(begin_of "$one")
begin2=$(begin_of "$two")
[ -n "$begin1" ] && [ -n "$begin2" ] ||
	blocked "a task has neither a started_at nor a job_created_at — the overlap cannot be established"
end1=$(terminal_of "$TASK1")
end2=$(terminal_of "$TASK2")
[ -n "$end1" ] && [ -n "$end2" ] ||
	blocked "a terminal log line is missing from the $LOG_SINCE window (task.terminal or task.failed_compensated)"
b1=$(epoch "$begin1") || blocked "cannot parse the begin of TASK1 '$begin1'"
b2=$(epoch "$begin2") || blocked "cannot parse the begin of TASK2 '$begin2'"
e1=$(epoch "$end1") || blocked "cannot parse task.terminal ts '$end1'"
e2=$(epoch "$end2") || blocked "cannot parse task.terminal ts '$end2'"
later_begin=$b1
if [ "$b2" -gt "$later_begin" ]; then later_begin=$b2; fi
earlier_end=$e1
if [ "$e2" -lt "$earlier_end" ]; then earlier_end=$e2; fi
observe "begins[$begin1,$begin2] terminals[$end1,$end2]"
if [ "$later_begin" -ge "$earlier_end" ]; then
	blocked "the two tasks were not in flight together — the concurrency window was never constructed"
fi

# The verdict on the case's own criterion: both tasks succeed. A task that
# settled any other way is a failure of TC-tech-cache-02 as written (任务全部
# 成功), and the failure is reported with what makes it decidable — the
# refused daemon's own line, the container's exit code and the server record.
result1=$(field "$one" '.result')
result2=$(field "$two" '.result')
if [ "$result1" != completed ] || [ "$result2" != completed ]; then
	[ "$result1" = completed ] || print_evidence "$TASK1" "$one"
	[ "$result2" = completed ] || print_evidence "$TASK2" "$two"
	fail "the window held on $NODE but not every task succeeded (TASK1=$result1, TASK2=$result2) — the case's criterion is 任务全部成功; the observations above are the failed task's own record"
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
