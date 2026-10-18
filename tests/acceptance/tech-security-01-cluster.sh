#!/bin/sh
# TC-tech-security-01 — the runtime half of AC-13's permission boundary: the
# agent runs as the fixed non-root user, prepare's chown took effect, the
# credential file is the single-file read-only mount with the fixed mode, and
# the mount face is the template's. The task Job must still be running. The
# manifest half is tech_security_01_permission_boundary_test.go.
#
#   TASK_ID=<task id> ./tech-security-01-cluster.sh   (or pass the id as $1)
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
TASK_ID=${TASK_ID:-${1:-}}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

if [ -z "$TASK_ID" ]; then
	TASK_ID=$(kubectl -n "$JOB_NS" get jobs -l foreman.tsic.top/task-id -o json 2>/dev/null |
		jq -r '[.items[] | select((.status.active // 0) > 0)]
			| sort_by(.metadata.creationTimestamp) | first
			| .metadata.labels["foreman.tsic.top/task-id"] // empty') ||
		blocked "cannot list task Jobs in $JOB_NS"
fi
[ -n "$TASK_ID" ] || blocked "no running task Job in $JOB_NS — trigger a task first"

JOB_NAME=$(kubectl -n "$JOB_NS" get jobs -l "foreman.tsic.top/task-id=$TASK_ID" \
	-o jsonpath='{.items[0].metadata.name}' 2>/dev/null) ||
	blocked "cannot read the task $TASK_ID's Job in $JOB_NS"
[ -n "$JOB_NAME" ] || blocked "no Job carries foreman.tsic.top/task-id=$TASK_ID"
observe "task=$TASK_ID job=$JOB_NAME"

# ---- Step 3: process identity and prepare's ownership correction -----------
rc=0
runtime=$(kubectl -n "$JOB_NS" exec "job/$JOB_NAME" -c agent -- sh -c '
  id -u
  stat -c "%u:%g %n" /home/agent /home/agent/.multica/profiles/foreman-job /state/workspaces
  stat -c "%a %u:%g %n" /home/agent/.multica/profiles/foreman-job/config.json
  stat -c "%d %n" /home/agent /home/agent/.multica/profiles/foreman-job
') || rc=$?
observe "runtime: ${runtime:-<empty>}"
if [ "$rc" -ne 0 ]; then
	blocked "cannot exec into $JOB_NAME (the pod may still be starting); exit $rc"
fi

uid=$(printf '%s\n' "$runtime" | sed -n '1p' | tr -d '[:space:]')
[ "$uid" = "1000" ] || fail "id -u = '$uid', want 1000 (agent is not the fixed non-root user)"

i=2
while [ "$i" -le 4 ]; do
	line=$(printf '%s\n' "$runtime" | sed -n "${i}p")
	case $line in
	1000:1000\ *) : ;;
	*) fail "ownership line '$line' is not 1000:1000 (prepare's chown did not take effect)" ;;
	esac
	i=$((i + 1))
done

cred=$(printf '%s\n' "$runtime" | sed -n '5p')
case $cred in
400\ 1000:1000\ *) : ;;
*) fail "credential file '$cred' is not mode 400 owned by 1000:1000" ;;
esac

device_a=$(printf '%s\n' "$runtime" | sed -n '6p' | awk '{print $1}')
device_b=$(printf '%s\n' "$runtime" | sed -n '7p' | awk '{print $1}')
[ -n "$device_a" ] && [ "$device_a" = "$device_b" ] ||
	fail "device numbers differ ($device_a vs $device_b): the profile directory is not in the home volume"

# ---- Step 4: the mount face ------------------------------------------------
rc=0
mounts=$(kubectl -n "$JOB_NS" exec "job/$JOB_NAME" -c agent -- sh -c '
  echo "home=$(grep -c " /home/agent " /proc/mounts)"
  echo "profile=$(grep -c " /home/agent/.multica/profiles/foreman-job " /proc/mounts)"
  echo "config=$(grep -c " /home/agent/.multica/profiles/foreman-job/config.json " /proc/mounts)"
  echo "config_line=$(grep " /home/agent/.multica/profiles/foreman-job/config.json " /proc/mounts)"
  echo "workspaces=$(grep -c " /state/workspaces " /proc/mounts)"
  echo "tmp=$(grep -c " /tmp " /proc/mounts)"
') || rc=$?
observe "mounts: ${mounts:-<empty>}"
[ "$rc" -eq 0 ] || blocked "cannot read /proc/mounts in $JOB_NAME (exit $rc)"

value() { printf '%s\n' "$mounts" | sed -n "s/^$1=//p"; }
[ "$(value home)" = "1" ] || fail "/home/agent is not exactly one mount entry (got '$(value home)')"
[ "$(value profile)" = "0" ] || fail "the profile directory is a separate mount (got '$(value profile)')"
[ "$(value config)" = "1" ] || fail "config.json is not exactly one mount entry (got '$(value config)')"
# /proc/mounts is "device mountpoint fstype options dump pass": the options
# live in $4 ($3 is the filesystem type). The raw line stays in the observation.
config_opts=$(value config_line | awk '{print $4}')
case ",$config_opts," in
*,ro,*) : ;;
*) fail "the credential mount is not read-only: $(value config_line)" ;;
esac
[ "$(value workspaces)" = "1" ] || fail "/state/workspaces is not exactly one mount entry (got '$(value workspaces)')"
[ "$(value tmp)" = "1" ] || fail "/tmp is not exactly one mount entry (got '$(value tmp)')"

echo "PASS: TC-tech-security-01 (task $TASK_ID)"
