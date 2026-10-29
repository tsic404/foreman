#!/bin/sh
# TC-uat-protocol-01 — G2: a Multica instance is taken over without changing
# anything upstream. The carrier compares the running test-environment objects
# with the pinned test-infra manifest (images are the upstream identity face),
# checks the server's version anchor, then follows one real task through
# registration, claim, start, report and the agent-api passthrough.
#
#   ./uat-protocol-01-cluster.sh snapshot    # before triggering the task
#   [TASK_ID=<id>] ./uat-protocol-01-cluster.sh
# The passthrough judgement is the agent-api 2xx counter's delta across the
# task window (the counter carries no task label by design), so the snapshot
# must be taken before the task is triggered. FORWARD_BEFORE overrides it.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

TASK=${TASK_ID:-}
LOG_SINCE=${LOG_SINCE:-6h}
STATE=${STATE:-$HOME/.foreman-uat-protocol-01}
ACTION=${1:-}

need kubectl
need curl
need jq

# agent_api_2xx: the sum of the agent-api passthrough counter's 2xx samples.
# The label order is not assumed; the counter has no task label (§9.2
# cardinality), which is why the window is measured as a delta.
agent_api_2xx() {
	_m=$(metrics)
	printf '%s\n' "$_m" | awk '
		$0 ~ /^foreman_forward_total\{/ && index($0, "endpoint=\"agent-api\"") > 0 && $0 ~ /code="2[0-9][0-9]"/ { sum += $NF }
		END { printf "%.0f\n", sum + 0 }'
}

case $ACTION in
snapshot)
	agent_api_2xx >"$STATE"
	echo "snapshot: agent-api 2xx counter = $(cat "$STATE") (trigger the task now)"
	exit 0
	;;
"") ;;
*) blocked "usage: $0 [snapshot]" ;;
esac

# ---- Upstream unmodified: the running images are the pinned manifest's -----
# Every image deploy.yaml pins must be running. The local model backend lives in
# its own manifest because it is only the model source of a round without an
# endpoint: its images are admitted, not required — an endpoint round never
# deploys them. Nothing outside the two pinned manifests may run.
manifest="$TEST_MULTICA_DIR/deploy.yaml"
model_manifest="$TEST_MULTICA_DIR/model-backend.yaml"
[ -r "$manifest" ] || blocked "no test-infra manifest at $manifest"
[ -r "$model_manifest" ] || blocked "no test-infra model backend manifest at $model_manifest"
kubectl get namespace "$TEST_MULTICA_NS" >/dev/null 2>&1 ||
	blocked "namespace $TEST_MULTICA_NS does not exist — deploy the test environment first"
pods_json=$(kubectl -n "$TEST_MULTICA_NS" get pods -o json) ||
	blocked "cannot list pods in $TEST_MULTICA_NS"
expected=$(sed -n 's/^[[:space:]]*image:[[:space:]]*//p' "$manifest" | tr -d '"' | sort -u)
admitted=$(cat "$manifest" "$model_manifest" |
	sed -n 's/^[[:space:]]*image:[[:space:]]*//p' | tr -d '"' | sort -u)
running=$(printf '%s' "$pods_json" |
	jq -r '.items[] | (.spec.containers[]?.image, .spec.initContainers[]?.image)' | sort -u)
observe "manifest images: $(printf '%s' "$expected" | tr '\n' ' ')"
observe "running images: $(printf '%s' "$running" | tr '\n' ' ')"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
printf '%s\n' "$expected" >"$tmp/expected"
printf '%s\n' "$admitted" >"$tmp/admitted"
printf '%s\n' "$running" >"$tmp/running"
missing=$(comm -23 "$tmp/expected" "$tmp/running")
extra=$(comm -13 "$tmp/admitted" "$tmp/running")
[ -z "$missing" ] || fail "manifest images not running (upstream was changed): $missing"
[ -z "$extra" ] || fail "running images not in the pinned manifests (upstream was changed): $extra"

config=$(curl -fsS -m 30 "$TEST_MULTICA_SERVER_URL/api/config") ||
	blocked "cannot read $TEST_MULTICA_SERVER_URL/api/config"
version=$(printf '%s' "$config" | jq -r '.server_version // empty')
[ "$version" = "$TEST_MULTICA_VERSION_ANCHOR" ] ||
	blocked "the test server reports $version, the artifact anchor is $TEST_MULTICA_VERSION_ANCHOR"
observe "upstream: images match the manifest, server_version=$version"

# ---- Registration -----------------------------------------------------------
runtimes=$(mcli runtime list --output json) ||
	blocked "the test-multica runtime list is unreadable"
omp=$(printf '%s' "$runtimes" | jq -c '[.[] | select(.provider == "omp")]')
count=$(printf '%s' "$omp" | jq 'length')
[ "$count" -eq 1 ] || fail "expected exactly 1 provider=omp runtime, found $count: $omp"
observe "runtime: $omp"
online=$(printf '%s' "$omp" | jq -r '.[0] | if has("online") then .online else "unknown" end')
[ "$online" != "false" ] || fail "the provider=omp runtime is offline: $omp"

# ---- Claim, start, report, passthrough -------------------------------------
if [ -z "$TASK" ]; then
	TASK=$(task_list | jq -r '[.[] | select(.state == "terminal" and .result == "completed")]
		| sort_by(.started_at) | last | .task_id // empty')
	[ -n "$TASK" ] ||
		blocked "no completed task in the live projection — pass TASK_ID (a task that ran after wiring)"
fi
body=$(wait_terminal "$TASK" 300) ||
	blocked "task $TASK did not reach terminal within 300s"
observe "task=$body"
printf '%s' "$body" | jq -e '.result == "completed"' >/dev/null ||
	fail "task $TASK result=$(printf '%s' "$body" | jq -r .result), want completed"
for key in daemon_registered_at started_at; do
	value=$(printf '%s' "$body" | jq -r ".$key // empty")
	[ -n "$value" ] || fail "$key is empty — the claim/start chain did not complete"
done

before=${FORWARD_BEFORE:-}
if [ -z "$before" ] && [ -r "$STATE" ]; then
	before=$(cat "$STATE")
fi
[ -n "$before" ] ||
	blocked "no agent-api 2xx baseline — run '$0 snapshot' before triggering the task (or set FORWARD_BEFORE)"
case $before in '' | *[!0-9]*) blocked "the agent-api 2xx baseline is not a number: '$before'" ;; esac
after=$(agent_api_2xx)
observe "passthrough: agent-api 2xx before=$before after=$after delta=$((after - before))"
[ "$after" -gt "$before" ] ||
	fail "the agent-api 2xx counter did not move across task $TASK (before=$before after=$after) — the passthrough is not bound to this task"
rm -f "$STATE"

run=$(server_run "$TASK") ||
	blocked "cannot list the test agent's runs (run ./reset.sh and ./bind-agent.sh first)"
[ -n "$run" ] || blocked "the test server has no run for task $TASK — the server-side face is unavailable"
status=$(run_status "$run")
case $status in
*completed* | *succeeded* | *success*) : ;;
*) fail "the server-side run for $TASK is '$status', want completed: $run" ;;
esac
observe "server run: $run"
echo "PASS: TC-uat-protocol-01 (upstream images unchanged at $version; task $TASK registered, claimed, started, reported and passed agent-api)"
