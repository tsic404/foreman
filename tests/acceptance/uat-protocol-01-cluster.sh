#!/bin/sh
# TC-uat-protocol-01 — G2: a Multica instance is taken over without changing
# anything upstream. The carrier compares the running test-environment objects
# with the pinned test-infra manifest (images are the upstream identity face),
# checks the server's version anchor, then follows one real task through
# registration, claim, start and report. The agent-to-server passthrough is not
# judged here: its evidence is AC-15's cases.
#
#   TASK_ID=<id> ./uat-protocol-01-cluster.sh
# TASK_ID is the task this round dispatched after wiring. The carrier takes it
# as given and never falls back to the projection: a completed task from an
# earlier round must not satisfy a run whose job is to judge this wiring.
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

. "$(dirname "$0")/uat-lib.sh"

TASK=${TASK_ID:-}

need kubectl
need curl
need jq

[ $# -eq 0 ] || blocked "usage: $0 (TASK_ID=<id>, no positional arguments)"
[ -n "$TASK" ] || blocked "TASK_ID is required (the task this round dispatched after wiring)"

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

# ---- Claim, start, report --------------------------------------------------
body=$(wait_terminal "$TASK" 300) ||
	blocked "task $TASK did not reach terminal within 300s"
observe "task=$body"
printf '%s' "$body" | jq -e '.result == "completed"' >/dev/null ||
	fail "task $TASK result=$(printf '%s' "$body" | jq -r .result), want completed"
for key in daemon_registered_at started_at; do
	value=$(printf '%s' "$body" | jq -r ".$key // empty")
	[ -n "$value" ] || fail "$key is empty — the claim/start chain did not complete"
done

run=$(server_run "$TASK") ||
	blocked "cannot list the test agent's runs (run ./reset.sh and ./bind-agent.sh first)"
[ -n "$run" ] || blocked "the test server has no run for task $TASK — the server-side face is unavailable"
status=$(run_status "$run")
case $status in
*completed* | *succeeded* | *success*) : ;;
*) fail "the server-side run for $TASK is '$status', want completed: $run" ;;
esac
observe "server run: $run"
echo "PASS: TC-uat-protocol-01 (upstream images unchanged at $version; task $TASK registered, claimed, started and reported)"
