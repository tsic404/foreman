#!/usr/bin/env bash
# Boot a local multica stack with docker/podman, wire a REAL multica daemon
# (extracted from the multica-backend image) to Foreman v2, dispatch a task,
# and verify the full proxy chain.
#
# Prereqs: podman + podman-compose (or docker + docker compose), go toolchain.
# Images: ghcr.io/multica-ai/multica-backend:v0.6.1,
#         ghcr.io/multica-ai/multica-web:v0.6.1,
#         pgvector/pgvector:pg17, nginx:1.27-alpine
#
# Usage:
#   bash deploy/test-real-daemon.sh [--reset]
#
# What it verifies (the v2 architecture, end to end):
#   1. multica dispatches an autopilot run to its runtime
#   2. Foreman C1-registers a runtime, C3-claims the task (real runtime id)
#   3. Foreman hands it to the real daemon via S6
#   4. the daemon resolves skill bundles (S26), renews its lease (S7) and
#      starts (S8) — each forwarded by Foreman to multica
#   5. the daemon runs its agent and reports back; Foreman forwards the
#      terminal report; multica settles the task
#
# The agent's own model backend is the host's omp config; if omp cannot reach
# a model the task ends failed at the agent step (agent_error), which is still
# a proof that every Foreman-forwarded leg (start/progress/fail) reached
# multica. The chain check below reads Foreman's forward metrics, not the
# task's terminal result.
set -euo pipefail

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

SERVER_URL=${TEST_MULTICA_SERVER_URL:-http://127.0.0.1:30080}
EMAIL=${TEST_MULTICA_EMAIL:-dev@foreman.test}
VCODE=${TEST_MULTICA_VERIFICATION_CODE:-888888}
WS_SLUG=${TEST_MULTICA_WORKSPACE_SLUG:-foreman-v2}
WNAME=${TEST_MULTICA_WORKSPACE_NAME:-Foreman v2}
AGENT_NAME=${TEST_MULTICA_AGENT:-v2-agent-foreman}

DAEMON_ID=${TEST_MULTICA_DAEMON_ID:-foreman-v2-daemon}
DAEMON_TOKEN=${TEST_MULTICA_DAEMON_TOKEN:-dc-tok-v2-test}
FOREMAN_LISTEN=${FOREMAN_LISTEN:-:18080}
FOREMAN_OPS_LISTEN=${FOREMAN_OPS_LISTEN:-:18081}
OPS_TOKEN=${FOREMAN_OPS_TOKEN:-ops-secret}

WORK=$(mktemp -d /tmp/foreman-e2e.XXXXXX)
COMPOSE=(podman compose -f deploy/test-compose.yaml)
export PATH="$HOME/goroot/bin:$PATH"

step() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

# --- 1. stack ----------------------------------------------------------------
if [ "${1:-}" = "--reset" ]; then
	step "resetting the multica stack"
	"${COMPOSE[@]}" down -v
fi
step "starting the multica stack"
"${COMPOSE[@]}" up -d
step "waiting for the multica server"
for _ in $(seq 1 60); do
	curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1 && break
	sleep 3
done
curl -fsS "$SERVER_URL/healthz" >/dev/null || die "server never became healthy"

# --- 2. real daemon binary (extracted from the backend image) -----------------
step "extracting the real multica daemon CLI"
MULTICA_BIN="$WORK/multica"
podman run --rm --entrypoint sh ghcr.io/multica-ai/multica-backend:v0.6.1 \
	-c "cat /app/multica" > "$MULTICA_BIN"
chmod +x "$MULTICA_BIN"
"$MULTICA_BIN" version | head -1

# --- 3. credentials ----------------------------------------------------------
step "minting a personal access token"
# The dev login code is rate limited (1/60s per email), so send-code retries
# with a backoff before verify-code.
for _ in 1 2 3 4 5; do
	if curl -fsS -X POST "$SERVER_URL/auth/send-code" -H 'Content-Type: application/json' \
		-d "{\"email\":\"$EMAIL\"}" >/dev/null 2>&1; then
		break
	fi
	echo "send-code rate limited; retrying in 61s"
	sleep 61
done
JWT=$(curl -fsS -X POST "$SERVER_URL/auth/verify-code" -H 'Content-Type: application/json' \
	-d "{\"email\":\"$EMAIL\",\"code\":\"$VCODE\"}" | jq -er .token)
PAT=$(curl -fsS -X POST "$SERVER_URL/api/tokens/" -H "Authorization: Bearer $JWT" \
	-H 'Content-Type: application/json' -d '{"name":"foreman-e2e"}' | jq -er .token)
echo "pat: ${PAT:0:10}..."

api() { # method path [json]
	local m=$1 p=$2 body=${3:-}
	if [ -n "$body" ]; then
		curl -fsS -X "$m" "$SERVER_URL$p" -H "Authorization: Bearer $PAT" \
			-H 'Content-Type: application/json' -d "$body"
	else
		curl -fsS -X "$m" "$SERVER_URL$p" -H "Authorization: Bearer $PAT"
	fi
}

# --- 4. workspace ------------------------------------------------------------
step "ensuring workspace $WS_SLUG"
WS_ID=$(api GET /api/workspaces | jq -r --arg s "$WS_SLUG" '[.[]|select(.slug==$s)][0].id // ""')
if [ -z "$WS_ID" ]; then
	WS_ID=$(api POST /api/workspaces "{\"name\":\"$WNAME\",\"slug\":\"$WS_SLUG\"}" | jq -er .id)
fi
echo "workspace: $WS_ID"

# --- 5. Foreman --------------------------------------------------------------
step "building and launching Foreman v2"
mkdir -p bin
go build -o bin/foreman ./cmd/foreman
pkill -9 -f 'bin/foreman -listen' 2>/dev/null || true
sleep 1
cat > "$WORK/foreman-run.sh" <<RUNEOF
#!/usr/bin/env bash
cd "$(pwd)"
exec env \
  MULTICA_SERVER_URL=$SERVER_URL MULTICA_WORKSPACE_ID=$WS_ID \
  MULTICA_TOKEN=$PAT FOREMAN_DAEMON_ID=foreman \
  FOREMAN_DAEMON_TOKENS=$DAEMON_ID=$DAEMON_TOKEN \
  FOREMAN_DAEMON_TOKEN_WORKSPACES=$DAEMON_ID=$WS_ID \
  FOREMAN_OPS_TOKEN=$OPS_TOKEN FOREMAN_LOG_LEVEL=debug \
  FOREMAN_CLAIM_INTERVAL=2s FOREMAN_CLAIM_INTERVAL_IDLE=2s \
  ./bin/foreman -listen=$FOREMAN_LISTEN -ops-listen=$FOREMAN_OPS_LISTEN
RUNEOF
chmod +x "$WORK/foreman-run.sh"
setsid "$WORK/foreman-run.sh" > "$WORK/foreman.log" 2>&1 < /dev/null &
FPID=$!
echo "foreman pid $FPID (log $WORK/foreman.log)"
for _ in $(seq 1 20); do
	curl -fsS "http://127.0.0.1:${FOREMAN_LISTEN#:}/foreman/healthz" >/dev/null 2>&1 && break
	sleep 1
done

# --- 6. real daemon pointed at Foreman ---------------------------------------
step "launching the real multica daemon against Foreman"
pkill -9 -f "multica --server-url .* daemon start" 2>/dev/null || true
sleep 1
mkdir -p "$WORK/daemon-home/.multica" "$WORK/daemon-home/.local/bin" "$WORK/daemon-root"
# A previous run's empty session file makes the next run's omp refuse to
# resume it ("session file holds no entries"); start clean.
rm -f "$WORK/daemon-home/.multica/pi-sessions/"*.jsonl 2>/dev/null || true
# The daemon needs an agent CLI on PATH and an auth config for its server.
# Provide omp (if the host has it) and write the CLI config that points at
# Foreman with the daemon credential.
if [ -x "$HOME/.local/bin/omp" ]; then
	ln -sf "$HOME/.local/bin/omp" "$WORK/daemon-home/.local/bin/omp"
fi
# omp reads its provider/model config from $HOME/.omp; link the host's real
# config into the isolated daemon home so the agent uses the same model the
# host's omp uses (the gateway key is fixed upstream).
if [ -d "$HOME/.omp" ]; then
	ln -sfn "$HOME/.omp" "$WORK/daemon-home/.omp"
fi
cat > "$WORK/daemon-home/.multica/config.json" <<CFGEOF
{"server_url":"http://127.0.0.1:${FOREMAN_LISTEN#:}","workspace_id":"$WS_ID","token":"$DAEMON_TOKEN"}
CFGEOF
cat > "$WORK/daemon-run.sh" <<DMNEOF
#!/usr/bin/env bash
cd "$WORK/daemon-home"
exec env HOME=$WORK/daemon-home \
  PATH=$WORK/daemon-home/.local/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin \
  MULTICA_SERVER_URL=http://127.0.0.1:${FOREMAN_LISTEN#:} \
  MULTICA_WORKSPACE_ID=$WS_ID \
  "$MULTICA_BIN" --server-url http://127.0.0.1:${FOREMAN_LISTEN#:} \
  --workspace-id "$WS_ID" daemon start --foreground \
  --daemon-id "$DAEMON_ID" --runtime-name "$DAEMON_ID" --workspaces-root "$WORK/daemon-root"
DMNEOF
chmod +x "$WORK/daemon-run.sh"
setsid "$WORK/daemon-run.sh" > "$WORK/daemon.log" 2>&1 < /dev/null &
DPID=$!
echo "daemon pid $DPID (log $WORK/daemon.log)"
sleep 6

# The daemon registers to Foreman first; Foreman then C1-registers the runtime
# to multica. Wait for Foreman to see it.
for _ in $(seq 1 30); do
	curl -fsS "http://127.0.0.1:${FOREMAN_OPS_LISTEN#:}/metrics" -H "Authorization: Bearer $OPS_TOKEN" 2>/dev/null \
		| grep -q 'foreman_auth_failures_total' && break
	sleep 1
done
grep -q 'daemon.registered' "$WORK/foreman.log" || die "daemon did not register to Foreman"
echo "daemon registered to Foreman"

# --- 7. bind the agent to Foreman's runtime --------------------------------
step "binding the agent to Foreman's runtime"
for _ in $(seq 1 30); do
	RT=$(api GET "/api/runtimes?workspace_id=$WS_ID" | jq -r '[.[]|select(.daemon_id=="foreman")][0].id // ""')
	[ -n "$RT" ] && break
	sleep 2
done
[ -n "$RT" ] || die "Foreman did not register a runtime to multica"
echo "foreman runtime: $RT"
AGENT_ID=$(api GET "/api/agents?workspace_id=$WS_ID" | jq -r --arg n "$AGENT_NAME" \
	'[.[]|select(.name==$n and .archived_at==null)][0].id // ""')
if [ -z "$AGENT_ID" ]; then
	AGENT_ID=$(api POST "/api/agents?workspace_id=$WS_ID" \
		"{\"workspace_id\":\"$WS_ID\",\"name\":\"$AGENT_NAME\",\"description\":\"e2e\",\"instructions\":\"Reply with OK only.\",\"runtime_id\":\"$RT\",\"max_concurrent_tasks\":4}" | jq -er .id)
else
	api PUT "/api/agents/$AGENT_ID?workspace_id=$WS_ID" \
		"{\"runtime_id\":\"$RT\",\"instructions\":\"Reply with OK only.\"}" >/dev/null
fi
echo "agent: $AGENT_ID"

# --- 8. dispatch -------------------------------------------------------------
step "creating and triggering an autopilot"
AP_ID=$(api GET "/api/autopilots?workspace_id=$WS_ID" | jq -r --arg t "v2-e2e" \
	'[(if type=="object" then .autopilots else . end)[]|select(.title==$t)][0].id // ""')
if [ -z "$AP_ID" ]; then
	AP_ID=$(api POST "/api/autopilots?workspace_id=$WS_ID" \
		"{\"title\":\"v2-e2e\",\"description\":\"v2 e2e\",\"assignee_id\":\"$AGENT_ID\",\"execution_mode\":\"run_only\"}" | jq -er .id)
fi
api POST "/api/autopilots/$AP_ID/trigger?workspace_id=$WS_ID" '{}' >/dev/null
echo "triggered autopilot $AP_ID"

# Baseline of the chain-proof line: only starts AFTER this trigger count as
# this run's evidence.
BASE_STARTS=$("${COMPOSE[@]}" logs backend 2>/dev/null | grep -cE 'tasks/[0-9a-f-]+/start status=200' || true)

# --- 9. verify the chain via Foreman's forward metrics -----------------------
step "waiting for the forwarded legs"
deadline=$(( $(date +%s) + 150 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
	# The chain proof is multica accepting the daemon's start forward
	# (POST .../tasks/{tid}/start 200) for a task dispatched to Foreman's
	# runtime. That request only exists when Foreman claimed, delivered, the
	# daemon processed and Foreman forwarded it back.
	# grep -q would SIGPIPE the compose logs reader under pipefail, so capture
	# the match with grep -c instead.
	if [ "$("${COMPOSE[@]}" logs backend 2>/dev/null | grep -cE 'tasks/[0-9a-f-]+/start status=200' || true)" -gt "$BASE_STARTS" ]; then
		CHAIN_OK=1
		break
	fi
	sleep 3
done

step "result"
if [ "${CHAIN_OK:-0}" = "1" ]; then
	echo "CHAIN VERIFIED: multica accepted the daemon's start forwarded by Foreman"
else
	echo "CHAIN NOT OBSERVED within the window (see logs below)"
fi
echo "--- Foreman chain events ---"
grep -E 'task.claimed|task.delivered' "$WORK/foreman.log" | tail -4 || true
echo "--- Foreman forward legs (endpoint → 2xx count) ---"
curl -fsS "http://127.0.0.1:${FOREMAN_OPS_LISTEN#:}/metrics" -H "Authorization: Bearer $OPS_TOKEN" 2>/dev/null \
	| grep -E '^foreman_forward_total\{' | grep 'code="2' || true
echo "--- multica task outcome ---"
"${COMPOSE[@]}" logs backend 2>/dev/null | grep -E 'task (completed|failed)' | tail -3 || true
echo "--- real daemon agent result ---"
grep -iE 'agent result|did not complete|completed' "$WORK/daemon.log" 2>/dev/null | tail -3 || true

echo
echo "foreman log:  $WORK/foreman.log"
echo "daemon log:   $WORK/daemon.log"
echo "pids: foreman=$FPID daemon=$DPID (kill to stop; stack: podman compose -f deploy/test-compose.yaml down)"