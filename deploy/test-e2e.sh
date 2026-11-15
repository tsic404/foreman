#!/usr/bin/env bash
# Boot local multica (compose) + Foreman v2 + a mock resident daemon, then run
# an end-to-end dispatch check.
#
# Prereqs on the host: podman + podman-compose; the multica images pulled.
# Usage:
#   bash deploy/test-e2e.sh [--reset]        # --reset drops the stack and rebuilds
#
# What it does:
#   1. up the compose stack (postgres + backend + web + proxy)
#   2. wait for /healthz
#   3. mint a PAT (send-code → verify-code → /api/tokens)
#   4. create the test workspace and an agent on it
#   5. launch Foreman v2 pointed at the local server, with one daemon credential
#   6. launch a mock resident daemon: register → heartbeat → claim loop
#   7. dispatch a task to the agent; wait for Foreman to claim + deliver it
set -euo pipefail

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

SERVER_URL=http://127.0.0.1:30080
ENTRY_URL=http://127.0.0.1:30300
EMAIL=${TEST_MULTICA_EMAIL:-dev@foreman.test}
VCODE=888888
WS=${TEST_MULTICA_WORKSPACE_SLUG:-foreman-v2-test}
WNAME=${TEST_MULTICA_WORKSPACE_NAME:-Foreman v2 Test}
AGENT=${TEST_MULTICA_AGENT:-foreman-v2-agent}
DAEMON_ID=worker-1
DAEMON_TOKEN=${DAEMON_TOKEN:-tcc-foreman-v2-daemon}
FOREMAN_DAEMON_ID=foreman

COMPOSE=(podman compose -f deploy/test-compose.yaml)

step() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }

# --- 1. stack ----------------------------------------------------------------
if [ "${1:-}" = "--reset" ]; then
	step "resetting the multica stack"
	"${COMPOSE[@]}" down -v
fi
step "starting the multica stack"
"${COMPOSE[@]}" up -d
step "waiting for the multica server"
for _ in $(seq 1 45); do
	if curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then break; fi
	sleep 4
done
curl -fsS "$SERVER_URL/healthz" >/dev/null || { echo "server never became healthy" >&2; exit 1; }

# --- 3. credentials ----------------------------------------------------------
step "minting a personal access token"
send_code() {
	curl -fsS -X POST "$SERVER_URL/auth/send-code" \
		-H 'Content-Type: application/json' -d "{\"email\":\"$EMAIL\"}" || return $?
}
for _ in 1 2 3; do
	send_code && break
	sleep 3
done
JWT=$(curl -fsS -X POST "$SERVER_URL/auth/verify-code" \
	-H 'Content-Type: application/json' \
	-d "{\"email\":\"$EMAIL\",\"code\":\"$VCODE\"}" | jq -er .token)
PAT=$(curl -fsS -X POST "$SERVER_URL/api/tokens/" \
	-H "Authorization: Bearer $JWT" \
	-H 'Content-Type: application/json' \
	-d '{"name":"foreman-v2-e2e"}' | jq -er .token)
echo "pat issued: ${PAT:0:8}…"

api() { # method path [json]
	local m=$1 p=$2 body=${3:-}
	if [ -n "$body" ]; then
		curl -fsS -X "$m" "$SERVER_URL$p" -H "Authorization: Bearer $PAT" \
			-H 'Content-Type: application/json' -d "$body"
	else
		curl -fsS -X "$m" "$SERVER_URL$p" -H "Authorization: Bearer $PAT"
	fi
}

# --- 4. workspace + agent ----------------------------------------------------
step "ensuring workspace $WS"
WS_ID=$(api GET /api/workspaces | jq -r --arg s "$WS" \
	'[.[] | select(.slug == $s)][0].id // ""')
if [ -z "$WS_ID" ]; then
	WS_ID=$(api POST /api/workspaces "{\"name\":\"$WNAME\",\"slug\":\"$WS\"}" | jq -er .id)
fi
echo "workspace: $WS_ID"

# The agent must be bound to a runtime the server knows. Foreman will register
# that runtime when the mock daemon registers to it; but CreateAgent needs a
# runtime_id now. Use a seed registration that we delete right after (mimics
# reset.sh), so the agent starts unbound and Foreman's later C1 supplies the
# real runtime.
step "ensuring agent $AGENT"
AGENT_ID=$(api GET /api/agents | jq -r --arg a "$AGENT" \
	'[.[] | select(.name == $a and (.archived_at == null) and .runtime_id != null)][0].id // ""')
if [ -z "$AGENT_ID" ]; then
	SEED_RT=$(api POST /api/daemon/register \
		"{\"workspace_id\":\"$WS_ID\",\"daemon_id\":\"seed-v2\",\"legacy_daemon_ids\":[],\"device_name\":\"seed-v2\",\"cli_version\":\"0.6.1\",\"launched_by\":\"\",\"runtimes\":[{\"name\":\"seed-omp\",\"type\":\"omp\",\"version\":\"18.6.1\",\"status\":\"online\",\"profile_id\":\"\"}],\"failed_profiles\":[]}" |
		jq -er '.runtimes[0].id')
	AGENT_ID=$(api POST /api/agents \
		"{\"name\":\"$AGENT\",\"description\":\"foreman v2 e2e agent\",\"instructions\":\"Reply with OK only.\",\"runtime_id\":\"$SEED_RT\",\"max_concurrent_tasks\":6}" |
		jq -er .id)
	echo "created agent $AGENT ($AGENT_ID), seed runtime $SEED_RT"
fi

# --- 5. Foreman --------------------------------------------------------------
step "launching Foreman v2"
pkill -f 'bin/foreman' 2>/dev/null || true
sleep 1
mkdir -p bin
go build -o bin/foreman ./cmd/foreman
DAEMON_WORKSPACES="$DAEMON_ID=$WS_ID"
(
	set -euo pipefail
	export MULTICA_SERVER_URL="$SERVER_URL"
	export MULTICA_WORKSPACE_ID="$WS_ID"
	export MULTICA_TOKEN="$PAT"
	export FOREMAN_DAEMON_ID="$FOREMAN_DAEMON_ID"
	export FOREMAN_DAEMON_TOKENS="$DAEMON_ID=$DAEMON_TOKEN"
	export FOREMAN_DAEMON_TOKEN_WORKSPACES="$DAEMON_WORKSPACES"
	export FOREMAN_OPS_TOKEN="ops-v2-test-token"
	export FOREMAN_LOG_LEVEL=debug
	export FOREMAN_CLAIM_INTERVAL=2s
	export FOREMAN_CLAIM_INTERVAL_IDLE=2s
	exec ./bin/foreman -listen=:18080 -ops-listen=:18081
) > /tmp/foreman-v2.log 2>&1 &
echo $! > /tmp/foreman-v2.pid
echo "foreman pid $(cat /tmp/foreman-v2.pid) (log /tmp/foreman-v2.log)"

# --- 6. mock resident daemon -------------------------------------------------
step "launching mock daemon $DAEMON_ID"
cat > /tmp/foreman-mock-daemon.sh <<EOF
#!/usr/bin/env bash
set -u
F=http://127.0.0.1:18080
AUTH="Authorization: Bearer $DAEMON_TOKEN"
# register
curl -fsS -X POST "\$F/api/daemon/register" -H "\$AUTH" -H 'Content-Type: application/json' \
	-d '{"daemon_id":"$DAEMON_ID","workspace_id":"$WS_ID","runtimes":[{"name":"$DAEMON_ID","type":"omp","version":"18.6.1","status":"online","profile_id":""}]}' >/dev/null
echo "daemon registered"
while :; do
	# heartbeat
	curl -fsS -X POST "\$F/api/daemon/heartbeat" -H "\$AUTH" -H 'Content-Type: application/json' \
		-d '{"runtime_id":"$DAEMON_ID"}' >/dev/null 2>&1 || true
	# claim
	RESP=\$(curl -fsS -X POST "\$F/api/daemon/tasks/claim" -H "\$AUTH" -H 'Content-Type: application/json' \
		-d '{"daemon_id":"$DAEMON_ID","runtime_ids":["$DAEMON_ID"],"max_tasks":1}' 2>/dev/null || echo '{"tasks":[]}')
	TASKS=\$(printf '%s' "\$RESP" | jq '.tasks')
	if [ "\$(printf '%s' "\$TASKS" | jq length)" -gt 0 ]; then
		TID=\$(printf '%s' "\$TASKS" | jq -r '.tasks[0].id')
		echo "claimed task \$TID"
		# start
		curl -fsS -X POST "\$F/api/daemon/tasks/\$TID/start" -H "\$AUTH" \
			-H 'Content-Type: application/json' -d "\$TASKS" >/dev/null
		echo "started \$TID"
		sleep 2
		# complete
		curl -fsS -X POST "\$F/api/daemon/tasks/\$TID/complete" -H "\$AUTH" \
			-H 'Content-Type: application/json' \
			-d '{"status":"completed","session_id":"sess-v2-1"}' >/dev/null
		echo "completed \$TID"
	fi
	sleep 1
done
EOF
chmod +x /tmp/foreman-mock-daemon.sh
nohup bash /tmp/foreman-mock-daemon.sh > /tmp/foreman-mock-daemon.log 2>&1 &
echo $! > /tmp/foreman-mock-daemon.pid
echo "mock daemon pid $(cat /tmp/foreman-mock-daemon.pid) (log /tmp/foreman-mock-daemon.log)"

# --- 7. dispatch a task ------------------------------------------------------
step "dispatching a task"
sleep 3
TASK_ID=$(api POST /api/chat/sessions "{\"agent_id\":\"$AGENT_ID\",\"title\":\"foreman-v2-e2e\"}" |
	jq -er .id)
echo "dispatched task/session: $TASK_ID"

step "waiting for foreman to claim + deliver"
for _ in $(seq 1 20); do
	claimed=$(grep -c 'task.claimed' /tmp/foreman-v2.log 2>/dev/null || true)
	if [ "$claimed" -ge 1 ]; then break; fi
	sleep 2
done
sleep 2
echo "--- foreman log tail ---"
tail -30 /tmp/foreman-v2.log
echo
echo "--- mock daemon log ---"
cat /tmp/foreman-mock-daemon.log