#!/bin/sh
# Reset the test multica cluster to its clean state — the reset entry of
# the test environment's 运维接口 (`README.md`), run before every
# acceptance round.
#
#   ./reset.sh                 # stop the instance, wipe rows, rebuild objects
#   ./reset.sh --print-token   # …and print the profile's token (wiring input)
#
# What it does, in order:
#
#   1. stop — scale the instance under test to 0: gate ② is a pre-start
#      assertion (the runtime list must be empty before the instance starts),
#      and a running instance would re-register the runtime this entry is
#      about to wipe;
#   2. credentials — ensure the CLI profile holds a working personal access
#      token for the test server (creating the account on first use);
#   3. workspace — ensure the test workspace exists and is selected;
#   4. wipe — delete every runtime row and task row of the test workspace
#      (可重置). The agent rows survive, unbound;
#   5. rebuild the test objects — writable repository, test agent (unbound),
#      chat entry. The autopilot is not here: the server refuses an autopilot
#      whose assignee agent has no runtime, and this entry leaves the runtime
#      list empty on purpose — ./bind-agent.sh creates it once the agent is
#      bound;
#   6. gate — ./selfcheck.sh: the environment gate ①②③ (the instance points at
#      the access point, the runtime list is empty, the server reports the
#      artifact anchor).
#
# It is idempotent: everything is looked up by name and only created when
# missing. The agent stays unbound on purpose — ./bind-agent.sh binds it to the
# runtime the instance registers.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need kubectl
need curl
need jq

print_token=0
for arg in "$@"; do
	case $arg in
	--print-token) print_token=1 ;;
	-h | --help)
		sed -n '2,29p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*) die "unknown argument: $arg" ;;
	esac
done

# --- 1. stop the instance under test -----------------------------------------

# The wipe below only sticks while nothing is registered: a running instance
# re-registers the runtime row it just lost, and gate ② asserts that list is
# empty *before* the instance starts.
stop_instance

# --- 2. credentials ----------------------------------------------------------

# A live code row is required even for the fixed development code, so the login
# is always send-code → verify-code.
login_token() {
	attempt=0
	while :; do
		status=$(curl -sS -m 30 -o /dev/null -w '%{http_code}' -X POST "$SERVER_URL/auth/send-code" \
			-H 'Content-Type: application/json' -d "{\"email\":\"$ACCOUNT_EMAIL\"}") ||
			die "send-code unreachable at $SERVER_URL"
		case $status in
		200) break ;;
		429)
			[ "$attempt" -lt 1 ] || die "login code is rate limited — retry in a minute"
			info "login code rate limited (1/60s) — waiting"
			sleep 61
			attempt=$((attempt + 1))
			;;
		*) die "send-code failed with HTTP $status" ;;
		esac
	done
	curl -fsS -m 30 -X POST "$SERVER_URL/auth/verify-code" -H 'Content-Type: application/json' \
		-d "{\"email\":\"$ACCOUNT_EMAIL\",\"code\":\"$VERIFICATION_CODE\"}" | jq -er .token
}

ensure_credentials() {
	if [ -r "$PROFILE_CONFIG" ]; then
		status=$(curl -sS -m 60 -o /dev/null -w '%{http_code}' "$SERVER_URL/api/me" \
			-H "Authorization: Bearer $(pat)") || die "cannot reach $SERVER_URL"
		case $status in
		200)
			# The profile is this component's own, so its address fields are
			# refreshed on every reset: the addresses can move under a server
			# that is still serving, and a stale one strands every consumer of
			# the profile — including the self-check below.
			tmp=$(mktemp)
			jq --arg server "$SERVER_URL" --arg app "$ENTRY_URL" \
				'.server_url = $server | .app_url = $app' "$PROFILE_CONFIG" >"$tmp" &&
				mv "$tmp" "$PROFILE_CONFIG"
			info "credentials ok (profile $PROFILE)"
			return 0
			;;
		401) info "profile token rejected by $SERVER_URL — re-issuing" ;;
		*) die "cannot verify the profile token (HTTP $status)" ;;
		esac
	fi
	info "issuing a credential for $ACCOUNT_EMAIL on $SERVER_URL"
	jwt=$(login_token) || die "login failed"
	token=$(curl -fsS -m 60 -X POST "$SERVER_URL/api/tokens/" -H "Authorization: Bearer $jwt" \
		-H 'Content-Type: application/json' -d '{"name":"foreman-daemon"}' | jq -er .token) ||
		die "personal access token creation failed"
	mkdir -p "$(dirname "$PROFILE_CONFIG")"
	umask 077
	jq -n --arg server "$SERVER_URL" --arg app "$ENTRY_URL" --arg token "$token" \
		'{server_url:$server, app_url:$app, token:$token}' >"$PROFILE_CONFIG"
	info "wrote profile $PROFILE ($PROFILE_CONFIG)"
}

# --- 3. workspace ------------------------------------------------------------

ensure_workspace() {
	# A profile that already names a workspace is the fast path — the workspace
	# list is the slowest endpoint on a loaded server — but the id is only
	# trusted after it reads back: a rebuilt environment (升级 = 整环境重建)
	# leaves the profile file behind while the workspace it names is gone.
	if [ -r "$PROFILE_CONFIG" ]; then
		ws=$(jq -r '.workspace_id // ""' "$PROFILE_CONFIG")
		if [ -n "$ws" ] && api GET "/api/workspaces/$ws" >/dev/null 2>&1; then
			info "workspace $ws (from profile)"
			return 0
		fi
	fi
	ws=$(api GET /api/workspaces |
		jq -r --arg slug "$WORKSPACE_SLUG" '[.[] | select(.slug == $slug)][0].id // ""')
	if [ -z "$ws" ]; then
		ws=$(api POST /api/workspaces \
			"{\"name\":\"$WORKSPACE_NAME\",\"slug\":\"$WORKSPACE_SLUG\"}" | jq -er .id) ||
			die "workspace creation failed"
		info "created workspace $WORKSPACE_SLUG ($ws)"
	else
		info "workspace $WORKSPACE_SLUG ($ws)"
	fi
	tmp=$(mktemp)
	jq --arg ws "$ws" '.workspace_id = $ws' "$PROFILE_CONFIG" >"$tmp" && mv "$tmp" "$PROFILE_CONFIG"
}

# --- 4. wipe -----------------------------------------------------------------

wipe_rows() {
	ws=$(workspace_id)
	# psql does not interpolate its own variables in -c commands (only on
	# stdin), so the id is pasted into the statement; the shape check is what
	# makes that safe — it also turns a corrupt profile into a clear error
	# instead of a wipe that silently matches nothing.
	case $ws in
	*[!0-9a-f-]*) die "workspace_id is not a UUID: $ws" ;;
	esac
	[ "${#ws}" -eq 36 ] || die "workspace_id is not a UUID: $ws"
	# agent.runtime_id is a RESTRICT foreign key: unbind before deleting the
	# runtime rows. Task rows are deleted by agent (their workspace is only
	# reachable through the agent).
	psql_exec "UPDATE agent SET runtime_id = NULL WHERE workspace_id = '$ws';
		DELETE FROM agent_task_queue
			WHERE agent_id IN (SELECT id FROM agent WHERE workspace_id = '$ws');
		DELETE FROM agent_runtime WHERE workspace_id = '$ws';" >/dev/null
	info "wiped runtime rows and task rows"
}

# --- 5. the test objects ------------------------------------------------------

ensure_repo() {
	if ! ws=$(api GET "/api/workspaces/$(workspace_id)"); then
		die "workspace $(workspace_id) is not readable on $SERVER_URL — remove $PROFILE_CONFIG and re-run"
	fi
	repos=$(printf '%s' "$ws" | jq -c '[.repos[]?.url]')
	if printf '%s' "$repos" | jq -e --arg url "$REPO_URL" 'index($url)' >/dev/null; then
		info "repository $REPO_URL"
		return 0
	fi
	api PATCH "/api/workspaces/$(workspace_id)" \
		"{\"repos\":[{\"url\":\"$REPO_URL\"}]}" >/dev/null
	info "registered repository $REPO_URL"
}

# A throwaway runtime registration: CreateAgent requires a runtime_id, and an
# unbound agent is the state the reset contract wants — so the agent is created
# on a seed runtime and the seed is deleted again. The delete has to cascade
# (it refuses while an agent is bound, and it is the cascade that unbinds).
seed_runtime() {
	ws=$(workspace_id)
	api POST /api/daemon/register \
		"{\"workspace_id\":\"$ws\",\"daemon_id\":\"reset-seed\",\"legacy_daemon_ids\":[],\"device_name\":\"reset-seed\",\"cli_version\":\"0.6.1\",\"launched_by\":\"\",\"runtimes\":[{\"name\":\"reset-seed-omp\",\"type\":\"omp\",\"version\":\"18.6.1\",\"status\":\"online\",\"profile_id\":\"\"}],\"failed_profiles\":[]}" |
		jq -er '.runtimes[0].id'
}

agent_id=""
ensure_agent() {
	agent_id=$(wsapi GET /api/agents |
		jq -r --arg name "$AGENT_NAME" '[.[] | select(.name == $name and (.archived_at == null))][0].id // ""')
	if [ -n "$agent_id" ]; then
		info "agent $AGENT_NAME ($agent_id)"
		return 0
	fi
	rt=$(seed_runtime) || die "seed runtime registration failed"
	agent_id=$(wsapi POST /api/agents \
		"{\"name\":\"$AGENT_NAME\",\"description\":\"Foreman acceptance test agent\",\"instructions\":\"Reply with a short acknowledgement. Do not modify any repository.\",\"runtime_id\":\"$rt\",\"max_concurrent_tasks\":6}" |
		jq -er .id) || die "agent creation failed"
	mcli runtime delete "$rt" --cascade >/dev/null ||
		die "could not unbind and delete the seed runtime"
	info "created agent $AGENT_NAME ($agent_id), unbound"
}

ensure_chat() {
	[ -n "$agent_id" ] || die "agent $AGENT_NAME is missing"
	id=$(wsapi GET /api/chat/sessions |
		jq -r --arg title "$CHAT_TITLE" '[.[] | select(.title == $title)][0].id // ""')
	if [ -n "$id" ]; then
		info "chat session $CHAT_TITLE ($id)"
		return 0
	fi
	id=$(wsapi POST /api/chat/sessions "{\"agent_id\":\"$agent_id\",\"title\":\"$CHAT_TITLE\"}" |
		jq -er .id) || die "chat session creation failed"
	info "created chat session $CHAT_TITLE ($id)"
}

# --- 6. gate and report ------------------------------------------------------

info "reset target: $SERVER_URL (namespace $NS, profile $PROFILE)"
curl -fsS -m 30 "$SERVER_URL/api/config" >/dev/null ||
	die "test multica server is not reachable at $SERVER_URL — run ./deploy.sh first"

ensure_credentials
ensure_workspace
wipe_rows
ensure_repo
ensure_agent
ensure_chat

# The environment gate ①②③, the gate every round opens with. Run here, with
# the instance under test stopped, ② is the reset-time state this entry sets
# up; a blocked environment exits 2 through this call.
"$TEST_MULTICA_DIR/selfcheck.sh"

printf '\n'
printf 'workspace:    %s\n' "$(workspace_id)"
printf 'profile:      %s (%s)\n' "$PROFILE" "$PROFILE_CONFIG"
printf 'server:       %s\n' "$SERVER_URL"
printf 'entry:        %s\n' "$ENTRY_URL"
printf 'agent:        %s\n' "$AGENT_NAME"
printf 'chat session: %s\n' "$CHAT_TITLE"
printf 'next:         ./wire.sh         # points the instance at the access point and starts it\n'
printf '              ./bind-agent.sh   # after the instance registered its runtime (creates the test autopilot %s)\n' "$AUTOPILOT_TITLE"
if [ "$print_token" -eq 1 ]; then
	printf 'token:        %s\n' "$(pat)"
fi
